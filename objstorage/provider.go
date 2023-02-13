// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorage

import (
	"io"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
)

// Provider is a singleton object used to access and manage objects.
//
// An object is conceptually like a large immutable file. The main use of
// objects is for storing sstables; in the future it could also be used for blob
// storage.
//
// The Provider can only manage objects that it knows about - either objects
// created by the provider, or existing objects the Provider was informed about
// via AddObjects.
//
// Objects are currently backed by a vfs.File.
type Provider struct {
	st Settings

	mu struct {
		sync.RWMutex

		// knownObjects maintains information about objects that are known to the provider.
		// It is initialized with the list of files in the manifest when we open a DB.
		knownObjects map[base.FileNum]ObjectMetadata
	}
}

// Readable is the handle for an object that is open for reading.
type Readable interface {
	io.ReaderAt
	io.Closer

	// Size returns the size of the object.
	Size() int64

	// NewReadaheadHandle creates a read-ahead handle which encapsulates
	// read-ahead state. To benefit from read-ahead, ReadaheadHandle.ReadAt must
	// be used (as opposed to Readable.ReadAt).
	//
	// The ReadaheadHandle must be closed before the Readable is closed.
	//
	// Multiple separate ReadaheadHandles can be used.
	NewReadaheadHandle() ReadaheadHandle
}

// ReadaheadHandle is used to perform reads that might benefit from read-ahead.
type ReadaheadHandle interface {
	io.ReaderAt
	io.Closer

	// MaxReadahead configures the implementation to expect large sequential
	// reads. Used to skip any initial read-ahead ramp-up.
	MaxReadahead()

	// RecordCacheHit informs the implementation that we were able to retrieve a
	// block from cache.
	RecordCacheHit(offset, size int64)
}

// Writable is the handle for an object that is open for writing.
type Writable interface {
	// Unlike the specification for io.Writer.Write(), the Writable.Write()
	// method *is* allowed to modify the slice passed in, whether temporarily
	// or permanently. Callers of Write() need to take this into account.
	io.Writer
	io.Closer

	Sync() error
}

// Settings that must be specified when creating the Provider.
type Settings struct {
	Logger base.Logger

	// Local filesystem configuration.
	FS        vfs.FS
	FSDirName string

	// FSDirInitialListing is a listing of FSDirName at the time of calling Open.
	//
	// This is an optional optimization to avoid double listing on Open when the
	// higher layer already has a listing. When nil, we obtain the listing on
	// Open.
	FSDirInitialListing []string

	// Cleaner cleans obsolete files from the local filesystem.
	//
	// The default cleaner uses the DeleteCleaner.
	FSCleaner base.Cleaner

	// NoSyncOnClose decides whether the implementation will enforce a
	// close-time synchronization (e.g., fdatasync() or sync_file_range())
	// on files it writes to. Setting this to true removes the guarantee for a
	// sync on close. Some implementations can still issue a non-blocking sync.
	NoSyncOnClose bool

	// BytesPerSync enables periodic syncing of files in order to smooth out
	// writes to disk. This option does not provide any persistence guarantee, but
	// is used to avoid latency spikes if the OS automatically decides to write
	// out a large chunk of dirty filesystem buffers.
	BytesPerSync int
}

// DefaultSettings initializes default settings, suitable for tests and tools.
func DefaultSettings(fs vfs.FS, dirName string) Settings {
	return Settings{
		Logger:        base.DefaultLogger,
		FS:            fs,
		FSDirName:     dirName,
		FSCleaner:     base.DeleteCleaner{},
		NoSyncOnClose: false,
		BytesPerSync:  512 * 1024, // 512KB
	}
}

// ObjectMetadata contains the metadata required to be able to access an object.
type ObjectMetadata struct {
	FileNum  base.FileNum
	FileType base.FileType
	// TODO(radu): this will also contain shared object metadata.
}

// IsShared returns true if the object is on shared storage.
func (m *ObjectMetadata) IsShared() bool {
	// TODO(radu)
	return false
}

// Open creates the Provider.
func Open(settings Settings) (*Provider, error) {
	p := &Provider{
		st: settings,
	}
	p.mu.knownObjects = make(map[base.FileNum]ObjectMetadata)

	// Add local FS objects.
	listing := settings.FSDirInitialListing
	if listing == nil {
		var err error
		listing, err = p.st.FS.List(p.st.FSDirName)
		if err != nil {
			return nil, errors.Wrapf(err, "pebble: could not list store directory")
		}
	}
	objects := p.vfsFindExisting(listing)
	for _, o := range objects {
		p.mu.knownObjects[o.FileNum] = o
	}

	return p, nil
}

// OpenForReading opens an existing object.
func (p *Provider) OpenForReading(fileType base.FileType, fileNum base.FileNum) (Readable, error) {
	meta, err := p.Lookup(fileType, fileNum)
	if err != nil {
		return nil, err
	}

	if !meta.IsShared() {
		return p.vfsOpenForReading(fileType, fileNum, false /* mustExist */)
	}

	panic("unimplemented")
}

// OpenForReadingMustExist is a variant of OpenForReading which causes a fatal
// error if the file does not exist. The fatal error message contains
// information helpful for debugging.
func (p *Provider) OpenForReadingMustExist(
	fileType base.FileType, fileNum base.FileNum,
) (Readable, error) {
	meta, err := p.Lookup(fileType, fileNum)
	if err != nil {
		p.st.Logger.Fatalf("%v", err)
		return nil, err
	}

	if !meta.IsShared() {
		return p.vfsOpenForReading(fileType, fileNum, true /* mustExist */)
	}

	panic("unimplemented")
}

// Create creates a new object and opens it for writing.
func (p *Provider) Create(
	fileType base.FileType, fileNum base.FileNum,
) (Writable, ObjectMetadata, error) {
	w, err := p.vfsCreate(fileType, fileNum)
	if err != nil {
		return nil, ObjectMetadata{}, err
	}
	meta := ObjectMetadata{
		FileNum:  fileNum,
		FileType: fileType,
	}

	p.addMetadata(meta)
	return w, meta, nil
}

// Remove removes an object.
func (p *Provider) Remove(fileType base.FileType, fileNum base.FileNum) error {
	meta, err := p.Lookup(fileType, fileNum)
	if err != nil {
		return err
	}

	if !meta.IsShared() {
		err = p.vfsRemove(fileType, fileNum)
	} else {
		panic("unimplemented")
	}
	if err != nil {
		// We want to be able to retry a Remove, so we keep the object in our list.
		// TODO(radu): we should mark the object as "zombie" and not allow any other
		// operations.
		return err
	}
	p.removeMetadata(fileNum)
	return nil
}

// IsNotExistError indicates whether the error is known to report that a file or
// directory does not exist.
func IsNotExistError(err error) bool {
	return oserror.IsNotExist(err)
}

// LinkOrCopyFromLocal creates a new object that is either a copy of a given
// local file or a hard link (if the new object is created on the same FS, and
// if the FS supports it).
func (p *Provider) LinkOrCopyFromLocal(
	srcFS vfs.FS, srcFilePath string, dstFileType base.FileType, dstFileNum base.FileNum,
) (ObjectMetadata, error) {
	if srcFS == p.st.FS {
		// Wrap the normal filesystem with one which wraps newly created files with
		// vfs.NewSyncingFile.
		fs := vfs.NewSyncingFS(p.st.FS, vfs.SyncingFileOptions{
			NoSyncOnClose: p.st.NoSyncOnClose,
			BytesPerSync:  p.st.BytesPerSync,
		})
		dstPath := p.vfsPath(dstFileType, dstFileNum)
		if err := vfs.LinkOrCopy(fs, srcFilePath, dstPath); err != nil {
			return ObjectMetadata{}, err
		}

		meta := ObjectMetadata{
			FileNum:  dstFileNum,
			FileType: dstFileType,
		}
		p.addMetadata(meta)
		return meta, nil
	}
	// TODO(radu): for the copy case, we should use `p.Create` and do the copy ourselves.
	panic("unimplemented")
}

// Path returns an internal, implementation-dependent path for the object. It is
// meant to be used for informational purposes (like logging).
func (p *Provider) Path(meta ObjectMetadata) string {
	if !meta.IsShared() {
		return p.vfsPath(meta.FileType, meta.FileNum)
	}
	panic("unimplemented")
}

// Lookup returns the metadata of an object that is already known to the Provider.
// Does not perform any I/O.
func (p *Provider) Lookup(fileType base.FileType, fileNum base.FileNum) (ObjectMetadata, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	meta, ok := p.mu.knownObjects[fileNum]
	if !ok {
		return ObjectMetadata{}, errors.Newf(
			"file %s (type %d) unknown to the provider",
			errors.Safe(fileNum), errors.Safe(fileType),
		)
	}
	if meta.FileType != fileType {
		return ObjectMetadata{}, errors.AssertionFailedf(
			"file %s type mismatch (known type %d, expected type %d)",
			errors.Safe(fileNum), errors.Safe(meta.FileType), errors.Safe(fileType),
		)
	}
	return meta, nil
}

func (p *Provider) addMetadata(meta ObjectMetadata) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.knownObjects[meta.FileNum] = meta
}

func (p *Provider) removeMetadata(fileNum base.FileNum) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.mu.knownObjects, fileNum)
}
