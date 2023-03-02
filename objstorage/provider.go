// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorage

import (
	"io"
	"os"
	"sort"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage/shared"
	"github.com/cockroachdb/pebble/objstorage/sharedobjcat"
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

	fsDir vfs.File

	shared sharedSubsystem

	mu struct {
		sync.RWMutex

		shared struct {
			// catalogBatch accumulates shared object creations and deletions until
			// Sync is called.
			catalogBatch sharedobjcat.Batch
		}

		// localObjectsChanged is set if non-shared objects were created or deleted
		// but Sync was not yet called.
		localObjectsChanged bool

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

	// NewReadHandle creates a read handle for ReadAt requests that are related
	// and can benefit from optimizations like read-ahead.
	//
	// The ReadHandle must be closed before the Readable is closed.
	//
	// Multiple separate ReadHandles can be used.
	NewReadHandle() ReadHandle
}

// ReadHandle is used to perform reads that are related and might benefit from
// optimizations like read-ahead.
type ReadHandle interface {
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
// Either Finish or Abort must be called.
type Writable interface {
	// Write writes len(p) bytes from p to the underlying object. The data is not
	// guaranteed to be durable until Finish is called.
	//
	// Note that Write *is* allowed to modify the slice passed in, whether
	// temporarily or permanently. Callers of Write need to take this into
	// account.
	Write(p []byte) error

	// Finish completes the object and makes the data durable.
	// No further calls are allowed after calling Finish.
	Finish() error

	// Abort gives up on finishing the object. There is no guarantee about whether
	// the object exists after calling Abort.
	// No further calls are allowed after calling Abort.
	Abort()
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

	// Fields here are set only if the provider is to support shared objects
	// (experimental).
	Shared struct {
		Storage shared.Storage
	}
}

// DefaultSettings initializes default settings (with no shared storage),
// suitable for tests and tools.
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

	// The fields below are only set if the object is on shared storage.
	Shared struct {
		// CreatorID identifies the DB instance that originally created the object.
		CreatorID CreatorID
		// CreatorFileNum is the identifier for the object within the context of the
		// DB instance that originally created the object.
		CreatorFileNum base.FileNum
	}
}

// CreatorID identifies the DB instance that originally created a shared object.
// This ID is incorporated in backing object names.
// Must be non-zero.
type CreatorID = sharedobjcat.CreatorID

// IsShared returns true if the object is on shared storage.
func (meta *ObjectMetadata) IsShared() bool {
	return meta.Shared.CreatorID.IsSet()
}

// Open creates the Provider.
func Open(settings Settings) (p *Provider, _ error) {
	fsDir, err := settings.FS.OpenDir(settings.FSDirName)
	if err != nil {
		return nil, err
	}

	defer func() {
		if p == nil {
			fsDir.Close()
		}
	}()

	p = &Provider{
		st:    settings,
		fsDir: fsDir,
	}
	p.mu.knownObjects = make(map[base.FileNum]ObjectMetadata)

	// Add local FS objects.
	if err := p.vfsInit(); err != nil {
		return nil, err
	}

	// Initialize shared subsystem (if configured) and add shared objects.
	if err := p.sharedInit(); err != nil {
		return nil, err
	}

	return p, nil
}

// Close the provider.
func (p *Provider) Close() error {
	var err error
	if p.fsDir != nil {
		err = p.fsDir.Close()
		p.fsDir = nil
	}
	return err
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
	return p.sharedOpenForReading(meta)
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

	// TODO(radu): implement "must exist" behavior.
	return p.sharedOpenForReading(meta)
}

// CreateOptions contains optional arguments for Create.
type CreateOptions struct {
	// PreferSharedStorage causes the object to be created on shared storage if
	// the provider has shared storage configured.
	PreferSharedStorage bool
}

// Create creates a new object and opens it for writing.
//
// The object is not guaranteed to be durable (accessible in case of crashes)
// until Sync is called.
func (p *Provider) Create(
	fileType base.FileType, fileNum base.FileNum, opts CreateOptions,
) (w Writable, meta ObjectMetadata, err error) {
	if opts.PreferSharedStorage && p.st.Shared.Storage != nil {
		w, meta, err = p.sharedCreate(fileType, fileNum)
	} else {
		w, meta, err = p.vfsCreate(fileType, fileNum)
	}
	if err != nil {
		err = errors.Wrapf(err, "creating object %s", errors.Safe(fileNum))
		return nil, ObjectMetadata{}, err
	}
	p.addMetadata(meta)
	return w, meta, nil
}

// Remove removes an object.
//
// The object is not guaranteed to be durably removed until Sync is called.
func (p *Provider) Remove(fileType base.FileType, fileNum base.FileNum) error {
	meta, err := p.Lookup(fileType, fileNum)
	if err != nil {
		return err
	}

	if !meta.IsShared() {
		err = p.vfsRemove(fileType, fileNum)
	}
	// TODO(radu): implement shared object removal (i.e. deref).

	if err != nil && !IsNotExistError(err) {
		// We want to be able to retry a Remove, so we keep the object in our list.
		// TODO(radu): we should mark the object as "zombie" and not allow any other
		// operations.
		return errors.Wrapf(err, "removing object %s", errors.Safe(fileNum))
	}
	p.removeMetadata(fileNum)
	return err
}

// IsNotExistError indicates whether the error is known to report that a file or
// directory does not exist.
func IsNotExistError(err error) bool {
	return oserror.IsNotExist(err)
}

// Sync flushes the metadata from creation or removal of objects since the last Sync.
func (p *Provider) Sync() error {
	if err := p.vfsSync(); err != nil {
		return err
	}
	if err := p.sharedSync(); err != nil {
		return err
	}
	return nil
}

// LinkOrCopyFromLocal creates a new object that is either a copy of a given
// local file or a hard link (if the new object is created on the same FS, and
// if the FS supports it).
//
// The object is not guaranteed to be durable (accessible in case of crashes)
// until Sync is called.
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

// Lookup returns the metadata of an object that is already known to the Provider.
// Does not perform any I/O.
func (p *Provider) Lookup(fileType base.FileType, fileNum base.FileNum) (ObjectMetadata, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	meta, ok := p.mu.knownObjects[fileNum]
	if !ok {
		return ObjectMetadata{}, errors.Wrapf(
			os.ErrNotExist,
			"file %s (type %d) unknown to the objstorage provider",
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

// Path returns an internal, implementation-dependent path for the object. It is
// meant to be used for informational purposes (like logging).
func (p *Provider) Path(meta ObjectMetadata) string {
	if !meta.IsShared() {
		return p.vfsPath(meta.FileType, meta.FileNum)
	}
	return p.sharedPath(meta)
}

// Size returns the size of the object.
func (p *Provider) Size(meta ObjectMetadata) (int64, error) {
	if !meta.IsShared() {
		return p.vfsSize(meta.FileType, meta.FileNum)
	}
	return p.sharedSize(meta)
}

// List returns the objects currently known to the provider. Does not perform any I/O.
func (p *Provider) List() []ObjectMetadata {
	p.mu.RLock()
	defer p.mu.RUnlock()
	res := make([]ObjectMetadata, 0, len(p.mu.knownObjects))
	for _, meta := range p.mu.knownObjects {
		res = append(res, meta)
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].FileNum < res[j].FileNum
	})
	return res
}

func (p *Provider) addMetadata(meta ObjectMetadata) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.knownObjects[meta.FileNum] = meta
	if meta.IsShared() {
		p.mu.shared.catalogBatch.AddObject(sharedobjcat.SharedObjectMetadata{
			FileNum:        meta.FileNum,
			FileType:       meta.FileType,
			CreatorID:      meta.Shared.CreatorID,
			CreatorFileNum: meta.Shared.CreatorFileNum,
		})
	} else {
		p.mu.localObjectsChanged = true
	}
}

func (p *Provider) removeMetadata(fileNum base.FileNum) {
	p.mu.Lock()
	defer p.mu.Unlock()

	meta, ok := p.mu.knownObjects[fileNum]
	if !ok {
		return
	}
	delete(p.mu.knownObjects, fileNum)
	if meta.IsShared() {
		p.mu.shared.catalogBatch.DeleteObject(fileNum)
	} else {
		p.mu.localObjectsChanged = true
	}
}
