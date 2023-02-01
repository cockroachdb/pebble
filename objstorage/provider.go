// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorage

import (
	"io"

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
// Objects are currently backed by a vfs.File.
type Provider struct {
	st Settings

	// TODO(radu): when we support shared storage as well, this object will need to
	// maintain a FileNum to backend type mapping.

	// TODO(radu): add more functionality around listing, copying, linking, etc.
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

// New creates the Provider.
func New(settings Settings) *Provider {
	return &Provider{
		st: settings,
	}
}

// Path returns an internal path for an object. It is used for informative
// purposes (e.g. logging).
func (p *Provider) Path(fileType base.FileType, fileNum base.FileNum) string {
	return base.MakeFilepath(p.st.FS, p.st.FSDirName, fileType, fileNum)
}

// OpenForReading opens an existing object.
func (p *Provider) OpenForReading(fileType base.FileType, fileNum base.FileNum) (Readable, error) {
	filename := p.Path(fileType, fileNum)
	file, err := p.st.FS.Open(filename, vfs.RandomReadsOption)
	if err != nil {
		return nil, err
	}
	// TODO(radu): we use the existence of the file descriptor as an indication
	// that the File might support Prefetch and SequentialReadsOption. We should
	// replace this with a cleaner way to obtain the capabilities of the FS / File.
	if fd := file.Fd(); fd != vfs.InvalidFd {
		return newFileReadable(file, p.st.FS, filename)
	}
	return newGenericFileReadable(file)
}

// OpenForReadingMustExist is a variant of OpenForReading which causes a fatal
// error if the file does not exist. The fatal error message contains
// information helpful for debugging.
func (p *Provider) OpenForReadingMustExist(
	fileType base.FileType, fileNum base.FileNum,
) (Readable, error) {
	r, err := p.OpenForReading(fileType, fileNum)
	if err != nil {
		filename := p.Path(fileType, fileNum)
		base.MustExist(p.st.FS, filename, p.st.Logger, err)
	}
	return r, err
}

// Create creates a new object and opens it for writing.
func (p *Provider) Create(fileType base.FileType, fileNum base.FileNum) (Writable, error) {
	file, err := p.st.FS.Create(p.Path(fileType, fileNum))
	if err != nil {
		return nil, err
	}
	file = vfs.NewSyncingFile(file, vfs.SyncingFileOptions{
		NoSyncOnClose: p.st.NoSyncOnClose,
		BytesPerSync:  p.st.BytesPerSync,
	})
	return newFileBufferedWritable(file), nil
}

// Remove removes an object.
func (p *Provider) Remove(fileType base.FileType, fileNum base.FileNum) error {
	return p.st.FSCleaner.Clean(p.st.FS, fileType, p.Path(fileType, fileNum))
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
) error {
	if srcFS == p.st.FS {
		// Wrap the normal filesystem with one which wraps newly created files with
		// vfs.NewSyncingFile.
		fs := vfs.NewSyncingFS(p.st.FS, vfs.SyncingFileOptions{
			NoSyncOnClose: p.st.NoSyncOnClose,
			BytesPerSync:  p.st.BytesPerSync,
		})
		return vfs.LinkOrCopy(fs, srcFilePath, p.Path(dstFileType, dstFileNum))
	}
	// TODO(radu): for the copy case, we should use `p.Create` and do the copy ourselves.
	panic("unimplemented")
}
