// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorage

import (
	"context"
	"fmt"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
)

// Readable is the handle for an object that is open for reading.
type Readable interface {
	// ReadAt reads len(p) bytes into p starting at offset off.
	//
	// Clients of ReadAt can execute parallel ReadAt calls on the
	// same Readable.
	ReadAt(ctx context.Context, p []byte, off int64) error

	Close() error

	// Size returns the size of the object.
	Size() int64

	// NewReadHandle creates a read handle for ReadAt requests that are related
	// and can benefit from optimizations like read-ahead.
	//
	// The ReadHandle must be closed before the Readable is closed.
	//
	// Multiple separate ReadHandles can be used.
	NewReadHandle(ctx context.Context) ReadHandle
}

// ReadHandle is used to perform reads that are related and might benefit from
// optimizations like read-ahead.
type ReadHandle interface {
	// ReadAt reads len(p) bytes into p starting at offset off.
	//
	// Parallel ReadAt calls on the same ReadHandle are not allowed.
	ReadAt(ctx context.Context, p []byte, off int64) error

	Close() error

	// MaxReadahead configures the implementation to expect large sequential
	// reads. Used to skip any initial read-ahead ramp-up.
	MaxReadahead()

	// RecordCacheHit informs the implementation that we were able to retrieve a
	// block from cache. This is useful for example when the implementation is
	// trying to detect a sequential reading pattern.
	RecordCacheHit(ctx context.Context, offset, size int64)
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

// ObjectMetadata contains the metadata required to be able to access an object.
type ObjectMetadata struct {
	DiskFileNum base.DiskFileNum
	FileType    base.FileType

	// The fields below are only set if the object is on shared storage.
	Shared struct {
		// CreatorID identifies the DB instance that originally created the object.
		CreatorID CreatorID
		// CreatorFileNum is the identifier for the object within the context of the
		// DB instance that originally created the object.
		CreatorFileNum base.DiskFileNum

		CleanupMethod SharedCleanupMethod
	}
}

// IsShared returns true if the object is on shared storage.
func (meta *ObjectMetadata) IsShared() bool {
	return meta.Shared.CreatorID.IsSet()
}

// CreatorID identifies the DB instance that originally created a shared object.
// This ID is incorporated in backing object names.
// Must be non-zero.
type CreatorID uint64

// IsSet returns true if the CreatorID is not zero.
func (c CreatorID) IsSet() bool { return c != 0 }

func (c CreatorID) String() string { return fmt.Sprintf("%020d", c) }

// SharedCleanupMethod indicates the method for cleaning up unused shared objects.
type SharedCleanupMethod uint8

const (
	// SharedRefTracking is used for shared objects for which objstorage providers
	// keep track of references via reference marker objects.
	SharedRefTracking SharedCleanupMethod = iota

	// SharedNoCleanup is used for shared objects that are managed externally; the
	// objstorage provider never deletes such objects.
	SharedNoCleanup
)

// OpenOptions contains optional arguments for OpenForReading.
type OpenOptions struct {
	// MustExist triggers a fatal error if the file does not exist. The fatal
	// error message contains extra information helpful for debugging.
	MustExist bool
}

// CreateOptions contains optional arguments for Create.
type CreateOptions struct {
	// PreferSharedStorage causes the object to be created on shared storage if
	// the provider has shared storage configured.
	PreferSharedStorage bool

	// SharedCleanupMethod is used for the object when it is created on shared storage.
	// The default (zero) value is SharedRefTracking.
	SharedCleanupMethod SharedCleanupMethod
}

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
// Objects are currently backed by a vfs.File or a shared.Storage object.
type Provider interface {
	// OpenForReading opens an existing object.
	OpenForReading(
		ctx context.Context, fileType base.FileType, FileNum base.DiskFileNum, opts OpenOptions,
	) (Readable, error)

	// Create creates a new object and opens it for writing.
	//
	// The object is not guaranteed to be durable (accessible in case of crashes)
	// until Sync is called.
	Create(
		ctx context.Context, fileType base.FileType, FileNum base.DiskFileNum, opts CreateOptions,
	) (w Writable, meta ObjectMetadata, err error)

	// Remove removes an object.
	//
	// The object is not guaranteed to be durably removed until Sync is called.
	Remove(fileType base.FileType, FileNum base.DiskFileNum) error

	// Sync flushes the metadata from creation or removal of objects since the last Sync.
	// This includes objects that have been Created but for which
	// Writable.Finish() has not yet been called.
	Sync() error

	// LinkOrCopyFromLocal creates a new object that is either a copy of a given
	// local file or a hard link (if the new object is created on the same FS, and
	// if the FS supports it).
	//
	// The object is not guaranteed to be durable (accessible in case of crashes)
	// until Sync is called.
	LinkOrCopyFromLocal(
		ctx context.Context,
		srcFS vfs.FS,
		srcFilePath string,
		dstFileType base.FileType,
		dstFileNum base.DiskFileNum,
		opts CreateOptions,
	) (ObjectMetadata, error)

	// Lookup returns the metadata of an object that is already known to the Provider.
	// Does not perform any I/O.
	Lookup(fileType base.FileType, FileNum base.DiskFileNum) (ObjectMetadata, error)

	// Path returns an internal, implementation-dependent path for the object. It is
	// meant to be used for informational purposes (like logging).
	Path(meta ObjectMetadata) string

	// Size returns the size of the object.
	Size(meta ObjectMetadata) (int64, error)

	// List returns the objects currently known to the provider. Does not perform any I/O.
	List() []ObjectMetadata

	// SetCreatorID sets the CreatorID which is needed in order to use shared
	// objects. Shared object usage is disabled until this method is called the
	// first time. Once set, the Creator ID is persisted and cannot change.
	//
	// Cannot be called if shared storage is not configured for the provider.
	SetCreatorID(creatorID CreatorID) error

	// SharedObjectBacking encodes the shared object metadata.
	SharedObjectBacking(meta *ObjectMetadata) (SharedObjectBackingHandle, error)

	// AttachSharedObjects registers existing shared objects with this provider.
	AttachSharedObjects(objs []SharedObjectToAttach) ([]ObjectMetadata, error)

	Close() error

	// IsNotExistError indicates whether the error is known to report that a file or
	// directory does not exist.
	IsNotExistError(err error) bool
}

// SharedObjectBacking encodes the metadata necessary to incorporate a shared
// object into a different Pebble instance. The encoding is specific to a given
// Provider implementation.
type SharedObjectBacking []byte

// SharedObjectBackingHandle is a container for a SharedObjectBacking which
// ensures that the backing stays valid. A backing can otherwise become invalid
// if this provider unrefs the shared object. The SharedObjectBackingHandle
// delays any unref until Close.
type SharedObjectBackingHandle interface {
	// Get returns the backing. The backing is only guaranteed to be valid until
	// Close is called (or until the Provider is closed). If Close was already
	// called, returns an error.
	Get() (SharedObjectBacking, error)
	Close()
}

// SharedObjectToAttach contains the arguments needed to attach an existing shared object.
type SharedObjectToAttach struct {
	// FileNum is the file number that will be used to refer to this object (in
	// the context of this instance).
	FileNum  base.DiskFileNum
	FileType base.FileType
	// Backing contains the metadata for the shared object backing (normally
	// generated from a different instance, but using the same Provider
	// implementation).
	Backing SharedObjectBacking
}
