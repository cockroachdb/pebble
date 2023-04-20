// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorageprovider

import (
	"context"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider/objiotracing"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider/sharedobjcat"
	"github.com/cockroachdb/pebble/objstorage/shared"
	"github.com/cockroachdb/pebble/vfs"
)

// provider is the implementation of objstorage.Provider.
type provider struct {
	st Settings

	fsDir vfs.File

	tracer *objiotracing.Tracer

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
		knownObjects map[base.DiskFileNum]objstorage.ObjectMetadata

		// protectedObjects are objects that cannot be unreferenced because they
		// have outstanding SharedObjectBackingHandles. The value is a count of outstanding handles
		protectedObjects map[base.DiskFileNum]int
	}
}

var _ objstorage.Provider = (*provider)(nil)

// Settings that must be specified when creating the provider.
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

// Open creates the provider.
func Open(settings Settings) (objstorage.Provider, error) {
	return open(settings)
}

func open(settings Settings) (p *provider, _ error) {
	fsDir, err := settings.FS.OpenDir(settings.FSDirName)
	if err != nil {
		return nil, err
	}

	defer func() {
		if p == nil {
			fsDir.Close()
		}
	}()

	p = &provider{
		st:    settings,
		fsDir: fsDir,
	}
	p.mu.knownObjects = make(map[base.DiskFileNum]objstorage.ObjectMetadata)
	p.mu.protectedObjects = make(map[base.DiskFileNum]int)

	if objiotracing.Enabled {
		p.tracer = objiotracing.Open(settings.FS, settings.FSDirName)
	}

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

// Close is part of the objstorage.Provider interface.
func (p *provider) Close() error {
	var err error
	if p.fsDir != nil {
		err = p.fsDir.Close()
		p.fsDir = nil
	}
	if objiotracing.Enabled {
		if p.tracer != nil {
			p.tracer.Close()
			p.tracer = nil
		}
	}
	return err
}

// OpenForReading opens an existing object.
func (p *provider) OpenForReading(
	ctx context.Context,
	fileType base.FileType,
	fileNum base.DiskFileNum,
	opts objstorage.OpenOptions,
) (objstorage.Readable, error) {
	meta, err := p.Lookup(fileType, fileNum)
	if err != nil {
		if opts.MustExist {
			p.st.Logger.Fatalf("%v", err)
		}
		return nil, err
	}

	var r objstorage.Readable
	if !meta.IsShared() {
		r, err = p.vfsOpenForReading(ctx, fileType, fileNum, opts)
	} else {
		r, err = p.sharedOpenForReading(ctx, meta, opts)
	}
	if err != nil {
		return nil, err
	}
	if objiotracing.Enabled {
		r = p.tracer.WrapReadable(ctx, r, fileNum)
	}
	return r, nil
}

// Create creates a new object and opens it for writing.
//
// The object is not guaranteed to be durable (accessible in case of crashes)
// until Sync is called.
func (p *provider) Create(
	ctx context.Context,
	fileType base.FileType,
	fileNum base.DiskFileNum,
	opts objstorage.CreateOptions,
) (w objstorage.Writable, meta objstorage.ObjectMetadata, err error) {
	if opts.PreferSharedStorage && p.st.Shared.Storage != nil {
		w, meta, err = p.sharedCreate(ctx, fileType, fileNum, opts)
	} else {
		w, meta, err = p.vfsCreate(ctx, fileType, fileNum)
	}
	if err != nil {
		err = errors.Wrapf(err, "creating object %s", errors.Safe(fileNum))
		return nil, objstorage.ObjectMetadata{}, err
	}
	p.addMetadata(meta)
	if objiotracing.Enabled {
		w = p.tracer.WrapWritable(ctx, w, fileNum)
	}
	return w, meta, nil
}

// Remove removes an object.
//
// Note that if the object is shared, the object is only (conceptually) removed
// from this provider. If other providers have references on the shared object,
// it will not be removed.
//
// The object is not guaranteed to be durably removed until Sync is called.
func (p *provider) Remove(fileType base.FileType, fileNum base.DiskFileNum) error {
	meta, err := p.Lookup(fileType, fileNum)
	if err != nil {
		return err
	}

	if !meta.IsShared() {
		err = p.vfsRemove(fileType, fileNum)
	} else {
		// TODO(radu): implement shared object removal (i.e. deref).
		err = p.sharedUnref(meta)
	}
	if err != nil && !p.IsNotExistError(err) {
		// We want to be able to retry a Remove, so we keep the object in our list.
		// TODO(radu): we should mark the object as "zombie" and not allow any other
		// operations.
		return errors.Wrapf(err, "removing object %s", errors.Safe(fileNum))
	}

	p.removeMetadata(fileNum)
	return err
}

// IsNotExistError is part of the objstorage.Provider interface.
func (p *provider) IsNotExistError(err error) bool {
	if oserror.IsNotExist(err) {
		return true
	}
	if p.sharedStorage() != nil && p.sharedStorage().IsNotExistError(err) {
		return true
	}
	return false
}

// Sync flushes the metadata from creation or removal of objects since the last Sync.
func (p *provider) Sync() error {
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
func (p *provider) LinkOrCopyFromLocal(
	ctx context.Context,
	srcFS vfs.FS,
	srcFilePath string,
	dstFileType base.FileType,
	dstFileNum base.DiskFileNum,
	opts objstorage.CreateOptions,
) (objstorage.ObjectMetadata, error) {
	shared := opts.PreferSharedStorage && p.st.Shared.Storage != nil
	if !shared && srcFS == p.st.FS {
		// Wrap the normal filesystem with one which wraps newly created files with
		// vfs.NewSyncingFile.
		fs := vfs.NewSyncingFS(p.st.FS, vfs.SyncingFileOptions{
			NoSyncOnClose: p.st.NoSyncOnClose,
			BytesPerSync:  p.st.BytesPerSync,
		})
		dstPath := p.vfsPath(dstFileType, dstFileNum)
		if err := vfs.LinkOrCopy(fs, srcFilePath, dstPath); err != nil {
			return objstorage.ObjectMetadata{}, err
		}

		meta := objstorage.ObjectMetadata{
			DiskFileNum: dstFileNum,
			FileType:    dstFileType,
		}
		p.addMetadata(meta)
		return meta, nil
	}
	// Create the object and copy the data.
	w, meta, err := p.Create(ctx, dstFileType, dstFileNum, opts)
	if err != nil {
		return objstorage.ObjectMetadata{}, err
	}
	f, err := srcFS.Open(srcFilePath, vfs.SequentialReadsOption)
	if err != nil {
		return objstorage.ObjectMetadata{}, err
	}
	defer f.Close()
	buf := make([]byte, 64*1024)
	for {
		n, readErr := f.Read(buf)
		if readErr != nil && readErr != io.EOF {
			w.Abort()
			return objstorage.ObjectMetadata{}, readErr
		}

		if n > 0 {
			if err := w.Write(buf[:n]); err != nil {
				w.Abort()
				return objstorage.ObjectMetadata{}, err
			}
		}

		if readErr == io.EOF {
			break
		}
	}
	if err := w.Finish(); err != nil {
		return objstorage.ObjectMetadata{}, err
	}
	return meta, nil
}

// Lookup is part of the objstorage.Provider interface.
func (p *provider) Lookup(
	fileType base.FileType, fileNum base.DiskFileNum,
) (objstorage.ObjectMetadata, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	meta, ok := p.mu.knownObjects[fileNum]
	if !ok {
		return objstorage.ObjectMetadata{}, errors.Wrapf(
			os.ErrNotExist,
			"file %s (type %d) unknown to the objstorage provider",
			errors.Safe(fileNum), errors.Safe(fileType),
		)
	}
	if meta.FileType != fileType {
		return objstorage.ObjectMetadata{}, errors.AssertionFailedf(
			"file %s type mismatch (known type %d, expected type %d)",
			errors.Safe(fileNum), errors.Safe(meta.FileType), errors.Safe(fileType),
		)
	}
	return meta, nil
}

// Path is part of the objstorage.Provider interface.
func (p *provider) Path(meta objstorage.ObjectMetadata) string {
	if !meta.IsShared() {
		return p.vfsPath(meta.FileType, meta.DiskFileNum)
	}
	return p.sharedPath(meta)
}

// Size returns the size of the object.
func (p *provider) Size(meta objstorage.ObjectMetadata) (int64, error) {
	if !meta.IsShared() {
		return p.vfsSize(meta.FileType, meta.DiskFileNum)
	}
	return p.sharedSize(meta)
}

// List is part of the objstorage.Provider interface.
func (p *provider) List() []objstorage.ObjectMetadata {
	p.mu.RLock()
	defer p.mu.RUnlock()
	res := make([]objstorage.ObjectMetadata, 0, len(p.mu.knownObjects))
	for _, meta := range p.mu.knownObjects {
		res = append(res, meta)
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].DiskFileNum.FileNum() < res[j].DiskFileNum.FileNum()
	})
	return res
}

func (p *provider) addMetadata(meta objstorage.ObjectMetadata) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.knownObjects[meta.DiskFileNum] = meta
	if meta.IsShared() {
		p.mu.shared.catalogBatch.AddObject(sharedobjcat.SharedObjectMetadata{
			FileNum:        meta.DiskFileNum,
			FileType:       meta.FileType,
			CreatorID:      meta.Shared.CreatorID,
			CreatorFileNum: meta.Shared.CreatorFileNum,
			CleanupMethod:  meta.Shared.CleanupMethod,
		})
	} else {
		p.mu.localObjectsChanged = true
	}
}

func (p *provider) removeMetadata(fileNum base.DiskFileNum) {
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

// protectObject prevents the unreferencing of a shared object until
// unprotectObject is called.
func (p *provider) protectObject(fileNum base.DiskFileNum) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.protectedObjects[fileNum] = p.mu.protectedObjects[fileNum] + 1
}

func (p *provider) unprotectObject(fileNum base.DiskFileNum) {
	p.mu.Lock()
	defer p.mu.Unlock()
	v := p.mu.protectedObjects[fileNum]
	if invariants.Enabled && v == 0 {
		panic("invalid protection count")
	}
	if v > 1 {
		p.mu.protectedObjects[fileNum] = v - 1
	} else {
		delete(p.mu.protectedObjects, fileNum)
		// TODO(radu): check if the object is still in knownObject; if not, unref it
		// now.
	}
}

func (p *provider) isProtected(fileNum base.DiskFileNum) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.mu.protectedObjects[fileNum] > 0
}
