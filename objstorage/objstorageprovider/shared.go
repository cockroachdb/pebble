// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorageprovider

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider/remoteobjcat"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider/sharedcache"
	"github.com/cockroachdb/pebble/objstorage/remote"
)

// sharedSubsystem contains the provider fields related to shared storage.
// All fields remain unset if shared storage is not configured.
type sharedSubsystem struct {
	catalog *remoteobjcat.Catalog
	cache   *sharedcache.Cache

	// checkRefsOnOpen controls whether we check the ref marker file when opening
	// an object. Normally this is true when invariants are enabled (but the provider
	// test tweaks this field).
	checkRefsOnOpen bool

	// initialized guards access to the creatorID field.
	initialized atomic.Bool
	creatorID   objstorage.CreatorID
	initOnce    sync.Once
}

func (ss *sharedSubsystem) init(creatorID objstorage.CreatorID) {
	ss.initOnce.Do(func() {
		ss.creatorID = creatorID
		ss.initialized.Store(true)
	})
}

// sharedInit initializes the shared object subsystem (if configured) and finds
// any shared objects.
func (p *provider) sharedInit() error {
	if p.st.Shared.StorageFactory == nil {
		return nil
	}
	catalog, contents, err := remoteobjcat.Open(p.st.FS, p.st.FSDirName)
	if err != nil {
		return errors.Wrapf(err, "pebble: could not open shared object catalog")
	}
	p.shared.catalog = catalog
	p.shared.checkRefsOnOpen = invariants.Enabled

	// The creator ID may or may not be initialized yet.
	if contents.CreatorID.IsSet() {
		p.shared.init(contents.CreatorID)
		p.st.Logger.Infof("shared storage configured; creatorID = %s", contents.CreatorID)
	} else {
		p.st.Logger.Infof("shared storage configured; no creatorID yet")
	}

	if p.st.Shared.CacheSizeBytes > 0 {
		const defaultBlockSize = 32 * 1024
		blockSize := p.st.Shared.CacheBlockSize
		if blockSize == 0 {
			blockSize = defaultBlockSize
		}

		const defaultShardingBlockSize = 1024 * 1024
		shardingBlockSize := p.st.Shared.ShardingBlockSize
		if shardingBlockSize == 0 {
			shardingBlockSize = defaultShardingBlockSize
		}

		numShards := p.st.Shared.CacheShardCount
		if numShards == 0 {
			numShards = 2 * runtime.GOMAXPROCS(0)
		}

		p.shared.cache, err = sharedcache.Open(
			p.st.FS, p.st.Logger, p.st.FSDirName, blockSize, shardingBlockSize, p.st.Shared.CacheSizeBytes, numShards)
		if err != nil {
			return errors.Wrapf(err, "pebble: could not open shared object cache")
		}
	}

	for _, meta := range contents.Objects {
		o := objstorage.ObjectMetadata{
			DiskFileNum: meta.FileNum,
			FileType:    meta.FileType,
		}
		o.Remote.CreatorID = meta.CreatorID
		o.Remote.CreatorFileNum = meta.CreatorFileNum
		o.Remote.CleanupMethod = meta.CleanupMethod
		o.Remote.Locator = meta.Locator
		o.Remote.CustomObjectName = meta.CustomObjectName
		o.Remote.Storage, err = p.ensureStorageLocked(o.Remote.Locator)
		if err != nil {
			return errors.Wrapf(err, "creating remote.Storage object for locator '%s'", o.Remote.Locator)
		}
		if invariants.Enabled {
			o.AssertValid()
		}
		p.mu.knownObjects[o.DiskFileNum] = o
	}
	return nil
}

func (p *provider) sharedClose() error {
	if p.st.Shared.StorageFactory == nil {
		return nil
	}
	var err error
	if p.shared.cache != nil {
		err = p.shared.cache.Close()
		p.shared.cache = nil
	}
	if p.shared.catalog != nil {
		err = firstError(err, p.shared.catalog.Close())
		p.shared.catalog = nil
	}
	return err
}

// SetCreatorID is part of the objstorage.Provider interface.
func (p *provider) SetCreatorID(creatorID objstorage.CreatorID) error {
	if p.st.Shared.StorageFactory == nil {
		return errors.AssertionFailedf("attempt to set CreatorID but shared storage not enabled")
	}
	// Note: this call is a cheap no-op if the creator ID was already set. This
	// call also checks if we are trying to change the ID.
	if err := p.shared.catalog.SetCreatorID(creatorID); err != nil {
		return err
	}
	if !p.shared.initialized.Load() {
		p.st.Logger.Infof("shared storage creatorID set to %s", creatorID)
		p.shared.init(creatorID)
	}
	return nil
}

// IsForeign is part of the objstorage.Provider interface.
func (p *provider) IsForeign(meta objstorage.ObjectMetadata) bool {
	if !p.shared.initialized.Load() {
		return false
	}
	return meta.IsRemote() && (meta.Remote.CustomObjectName != "" || meta.Remote.CreatorID != p.shared.creatorID)
}

func (p *provider) sharedCheckInitialized() error {
	if p.st.Shared.StorageFactory == nil {
		return errors.Errorf("shared object support not configured")
	}
	if !p.shared.initialized.Load() {
		return errors.Errorf("shared object support not available: shared creator ID not yet set")
	}
	return nil
}

func (p *provider) sharedSync() error {
	batch := func() remoteobjcat.Batch {
		p.mu.Lock()
		defer p.mu.Unlock()
		res := p.mu.shared.catalogBatch.Copy()
		p.mu.shared.catalogBatch.Reset()
		return res
	}()

	if batch.IsEmpty() {
		return nil
	}

	err := p.shared.catalog.ApplyBatch(batch)
	if err != nil {
		// We have to put back the batch (for the next Sync).
		p.mu.Lock()
		defer p.mu.Unlock()
		batch.Append(p.mu.shared.catalogBatch)
		p.mu.shared.catalogBatch = batch
		return err
	}

	return nil
}

func (p *provider) sharedPath(meta objstorage.ObjectMetadata) string {
	return "shared://" + sharedObjectName(meta)
}

// sharedCreateRef creates a reference marker object.
func (p *provider) sharedCreateRef(meta objstorage.ObjectMetadata) error {
	if meta.Remote.CleanupMethod != objstorage.SharedRefTracking {
		return nil
	}
	refName := p.sharedObjectRefName(meta)
	writer, err := meta.Remote.Storage.CreateObject(refName)
	if err == nil {
		// The object is empty, just close the writer.
		err = writer.Close()
	}
	if err != nil {
		return errors.Wrapf(err, "creating marker object %q", refName)
	}
	return nil
}

func (p *provider) sharedCreate(
	_ context.Context,
	fileType base.FileType,
	fileNum base.DiskFileNum,
	locator remote.Locator,
	opts objstorage.CreateOptions,
) (objstorage.Writable, objstorage.ObjectMetadata, error) {
	if err := p.sharedCheckInitialized(); err != nil {
		return nil, objstorage.ObjectMetadata{}, err
	}
	storage, err := p.ensureStorage(locator)
	if err != nil {
		return nil, objstorage.ObjectMetadata{}, err
	}
	meta := objstorage.ObjectMetadata{
		DiskFileNum: fileNum,
		FileType:    fileType,
	}
	meta.Remote.CreatorID = p.shared.creatorID
	meta.Remote.CreatorFileNum = fileNum
	meta.Remote.CleanupMethod = opts.SharedCleanupMethod
	meta.Remote.Locator = locator
	meta.Remote.Storage = storage

	objName := sharedObjectName(meta)
	writer, err := storage.CreateObject(objName)
	if err != nil {
		return nil, objstorage.ObjectMetadata{}, errors.Wrapf(err, "creating object %q", objName)
	}
	return &sharedWritable{
		p:             p,
		meta:          meta,
		storageWriter: writer,
	}, meta, nil
}

func (p *provider) sharedOpenForReading(
	ctx context.Context, meta objstorage.ObjectMetadata, opts objstorage.OpenOptions,
) (objstorage.Readable, error) {
	if err := p.sharedCheckInitialized(); err != nil {
		return nil, err
	}
	// Verify we have a reference on this object; for performance reasons, we only
	// do this in testing scenarios.
	if p.shared.checkRefsOnOpen && meta.Remote.CleanupMethod == objstorage.SharedRefTracking {
		refName := p.sharedObjectRefName(meta)
		if _, err := meta.Remote.Storage.Size(refName); err != nil {
			if meta.Remote.Storage.IsNotExistError(err) {
				if opts.MustExist {
					p.st.Logger.Fatalf("marker object %q does not exist", refName)
					// TODO(radu): maybe list references for the object.
				}
				return nil, errors.Errorf("marker object %q does not exist", refName)
			}
			return nil, errors.Wrapf(err, "checking marker object %q", refName)
		}
	}
	objName := sharedObjectName(meta)
	reader, size, err := meta.Remote.Storage.ReadObject(ctx, objName)
	if err != nil {
		if opts.MustExist && meta.Remote.Storage.IsNotExistError(err) {
			p.st.Logger.Fatalf("object %q does not exist", objName)
			// TODO(radu): maybe list references for the object.
		}
		return nil, err
	}
	return p.newSharedReadable(reader, size, meta.DiskFileNum), nil
}

func (p *provider) sharedSize(meta objstorage.ObjectMetadata) (int64, error) {
	if err := p.sharedCheckInitialized(); err != nil {
		return 0, err
	}
	objName := sharedObjectName(meta)
	return meta.Remote.Storage.Size(objName)
}

// sharedUnref implements object "removal" with the shared backend. The ref
// marker object is removed and the backing object is removed only if there are
// no other ref markers.
func (p *provider) sharedUnref(meta objstorage.ObjectMetadata) error {
	if meta.Remote.CleanupMethod == objstorage.SharedNoCleanup {
		// Never delete objects in this mode.
		return nil
	}
	if p.isProtected(meta.DiskFileNum) {
		// TODO(radu): we need a mechanism to unref the object when it becomes
		// unprotected.
		return nil
	}

	refName := p.sharedObjectRefName(meta)
	// Tolerate a not-exists error.
	if err := meta.Remote.Storage.Delete(refName); err != nil && !meta.Remote.Storage.IsNotExistError(err) {
		return err
	}
	otherRefs, err := meta.Remote.Storage.List(sharedObjectRefPrefix(meta), "" /* delimiter */)
	if err != nil {
		return err
	}
	if len(otherRefs) == 0 {
		objName := sharedObjectName(meta)
		if err := meta.Remote.Storage.Delete(objName); err != nil && !meta.Remote.Storage.IsNotExistError(err) {
			return err
		}
	}
	return nil
}

func (p *provider) ensureStorageLocked(locator remote.Locator) (remote.Storage, error) {
	if p.mu.shared.storageObjects == nil {
		p.mu.shared.storageObjects = make(map[remote.Locator]remote.Storage)
	}
	if res, ok := p.mu.shared.storageObjects[locator]; ok {
		return res, nil
	}
	res, err := p.st.Shared.StorageFactory.CreateStorage(locator)
	if err != nil {
		return nil, err
	}

	p.mu.shared.storageObjects[locator] = res
	return res, nil
}
func (p *provider) ensureStorage(locator remote.Locator) (remote.Storage, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ensureStorageLocked(locator)
}
