// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorage

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage/sharedobjcat"
)

// sharedSubsystem contains the provider fields related to shared storage.
// All fields remain unset if shared storage is not configured.
type sharedSubsystem struct {
	catalog *sharedobjcat.Catalog
	// initialized guards access to the creatorID field.
	initialized atomic.Bool
	creatorID   CreatorID
	initOnce    sync.Once
}

func (ss *sharedSubsystem) init(creatorID CreatorID) {
	ss.initOnce.Do(func() {
		ss.creatorID = creatorID
		ss.initialized.Store(true)
	})
}

// sharedInit initializes the shared object subsystem (if configured) and finds
// any shared objects.
func (p *Provider) sharedInit() error {
	if p.st.Shared.Storage == nil {
		return nil
	}
	catalog, contents, err := sharedobjcat.Open(p.st.FS, p.st.FSDirName)
	if err != nil {
		return errors.Wrapf(err, "pebble: could not open shared object catalog")
	}
	p.shared.catalog = catalog

	// The creator ID may or may not be initialized yet.
	if contents.CreatorID.IsSet() {
		p.shared.init(contents.CreatorID)
		p.st.Logger.Infof("shared storage configured; creatorID = %s", contents.CreatorID)
	} else {
		p.st.Logger.Infof("shared storage configured; no creatorID yet")
	}

	for _, meta := range contents.Objects {
		o := ObjectMetadata{
			FileNum:  meta.FileNum,
			FileType: meta.FileType,
		}
		o.Shared.CreatorID = meta.CreatorID
		o.Shared.CreatorFileNum = meta.CreatorFileNum
		p.mu.knownObjects[o.FileNum] = o
	}
	return nil
}

// SetCreatorID sets the CreatorID which is needed in order to use shared
// objects. Shared object usage is disabled until this method is called the
// first time. Once set, the Creator ID is persisted and cannot change.
//
// Cannot be called if shared storage is not configured for the provider.
func (p *Provider) SetCreatorID(creatorID CreatorID) error {
	if p.st.Shared.Storage == nil {
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

func (p *Provider) sharedCheckInitialized() error {
	if p.st.Shared.Storage == nil {
		return errors.Errorf("shared object support not configured")
	}
	if !p.shared.initialized.Load() {
		return errors.Errorf("shared object support not available: shared creator ID not yet set")
	}
	return nil
}

func (p *Provider) sharedSync() error {
	batch := func() sharedobjcat.Batch {
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

func (p *Provider) sharedPath(meta ObjectMetadata) string {
	return "shared://" + sharedObjectName(meta)
}

func sharedObjectName(meta ObjectMetadata) string {
	// TODO(radu): prepend a "shard" value for better distribution within the bucket?
	return fmt.Sprintf(
		"%s-%s",
		meta.Shared.CreatorID, base.MakeFilename(meta.FileType, meta.Shared.CreatorFileNum),
	)
}

func (p *Provider) sharedCreate(
	fileType base.FileType, fileNum base.FileNum,
) (Writable, ObjectMetadata, error) {
	if err := p.sharedCheckInitialized(); err != nil {
		return nil, ObjectMetadata{}, err
	}
	meta := ObjectMetadata{
		FileNum:  fileNum,
		FileType: fileType,
	}
	meta.Shared.CreatorID = p.shared.creatorID
	meta.Shared.CreatorFileNum = fileNum

	objName := sharedObjectName(meta)
	writer, err := p.st.Shared.Storage.CreateObject(objName)
	if err != nil {
		return nil, ObjectMetadata{}, err
	}
	return &sharedWritable{
		storageWriter: writer,
	}, meta, nil
}

func (p *Provider) sharedOpenForReading(meta ObjectMetadata) (Readable, error) {
	if err := p.sharedCheckInitialized(); err != nil {
		return nil, err
	}
	objName := sharedObjectName(meta)
	size, err := p.st.Shared.Storage.Size(objName)
	if err != nil {
		return nil, err
	}
	return newSharedReadable(p.st.Shared.Storage, objName, size), nil
}

func (p *Provider) sharedSize(meta ObjectMetadata) (int64, error) {
	if err := p.sharedCheckInitialized(); err != nil {
		return 0, err
	}
	objName := sharedObjectName(meta)
	return p.st.Shared.Storage.Size(objName)
}
