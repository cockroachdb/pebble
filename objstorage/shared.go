// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorage

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage/sharedobjcat"
)

// sharedInit initializes the shared object subsystem and finds any shared
// objects.
func (p *Provider) sharedInit() error {
	catalog, contents, err := sharedobjcat.Open(p.st.FS, p.st.FSDirName)
	if err != nil {
		return errors.Wrapf(err, "pebble: could not open shared object catalog")
	}
	p.shared.catalog = catalog
	// The creator ID may or may not be initialized yet.
	p.shared.creatorID = contents.CreatorID

	if p.st.Shared.CreatorID.IsSet() {
		if err := catalog.SetCreatorID(p.st.Shared.CreatorID); err != nil {
			catalog.Close()
			return err
		}
		p.shared.creatorID = p.st.Shared.CreatorID
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
	return fmt.Sprintf("%s-%s", meta.Shared.CreatorID, base.MakeFilename(meta.FileType, meta.FileNum))
}

func (p *Provider) sharedCreate(
	fileType base.FileType, fileNum base.FileNum,
) (Writable, ObjectMetadata, error) {
	if !p.shared.creatorID.IsSet() {
		return nil, ObjectMetadata{}, errors.Errorf("creator ID not set; shared object creation not possible")
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
	objName := sharedObjectName(meta)
	size, err := p.st.Shared.Storage.Size(objName)
	if err != nil {
		return nil, err
	}
	return newSharedReadable(p.st.Shared.Storage, objName, size), nil
}

func (p *Provider) sharedSize(meta ObjectMetadata) (int64, error) {
	objName := sharedObjectName(meta)
	return p.st.Shared.Storage.Size(objName)
}
