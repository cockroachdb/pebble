// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorage

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage/sharedobjcat"
)

// SharedObjectBacking encodes the metadata necessary to incorporate a shared
// object into a different Pebble instance.
type SharedObjectBacking []byte

const (
	tagCreatorID      = 1
	tagCreatorFileNum = 2

	// Any new tags that don't have the tagNotSafeToIgnoreMask bit set must be
	// followed by the length of the data (so they can be skipped).

	// Any new tags that have the tagNotSafeToIgnoreMask bit set cause errors if
	// they are encountered by earlier code that doesn't know the tag.
	tagNotSafeToIgnoreMask = 64
)

// SharedObjectBacking encodes the shared object metadata.
func (meta *ObjectMetadata) SharedObjectBacking() (SharedObjectBacking, error) {
	if !meta.IsShared() {
		return nil, errors.AssertionFailedf("object %s not on shared storage", meta.FileNum)
	}

	buf := make([]byte, 0, binary.MaxVarintLen64*4)
	buf = binary.AppendUvarint(buf, tagCreatorID)
	buf = binary.AppendUvarint(buf, uint64(meta.Shared.CreatorID))
	buf = binary.AppendUvarint(buf, tagCreatorFileNum)
	buf = binary.AppendUvarint(buf, uint64(meta.Shared.CreatorFileNum))
	return buf, nil
}

// fromSharedObjectBacking decodes the shared object metadata.
func fromSharedObjectBacking(
	fileType base.FileType, fileNum base.FileNum, buf SharedObjectBacking,
) (ObjectMetadata, error) {
	var creatorID uint64
	var creatorFileNum uint64
	br := bytes.NewReader(buf)
	for {
		tag, err := binary.ReadUvarint(br)
		if err == io.EOF {
			break
		}
		if err != nil {
			return ObjectMetadata{}, err
		}
		switch tag {
		case tagCreatorID:
			creatorID, err = binary.ReadUvarint(br)

		case tagCreatorFileNum:
			creatorFileNum, err = binary.ReadUvarint(br)

		// TODO(radu): encode file type as well?

		default:
			// Ignore unknown tags, unless they're not safe to ignore.
			if tag&tagNotSafeToIgnoreMask != 0 {
				return ObjectMetadata{}, errors.Newf("unknown tag %d", tag)
			}
			var dataLen uint64
			dataLen, err = binary.ReadUvarint(br)
			if err == nil {
				_, err = br.Seek(int64(dataLen), io.SeekCurrent)
			}
		}
		if err != nil {
			return ObjectMetadata{}, err
		}
	}
	if creatorID == 0 {
		return ObjectMetadata{}, errors.Newf("shared object backing missing creator ID")
	}
	if creatorFileNum == 0 {
		return ObjectMetadata{}, errors.Newf("shared object backing missing creator file num")
	}
	meta := ObjectMetadata{
		FileNum:  fileNum,
		FileType: fileType,
	}
	meta.Shared.CreatorID = CreatorID(creatorID)
	meta.Shared.CreatorFileNum = base.FileNum(creatorFileNum)
	return meta, nil
}

// SharedObjectToAttach contains the arguments needed to attach an existing shared object.
type SharedObjectToAttach struct {
	// FileNum is the file number that will be used to refer to this object (in
	// the context of this instance).
	FileNum  base.FileNum
	FileType base.FileType
	// Backing contains the metadata for the share dobject backing (normally
	// generated from a different instance).
	Backing SharedObjectBacking
}

// AttachSharedObjects registers existing shared objects with this provider.
func (p *Provider) AttachSharedObjects(objs []SharedObjectToAttach) ([]ObjectMetadata, error) {
	metas := make([]ObjectMetadata, len(objs))
	for i, o := range objs {
		meta, err := fromSharedObjectBacking(o.FileType, o.FileNum, o.Backing)
		if err != nil {
			return nil, err
		}
		metas[i] = meta
	}

	func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, meta := range metas {
			p.mu.shared.catalogBatch.AddObject(sharedobjcat.SharedObjectMetadata{
				FileNum:        meta.FileNum,
				FileType:       meta.FileType,
				CreatorID:      meta.Shared.CreatorID,
				CreatorFileNum: meta.Shared.CreatorFileNum,
			})
		}
	}()
	if err := p.sharedSync(); err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	for _, meta := range metas {
		p.mu.knownObjects[meta.FileNum] = meta
	}
	return metas, nil
}
