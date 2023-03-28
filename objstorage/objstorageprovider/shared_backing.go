// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorageprovider

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider/sharedobjcat"
)

const (
	tagCreatorID      = 1
	tagCreatorFileNum = 2
	tagCleanupMethod  = 3
	// tagRefCheckID encodes the ID of a ref marker that needs to be checked when
	// attaching this object to another provider. This is set to the creator ID of
	// the provider that encodes the backing, and allows the "target" provider to
	// check that the "source" provider kept its reference on the object alive.
	tagRefCheckID = 4

	// Any new tags that don't have the tagNotSafeToIgnoreMask bit set must be
	// followed by the length of the data (so they can be skipped).

	// Any new tags that have the tagNotSafeToIgnoreMask bit set cause errors if
	// they are encountered by earlier code that doesn't know the tag.
	tagNotSafeToIgnoreMask = 64
)

func (p *provider) encodeSharedObjectBacking(
	meta *objstorage.ObjectMetadata,
) (objstorage.SharedObjectBacking, error) {
	if !meta.IsShared() {
		return nil, errors.AssertionFailedf("object %s not on shared storage", meta.FileNum)
	}

	buf := make([]byte, 0, binary.MaxVarintLen64*4)
	buf = binary.AppendUvarint(buf, tagCreatorID)
	buf = binary.AppendUvarint(buf, uint64(meta.Shared.CreatorID))
	// TODO(radu): encode file type as well?
	buf = binary.AppendUvarint(buf, tagCreatorFileNum)
	buf = binary.AppendUvarint(buf, uint64(meta.Shared.CreatorFileNum))
	buf = binary.AppendUvarint(buf, tagCleanupMethod)
	buf = binary.AppendUvarint(buf, uint64(meta.Shared.CleanupMethod))
	if meta.Shared.CleanupMethod == objstorage.SharedRefTracking {
		buf = binary.AppendUvarint(buf, tagRefCheckID)
		buf = binary.AppendUvarint(buf, uint64(p.shared.creatorID))
	}
	return buf, nil
}

type sharedObjectBackingHandle struct {
	backing objstorage.SharedObjectBacking
	fileNum base.FileNum
	p       *provider
}

func (s *sharedObjectBackingHandle) Get() (objstorage.SharedObjectBacking, error) {
	if s.backing == nil {
		return nil, errors.Errorf("SharedObjectBackingHandle.Get() called after Close()")
	}
	return s.backing, nil
}

func (s *sharedObjectBackingHandle) Close() {
	if s.backing != nil {
		s.backing = nil
		s.p.unprotectObject(s.fileNum)
	}
}

var _ objstorage.SharedObjectBackingHandle = (*sharedObjectBackingHandle)(nil)

// SharedObjectBacking is part of the objstorage.Provider interface.
func (p *provider) SharedObjectBacking(
	meta *objstorage.ObjectMetadata,
) (objstorage.SharedObjectBackingHandle, error) {
	backing, err := p.encodeSharedObjectBacking(meta)
	if err != nil {
		return nil, err
	}
	p.protectObject(meta.FileNum)
	return &sharedObjectBackingHandle{
		backing: backing,
		fileNum: meta.FileNum,
		p:       p,
	}, nil
}

// fromSharedObjectBacking decodes the shared object metadata.
// Returns the object metadata and (optionally) the creator ID of the provider
// that encoded the backing whose ref marker needs to be checked.
func fromSharedObjectBacking(
	fileType base.FileType, fileNum base.FileNum, buf objstorage.SharedObjectBacking,
) (_ objstorage.ObjectMetadata, refToCheck objstorage.CreatorID, _ error) {
	var creatorID, creatorFileNum, cleanupMethod, refCheckID uint64
	br := bytes.NewReader(buf)
	for {
		tag, err := binary.ReadUvarint(br)
		if err == io.EOF {
			break
		}
		if err != nil {
			return objstorage.ObjectMetadata{}, 0, err
		}
		switch tag {
		case tagCreatorID:
			creatorID, err = binary.ReadUvarint(br)

		case tagCreatorFileNum:
			creatorFileNum, err = binary.ReadUvarint(br)

		case tagCleanupMethod:
			cleanupMethod, err = binary.ReadUvarint(br)

		case tagRefCheckID:
			refCheckID, err = binary.ReadUvarint(br)

		default:
			// Ignore unknown tags, unless they're not safe to ignore.
			if tag&tagNotSafeToIgnoreMask != 0 {
				return objstorage.ObjectMetadata{}, 0, errors.Newf("unknown tag %d", tag)
			}
			var dataLen uint64
			dataLen, err = binary.ReadUvarint(br)
			if err == nil {
				_, err = br.Seek(int64(dataLen), io.SeekCurrent)
			}
		}
		if err != nil {
			return objstorage.ObjectMetadata{}, 0, err
		}
	}
	if creatorID == 0 {
		return objstorage.ObjectMetadata{}, 0, errors.Newf("shared object backing missing creator ID")
	}
	if creatorFileNum == 0 {
		return objstorage.ObjectMetadata{}, 0, errors.Newf("shared object backing missing creator file num")
	}
	meta := objstorage.ObjectMetadata{
		FileNum:  fileNum,
		FileType: fileType,
	}
	meta.Shared.CreatorID = objstorage.CreatorID(creatorID)
	meta.Shared.CreatorFileNum = base.FileNum(creatorFileNum)
	meta.Shared.CleanupMethod = objstorage.SharedCleanupMethod(cleanupMethod)
	return meta, objstorage.CreatorID(refCheckID), nil
}

// AttachSharedObjects is part of the objstorage.Provider interface.
func (p *provider) AttachSharedObjects(
	objs []objstorage.SharedObjectToAttach,
) ([]objstorage.ObjectMetadata, error) {
	metas := make([]objstorage.ObjectMetadata, len(objs))
	refsToCheck := make([]objstorage.CreatorID, len(objs))
	for i, o := range objs {
		meta, refToCheck, err := fromSharedObjectBacking(o.FileType, o.FileNum, o.Backing)
		if err != nil {
			return nil, err
		}
		metas[i] = meta
		refsToCheck[i] = refToCheck
	}

	// Create the reference marker objects.
	// TODO(radu): check that the actual objects exist so we don't error out later
	// TODO(radu): parallelize this.
	for i, meta := range metas {
		if meta.Shared.CleanupMethod != objstorage.SharedRefTracking {
			continue
		}
		if err := p.sharedCreateRef(meta); err != nil {
			// TODO(radu): clean up already created references.
			return nil, err
		}
		// Check the "originator's" reference.
		refName := sharedObjectRefName(meta, refsToCheck[i])
		if _, err := p.st.Shared.Storage.Size(refName); err != nil {
			// TODO(radu): better error message if it doesn't exist.
			_ = p.sharedUnref(meta)
			return nil, errors.Wrapf(err, "checking originator's marker object %s", refName)
		}
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
