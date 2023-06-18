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
	"github.com/cockroachdb/pebble/objstorage/shared"
)

const (
	tagCreatorID      = 1
	tagCreatorFileNum = 2
	tagCleanupMethod  = 3
	// tagRefCheckID encodes the information for a ref marker that needs to be
	// checked when attaching this object to another provider. This is set to the
	// creator ID and FileNum for the provider that encodes the backing, and
	// allows the "target" provider to check that the "source" provider kept its
	// reference on the object alive.
	tagRefCheckID = 4
	// tagLocator encodes the shared.Locator; if absent the locator is "". It is
	// followed by the locator string length and the locator string.
	tagLocator = 5
	// tagLocator encodes a custom object name (if present). It is followed by the
	// custom name string length and the string.
	tagCustomObjectName = 6

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
		return nil, errors.AssertionFailedf("object %s not on shared storage", meta.DiskFileNum)
	}

	buf := make([]byte, 0, binary.MaxVarintLen64*4)
	buf = binary.AppendUvarint(buf, tagCreatorID)
	buf = binary.AppendUvarint(buf, uint64(meta.Shared.CreatorID))
	// TODO(radu): encode file type as well?
	buf = binary.AppendUvarint(buf, tagCreatorFileNum)
	buf = binary.AppendUvarint(buf, uint64(meta.Shared.CreatorFileNum.FileNum()))
	buf = binary.AppendUvarint(buf, tagCleanupMethod)
	buf = binary.AppendUvarint(buf, uint64(meta.Shared.CleanupMethod))
	if meta.Shared.CleanupMethod == objstorage.SharedRefTracking {
		buf = binary.AppendUvarint(buf, tagRefCheckID)
		buf = binary.AppendUvarint(buf, uint64(p.shared.creatorID))
		buf = binary.AppendUvarint(buf, uint64(meta.DiskFileNum.FileNum()))
	}
	if meta.Shared.Locator != "" {
		buf = binary.AppendUvarint(buf, tagLocator)
		buf = encodeString(buf, string(meta.Shared.Locator))
	}
	if meta.Shared.CustomObjectName != "" {
		buf = binary.AppendUvarint(buf, tagCustomObjectName)
		buf = encodeString(buf, meta.Shared.CustomObjectName)
	}
	return buf, nil
}

type sharedObjectBackingHandle struct {
	backing objstorage.SharedObjectBacking
	fileNum base.DiskFileNum
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
	p.protectObject(meta.DiskFileNum)
	return &sharedObjectBackingHandle{
		backing: backing,
		fileNum: meta.DiskFileNum,
		p:       p,
	}, nil
}

// CreateSharedObjectBacking is part of the objstorage.Provider interface.
func (p *provider) CreateSharedObjectBacking(
	locator shared.Locator, objName string,
) (objstorage.SharedObjectBacking, error) {
	var meta objstorage.ObjectMetadata
	meta.Shared.Locator = locator
	meta.Shared.CustomObjectName = objName
	meta.Shared.CleanupMethod = objstorage.SharedNoCleanup
	return p.encodeSharedObjectBacking(&meta)
}

type decodedBacking struct {
	meta objstorage.ObjectMetadata
	// refToCheck is set only when meta.Shared.CleanupMethod is RefTracking
	refToCheck struct {
		creatorID objstorage.CreatorID
		fileNum   base.DiskFileNum
	}
}

// decodeSharedObjectBacking decodes the shared object metadata.
//
// Note that the meta.Shared.Storage field is not set.
func decodeSharedObjectBacking(
	fileType base.FileType, fileNum base.DiskFileNum, buf objstorage.SharedObjectBacking,
) (decodedBacking, error) {
	var creatorID, creatorFileNum, cleanupMethod, refCheckCreatorID, refCheckFileNum uint64
	var locator, customObjName string
	br := bytes.NewReader(buf)
	for {
		tag, err := binary.ReadUvarint(br)
		if err == io.EOF {
			break
		}
		if err != nil {
			return decodedBacking{}, err
		}
		switch tag {
		case tagCreatorID:
			creatorID, err = binary.ReadUvarint(br)

		case tagCreatorFileNum:
			creatorFileNum, err = binary.ReadUvarint(br)

		case tagCleanupMethod:
			cleanupMethod, err = binary.ReadUvarint(br)

		case tagRefCheckID:
			refCheckCreatorID, err = binary.ReadUvarint(br)
			if err == nil {
				refCheckFileNum, err = binary.ReadUvarint(br)
			}

		case tagLocator:
			locator, err = decodeString(br)

		case tagCustomObjectName:
			customObjName, err = decodeString(br)

		default:
			// Ignore unknown tags, unless they're not safe to ignore.
			if tag&tagNotSafeToIgnoreMask != 0 {
				return decodedBacking{}, errors.Newf("unknown tag %d", tag)
			}
			var dataLen uint64
			dataLen, err = binary.ReadUvarint(br)
			if err == nil {
				_, err = br.Seek(int64(dataLen), io.SeekCurrent)
			}
		}
		if err != nil {
			return decodedBacking{}, err
		}
	}
	if customObjName == "" {
		if creatorID == 0 {
			return decodedBacking{}, errors.Newf("shared object backing missing creator ID")
		}
		if creatorFileNum == 0 {
			return decodedBacking{}, errors.Newf("shared object backing missing creator file num")
		}
	}
	var res decodedBacking
	res.meta.DiskFileNum = fileNum
	res.meta.FileType = fileType
	res.meta.Shared.CreatorID = objstorage.CreatorID(creatorID)
	res.meta.Shared.CreatorFileNum = base.FileNum(creatorFileNum).DiskFileNum()
	res.meta.Shared.CleanupMethod = objstorage.SharedCleanupMethod(cleanupMethod)
	if res.meta.Shared.CleanupMethod == objstorage.SharedRefTracking {
		if refCheckCreatorID == 0 || refCheckFileNum == 0 {
			return decodedBacking{}, errors.Newf("shared object backing missing ref to check")
		}
		res.refToCheck.creatorID = objstorage.CreatorID(refCheckCreatorID)
		res.refToCheck.fileNum = base.FileNum(refCheckFileNum).DiskFileNum()
	}
	res.meta.Shared.Locator = shared.Locator(locator)
	res.meta.Shared.CustomObjectName = customObjName
	return res, nil
}

func encodeString(buf []byte, s string) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(s)))
	buf = append(buf, []byte(s)...)
	return buf
}

func decodeString(br io.ByteReader) (string, error) {
	length, err := binary.ReadUvarint(br)
	if err != nil || length == 0 {
		return "", err
	}
	buf := make([]byte, length)
	for i := range buf {
		buf[i], err = br.ReadByte()
		if err != nil {
			return "", err
		}
	}
	return string(buf), nil
}

// AttachSharedObjects is part of the objstorage.Provider interface.
func (p *provider) AttachSharedObjects(
	objs []objstorage.SharedObjectToAttach,
) ([]objstorage.ObjectMetadata, error) {
	decoded := make([]decodedBacking, len(objs))
	for i, o := range objs {
		var err error
		decoded[i], err = decodeSharedObjectBacking(o.FileType, o.FileNum, o.Backing)
		if err != nil {
			return nil, err
		}
		decoded[i].meta.Shared.Storage, err = p.ensureStorage(decoded[i].meta.Shared.Locator)
		if err != nil {
			return nil, err
		}
	}

	// Create the reference marker objects.
	// TODO(radu): parallelize this.
	for _, d := range decoded {
		if d.meta.Shared.CleanupMethod != objstorage.SharedRefTracking {
			continue
		}
		if err := p.sharedCreateRef(d.meta); err != nil {
			// TODO(radu): clean up references previously created in this loop.
			return nil, err
		}
		// Check the "origin"'s reference.
		refName := sharedObjectRefName(d.meta, d.refToCheck.creatorID, d.refToCheck.fileNum)
		if _, err := d.meta.Shared.Storage.Size(refName); err != nil {
			_ = p.sharedUnref(d.meta)
			// TODO(radu): clean up references previously created in this loop.
			if d.meta.Shared.Storage.IsNotExistError(err) {
				return nil, errors.Errorf("origin marker object %q does not exist;"+
					" object probably removed from the provider which created the backing", refName)
			}
			return nil, errors.Wrapf(err, "checking origin's marker object %s", refName)
		}
	}

	func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, d := range decoded {
			p.mu.shared.catalogBatch.AddObject(sharedobjcat.SharedObjectMetadata{
				FileNum:        d.meta.DiskFileNum,
				FileType:       d.meta.FileType,
				CreatorID:      d.meta.Shared.CreatorID,
				CreatorFileNum: d.meta.Shared.CreatorFileNum,
				CleanupMethod:  d.meta.Shared.CleanupMethod,
				Locator:        d.meta.Shared.Locator,
			})
		}
	}()
	if err := p.sharedSync(); err != nil {
		return nil, err
	}

	metas := make([]objstorage.ObjectMetadata, len(decoded))
	for i, d := range decoded {
		metas[i] = d.meta
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	for _, meta := range metas {
		p.mu.knownObjects[meta.DiskFileNum] = meta
	}
	return metas, nil
}
