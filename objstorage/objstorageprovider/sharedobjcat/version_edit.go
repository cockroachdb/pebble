// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sharedobjcat

import (
	"bufio"
	"encoding/binary"
	"io"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage"
)

// versionEdit is a modification to the shared object state which can be encoded
// into a record.
//
// TODO(radu): consider adding creation and deletion time for debugging purposes.
type versionEdit struct {
	NewObjects     []SharedObjectMetadata
	DeletedObjects []base.DiskFileNum
	CreatorID      objstorage.CreatorID
}

const (
	// tagNewObject is followed by the FileNum, creator ID, creator FileNum, and
	// cleanup method.
	tagNewObject = 1
	// tagDeletedObject is followed by the FileNum.
	tagDeletedObject = 2
	// tagCreatorID is followed by the Creator ID for this store. This ID can
	// never change.
	tagCreatorID = 3
)

// Object type values. We don't want to encode FileType directly because it is
// more general (and we want freedom to change it in the future).
const (
	objTypeTable = 1
)

func objTypeToFileType(objType uint64) (base.FileType, error) {
	switch objType {
	case objTypeTable:
		return base.FileTypeTable, nil
	default:
		return 0, errors.Newf("unknown object type %d", objType)
	}
}

func fileTypeToObjType(fileType base.FileType) (uint64, error) {
	switch fileType {
	case base.FileTypeTable:
		return objTypeTable, nil

	default:
		return 0, errors.Newf("unknown object type for file type %d", fileType)
	}
}

// Encode encodes an edit to the specified writer.
func (v *versionEdit) Encode(w io.Writer) error {
	buf := make([]byte, 0, binary.MaxVarintLen64*(len(v.NewObjects)*4+len(v.DeletedObjects)*2+2))
	for _, meta := range v.NewObjects {
		objType, err := fileTypeToObjType(meta.FileType)
		if err != nil {
			return err
		}
		buf = binary.AppendUvarint(buf, uint64(tagNewObject))
		buf = binary.AppendUvarint(buf, uint64(meta.FileNum.FileNum()))
		buf = binary.AppendUvarint(buf, objType)
		buf = binary.AppendUvarint(buf, uint64(meta.CreatorID))
		buf = binary.AppendUvarint(buf, uint64(meta.CreatorFileNum.FileNum()))
		buf = binary.AppendUvarint(buf, uint64(meta.CleanupMethod))
	}

	for _, dfn := range v.DeletedObjects {
		buf = binary.AppendUvarint(buf, uint64(tagDeletedObject))
		buf = binary.AppendUvarint(buf, uint64(dfn.FileNum()))
	}
	if v.CreatorID.IsSet() {
		buf = binary.AppendUvarint(buf, uint64(tagCreatorID))
		buf = binary.AppendUvarint(buf, uint64(v.CreatorID))
	}
	_, err := w.Write(buf)
	return err
}

// Decode decodes an edit from the specified reader.
func (v *versionEdit) Decode(r io.Reader) error {
	br, ok := r.(io.ByteReader)
	if !ok {
		br = bufio.NewReader(r)
	}
	for {
		tag, err := binary.ReadUvarint(br)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		err = nil
		switch tag {
		case tagNewObject:
			var fileNum, creatorID, creatorFileNum, cleanupMethod uint64
			var fileType base.FileType
			fileNum, err = binary.ReadUvarint(br)
			if err == nil {
				var objType uint64
				objType, err = binary.ReadUvarint(br)
				if err == nil {
					fileType, err = objTypeToFileType(objType)
				}
			}
			if err == nil {
				creatorID, err = binary.ReadUvarint(br)
			}
			if err == nil {
				creatorFileNum, err = binary.ReadUvarint(br)
			}
			if err == nil {
				cleanupMethod, err = binary.ReadUvarint(br)
			}
			if err == nil {
				v.NewObjects = append(v.NewObjects, SharedObjectMetadata{
					FileNum:        base.FileNum(fileNum).DiskFileNum(),
					FileType:       fileType,
					CreatorID:      objstorage.CreatorID(creatorID),
					CreatorFileNum: base.FileNum(creatorFileNum).DiskFileNum(),
					CleanupMethod:  objstorage.SharedCleanupMethod(cleanupMethod),
				})
			}

		case tagDeletedObject:
			var fileNum uint64
			fileNum, err = binary.ReadUvarint(br)
			if err == nil {
				v.DeletedObjects = append(v.DeletedObjects, base.FileNum(fileNum).DiskFileNum())
			}

		case tagCreatorID:
			var id uint64
			id, err = binary.ReadUvarint(br)
			if err == nil {
				v.CreatorID = objstorage.CreatorID(id)
			}

		default:
			// Ignore unknown tags.
		}
		if err != nil {
			if err == io.EOF {
				return errCorruptCatalog
			}
			return err
		}
	}
	return nil
}

var errCorruptCatalog = base.CorruptionErrorf("pebble: corrupt shared object catalog")
