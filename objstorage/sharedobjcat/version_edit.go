// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sharedobjcat

import (
	"bufio"
	"encoding/binary"
	"io"

	"github.com/cockroachdb/pebble/internal/base"
)

// versionEdit is a modification to the shared object state which can be encoded
// into a record.
//
// TODO(radu): consider adding creation and deletion time for debugging purposes.
type versionEdit struct {
	NewObjects     []SharedObjectMetadata
	DeletedObjects []base.FileNum
}

const (
	// tagNewObject is followed by the FileNum, creator ID and creator FileNum.
	tagNewObject = 1
	// tagDeletedObject is followed by the FileNum.
	tagDeletedObject = 2
)

// Encode encodes an edit to the specified writer.
func (v *versionEdit) Encode(w io.Writer) error {
	buf := make([]byte, 0, binary.MaxVarintLen64*(len(v.NewObjects)*4+len(v.DeletedObjects)*2))
	for _, meta := range v.NewObjects {
		buf = binary.AppendUvarint(buf, uint64(tagNewObject))
		buf = binary.AppendUvarint(buf, uint64(meta.FileNum))
		buf = binary.AppendUvarint(buf, meta.CreatorID)
		buf = binary.AppendUvarint(buf, uint64(meta.CreatorFileNum))
	}

	for _, fileNum := range v.DeletedObjects {
		buf = binary.AppendUvarint(buf, uint64(tagDeletedObject))
		buf = binary.AppendUvarint(buf, uint64(fileNum))
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
			var fileNum, creatorID, creatorFileNum uint64
			fileNum, err = binary.ReadUvarint(br)
			if err == nil {
				creatorID, err = binary.ReadUvarint(br)
			}
			if err == nil {
				creatorFileNum, err = binary.ReadUvarint(br)
			}
			if err == nil {
				v.NewObjects = append(v.NewObjects, SharedObjectMetadata{
					FileNum:        base.FileNum(fileNum),
					CreatorID:      creatorID,
					CreatorFileNum: base.FileNum(creatorFileNum),
				})
			}

		case tagDeletedObject:
			var fileNum uint64
			fileNum, err = binary.ReadUvarint(br)
			if err == nil {
				v.DeletedObjects = append(v.DeletedObjects, base.FileNum(fileNum))
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
