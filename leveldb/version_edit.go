// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package leveldb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
)

// TODO: describe the MANIFEST file format, independently of the C++ project.

var errCorruptManifest = errors.New("leveldb: corrupt manifest")

type byteReader interface {
	io.ByteReader
	io.Reader
}

// Tags for the versionEdit disk format.
// Tag 8 is no longer used.
const (
	tagComparator     = 1
	tagLogNumber      = 2
	tagNextFileNumber = 3
	tagLastSequence   = 4
	tagCompactPointer = 5
	tagDeletedFile    = 6
	tagNewFile        = 7
	tagPrevLogNumber  = 9
)

type compactPointerEntry struct {
	level int
	key   internalKey
}

type deletedFileEntry struct {
	level   int
	fileNum uint64
}

type newFileEntry struct {
	level int
	meta  fileMetadata
}

type versionEdit struct {
	comparatorName  string
	logNumber       uint64
	prevLogNumber   uint64
	nextFileNumber  uint64
	lastSequence    uint64
	compactPointers []compactPointerEntry
	deletedFiles    map[deletedFileEntry]bool
	newFiles        []newFileEntry
}

func (v *versionEdit) decode(r io.Reader) error {
	br, ok := r.(byteReader)
	if !ok {
		br = bufio.NewReader(r)
	}
	d := versionEditDecoder{br}
	for {
		tag, err := binary.ReadUvarint(br)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		switch tag {

		case tagComparator:
			s, err := d.readBytes()
			if err != nil {
				return err
			}
			v.comparatorName = string(s)

		case tagLogNumber:
			n, err := d.readUvarint()
			if err != nil {
				return err
			}
			v.logNumber = n

		case tagNextFileNumber:
			n, err := d.readUvarint()
			if err != nil {
				return err
			}
			v.nextFileNumber = n

		case tagLastSequence:
			n, err := d.readUvarint()
			if err != nil {
				return err
			}
			v.lastSequence = n

		case tagCompactPointer:
			level, err := d.readLevel()
			if err != nil {
				return err
			}
			key, err := d.readBytes()
			if err != nil {
				return err
			}
			v.compactPointers = append(v.compactPointers, compactPointerEntry{level, key})

		case tagDeletedFile:
			level, err := d.readLevel()
			if err != nil {
				return err
			}
			fileNum, err := d.readUvarint()
			if err != nil {
				return err
			}
			if v.deletedFiles == nil {
				v.deletedFiles = make(map[deletedFileEntry]bool)
			}
			v.deletedFiles[deletedFileEntry{level, fileNum}] = true

		case tagNewFile:
			level, err := d.readLevel()
			if err != nil {
				return err
			}
			fileNum, err := d.readUvarint()
			if err != nil {
				return err
			}
			size, err := d.readUvarint()
			if err != nil {
				return err
			}
			smallest, err := d.readBytes()
			if err != nil {
				return err
			}
			largest, err := d.readBytes()
			if err != nil {
				return err
			}
			v.newFiles = append(v.newFiles, newFileEntry{
				level: level,
				meta: fileMetadata{
					fileNum:  fileNum,
					size:     size,
					smallest: smallest,
					largest:  largest,
				},
			})

		case tagPrevLogNumber:
			n, err := d.readUvarint()
			if err != nil {
				return err
			}
			v.prevLogNumber = n

		default:
			return errCorruptManifest
		}
	}
	return nil
}

type versionEditDecoder struct {
	r byteReader
}

func (d versionEditDecoder) readBytes() ([]byte, error) {
	n, err := d.readUvarint()
	if err != nil {
		return nil, err
	}
	s := make([]byte, n)
	_, err = io.ReadFull(d.r, s)
	if err != nil {
		if err == io.ErrUnexpectedEOF {
			return nil, errCorruptManifest
		}
		return nil, err
	}
	return s, nil
}

func (d versionEditDecoder) readLevel() (int, error) {
	u, err := d.readUvarint()
	if err != nil {
		return 0, err
	}
	if u >= numLevels {
		return 0, errCorruptManifest
	}
	return int(u), nil
}

func (d versionEditDecoder) readUvarint() (uint64, error) {
	u, err := binary.ReadUvarint(d.r)
	if err != nil {
		if err == io.EOF {
			return 0, errCorruptManifest
		}
		return 0, err
	}
	return u, nil
}
