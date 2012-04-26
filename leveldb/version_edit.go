// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package leveldb

import (
	"bufio"
	"bytes"
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

func (v *versionEdit) encode(w io.Writer) error {
	e := versionEditEncoder{new(bytes.Buffer)}
	if v.comparatorName != "" {
		e.writeUvarint(tagComparator)
		e.writeString(v.comparatorName)
	}
	if v.logNumber != 0 {
		e.writeUvarint(tagLogNumber)
		e.writeUvarint(v.logNumber)
	}
	if v.prevLogNumber != 0 {
		e.writeUvarint(tagPrevLogNumber)
		e.writeUvarint(v.prevLogNumber)
	}
	if v.nextFileNumber != 0 {
		e.writeUvarint(tagNextFileNumber)
		e.writeUvarint(v.nextFileNumber)
	}
	if v.lastSequence != 0 {
		e.writeUvarint(tagLastSequence)
		e.writeUvarint(v.lastSequence)
	}
	for _, x := range v.compactPointers {
		e.writeUvarint(tagCompactPointer)
		e.writeUvarint(uint64(x.level))
		e.writeBytes(x.key)
	}
	for x := range v.deletedFiles {
		e.writeUvarint(tagDeletedFile)
		e.writeUvarint(uint64(x.level))
		e.writeUvarint(x.fileNum)
	}
	for _, x := range v.newFiles {
		e.writeUvarint(tagNewFile)
		e.writeUvarint(uint64(x.level))
		e.writeUvarint(x.meta.fileNum)
		e.writeUvarint(x.meta.size)
		e.writeBytes(x.meta.smallest)
		e.writeBytes(x.meta.largest)
	}
	_, err := w.Write(e.Bytes())
	return err
}

type versionEditDecoder struct {
	byteReader
}

func (d versionEditDecoder) readBytes() ([]byte, error) {
	n, err := d.readUvarint()
	if err != nil {
		return nil, err
	}
	s := make([]byte, n)
	_, err = io.ReadFull(d, s)
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
	u, err := binary.ReadUvarint(d)
	if err != nil {
		if err == io.EOF {
			return 0, errCorruptManifest
		}
		return 0, err
	}
	return u, nil
}

type versionEditEncoder struct {
	*bytes.Buffer
}

func (e versionEditEncoder) writeBytes(p []byte) {
	e.writeUvarint(uint64(len(p)))
	e.Write(p)
}

func (e versionEditEncoder) writeString(s string) {
	e.writeUvarint(uint64(len(s)))
	e.WriteString(s)
}

func (e versionEditEncoder) writeUvarint(u uint64) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], u)
	e.Write(buf[:n])
}
