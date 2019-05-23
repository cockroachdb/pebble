// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync/atomic"

	"github.com/petermattis/pebble/internal/base"
)

// TODO(peter): describe the MANIFEST file format, independently of the C++
// project.

var errCorruptManifest = errors.New("pebble: corrupt manifest")

type byteReader interface {
	io.ByteReader
	io.Reader
}

// Tags for the versionEdit disk format.
// Tag 8 is no longer used.
const (
	// LevelDB tags.
	tagComparator     = 1
	tagLogNumber      = 2
	tagNextFileNumber = 3
	tagLastSequence   = 4
	tagCompactPointer = 5
	tagDeletedFile    = 6
	tagNewFile        = 7
	tagPrevLogNumber  = 9

	// RocksDB tags.
	tagNewFile2         = 100
	tagNewFile3         = 102
	tagNewFile4         = 103
	tagColumnFamily     = 200
	tagColumnFamilyAdd  = 201
	tagColumnFamilyDrop = 202
	tagMaxColumnFamily  = 203

	// The custom tags sub-format used by tagNewFile4.
	customTagTerminate         = 1
	customTagNeedsCompaction   = 2
	customTagPathID            = 65
	customTagNonSafeIgnoreMask = 1 << 6
)

type deletedFileEntry struct {
	level   int
	fileNum uint64
}

type newFileEntry struct {
	level int
	meta  fileMetadata
}

type versionEdit struct {
	comparatorName string
	logNumber      uint64
	prevLogNumber  uint64
	nextFileNumber uint64
	lastSequence   uint64
	deletedFiles   map[deletedFileEntry]bool // A set of deletedFileEntry values.
	newFiles       []newFileEntry
	metrics        map[int]*LevelMetrics // level -> metrics update
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
			if _, err := d.readLevel(); err != nil {
				return err
			}
			if _, err := d.readBytes(); err != nil {
				return err
			}
			// NB: RocksDB does not use compaction pointers anymore.

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

		case tagNewFile, tagNewFile2, tagNewFile3, tagNewFile4:
			level, err := d.readLevel()
			if err != nil {
				return err
			}
			fileNum, err := d.readUvarint()
			if err != nil {
				return err
			}
			if tag == tagNewFile3 {
				// The pathID field appears unused in RocksDB.
				_ /* pathID */, err := d.readUvarint()
				if err != nil {
					return err
				}
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
			var smallestSeqNum uint64
			var largestSeqNum uint64
			if tag != tagNewFile {
				smallestSeqNum, err = d.readUvarint()
				if err != nil {
					return err
				}
				largestSeqNum, err = d.readUvarint()
				if err != nil {
					return err
				}
			}
			var markedForCompaction bool
			if tag == tagNewFile4 {
				for {
					customTag, err := d.readUvarint()
					if err != nil {
						return err
					}
					if customTag == customTagTerminate {
						break
					}
					field, err := d.readBytes()
					if err != nil {
						return err
					}
					switch customTag {
					case customTagNeedsCompaction:
						if len(field) != 1 {
							return fmt.Errorf("new-file4: need-compaction field wrong size")
						}
						markedForCompaction = (field[0] == 1)

					case customTagPathID:
						return fmt.Errorf("new-file4: path-id field not supported")

					default:
						if (customTag & customTagNonSafeIgnoreMask) != 0 {
							return fmt.Errorf("new-file4: custom field not supported: %d", customTag)
						}
					}
				}
			}
			v.newFiles = append(v.newFiles, newFileEntry{
				level: level,
				meta: fileMetadata{
					fileNum:             fileNum,
					size:                size,
					smallest:            base.DecodeInternalKey(smallest),
					largest:             base.DecodeInternalKey(largest),
					smallestSeqNum:      smallestSeqNum,
					largestSeqNum:       largestSeqNum,
					markedForCompaction: markedForCompaction,
				},
			})

		case tagPrevLogNumber:
			n, err := d.readUvarint()
			if err != nil {
				return err
			}
			v.prevLogNumber = n

		case tagColumnFamily, tagColumnFamilyAdd, tagColumnFamilyDrop, tagMaxColumnFamily:
			return fmt.Errorf("column families are not supported")

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
	for x := range v.deletedFiles {
		e.writeUvarint(tagDeletedFile)
		e.writeUvarint(uint64(x.level))
		e.writeUvarint(x.fileNum)
	}
	for _, x := range v.newFiles {
		var customFields bool
		if x.meta.markedForCompaction {
			customFields = true
			e.writeUvarint(tagNewFile4)
		} else {
			e.writeUvarint(tagNewFile2)
		}
		e.writeUvarint(uint64(x.level))
		e.writeUvarint(x.meta.fileNum)
		e.writeUvarint(x.meta.size)
		e.writeKey(x.meta.smallest)
		e.writeKey(x.meta.largest)
		e.writeUvarint(x.meta.smallestSeqNum)
		e.writeUvarint(x.meta.largestSeqNum)
		if customFields {
			if x.meta.markedForCompaction {
				e.writeUvarint(customTagNeedsCompaction)
				e.writeBytes([]byte{1})
			}
			e.writeUvarint(customTagTerminate)
		}
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

func (e versionEditEncoder) writeKey(k InternalKey) {
	e.writeUvarint(uint64(k.Size()))
	e.Write(k.UserKey)
	buf := k.EncodeTrailer()
	e.Write(buf[:])
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

// bulkVersionEdit summarizes the files added and deleted from a set of version
// edits.
//
// The C++ LevelDB code calls this concept a VersionSet::Builder.
type bulkVersionEdit struct {
	added   [numLevels][]fileMetadata
	deleted [numLevels]map[uint64]bool // map[uint64]bool is a set of fileNums.
}

func (b *bulkVersionEdit) accumulate(ve *versionEdit) {
	for df := range ve.deletedFiles {
		dmap := b.deleted[df.level]
		if dmap == nil {
			dmap = make(map[uint64]bool)
			b.deleted[df.level] = dmap
		}
		dmap[df.fileNum] = true
	}

	for _, nf := range ve.newFiles {
		if dmap := b.deleted[nf.level]; dmap != nil {
			delete(dmap, nf.meta.fileNum)
		}
		b.added[nf.level] = append(b.added[nf.level], nf.meta)
	}
}

// apply applies the delta b to a base version to produce a new version. The
// new version is consistent with respect to the internal key comparer icmp.
//
// base may be nil, which is equivalent to a pointer to a zero version.
func (b *bulkVersionEdit) apply(
	opts *Options, base *version, cmp Compare,
) (*version, error) {
	v := new(version)
	for level := range v.files {
		if len(b.added[level]) == 0 && len(b.deleted[level]) == 0 {
			// There are no edits on this level.
			if base == nil {
				continue
			}
			files := base.files[level]
			v.files[level] = files
			// We still have to bump the ref count for all files.
			for i := range files {
				atomic.AddInt32(files[i].refs, 1)
			}
			continue
		}

		combined := [2][]fileMetadata{
			nil,
			b.added[level],
		}
		if base != nil {
			combined[0] = base.files[level]
		}
		n := len(combined[0]) + len(combined[1])
		if n == 0 {
			continue
		}
		v.files[level] = make([]fileMetadata, 0, n)
		dmap := b.deleted[level]

		for _, ff := range combined {
			for _, f := range ff {
				if dmap != nil && dmap[f.fileNum] {
					continue
				}
				if f.refs == nil {
					f.refs = new(int32)
				}
				atomic.AddInt32(f.refs, 1)
				v.files[level] = append(v.files[level], f)
			}
		}

		// TODO(peter): base.files[level] is already sorted. Instead of appending
		// b.addFiles[level] to the end and sorting afterwards, it might be more
		// efficient to sort b.addFiles[level] and then merge the two sorted
		// slices.
		if level == 0 {
			sort.Sort(bySeqNum(v.files[level]))
		} else {
			sort.Sort(bySmallest{v.files[level], cmp})
		}
	}
	if err := v.checkOrdering(cmp); err != nil {
		return nil, fmt.Errorf("pebble: internal error: %v", err)
	}
	return v, nil
}
