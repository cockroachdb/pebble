// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/cockroachdb/pebble/internal/base"
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

// DeletedFileEntry holds the state for a file deletion from a level. The file
// itself might still be referenced by another level.
type DeletedFileEntry struct {
	Level   int
	FileNum uint64
}

// NewFileEntry holds the state for a new file or one moved from a different
// level.
type NewFileEntry struct {
	Level int
	Meta  FileMetadata
}

// VersionEdit holds the state for an edit to a Version along with other
// on-disk state (log numbers, next file number, and the last sequence number).
type VersionEdit struct {
	// ComparerName is the value of Options.Comparer.Name. This is only set in
	// the first VersionEdit in a manifest (either when the DB is created, or
	// when a new manifest is created) and is used to verify that the comparer
	// specified at Open matches the comparer that was previously used.
	ComparerName string

	// MinUnflushedLogNum is the smallest WAL log file number corresponding to
	// mutations that have not been flushed to an sstable.
	//
	// This is an optional field, and 0 represents it is not set.
	MinUnflushedLogNum uint64

	// ObsoletePrevLogNum is a historic artifact from LevelDB that is not used by
	// Pebble (or RocksDB).
	ObsoletePrevLogNum uint64

	// The next file number. A single counter is used to assign file numbers
	// for the WAL, MANIFEST, sstable, and OPTIONS files.
	NextFileNum uint64

	// LastSeqNum is an upper bound on the sequence numbers that have been
	// assigned in flushed WALs. Unflushed WALs (that will be replayed during
	// recovery) may contain sequence numbers greater than this value.
	LastSeqNum uint64

	// A file num may be present in both deleted files and new files when it
	// is moved from a lower level to a higher level (when the compaction
	// found that there was no overlapping file at the higher level).
	DeletedFiles map[DeletedFileEntry]bool
	NewFiles     []NewFileEntry
}

// Decode decodes an edit from the specified reader.
func (v *VersionEdit) Decode(r io.Reader) error {
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
			v.ComparerName = string(s)

		case tagLogNumber:
			n, err := d.readUvarint()
			if err != nil {
				return err
			}
			v.MinUnflushedLogNum = n

		case tagNextFileNumber:
			n, err := d.readUvarint()
			if err != nil {
				return err
			}
			v.NextFileNum = n

		case tagLastSequence:
			n, err := d.readUvarint()
			if err != nil {
				return err
			}
			v.LastSeqNum = n

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
			if v.DeletedFiles == nil {
				v.DeletedFiles = make(map[DeletedFileEntry]bool)
			}
			v.DeletedFiles[DeletedFileEntry{level, fileNum}] = true

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
			v.NewFiles = append(v.NewFiles, NewFileEntry{
				Level: level,
				Meta: FileMetadata{
					FileNum:             fileNum,
					Size:                size,
					Smallest:            base.DecodeInternalKey(smallest),
					Largest:             base.DecodeInternalKey(largest),
					SmallestSeqNum:      smallestSeqNum,
					LargestSeqNum:       largestSeqNum,
					MarkedForCompaction: markedForCompaction,
				},
			})

		case tagPrevLogNumber:
			n, err := d.readUvarint()
			if err != nil {
				return err
			}
			v.ObsoletePrevLogNum = n

		case tagColumnFamily, tagColumnFamilyAdd, tagColumnFamilyDrop, tagMaxColumnFamily:
			return fmt.Errorf("column families are not supported")

		default:
			return errCorruptManifest
		}
	}
	return nil
}

// Encode encodes an edit to the specified writer.
func (v *VersionEdit) Encode(w io.Writer) error {
	e := versionEditEncoder{new(bytes.Buffer)}
	if v.ComparerName != "" {
		e.writeUvarint(tagComparator)
		e.writeString(v.ComparerName)
	}
	if v.MinUnflushedLogNum != 0 {
		e.writeUvarint(tagLogNumber)
		e.writeUvarint(v.MinUnflushedLogNum)
	}
	if v.ObsoletePrevLogNum != 0 {
		e.writeUvarint(tagPrevLogNumber)
		e.writeUvarint(v.ObsoletePrevLogNum)
	}
	if v.NextFileNum != 0 {
		e.writeUvarint(tagNextFileNumber)
		e.writeUvarint(v.NextFileNum)
	}
	// RocksDB requires LastSeqNum to be encoded for the first MANIFEST entry,
	// even though its value is zero. We detect this by encoding LastSeqNum when
	// ComparerName is set.
	if v.LastSeqNum != 0 || v.ComparerName != "" {
		e.writeUvarint(tagLastSequence)
		e.writeUvarint(v.LastSeqNum)
	}
	for x := range v.DeletedFiles {
		e.writeUvarint(tagDeletedFile)
		e.writeUvarint(uint64(x.Level))
		e.writeUvarint(x.FileNum)
	}
	for _, x := range v.NewFiles {
		var customFields bool
		if x.Meta.MarkedForCompaction {
			customFields = true
			e.writeUvarint(tagNewFile4)
		} else {
			e.writeUvarint(tagNewFile2)
		}
		e.writeUvarint(uint64(x.Level))
		e.writeUvarint(x.Meta.FileNum)
		e.writeUvarint(x.Meta.Size)
		e.writeKey(x.Meta.Smallest)
		e.writeKey(x.Meta.Largest)
		e.writeUvarint(x.Meta.SmallestSeqNum)
		e.writeUvarint(x.Meta.LargestSeqNum)
		if customFields {
			if x.Meta.MarkedForCompaction {
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
	if u >= NumLevels {
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

// BulkVersionEdit summarizes the files added and deleted from a set of version
// edits.
type BulkVersionEdit struct {
	Added   [NumLevels][]FileMetadata
	Deleted [NumLevels]map[uint64]bool // map[uint64]bool is a set of fileNums
}

// Accumulate adds the file addition and deletions in the specified version
// edit to the bulk edit's internal state.
func (b *BulkVersionEdit) Accumulate(ve *VersionEdit) {
	for df := range ve.DeletedFiles {
		dmap := b.Deleted[df.Level]
		if dmap == nil {
			dmap = make(map[uint64]bool)
			b.Deleted[df.Level] = dmap
		}
		dmap[df.FileNum] = true
	}

	for _, nf := range ve.NewFiles {
		// A new file should not have been deleted in this or a preceding
		// VersionEdit at the same level (though files can move across levels).
		if dmap := b.Deleted[nf.Level]; dmap != nil {
			if _, ok := dmap[nf.Meta.FileNum]; ok {
				panic(fmt.Sprintf("file deleted %d before it was inserted\n", nf.Meta.FileNum))
			}
		}
		b.Added[nf.Level] = append(b.Added[nf.Level], nf.Meta)
	}
}

// Apply applies the delta b to a base version to produce a new version. The
// new version is consistent with respect to the internal key comparer icmp.
//
// base may be nil, which is equivalent to a pointer to a zero version.
func (b *BulkVersionEdit) Apply(
	base *Version, cmp Compare, format base.Formatter,
) (*Version, error) {
	v := new(Version)
	for level := range v.Files {
		if len(b.Added[level]) == 0 && len(b.Deleted[level]) == 0 {
			// There are no edits on this level.
			if base == nil {
				continue
			}
			files := base.Files[level]
			v.Files[level] = files
			// We still have to bump the ref count for all files.
			for i := range files {
				atomic.AddInt32(files[i].refs, 1)
			}
			continue
		}

		combined := [2][]FileMetadata{
			nil,
			b.Added[level],
		}
		if base != nil {
			combined[0] = base.Files[level]
		}
		n := len(combined[0]) + len(combined[1])
		if n == 0 {
			continue
		}
		v.Files[level] = make([]FileMetadata, 0, n)
		dmap := b.Deleted[level]

		for _, ff := range combined {
			for _, f := range ff {
				if dmap != nil && dmap[f.FileNum] {
					continue
				}
				if f.refs == nil {
					f.refs = new(int32)
				}
				atomic.AddInt32(f.refs, 1)
				v.Files[level] = append(v.Files[level], f)
			}
		}

		// TODO(peter): base.files[level] is already sorted. Instead of appending
		// b.addFiles[level] to the end and sorting afterwards, it might be more
		// efficient to sort b.addFiles[level] and then merge the two sorted
		// slices.
		if level == 0 {
			// Since we iterated over files in the base version before the
			// added files, and the added files are accumulated in order of the
			// version edits, this slice should already be sorted in order of
			// increasing sequence number. But we sort it just to be sure.
			SortBySeqNum(v.Files[level])
		} else {
			SortBySmallest(v.Files[level], cmp)
		}
	}
	if err := v.CheckOrdering(cmp, format); err != nil {
		return nil, fmt.Errorf("pebble: internal error: %v", err)
	}
	return v, nil
}
