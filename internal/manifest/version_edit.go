// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"bufio"
	"bytes"
	stdcmp "cmp"
	"encoding/binary"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/strparse"
	"github.com/cockroachdb/pebble/sstable"
)

// TODO(peter): describe the MANIFEST file format, independently of the C++
// project.

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

	// Pebble tags.
	tagNewFile5            = 104 // Range keys.
	tagCreatedBackingTable = 105
	tagRemovedBackingTable = 106
	tagNewBlobFile         = 107
	tagDeletedBlobFile     = 108

	// The custom tags sub-format used by tagNewFile4 and above. All tags less
	// than customTagNonSafeIgnoreMask are safe to ignore and their format must be
	// a single bytes field.
	customTagTerminate         = 1
	customTagNeedsCompaction   = 2
	customTagCreationTime      = 6
	customTagPathID            = 65
	customTagNonSafeIgnoreMask = 1 << 6
	customTagVirtual           = 66
	customTagSyntheticPrefix   = 67
	customTagSyntheticSuffix   = 68
	customTagBlobReferences    = 69
)

// DeletedTableEntry holds the state for a sstable deletion from a level. The
// table itself might still be referenced by another level.
type DeletedTableEntry struct {
	Level   int
	FileNum base.FileNum
}

// DeletedBlobFileEntry holds the state for a blob file deletion. The blob file
// ID may still be in-use with a different physical blob file.
type DeletedBlobFileEntry struct {
	FileID  base.BlobFileID
	FileNum base.DiskFileNum
}

// NewTableEntry holds the state for a new sstable or one moved from a different
// level.
type NewTableEntry struct {
	Level int
	Meta  *TableMetadata
	// BackingFileNum is only set during manifest replay, and only for virtual
	// sstables.
	BackingFileNum base.DiskFileNum
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
	MinUnflushedLogNum base.DiskFileNum

	// ObsoletePrevLogNum is a historic artifact from LevelDB that is not used by
	// Pebble, RocksDB, or even LevelDB. Its use in LevelDB was deprecated in
	// 6/2011. We keep it around purely for informational purposes when
	// displaying MANIFEST contents.
	ObsoletePrevLogNum uint64

	// The next file number. A single counter is used to assign file numbers
	// for the WAL, MANIFEST, sstable, and OPTIONS files.
	NextFileNum uint64

	// LastSeqNum is an upper bound on the sequence numbers that have been
	// assigned in flushed WALs. Unflushed WALs (that will be replayed during
	// recovery) may contain sequence numbers greater than this value.
	LastSeqNum base.SeqNum

	// A file num may be present in both deleted files and new files when it
	// is moved from a lower level to a higher level (when the compaction
	// found that there was no overlapping file at the higher level).
	DeletedTables map[DeletedTableEntry]*TableMetadata
	NewTables     []NewTableEntry
	// CreatedBackingTables can be used to preserve the TableBacking associated
	// with a physical sstable. This is useful when virtual sstables in the
	// latest version are reconstructed during manifest replay, and we also need
	// to reconstruct the TableBacking which is required by these virtual
	// sstables.
	//
	// INVARIANT: The TableBacking associated with a physical sstable must only
	// be added as a backing file in the same version edit where the physical
	// sstable is first virtualized. This means that the physical sstable must
	// be present in DeletedFiles and that there must be at least one virtual
	// sstable with the same TableBacking as the physical sstable in NewFiles. A
	// file must be present in CreatedBackingTables in exactly one version edit.
	// The physical sstable associated with the TableBacking must also not be
	// present in NewFiles.
	CreatedBackingTables []*TableBacking
	// RemovedBackingTables is used to remove the TableBacking associated with a
	// virtual sstable. Note that a backing sstable can be removed as soon as
	// there are no virtual sstables in the latest version which are using the
	// backing sstable, but the backing sstable doesn't necessarily have to be
	// removed atomically with the version edit which removes the last virtual
	// sstable associated with the backing sstable. The removal can happen in a
	// future version edit.
	//
	// INVARIANT: A file must only be added to RemovedBackingTables if it was
	// added to CreateBackingTables in a prior version edit. The same version
	// edit also cannot have the same file present in both CreateBackingTables
	// and RemovedBackingTables. A file must be present in RemovedBackingTables
	// in exactly one version edit.
	RemovedBackingTables []base.DiskFileNum
	// NewBlobFiles holds the metadata for all new blob files introduced within
	// the version edit.
	NewBlobFiles []BlobFileMetadata
	// DeletedBlobFiles holds all physical blob files that became unused during
	// the version edit.
	//
	// A physical blob file may become unused if the corresponding BlobFileID
	// becomes unreferenced during the version edit. In this case the BlobFileID
	// is not referenced by any sstable in the resulting Version.
	//
	// A physical blob file may also become unused if it is being replaced by a
	// new physical blob file. In this case NewBlobFiles must contain a
	// BlobFileMetadata with the same BlobFileID.
	//
	// While replaying a MANIFEST, the values are nil. Otherwise the values must
	// not be nil.
	DeletedBlobFiles map[DeletedBlobFileEntry]*PhysicalBlobFile
}

// Decode decodes an edit from the specified reader.
//
// Note that the Decode step will not set the TableBacking for virtual sstables
// and the responsibility is left to the caller. However, the Decode step will
// populate the NewFileEntry.BackingFileNum in VersionEdit.NewFiles.
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
			v.MinUnflushedLogNum = base.DiskFileNum(n)

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
			v.LastSeqNum = base.SeqNum(n)

		case tagCompactPointer:
			if _, err := d.readLevel(); err != nil {
				return err
			}
			if _, err := d.readBytes(); err != nil {
				return err
			}
			// NB: RocksDB does not use compaction pointers anymore.

		case tagRemovedBackingTable:
			n, err := d.readUvarint()
			if err != nil {
				return err
			}
			v.RemovedBackingTables = append(
				v.RemovedBackingTables, base.DiskFileNum(n),
			)
		case tagCreatedBackingTable:
			dfn, err := d.readUvarint()
			if err != nil {
				return err
			}
			size, err := d.readUvarint()
			if err != nil {
				return err
			}
			fileBacking := &TableBacking{
				DiskFileNum: base.DiskFileNum(dfn),
				Size:        size,
			}
			v.CreatedBackingTables = append(v.CreatedBackingTables, fileBacking)
		case tagDeletedFile:
			level, err := d.readLevel()
			if err != nil {
				return err
			}
			fileNum, err := d.readFileNum()
			if err != nil {
				return err
			}
			if v.DeletedTables == nil {
				v.DeletedTables = make(map[DeletedTableEntry]*TableMetadata)
			}
			v.DeletedTables[DeletedTableEntry{level, fileNum}] = nil

		case tagNewFile, tagNewFile2, tagNewFile3, tagNewFile4, tagNewFile5:
			level, err := d.readLevel()
			if err != nil {
				return err
			}
			fileNum, err := d.readFileNum()
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
			// We read the smallest / largest key bounds differently depending on
			// whether we have point, range or both types of keys present in the
			// table.
			var (
				smallestPointKey, largestPointKey []byte
				smallestRangeKey, largestRangeKey []byte
				parsedPointBounds                 bool
				boundsMarker                      byte
			)
			if tag != tagNewFile5 {
				// Range keys not present in the table. Parse the point key bounds.
				smallestPointKey, err = d.readBytes()
				if err != nil {
					return err
				}
				largestPointKey, err = d.readBytes()
				if err != nil {
					return err
				}
			} else {
				// Range keys are present in the table. Determine whether we have point
				// keys to parse, in addition to the bounds.
				boundsMarker, err = d.ReadByte()
				if err != nil {
					return err
				}
				// Parse point key bounds, if present.
				if boundsMarker&maskContainsPointKeys > 0 {
					smallestPointKey, err = d.readBytes()
					if err != nil {
						return err
					}
					largestPointKey, err = d.readBytes()
					if err != nil {
						return err
					}
					parsedPointBounds = true
				} else {
					// The table does not have point keys.
					// Sanity check: the bounds must be range keys.
					if boundsMarker&maskSmallest != 0 || boundsMarker&maskLargest != 0 {
						return base.CorruptionErrorf(
							"new-file-4-range-keys: table without point keys has point key bounds: marker=%x",
							boundsMarker,
						)
					}
				}
				// Parse range key bounds.
				smallestRangeKey, err = d.readBytes()
				if err != nil {
					return err
				}
				largestRangeKey, err = d.readBytes()
				if err != nil {
					return err
				}
			}
			var smallestSeqNum base.SeqNum
			var largestSeqNum base.SeqNum
			if tag != tagNewFile {
				n, err := d.readUvarint()
				if err != nil {
					return err
				}
				smallestSeqNum = base.SeqNum(n)
				n, err = d.readUvarint()
				if err != nil {
					return err
				}
				largestSeqNum = base.SeqNum(n)
			}
			var markedForCompaction bool
			var creationTime uint64
			virtualState := struct {
				virtual        bool
				backingFileNum uint64
			}{}
			var syntheticPrefix sstable.SyntheticPrefix
			var syntheticSuffix sstable.SyntheticSuffix
			var blobReferences BlobReferences
			var blobReferenceDepth BlobReferenceDepth
			if tag == tagNewFile4 || tag == tagNewFile5 {
				for {
					customTag, err := d.readUvarint()
					if err != nil {
						return err
					}
					if customTag == customTagTerminate {
						break
					}
					switch customTag {
					case customTagNeedsCompaction:
						field, err := d.readBytes()
						if err != nil {
							return err
						}
						if len(field) != 1 {
							return base.CorruptionErrorf("new-file4: need-compaction field wrong size")
						}
						markedForCompaction = (field[0] == 1)

					case customTagCreationTime:
						field, err := d.readBytes()
						if err != nil {
							return err
						}
						var n int
						creationTime, n = binary.Uvarint(field)
						if n != len(field) {
							return base.CorruptionErrorf("new-file4: invalid file creation time")
						}

					case customTagPathID:
						return base.CorruptionErrorf("new-file4: path-id field not supported")

					case customTagVirtual:
						virtualState.virtual = true
						if virtualState.backingFileNum, err = d.readUvarint(); err != nil {
							return err
						}

					case customTagSyntheticPrefix:
						synthetic, err := d.readBytes()
						if err != nil {
							return err
						}
						syntheticPrefix = synthetic

					case customTagSyntheticSuffix:
						if syntheticSuffix, err = d.readBytes(); err != nil {
							return err
						}

					case customTagBlobReferences:
						// The first varint encodes the 'blob reference depth'
						// of the table.
						v, err := d.readUvarint()
						if err != nil {
							return err
						}
						blobReferenceDepth = BlobReferenceDepth(v)
						n, err := d.readUvarint()
						if err != nil {
							return err
						}
						blobReferences = make([]BlobReference, n)
						for i := 0; i < int(n); i++ {
							fileID, err := d.readUvarint()
							if err != nil {
								return err
							}
							valueSize, err := d.readUvarint()
							if err != nil {
								return err
							}
							blobReferences[i] = BlobReference{
								FileID:    base.BlobFileID(fileID),
								ValueSize: valueSize,
							}
						}
						continue

					default:
						if (customTag & customTagNonSafeIgnoreMask) != 0 {
							return base.CorruptionErrorf("new-file4: custom field not supported: %d", customTag)
						}
						if _, err := d.readBytes(); err != nil {
							return err
						}
					}
				}
			}
			m := &TableMetadata{
				TableNum:                 fileNum,
				Size:                     size,
				CreationTime:             int64(creationTime),
				SmallestSeqNum:           smallestSeqNum,
				LargestSeqNum:            largestSeqNum,
				LargestSeqNumAbsolute:    largestSeqNum,
				BlobReferences:           blobReferences,
				BlobReferenceDepth:       blobReferenceDepth,
				MarkedForCompaction:      markedForCompaction,
				Virtual:                  virtualState.virtual,
				SyntheticPrefixAndSuffix: sstable.MakeSyntheticPrefixAndSuffix(syntheticPrefix, syntheticSuffix),
			}

			if tag != tagNewFile5 { // no range keys present
				m.PointKeyBounds.SetInternalKeyBounds(base.DecodeInternalKey(smallestPointKey),
					base.DecodeInternalKey(largestPointKey))
				m.HasPointKeys = true
				m.boundTypeSmallest, m.boundTypeLargest = boundTypePointKey, boundTypePointKey
			} else { // range keys present
				// Set point key bounds, if parsed.
				if parsedPointBounds {
					m.PointKeyBounds.SetInternalKeyBounds(base.DecodeInternalKey(smallestPointKey),
						base.DecodeInternalKey(largestPointKey))
					m.HasPointKeys = true
				}
				// Set range key bounds.
				m.RangeKeyBounds = &InternalKeyBounds{}
				m.RangeKeyBounds.SetInternalKeyBounds(base.DecodeInternalKey(smallestRangeKey),
					base.DecodeInternalKey(largestRangeKey))
				m.HasRangeKeys = true
				// Set overall bounds (by default assume range keys).
				m.boundTypeSmallest, m.boundTypeLargest = boundTypeRangeKey, boundTypeRangeKey
				if boundsMarker&maskSmallest == maskSmallest {
					m.boundTypeSmallest = boundTypePointKey
				}
				if boundsMarker&maskLargest == maskLargest {
					m.boundTypeLargest = boundTypePointKey
				}
			}
			m.boundsSet = true
			if !virtualState.virtual {
				m.InitPhysicalBacking()
			}

			nfe := NewTableEntry{
				Level: level,
				Meta:  m,
			}
			if virtualState.virtual {
				nfe.BackingFileNum = base.DiskFileNum(virtualState.backingFileNum)
			}
			v.NewTables = append(v.NewTables, nfe)

		case tagNewBlobFile:
			fileID, err := d.readUvarint()
			if err != nil {
				return err
			}
			diskFileNum, err := d.readFileNum()
			if err != nil {
				return err
			}
			size, err := d.readUvarint()
			if err != nil {
				return err
			}
			valueSize, err := d.readUvarint()
			if err != nil {
				return err
			}
			creationTime, err := d.readUvarint()
			if err != nil {
				return err
			}
			v.NewBlobFiles = append(v.NewBlobFiles, BlobFileMetadata{
				FileID: base.BlobFileID(fileID),
				Physical: &PhysicalBlobFile{
					FileNum:      base.DiskFileNum(diskFileNum),
					Size:         size,
					ValueSize:    valueSize,
					CreationTime: creationTime,
				},
			})

		case tagDeletedBlobFile:
			fileID, err := d.readUvarint()
			if err != nil {
				return err
			}
			fileNum, err := d.readFileNum()
			if err != nil {
				return err
			}
			if v.DeletedBlobFiles == nil {
				v.DeletedBlobFiles = make(map[DeletedBlobFileEntry]*PhysicalBlobFile)
			}
			v.DeletedBlobFiles[DeletedBlobFileEntry{
				FileID:  base.BlobFileID(fileID),
				FileNum: base.DiskFileNum(fileNum),
			}] = nil

		case tagPrevLogNumber:
			n, err := d.readUvarint()
			if err != nil {
				return err
			}
			v.ObsoletePrevLogNum = n

		case tagColumnFamily, tagColumnFamilyAdd, tagColumnFamilyDrop, tagMaxColumnFamily:
			return base.CorruptionErrorf("column families are not supported")

		default:
			return base.CorruptionErrorf("MANIFEST: unknown tag: %d", tag)
		}
	}
	return nil
}

func (v *VersionEdit) string(verbose bool, fmtKey base.FormatKey) string {
	var buf bytes.Buffer
	if v.ComparerName != "" {
		fmt.Fprintf(&buf, "  comparer:     %s\n", v.ComparerName)
	}
	if v.MinUnflushedLogNum != 0 {
		fmt.Fprintf(&buf, "  log-num:       %d\n", v.MinUnflushedLogNum)
	}
	if v.ObsoletePrevLogNum != 0 {
		fmt.Fprintf(&buf, "  prev-log-num:  %d\n", v.ObsoletePrevLogNum)
	}
	if v.NextFileNum != 0 {
		fmt.Fprintf(&buf, "  next-file-num: %d\n", v.NextFileNum)
	}
	if v.LastSeqNum != 0 {
		fmt.Fprintf(&buf, "  last-seq-num:  %d\n", v.LastSeqNum)
	}
	entries := slices.Collect(maps.Keys(v.DeletedTables))
	slices.SortFunc(entries, func(a, b DeletedTableEntry) int {
		if v := stdcmp.Compare(a.Level, b.Level); v != 0 {
			return v
		}
		return stdcmp.Compare(a.FileNum, b.FileNum)
	})
	for _, df := range entries {
		fmt.Fprintf(&buf, "  del-table:     L%d %s\n", df.Level, df.FileNum)
	}
	for _, nf := range v.NewTables {
		fmt.Fprintf(&buf, "  add-table:     L%d", nf.Level)
		fmt.Fprintf(&buf, " %s", nf.Meta.DebugString(fmtKey, verbose))
		if nf.Meta.CreationTime != 0 {
			fmt.Fprintf(&buf, " (%s)",
				time.Unix(nf.Meta.CreationTime, 0).UTC().Format(time.RFC3339))
		}
		fmt.Fprintln(&buf)
	}

	for _, b := range v.CreatedBackingTables {
		fmt.Fprintf(&buf, "  add-backing:   %s\n", b.DiskFileNum)
	}
	for _, n := range v.RemovedBackingTables {
		fmt.Fprintf(&buf, "  del-backing:   %s\n", n)
	}
	for _, f := range v.NewBlobFiles {
		fmt.Fprintf(&buf, "  add-blob-file: %s\n", f.String())
	}
	deletedBlobFileEntries := slices.Collect(maps.Keys(v.DeletedBlobFiles))
	slices.SortFunc(deletedBlobFileEntries, func(a, b DeletedBlobFileEntry) int {
		if v := stdcmp.Compare(a.FileID, b.FileID); v != 0 {
			return v
		}
		return stdcmp.Compare(a.FileNum, b.FileNum)
	})
	for _, df := range deletedBlobFileEntries {
		fmt.Fprintf(&buf, "  del-blob-file: %s %s\n", df.FileID, df.FileNum)
	}
	return buf.String()
}

// DebugString is a more verbose version of String(). Use this in tests.
func (v *VersionEdit) DebugString(fmtKey base.FormatKey) string {
	return v.string(true /* verbose */, fmtKey)
}

// String implements fmt.Stringer for a VersionEdit.
func (v *VersionEdit) String() string {
	return v.string(false /* verbose */, base.DefaultFormatter)
}

// ParseVersionEditDebug parses a VersionEdit from its DebugString
// implementation.
//
// It doesn't recognize all fields; this implementation can be filled in as
// needed.
func ParseVersionEditDebug(s string) (_ *VersionEdit, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.CombineErrors(err, errFromPanic(r))
		}
	}()

	var ve VersionEdit
	for _, l := range strings.Split(s, "\n") {
		l = strings.TrimSpace(l)
		if l == "" {
			continue
		}
		field, value, ok := strings.Cut(l, ":")
		if !ok {
			return nil, errors.Errorf("malformed line %q", l)
		}
		field = strings.TrimSpace(field)
		p := strparse.MakeParser(debugParserSeparators, value)
		switch field {
		case "add-table":
			level := p.Level()
			meta, err := ParseTableMetadataDebug(p.Remaining())
			if err != nil {
				return nil, err
			}
			ve.NewTables = append(ve.NewTables, NewTableEntry{
				Level: level,
				Meta:  meta,
			})

		case "del-table":
			level := p.Level()
			num := p.FileNum()
			if ve.DeletedTables == nil {
				ve.DeletedTables = make(map[DeletedTableEntry]*TableMetadata)
			}
			ve.DeletedTables[DeletedTableEntry{
				Level:   level,
				FileNum: num,
			}] = nil

		case "add-backing":
			n := p.DiskFileNum()
			ve.CreatedBackingTables = append(ve.CreatedBackingTables, &TableBacking{
				DiskFileNum: n,
				Size:        100,
			})

		case "del-backing":
			n := p.DiskFileNum()
			ve.RemovedBackingTables = append(ve.RemovedBackingTables, n)

		case "add-blob-file":
			meta, err := ParseBlobFileMetadataDebug(p.Remaining())
			if err != nil {
				return nil, err
			}
			ve.NewBlobFiles = append(ve.NewBlobFiles, meta)

		case "del-blob-file":
			if ve.DeletedBlobFiles == nil {
				ve.DeletedBlobFiles = make(map[DeletedBlobFileEntry]*PhysicalBlobFile)
			}
			ve.DeletedBlobFiles[DeletedBlobFileEntry{
				FileID:  p.BlobFileID(),
				FileNum: p.DiskFileNum(),
			}] = nil

		default:
			return nil, errors.Errorf("field %q not implemented", field)
		}
	}
	return &ve, nil
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
		e.writeUvarint(uint64(v.MinUnflushedLogNum))
	}
	if v.ObsoletePrevLogNum != 0 {
		e.writeUvarint(tagPrevLogNumber)
		e.writeUvarint(v.ObsoletePrevLogNum)
	}
	if v.NextFileNum != 0 {
		e.writeUvarint(tagNextFileNumber)
		e.writeUvarint(uint64(v.NextFileNum))
	}
	for _, dfn := range v.RemovedBackingTables {
		e.writeUvarint(tagRemovedBackingTable)
		e.writeUvarint(uint64(dfn))
	}
	for _, fileBacking := range v.CreatedBackingTables {
		e.writeUvarint(tagCreatedBackingTable)
		e.writeUvarint(uint64(fileBacking.DiskFileNum))
		e.writeUvarint(fileBacking.Size)
	}
	// RocksDB requires LastSeqNum to be encoded for the first MANIFEST entry,
	// even though its value is zero. We detect this by encoding LastSeqNum when
	// ComparerName is set.
	if v.LastSeqNum != 0 || v.ComparerName != "" {
		e.writeUvarint(tagLastSequence)
		e.writeUvarint(uint64(v.LastSeqNum))
	}
	for x := range v.DeletedTables {
		e.writeUvarint(tagDeletedFile)
		e.writeUvarint(uint64(x.Level))
		e.writeUvarint(uint64(x.FileNum))
	}
	for _, x := range v.NewTables {
		customFields := x.Meta.MarkedForCompaction || x.Meta.CreationTime != 0 || x.Meta.Virtual || len(x.Meta.BlobReferences) > 0
		var tag uint64
		switch {
		case x.Meta.HasRangeKeys:
			tag = tagNewFile5
		case customFields:
			tag = tagNewFile4
		default:
			tag = tagNewFile2
		}
		e.writeUvarint(tag)
		e.writeUvarint(uint64(x.Level))
		e.writeUvarint(uint64(x.Meta.TableNum))
		e.writeUvarint(x.Meta.Size)
		if !x.Meta.HasRangeKeys {
			// If we have no range keys, preserve the original format and write the
			// smallest and largest point keys.
			e.writeKey(x.Meta.PointKeyBounds.Smallest())
			e.writeKey(x.Meta.PointKeyBounds.Largest())
		} else {
			// When range keys are present, we first write a marker byte that
			// indicates if the table also contains point keys, in addition to how the
			// overall bounds for the table should be reconstructed. This byte is
			// followed by the keys themselves.
			b, err := x.Meta.boundsMarker()
			if err != nil {
				return err
			}
			if err = e.WriteByte(b); err != nil {
				return err
			}
			// Write point key bounds (if present).
			if x.Meta.HasPointKeys {
				e.writeKey(x.Meta.PointKeyBounds.Smallest())
				e.writeKey(x.Meta.PointKeyBounds.Largest())
			}
			// Write range key bounds (if present).
			if x.Meta.HasRangeKeys {
				e.writeKey(x.Meta.RangeKeyBounds.Smallest())
				e.writeKey(x.Meta.RangeKeyBounds.Largest())
			}
		}
		e.writeUvarint(uint64(x.Meta.SmallestSeqNum))
		e.writeUvarint(uint64(x.Meta.LargestSeqNum))
		if customFields {
			if x.Meta.CreationTime != 0 {
				e.writeUvarint(customTagCreationTime)
				var buf [binary.MaxVarintLen64]byte
				n := binary.PutUvarint(buf[:], uint64(x.Meta.CreationTime))
				e.writeBytes(buf[:n])
			}
			if x.Meta.MarkedForCompaction {
				e.writeUvarint(customTagNeedsCompaction)
				e.writeBytes([]byte{1})
			}
			if x.Meta.Virtual {
				e.writeUvarint(customTagVirtual)
				e.writeUvarint(uint64(x.Meta.TableBacking.DiskFileNum))
			}
			if x.Meta.SyntheticPrefixAndSuffix.HasPrefix() {
				e.writeUvarint(customTagSyntheticPrefix)
				e.writeBytes(x.Meta.SyntheticPrefixAndSuffix.Prefix())
			}
			if x.Meta.SyntheticPrefixAndSuffix.HasSuffix() {
				e.writeUvarint(customTagSyntheticSuffix)
				e.writeBytes(x.Meta.SyntheticPrefixAndSuffix.Suffix())
			}
			if len(x.Meta.BlobReferences) > 0 {
				e.writeUvarint(customTagBlobReferences)
				e.writeUvarint(uint64(x.Meta.BlobReferenceDepth))
				e.writeUvarint(uint64(len(x.Meta.BlobReferences)))
				for _, ref := range x.Meta.BlobReferences {
					e.writeUvarint(uint64(ref.FileID))
					e.writeUvarint(ref.ValueSize)
				}
			}
			e.writeUvarint(customTagTerminate)
		}
	}
	for _, x := range v.NewBlobFiles {
		e.writeUvarint(tagNewBlobFile)
		e.writeUvarint(uint64(x.FileID))
		e.writeUvarint(uint64(x.Physical.FileNum))
		e.writeUvarint(x.Physical.Size)
		e.writeUvarint(x.Physical.ValueSize)
		e.writeUvarint(x.Physical.CreationTime)
	}
	for x := range v.DeletedBlobFiles {
		e.writeUvarint(tagDeletedBlobFile)
		e.writeUvarint(uint64(x.FileID))
		e.writeUvarint(uint64(x.FileNum))
	}
	_, err := w.Write(e.Bytes())
	return err
}

// versionEditDecoder should be used to decode version edits.
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
			return nil, base.CorruptionErrorf("pebble: corrupt manifest: failed to read %d bytes", n)
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
		return 0, base.CorruptionErrorf("pebble: corrupt manifest: level %d >= %d", u, NumLevels)
	}
	return int(u), nil
}

func (d versionEditDecoder) readFileNum() (base.FileNum, error) {
	u, err := d.readUvarint()
	if err != nil {
		return 0, err
	}
	return base.FileNum(u), nil
}

func (d versionEditDecoder) readUvarint() (uint64, error) {
	u, err := binary.ReadUvarint(d)
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return 0, base.CorruptionErrorf("pebble: corrupt manifest: failed to read uvarint")
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
//
// INVARIANTS:
// No file can be added to a level more than once. This is true globally, and
// also true for all of the calls to Accumulate for a single bulk version edit.
//
// No file can be removed from a level more than once. This is true globally,
// and also true for all of the calls to Accumulate for a single bulk version
// edit.
//
// A sstable file must not be added and removed from a given level in the same
// version edit, and a blob file must not be both added and deleted in the same
// version edit.
//
// A file that is being removed from a level must have been added to that level
// before (in a prior version edit). Note that a given file can be deleted from
// a level and added to another level in a single version edit
type BulkVersionEdit struct {
	AddedTables   [NumLevels]map[base.FileNum]*TableMetadata
	DeletedTables [NumLevels]map[base.FileNum]*TableMetadata

	BlobFiles struct {
		// Added holds the metadata of all new blob files introduced within the
		// aggregated version edit, keyed by file number.
		Added map[base.BlobFileID]*PhysicalBlobFile
		// Deleted holds a list of all blob files that became unreferenced by
		// any sstables, making them obsolete within the resulting version (a
		// zombie if still referenced by previous versions). Deleted file
		// numbers must not exist in Added.
		//
		// Deleted is keyed by blob file ID and points to the physical blob file.
		Deleted map[base.BlobFileID]*PhysicalBlobFile
	}

	// AddedFileBacking is a map to support lookup so that we can populate the
	// TableBacking of virtual sstables during manifest replay.
	AddedFileBacking   map[base.DiskFileNum]*TableBacking
	RemovedFileBacking []base.DiskFileNum

	// AllAddedTables maps table number to table metadata for all added sstables
	// from accumulated version edits. AllAddedTables is only populated if set to
	// non-nil by a caller. It must be set to non-nil when replaying version edits
	// read from a MANIFEST (as opposed to VersionEdits constructed in-memory).
	// While replaying a MANIFEST file, VersionEdit.DeletedFiles map entries have
	// nil values, because the on-disk deletion record encodes only the file
	// number. Accumulate uses AllAddedTables to correctly populate the
	// BulkVersionEdit's Deleted field with non-nil *TableMetadata.
	AllAddedTables map[base.FileNum]*TableMetadata

	// MarkedForCompactionCountDiff holds the aggregated count of files
	// marked for compaction added or removed.
	MarkedForCompactionCountDiff int
}

// Accumulate adds the file addition and deletions in the specified version
// edit to the bulk edit's internal state.
//
// INVARIANTS:
// (1) If a table is added to a given level in a call to Accumulate and then
// removed from that level in a subsequent call, the file will not be present in
// the resulting BulkVersionEdit.Deleted for that level.
// (2) If a new table is added and it includes a reference to a blob file, that
// blob file must either appear in BlobFiles.Added, or the blob file must be
// referenced by a table deleted in the same bulk version edit.
//
// After accumulation of version edits, the bulk version edit may have
// information about a file which has been deleted from a level, but it may not
// have information about the same file added to the same level. The add
// could've occurred as part of a previous bulk version edit. In this case, the
// deleted file must be present in BulkVersionEdit.Deleted, at the end of the
// accumulation, because we need to decrease the refcount of the deleted file in
// Apply.
func (b *BulkVersionEdit) Accumulate(ve *VersionEdit) error {
	// Add any blob files that were introduced.
	for _, nbf := range ve.NewBlobFiles {
		if b.BlobFiles.Added == nil {
			b.BlobFiles.Added = make(map[base.BlobFileID]*PhysicalBlobFile)
		}
		b.BlobFiles.Added[nbf.FileID] = nbf.Physical
	}

	for entry, physicalBlobFile := range ve.DeletedBlobFiles {
		// If the blob file was added in a prior, accumulated version edit we
		// can resolve the deletion by removing it from the added files map.
		if b.BlobFiles.Added != nil {
			added := b.BlobFiles.Added[entry.FileID]
			if added != nil && added.FileNum == entry.FileNum {
				delete(b.BlobFiles.Added, entry.FileID)
				continue
			}
		}
		// Otherwise the blob file deleted was added prior to this bulk edit,
		// and we insert it into BlobFiles.Deleted so that Apply may remove it
		// from the resulting version.
		if b.BlobFiles.Deleted == nil {
			b.BlobFiles.Deleted = make(map[base.BlobFileID]*PhysicalBlobFile)
		}
		b.BlobFiles.Deleted[entry.FileID] = physicalBlobFile

	}

	for df, m := range ve.DeletedTables {
		dmap := b.DeletedTables[df.Level]
		if dmap == nil {
			dmap = make(map[base.FileNum]*TableMetadata)
			b.DeletedTables[df.Level] = dmap
		}

		if m == nil {
			// m is nil only when replaying a MANIFEST.
			if b.AllAddedTables == nil {
				return errors.Errorf("deleted file L%d.%s's metadata is absent and bve.AddedByFileNum is nil", df.Level, df.FileNum)
			}
			m = b.AllAddedTables[df.FileNum]
			if m == nil {
				return base.CorruptionErrorf("pebble: file deleted L%d.%s before it was inserted", df.Level, df.FileNum)
			}
		}
		if m.MarkedForCompaction {
			b.MarkedForCompactionCountDiff--
		}
		if _, ok := b.AddedTables[df.Level][df.FileNum]; !ok {
			dmap[df.FileNum] = m
		} else {
			// Present in b.Added for the same level.
			delete(b.AddedTables[df.Level], df.FileNum)
		}
	}

	// Generate state for Added backing files. Note that these must be generated
	// before we loop through the NewFiles, because we need to populate the
	// FileBackings which might be used by the NewFiles loop.
	if b.AddedFileBacking == nil {
		b.AddedFileBacking = make(map[base.DiskFileNum]*TableBacking)
	}
	for _, fb := range ve.CreatedBackingTables {
		if _, ok := b.AddedFileBacking[fb.DiskFileNum]; ok {
			// There is already a TableBacking associated with fb.DiskFileNum.
			// This should never happen. There must always be only one TableBacking
			// associated with a backing sstable.
			panic(fmt.Sprintf("pebble: duplicate file backing %s", fb.DiskFileNum.String()))
		}
		b.AddedFileBacking[fb.DiskFileNum] = fb
	}

	for _, nf := range ve.NewTables {
		// A new file should not have been deleted in this or a preceding
		// VersionEdit at the same level (though files can move across levels).
		if dmap := b.DeletedTables[nf.Level]; dmap != nil {
			if _, ok := dmap[nf.Meta.TableNum]; ok {
				return base.CorruptionErrorf("pebble: file deleted L%d.%s before it was inserted", nf.Level, nf.Meta.TableNum)
			}
		}
		if nf.Meta.Virtual && nf.Meta.TableBacking == nil {
			// TableBacking for a virtual sstable must only be nil if we're performing
			// manifest replay.
			backing := b.AddedFileBacking[nf.BackingFileNum]
			if backing == nil {
				return errors.Errorf("TableBacking for virtual sstable must not be nil")
			}
			nf.Meta.AttachVirtualBacking(backing)
		} else if nf.Meta.TableBacking == nil {
			return errors.Errorf("Added file L%d.%s's has no TableBacking", nf.Level, nf.Meta.TableNum)
		}

		if b.AddedTables[nf.Level] == nil {
			b.AddedTables[nf.Level] = make(map[base.FileNum]*TableMetadata)
		}
		b.AddedTables[nf.Level][nf.Meta.TableNum] = nf.Meta
		if b.AllAddedTables != nil {
			b.AllAddedTables[nf.Meta.TableNum] = nf.Meta
		}
		if nf.Meta.MarkedForCompaction {
			b.MarkedForCompactionCountDiff++
		}
	}

	for _, n := range ve.RemovedBackingTables {
		if _, ok := b.AddedFileBacking[n]; ok {
			delete(b.AddedFileBacking, n)
		} else {
			// Since a file can be removed from backing files in exactly one version
			// edit it is safe to just append without any de-duplication.
			b.RemovedFileBacking = append(b.RemovedFileBacking, n)
		}
	}

	return nil
}

// Apply applies the delta b to the current version to produce a new version.
// The ordering of tables within the new version is consistent with respect to
// the comparer.
//
// Apply updates the backing refcounts (Ref/Unref) as files are installed into
// the levels.
//
// curr may be nil, which is equivalent to a pointer to a zero version.
//
// Not that L0SublevelFiles is not initialized in the returned version; it is
// the caller's responsibility to set it using L0Organizer.PerformUpdate().
func (b *BulkVersionEdit) Apply(curr *Version, readCompactionRate int64) (*Version, error) {
	comparer := curr.cmp
	v := &Version{
		BlobFiles: curr.BlobFiles.clone(),
		cmp:       comparer,
	}

	// Adjust the count of files marked for compaction.
	v.Stats.MarkedForCompaction = curr.Stats.MarkedForCompaction
	v.Stats.MarkedForCompaction += b.MarkedForCompactionCountDiff
	if v.Stats.MarkedForCompaction < 0 {
		return nil, base.CorruptionErrorf("pebble: version marked for compaction count negative")
	}

	// Update the BlobFileSet to record blob files added and deleted. The
	// BlobFileSet ensures any physical blob files that are referenced by the
	// version remain on storage until they're no longer referenced by any
	// version.
	//
	// We remove deleted blob files first, because during a blob file
	// replacement the BlobFileID is reused. The B-Tree insert will fail if the
	// old blob file is still present in the tree.
	for blobFileID := range b.BlobFiles.Deleted {
		v.BlobFiles.remove(BlobFileMetadata{FileID: blobFileID})
	}
	for blobFileID, physical := range b.BlobFiles.Added {
		if err := v.BlobFiles.insert(BlobFileMetadata{
			FileID:   blobFileID,
			Physical: physical,
		}); err != nil {
			return nil, err
		}
	}

	for level := range v.Levels {
		v.Levels[level] = curr.Levels[level].clone()
		v.RangeKeyLevels[level] = curr.RangeKeyLevels[level].clone()

		if len(b.AddedTables[level]) == 0 && len(b.DeletedTables[level]) == 0 {
			// There are no edits on this level.
			continue
		}

		// Some edits on this level.
		lm := &v.Levels[level]
		lmRange := &v.RangeKeyLevels[level]

		addedTablesMap := b.AddedTables[level]
		deletedTablesMap := b.DeletedTables[level]
		if n := v.Levels[level].Len() + len(addedTablesMap); n == 0 {
			return nil, base.CorruptionErrorf(
				"pebble: internal error: No current or added files but have deleted files: %d",
				errors.Safe(len(deletedTablesMap)))
		}

		// NB: addedFilesMap may be empty. If a file is present in addedFilesMap
		// for a level, it won't be present in deletedFilesMap for the same
		// level.

		for _, f := range deletedTablesMap {
			// Removing a table from the B-Tree may decrement file reference
			// counts.  However, because we cloned the previous level's B-Tree,
			// this should never result in a file's reference count dropping to
			// zero. The remove call will panic if this happens.
			v.Levels[level].remove(f)
			v.RangeKeyLevels[level].remove(f)
		}

		addedTables := make([]*TableMetadata, 0, len(addedTablesMap))
		for _, f := range addedTablesMap {
			addedTables = append(addedTables, f)
		}
		// Sort addedFiles by file number. This isn't necessary, but tests which
		// replay invalid manifests check the error output, and the error output
		// depends on the order in which files are added to the btree.
		slices.SortFunc(addedTables, func(a, b *TableMetadata) int {
			return stdcmp.Compare(a.TableNum, b.TableNum)
		})

		var sm, la *TableMetadata
		for _, f := range addedTables {
			// NB: allowedSeeks is used for read triggered compactions. It is set using
			// Options.Experimental.ReadCompactionRate which defaults to 32KB.
			var allowedSeeks int64
			if readCompactionRate != 0 {
				allowedSeeks = int64(f.Size) / readCompactionRate
			}
			if allowedSeeks < 100 {
				allowedSeeks = 100
			}
			f.AllowedSeeks.Store(allowedSeeks)
			f.InitAllowedSeeks = allowedSeeks

			// Validate that all referenced blob files exist.
			for _, ref := range f.BlobReferences {
				if _, ok := v.BlobFiles.LookupPhysical(ref.FileID); !ok {
					return nil, errors.AssertionFailedf("pebble: blob file %s referenced by L%d.%s not found", ref.FileID, level, f.TableNum)
				}
			}

			err := lm.insert(f)
			if err != nil {
				return nil, errors.Wrap(err, "pebble")
			}
			if f.HasRangeKeys {
				err = lmRange.insert(f)
				if err != nil {
					return nil, errors.Wrap(err, "pebble")
				}
			}
			// Track the keys with the smallest and largest keys, so that we can
			// check consistency of the modified span.
			if sm == nil || base.InternalCompare(comparer.Compare, sm.Smallest(), f.Smallest()) > 0 {

				sm = f
			}
			if la == nil || base.InternalCompare(comparer.Compare, la.Largest(), f.Largest()) < 0 {
				la = f
			}
		}

		if level == 0 {
			if err := CheckOrdering(comparer.Compare, comparer.FormatKey, Level(0), v.Levels[level].Iter()); err != nil {
				return nil, errors.Wrap(err, "pebble: internal error")
			}
			continue
		}

		// Check consistency of the level in the vicinity of our edits.
		if sm != nil && la != nil {
			overlap := v.Levels[level].Slice().Overlaps(comparer.Compare, sm.UserKeyBounds())
			// overlap contains all of the added tables. We want to ensure that
			// the added tables are consistent with neighboring existing tables
			// too, so reslice the overlap to pull in a neighbor on each side.
			check := overlap.Reslice(func(start, end *LevelIterator) {
				if m := start.Prev(); m == nil {
					start.Next()
				}
				if m := end.Next(); m == nil {
					end.Prev()
				}
			})
			if err := CheckOrdering(comparer.Compare, comparer.FormatKey, Level(level), check.Iter()); err != nil {
				return nil, errors.Wrap(err, "pebble: internal error")
			}
		}
	}

	// In invariants builds, sometimes check invariants across all blob files
	// and their references.
	if invariants.Sometimes(20) {
		if err := v.validateBlobFileInvariants(); err != nil {
			return nil, err
		}
	}

	return v, nil
}
