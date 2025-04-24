// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	stdcmp "cmp"
	"fmt"
	"slices"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/humanize"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/strparse"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/cockroachdb/redact"
)

// A BlobReference describes a sstable's reference to a blob value file.
type BlobReference struct {
	// FileNum identifies the referenced blob file.
	FileNum base.DiskFileNum
	// ValueSize is the sum of the lengths of the uncompressed values within the
	// blob file for which there exists a reference in the sstable. Note that if
	// any of the referencing tables are virtualized tables, the ValueSize may
	// be approximate.
	//
	// INVARIANT: ValueSize <= Metadata.ValueSize
	ValueSize uint64

	// Metadata is the metadata for the blob file. It is non-nil for blob
	// references contained within active Versions. It is expected to initially
	// be nil when decoding a version edit as a part of manfiest replay. When
	// the version edit is accumulated into a bulk version edit, the metadata
	// is populated.
	Metadata *BlobFileMetadata
}

// EstimatedPhysicalSize returns an estimate of the physical size of the blob
// reference, in bytes. It's calculated by scaling the blob file's physical size
// according to the ValueSize of the blob reference relative to the total
// ValueSize of the blob file.
func (br *BlobReference) EstimatedPhysicalSize() uint64 {
	if invariants.Enabled {
		if br.ValueSize > br.Metadata.ValueSize {
			panic(errors.AssertionFailedf("pebble: blob reference value size %d is greater than the blob file's value size %d",
				br.ValueSize, br.Metadata.ValueSize))
		}
		if br.ValueSize == 0 {
			panic(errors.AssertionFailedf("pebble: blob reference value size %d is zero", br.ValueSize))
		}
		if br.Metadata.ValueSize == 0 {
			panic(errors.AssertionFailedf("pebble: blob file value size %d is zero", br.Metadata.ValueSize))
		}
	}
	//                         br.ValueSize
	//   Reference size =  --------------------   ×  br.Metadata.Size
	//                     br.Metadata.ValueSize
	//
	// We perform the multiplication first to avoid floating point arithmetic.
	return (br.ValueSize * br.Metadata.Size) / br.Metadata.ValueSize
}

// BlobFileMetadata is metadata describing a blob value file.
type BlobFileMetadata struct {
	// FileNum is the file number.
	FileNum base.DiskFileNum
	// Size is the size of the file, in bytes.
	Size uint64
	// ValueSize is the sum of the length of the uncompressed values stored in
	// this blob file.
	ValueSize uint64
	// File creation time in seconds since the epoch (1970-01-01 00:00:00
	// UTC).
	CreationTime uint64

	// Mutable state

	// refs holds the reference count for the blob file. Each ref is from a
	// TableMetadata in a Version with a positive reference count. Each
	// TableMetadata can refer to multiple blob files and will count as 1 ref
	// for each of those blob files. The ref count is incremented when a new
	// referencing TableMetadata is installed in a Version and decremented when
	// that TableMetadata becomes obsolete.
	refs atomic.Int32
}

// SafeFormat implements redact.SafeFormatter.
func (m *BlobFileMetadata) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("%s size:[%d (%s)] vals:[%d (%s)]",
		m.FileNum, redact.Safe(m.Size), humanize.Bytes.Uint64(m.Size),
		redact.Safe(m.ValueSize), humanize.Bytes.Uint64(m.ValueSize))
}

// String implements fmt.Stringer.
func (m *BlobFileMetadata) String() string {
	return redact.StringWithoutMarkers(m)
}

// ref increments the reference count for the blob file.
func (m *BlobFileMetadata) ref() {
	m.refs.Add(+1)
}

// unref decrements the reference count for the blob file. It returns the
// resulting reference count.
func (m *BlobFileMetadata) unref() int32 {
	refs := m.refs.Add(-1)
	if refs < 0 {
		panic(errors.AssertionFailedf("pebble: refs for blob file %d equal to %d", m.FileNum, refs))
	}
	return refs
}

// ParseBlobFileMetadataDebug parses a BlobFileMetadata from its string
// representation. This function is intended for use in tests. It's the inverse
// of BlobFileMetadata.String().
//
// In production code paths, the BlobFileMetadata is serialized in a binary
// format within a version edit under the tag tagNewBlobFile.
func ParseBlobFileMetadataDebug(s string) (_ *BlobFileMetadata, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.CombineErrors(err, errFromPanic(r))
		}
	}()

	// Input format:
	//  000000: size:[206536 (201KiB)], vals:[393256 (384KiB)]
	m := &BlobFileMetadata{}
	p := strparse.MakeParser(debugParserSeparators, s)
	m.FileNum = base.DiskFileNum(p.FileNum())

	maybeSkipParens := func() {
		if p.Peek() != "(" {
			return
		}
		for p.Next() != ")" {
			// Skip.
		}
	}

	for !p.Done() {
		field := p.Next()
		p.Expect(":")
		switch field {
		case "size":
			p.Expect("[")
			m.Size = p.Uint64()
			maybeSkipParens()
			p.Expect("]")
		case "vals":
			p.Expect("[")
			m.ValueSize = p.Uint64()
			maybeSkipParens()
			p.Expect("]")
		default:
			p.Errf("unknown field %q", field)
		}
	}
	return m, nil
}

// BlobReferenceDepth is a statistic maintained per-sstable, indicating an upper
// bound on the number of blob files that a reader scanning the table would need
// to keep open if they only open and close referenced blob files once. In other
// words, it's the stack depth of blob files referenced by a sstable. If a
// flush or compaction rewrites an sstable's values to a new blob file, the
// resulting sstable has a blob reference depth of 1. When a compaction reuses
// blob references, the max blob reference depth of the files in each level is
// used, and then the depth is summed, and assigned to the output. This is a
// loose upper bound (assuming worst case distribution of keys in all inputs)
// but avoids tracking key spans for references and using key comparisons.
//
// Because the blob reference depth is the size of the working set of blob files
// referenced by the table, it cannot exceed the count of distinct blob file
// references.
//
// Example: Consider a compaction of file f0 from L0 and files f1, f2, f3 from
// L1, where the former has blob reference depth of 1 and files f1, f2, f3 all
// happen to have a blob-reference-depth of 1. Say we produce many output files,
// one of which is f4. We are assuming here that the blobs referenced by f0
// whose keys happened to be written to f4 are spread all across the key span of
// f4. Say keys from f1 and f2 also made their way to f4. Then we will first
// have keys that refer to blobs referenced by f1,f0 and at some point once we
// move past the keys of f1, we will have keys that refer to blobs referenced by
// f2,f0. In some sense, we have a working set of 2 blob files at any point in
// time, and this is similar to the idea of level stack depth for reads -- hence
// we adopt the depth terminology.  We want to keep this stack depth in check,
// since locality is important, while allowing it to be higher than 1, since
// otherwise we will need to rewrite blob files in every compaction (defeating
// the write amp benefit we are looking for). Similar to the level depth, this
// simplistic analysis does not take into account distribution of keys involved
// in the compaction and which of them have blob references. Also the locality
// is actually better than in this analysis because more of the keys will be
// from the lower level.
type BlobReferenceDepth int

// BlobReferences is a slice of BlobReference. The order of the slice is
// significant and should be maintained. In practice, a sstable's BlobReferences
// are ordered by earliest appearance within the sstable. The ordering is
// persisted to the manifest.
type BlobReferences []BlobReference

// Assert that *BlobReferences implements sstable.BlobReferences.
var _ sstable.BlobReferences = (*BlobReferences)(nil)

// FileNumByID returns the FileNum for the identified BlobReference.
func (br *BlobReferences) FileNumByID(i blob.ReferenceID) base.DiskFileNum {
	return (*br)[i].FileNum
}

// IDByFileNum returns the reference ID for the given FileNum. If the file
// number is not found, the second return value is false. IDByFileNum is linear
// in the length of the BlobReferences slice.
func (br *BlobReferences) IDByFileNum(fileNum base.DiskFileNum) (blob.ReferenceID, bool) {
	for i, ref := range *br {
		if ref.FileNum == fileNum {
			return blob.ReferenceID(i), true
		}
	}
	return blob.ReferenceID(len(*br)), false
}

// AggregateBlobFileStats records cumulative stats across blob files.
type AggregateBlobFileStats struct {
	// Count is the number of blob files in the set.
	Count uint64
	// PhysicalSize is the sum of the size of all blob files in the set.  This
	// is the size of the blob files on physical storage. Data within blob files
	// is compressed, so this value may be less than ValueSize.
	PhysicalSize uint64
	// ValueSize is the sum of the length of the uncompressed values in all blob
	// files in the set.
	ValueSize uint64
	// ReferencedValueSize is the sum of the length of the uncompressed values
	// in all blob files in the set that are still referenced by live tables
	// (i.e., in the latest version).
	ReferencedValueSize uint64
	// ReferencesCount is the total number of tracked references in live tables
	// (i.e., in the latest version). When virtual sstables are present, this
	// count is per-virtual sstable (not per backing physical sstable).
	ReferencesCount uint64
}

// String implements fmt.Stringer.
func (s AggregateBlobFileStats) String() string {
	return fmt.Sprintf("Files:{Count: %d, Size: %d, ValueSize: %d}, References:{ValueSize: %d, Count: %d}",
		s.Count, s.PhysicalSize, s.ValueSize, s.ReferencedValueSize, s.ReferencesCount)
}

// CurrentBlobFileSet describes the set of blob files that are currently live in
// the latest Version. CurrentBlobFileSet is not thread-safe. In practice its
// use is protected by the versionSet logLock.
type CurrentBlobFileSet struct {
	// files is a map of blob file numbers to a *currentBlobFile, recording
	// metadata about the blob file's active references in the latest Version.
	files map[base.DiskFileNum]*currentBlobFile
	// stats records cumulative stats across all blob files in the set.
	stats AggregateBlobFileStats
}

type currentBlobFile struct {
	metadata *BlobFileMetadata
	// references holds pointers to TableMetadatas that exist in the latest
	// version and reference this blob file. When the length of references falls
	// to zero, the blob file is either a zombie file (if BlobFileMetadata.refs
	// > 0), or obsolete and ready to be deleted.
	//
	// INVARIANT: BlobFileMetadata.refs >= len(references)
	//
	// TODO(jackson): Rather than using 1 map per blob file which needs to grow
	// and shrink over the lifetime of the blob file, we could use a single
	// B-Tree that holds all blob references, sorted by blob file then by
	// referencing table number. This would likely be more memory efficient,
	// reduce overall number of pointers to chase and suffer fewer allocations
	// (and we can pool the B-Tree nodes to further reduce allocs)
	references map[*TableMetadata]struct{}
	// referencedValueSize is the sum of the length of uncompressed values in
	// this blob file that are still live.
	referencedValueSize uint64
}

// Init initializes the CurrentBlobFileSet with the state of the provided
// BulkVersionEdit. This is used after replaying a manifest.
func (s *CurrentBlobFileSet) Init(bve *BulkVersionEdit) {
	*s = CurrentBlobFileSet{files: make(map[base.DiskFileNum]*currentBlobFile)}
	if bve == nil {
		return
	}
	for _, m := range bve.BlobFiles.Added {
		s.files[m.FileNum] = &currentBlobFile{
			metadata:   m,
			references: make(map[*TableMetadata]struct{}),
		}
		s.stats.Count++
		s.stats.PhysicalSize += m.Size
		s.stats.ValueSize += m.ValueSize
	}
	// Record references to blob files from extant tables. Any referenced blob
	// files should already exist in s.files.
	for _, levelFiles := range bve.AddedTables {
		for _, m := range levelFiles {
			for _, ref := range m.BlobReferences {
				cbf, ok := s.files[ref.FileNum]
				if !ok {
					panic(errors.AssertionFailedf("pebble: referenced blob file %d not found", ref.FileNum))
				}
				cbf.references[m] = struct{}{}
				cbf.referencedValueSize += ref.ValueSize
				s.stats.ReferencedValueSize += ref.ValueSize
				s.stats.ReferencesCount++
			}
		}
	}
}

// Stats returns the cumulative stats across all blob files in the set.
func (s *CurrentBlobFileSet) Stats() AggregateBlobFileStats {
	return s.stats
}

// Metadatas returns a slice of all blob file metadata in the set, sorted by
// file number for determinism.
func (s *CurrentBlobFileSet) Metadatas() []*BlobFileMetadata {
	m := make([]*BlobFileMetadata, 0, len(s.files))
	for _, cbf := range s.files {
		m = append(m, cbf.metadata)
	}
	slices.SortFunc(m, func(a, b *BlobFileMetadata) int {
		return stdcmp.Compare(a.FileNum, b.FileNum)
	})
	return m
}

// ApplyAndUpdateVersionEdit applies a version edit to the current blob file
// set, updating its internal tracking of extant blob file references. If after
// applying the version edit a blob file has no more references, the version
// edit is modified to record the blob file removal.
func (s *CurrentBlobFileSet) ApplyAndUpdateVersionEdit(ve *VersionEdit) error {
	// Insert new blob files into the set.
	for _, nf := range ve.NewBlobFiles {
		if _, ok := s.files[nf.FileNum]; ok {
			return errors.AssertionFailedf("pebble: new blob file %d already exists", nf.FileNum)
		}
		cbf := &currentBlobFile{references: make(map[*TableMetadata]struct{})}
		cbf.metadata = nf
		s.files[nf.FileNum] = cbf
		s.stats.Count++
		s.stats.PhysicalSize += nf.Size
		s.stats.ValueSize += nf.ValueSize
	}

	// Update references to blob files from new tables. Any referenced blob
	// files should already exist in s.files.
	newTables := make(map[base.FileNum]struct{})
	for _, e := range ve.NewTables {
		newTables[e.Meta.FileNum] = struct{}{}
		for _, ref := range e.Meta.BlobReferences {
			cbf, ok := s.files[ref.FileNum]
			if !ok {
				return errors.AssertionFailedf("pebble: referenced blob file %d not found", ref.FileNum)
			}
			cbf.references[e.Meta] = struct{}{}
			cbf.referencedValueSize += ref.ValueSize
			s.stats.ReferencedValueSize += ref.ValueSize
			s.stats.ReferencesCount++
		}
	}

	// Remove references to blob files from deleted tables. Any referenced blob
	// files should already exist in s.files. If the removal of a reference
	// causes the blob file's ref count to drop to zero, the blob file is a
	// zombie. We update the version edit to record the blob file removal and
	// remove it from the set.
	for _, meta := range ve.DeletedTables {
		for _, ref := range meta.BlobReferences {
			cbf, ok := s.files[ref.FileNum]
			if !ok {
				return errors.AssertionFailedf("pebble: referenced blob file %d not found", ref.FileNum)
			}
			if invariants.Enabled {
				if ref.ValueSize > cbf.referencedValueSize {
					return errors.AssertionFailedf("pebble: referenced value size %d for blob file %d is greater than the referenced value size %d",
						ref.ValueSize, cbf.metadata.FileNum, cbf.referencedValueSize)
				}
				if _, ok := cbf.references[meta]; !ok {
					return errors.AssertionFailedf("pebble: deleted table %s's reference to blob file %d not known",
						meta.FileNum, ref.FileNum)
				}
			}

			// Decrement the stats for this reference.
			cbf.referencedValueSize -= ref.ValueSize
			s.stats.ReferencedValueSize -= ref.ValueSize
			s.stats.ReferencesCount--
			if _, ok := newTables[meta.FileNum]; ok {
				// This table was added to a different level of the LSM in the
				// same version edit. It's being moved. We can preserve the
				// existing reference.  We still needed to reduce the counts
				// above because we doubled it when we incremented stats on
				// account of files in NewTables.
				continue
			}
			// Remove the reference of this table to this blob file.
			delete(cbf.references, meta)

			// If there are no more references to the blob file, remove it from
			// the set and add the removal of the blob file to the version edit.
			if len(cbf.references) == 0 {
				if cbf.referencedValueSize != 0 {
					return errors.AssertionFailedf("pebble: referenced value size %d is non-zero for blob file %s with no refs",
						cbf.referencedValueSize, cbf.metadata.FileNum)
				}
				if ve.DeletedBlobFiles == nil {
					ve.DeletedBlobFiles = make(map[base.DiskFileNum]*BlobFileMetadata)
				}
				ve.DeletedBlobFiles[cbf.metadata.FileNum] = cbf.metadata
				s.stats.Count--
				s.stats.PhysicalSize -= cbf.metadata.Size
				s.stats.ValueSize -= cbf.metadata.ValueSize
				delete(s.files, cbf.metadata.FileNum)
			}
		}
	}
	return nil
}
