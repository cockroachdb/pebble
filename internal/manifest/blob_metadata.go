// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/humanize"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/strparse"
	"github.com/cockroachdb/redact"
)

// A BlobReference describes a sstable's reference to a blob value file.
type BlobReference struct {
	// FileNum identifies the referenced blob file.
	FileNum base.DiskFileNum
	// ValueSize is the sum of the lengths of the uncompressed values within the
	// blob file for which there exists a reference in the sstable.
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

	// ActiveRefs holds state that describes the latest (the 'live') Version
	// containing this blob file and the references to the file within that
	// version.
	ActiveRefs BlobFileActiveRefs
}

// BlobFileActiveRefs describes the state of a blob file within the latest
// Version and the references to the file within that version.
type BlobFileActiveRefs struct {
	// count holds the number of tables in the latest version that reference
	// this blob file. When this reference count falls to zero, the blob file is
	// either a zombie file (if BlobFileMetadata.refs > 0), or obsolete and
	// ready to be deleted.
	//
	// INVARIANT: BlobFileMetadata.refs > BlobFileMetadata.ActiveRefs.count
	count int32
	// valueSize is the sum of the length of uncompressed values in this blob
	// file that are still live (i.e., referred to by sstables in the latest
	// version).
	valueSize uint64
}

// AddRef records a new reference to the blob file from a sstable in the latest
// Version. The provided valueSize should be the value of the sstable's
// BlobReference.ValueSize.
//
// Requires the manifest logLock be held.
func (r *BlobFileActiveRefs) AddRef(valueSize uint64) {
	r.count++
	r.valueSize += valueSize
}

// RemoveRef removes a reference that no longer exists in the latest Version.
// The provided valueSize should be the value of the removed sstable's
// BlobReference.ValueSize.
//
// Requires the manifest logLock be held.
func (r *BlobFileActiveRefs) RemoveRef(valueSize uint64) {
	if invariants.Enabled {
		if r.count <= 0 {
			panic(errors.AssertionFailedf("pebble: negative active ref count"))
		}
		if valueSize > r.valueSize {
			panic(errors.AssertionFailedf("pebble: negative active value size"))
		}
	}
	r.count--
	r.valueSize -= valueSize
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
