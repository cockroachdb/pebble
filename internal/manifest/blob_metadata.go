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
