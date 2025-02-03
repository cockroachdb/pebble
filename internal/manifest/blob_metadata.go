// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/humanize"
	"github.com/cockroachdb/redact"
)

// A BlobReference describes a sstable's reference to a blob value file.
type BlobReference struct {
	FileNum   base.DiskFileNum
	ValueSize uint64
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
	p := makeDebugParser(s)
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
