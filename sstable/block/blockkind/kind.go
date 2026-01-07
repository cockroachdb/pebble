// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package blockkind

import "iter"

// Kind identifies the type of block.
type Kind uint8

const (
	Unknown Kind = iota
	SSTableData
	SSTableIndex
	SSTableValue
	BlobValue
	BlobReferenceValueLivenessIndex
	TieringHistogram
	Filter
	RangeDel
	RangeKey
	Metadata

	NumKinds
)

var kindString = [...]string{
	Unknown:                         "unknown",
	SSTableData:                     "data",
	SSTableIndex:                    "index",
	SSTableValue:                    "sstval",
	BlobValue:                       "blobval",
	BlobReferenceValueLivenessIndex: "blobrefval",
	TieringHistogram:                "tieringhist",
	Filter:                          "filter",
	RangeDel:                        "rangedel",
	RangeKey:                        "rangekey",
	Metadata:                        "metadata",
}

func (k Kind) String() string {
	return kindString[k]
}

// All returns all block kinds.
func All() iter.Seq[Kind] {
	return func(yield func(Kind) bool) {
		for i := Kind(1); i < NumKinds; i++ {
			if !yield(i) {
				break
			}
		}
	}
}
