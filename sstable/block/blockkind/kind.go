// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package blockkind

// Kind identifies the type of block.
type Kind uint8

const (
	Unknown Kind = iota
	SSTableData
	SSTableIndex
	SSTableValue
	BlobValue
	Index
	Filter
	RangeDel
	RangeKey
	Metadata

	NumKinds
)

var kindString = [...]string{
	Unknown:      "unknown",
	SSTableData:  "data",
	SSTableValue: "sstval",
	SSTableIndex: "index",
	BlobValue:    "blobval",
	Filter:       "filter",
	RangeDel:     "rangedel",
	RangeKey:     "rangekey",
	Metadata:     "metadata",
}

func (k Kind) String() string {
	return kindString[k]
}

// All returns all block kinds.
func All() []Kind {
	kinds := make([]Kind, NumKinds-1)
	for i := range kinds {
		kinds[i] = Kind(i + 1)
	}
	return kinds
}
