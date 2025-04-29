// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import "strings"

// Attributes is a bitset containing features in use in an sstable.
type Attributes uint32

const (
	AttributeValueBlocks Attributes = 1 << iota
	AttributeRangeKeySets
	AttributeRangeKeyUnsets
	AttributeRangeKeyDels
	AttributeRangeDels
	AttributeTwoLevelIndex
	AttributeBlobValues
)

// Intersects checks if any bits in attr are set in a.
func (a Attributes) Intersects(attr Attributes) bool {
	return a&attr != 0
}

// Has checks if all bits in attr are set in a.
func (a Attributes) Has(attr Attributes) bool {
	return a&attr == attr
}

// Add sets the bits in attr to a.
func (a *Attributes) Add(attr Attributes) {
	*a = *a | attr
}

// String converts the Attributes fs to a string representation for testing.
func (a Attributes) String() string {
	var attributes []string
	if a.Has(AttributeValueBlocks) {
		attributes = append(attributes, "ValueBlocks")
	}
	if a.Has(AttributeRangeKeySets) {
		attributes = append(attributes, "RangeKeySets")
	}
	if a.Has(AttributeRangeKeyUnsets) {
		attributes = append(attributes, "RangeKeyUnsets")
	}
	if a.Has(AttributeRangeKeyDels) {
		attributes = append(attributes, "RangeKeyDels")
	}
	if a.Has(AttributeRangeDels) {
		attributes = append(attributes, "RangeDels")
	}
	if a.Has(AttributeTwoLevelIndex) {
		attributes = append(attributes, "TwoLevelIndex")
	}
	if a.Has(AttributeBlobValues) {
		attributes = append(attributes, "BlobValues")
	}
	return "[" + strings.Join(attributes, ",") + "]"
}
