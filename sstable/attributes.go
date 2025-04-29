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

// AnyIntersection checks if any bits in attr are set in a.
func (a Attributes) AnyIntersection(attr Attributes) bool {
	return attr&a != 0
}

// Add sets the bits in attr to a.
func (a *Attributes) Add(attr Attributes) {
	*a = *a | attr
}

// String converts the Attributes fs to a string representation for testing.
func (a Attributes) String() string {
	var features []string
	if a.AnyIntersection(AttributeValueBlocks) {
		features = append(features, "ValueBlocks")
	}
	if a.AnyIntersection(AttributeRangeKeySets) {
		features = append(features, "RangeKeySets")
	}
	if a.AnyIntersection(AttributeRangeKeyUnsets) {
		features = append(features, "RangeKeyUnsets")
	}
	if a.AnyIntersection(AttributeRangeKeyDels) {
		features = append(features, "RangeKeyDels")
	}
	if a.AnyIntersection(AttributeRangeDels) {
		features = append(features, "RangeDels")
	}
	if a.AnyIntersection(AttributeTwoLevelIndex) {
		features = append(features, "TwoLevelIndex")
	}
	if a.AnyIntersection(AttributeBlobValues) {
		features = append(features, "BlobValues")
	}
	return "[" + strings.Join(features, ",") + "]"
}
