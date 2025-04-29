// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import "strings"

type Feature uint32

const (
	FeatureValueBlocks Feature = 1 << iota
	FeatureRangeKeySets
	FeatureRangeKeyUnsets
	FeatureRangeKeyDels
	FeatureRangeDels
	FeatureTwoLevelIndex
	FeatureBlobValues
)

// FeatureSet is a bitset containing features in use in an sstable.
type FeatureSet uint32

// IsSet checks if the feature f is set in the FeatureSet fs.
func (fs *FeatureSet) IsSet(f Feature) bool {
	return *fs&FeatureSet(f) != 0
}

// Set sets the feature f in the FeatureSet fs and returns the new FeatureSet.
func (fs *FeatureSet) Set(f Feature) *FeatureSet {
	*fs = *fs | FeatureSet(f)
	return fs
}

// String converts the FeatureSet fs to a string representation for testing.
func (fs *FeatureSet) String() string {
	var features []string
	if fs.IsSet(FeatureValueBlocks) {
		features = append(features, "ValueBlocks")
	}
	if fs.IsSet(FeatureRangeKeySets) {
		features = append(features, "RangeKeySets")
	}
	if fs.IsSet(FeatureRangeKeyUnsets) {
		features = append(features, "RangeKeyUnsets")
	}
	if fs.IsSet(FeatureRangeKeyDels) {
		features = append(features, "RangeKeyDels")
	}
	if fs.IsSet(FeatureRangeDels) {
		features = append(features, "RangeDels")
	}
	if fs.IsSet(FeatureTwoLevelIndex) {
		features = append(features, "TwoLevelIndex")
	}
	if fs.IsSet(FeatureBlobValues) {
		features = append(features, "BlobValues")
	}
	return "[" + strings.Join(features, "|") + "]"
}
