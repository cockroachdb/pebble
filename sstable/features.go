// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

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

type FeatureSet uint32

func (fs FeatureSet) IsSet(f Feature) bool {
	return fs&FeatureSet(f) != 0
}

func (fs FeatureSet) Set(f Feature) FeatureSet {
	fs = fs | FeatureSet(f)
	return fs
}
