// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"math"

	"github.com/cockroachdb/pebble/sstable/block"
)

// FilterBlockSizeLimit is a size limit for bloom filter blocks - if a bloom
// filter is present, it is used only when it is at most this size.
type FilterBlockSizeLimit uint32

const (
	// NeverUseFilterBlock indicates that bloom filter blocks should never be used.
	NeverUseFilterBlock FilterBlockSizeLimit = 0
	// AlwaysUseFilterBlock indicates that bloom filter blocks should always be
	// used, regardless of size.
	AlwaysUseFilterBlock FilterBlockSizeLimit = math.MaxUint32
)

type (
	// BufferPool re-exports block.BufferPool.
	BufferPool = block.BufferPool
	// IterTransforms re-exports block.IterTransforms.
	IterTransforms = block.IterTransforms
	// FragmentIterTransforms re-exports block.FragmentIterTransforms.
	FragmentIterTransforms = block.FragmentIterTransforms
	// SyntheticSeqNum re-exports block.SyntheticSeqNum.
	SyntheticSeqNum = block.SyntheticSeqNum
	// SyntheticSuffix re-exports block.SyntheticSuffix.
	SyntheticSuffix = block.SyntheticSuffix
	// SyntheticPrefix re-exports block.SyntheticPrefix.
	SyntheticPrefix = block.SyntheticPrefix
	// SyntheticPrefixAndSuffix re-exports block.SyntheticPrefixAndSuffix.
	SyntheticPrefixAndSuffix = block.SyntheticPrefixAndSuffix
)

// NoTransforms is the default value for IterTransforms.
var NoTransforms = block.NoTransforms

// NoFragmentTransforms is the default value for FragmentIterTransforms.
var NoFragmentTransforms = block.NoFragmentTransforms

// MakeSyntheticPrefixAndSuffix returns a SyntheticPrefixAndSuffix with the
// given prefix and suffix.
func MakeSyntheticPrefixAndSuffix(
	prefix SyntheticPrefix, suffix SyntheticSuffix,
) SyntheticPrefixAndSuffix {
	return block.MakeSyntheticPrefixAndSuffix(prefix, suffix)
}

// NoSyntheticSeqNum is the default zero value for SyntheticSeqNum, which
// disables overriding the sequence number.
const NoSyntheticSeqNum = block.NoSyntheticSeqNum
