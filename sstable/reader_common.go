// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"context"
	"math"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
)

// CommonReader abstracts functionality over a Reader or a VirtualReader. This
// can be used by code which doesn't care to distinguish between a reader and a
// virtual reader.
type CommonReader interface {
	NewRawRangeKeyIter(transforms IterTransforms) (keyspan.FragmentIterator, error)

	NewRawRangeDelIter(transforms IterTransforms) (keyspan.FragmentIterator, error)

	NewIterWithBlockPropertyFiltersAndContextEtc(
		ctx context.Context,
		transforms IterTransforms,
		lower, upper []byte,
		filterer *BlockPropertiesFilterer,
		filterBlockSizeLimit FilterBlockSizeLimit,
		stats *base.InternalIteratorStats,
		categoryAndQoS CategoryAndQoS,
		statsCollector *CategoryStatsCollector,
		rp ReaderProvider,
	) (Iterator, error)

	NewCompactionIter(
		transforms IterTransforms,
		bytesIterated *uint64,
		categoryAndQoS CategoryAndQoS,
		statsCollector *CategoryStatsCollector,
		rp ReaderProvider,
		bufferPool *BufferPool,
	) (Iterator, error)

	EstimateDiskUsage(start, end []byte) (uint64, error)

	CommonProperties() *CommonProperties
}

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

// IterTransforms allow on-the-fly transformation of data at iteration time.
//
// These transformations could in principle be implemented as block transforms
// (at least for non-virtual sstables), but applying them during iteration is
// preferable.
type IterTransforms struct {
	SyntheticSeqNum    SyntheticSeqNum
	HideObsoletePoints bool
	SyntheticPrefix    SyntheticPrefix
	SyntheticSuffix    SyntheticSuffix
}

// NoTransforms is the default value for IterTransforms.
var NoTransforms = IterTransforms{}

// SyntheticSeqNum is used to override all sequence numbers in a table. It is
// set to a non-zero value when the table was created externally and ingested
// whole.
type SyntheticSeqNum uint64

// NoSyntheticSeqNum is the default zero value for SyntheticSeqNum, which
// disables overriding the sequence number.
const NoSyntheticSeqNum SyntheticSeqNum = 0
