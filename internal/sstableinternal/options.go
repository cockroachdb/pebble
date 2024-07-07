// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstableinternal

import (
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
)

// CacheOptions contains the information needed to interact with the block
// cache.
type CacheOptions struct {
	// Cache can be nil, in which case no cache is used. When non-nil, the other
	// fields must be set accordingly.
	Cache   *cache.Cache
	CacheID uint64
	FileNum base.DiskFileNum
}

// ReaderOptions are fields of sstable.ReaderOptions that can only be set from
// within the pebble package.
type ReaderOptions struct {
	CacheOpts CacheOptions

	// RawTombstones specifies that range tombstones returned by
	// Reader.NewRangeDelIter() should not be fragmented. Used by debug tools to
	// get a raw view of the tombstones contained in an sstable.
	RawTombstones bool

	Comparers map[string]*base.Comparer
	Mergers   Mergers
}

// Comparers is a map from comparer name to comparer. It is used for debugging
// tools which may be used on multiple databases configured with different
// comparers.
type Comparers map[string]*base.Comparer

// Mergers is a map from merger name to merger. It is used for debugging tools
// which may be used on multiple databases configured with different
// mergers.
type Mergers map[string]*base.Merger

// WriterOptions are fields of sstable.ReaderOptions that can only be set from
// within the pebble package.
type WriterOptions struct {
	CacheOpts CacheOptions

	// DisableKeyOrderChecks disables the checks that keys are added to an sstable
	// in order. It is intended for use only in the construction of invalid
	// sstables for testing. See tool/make_test_sstables.go.
	DisableKeyOrderChecks bool
}
