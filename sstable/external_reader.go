// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/sstableinternal"
)

// This file provides shims for external callers who use sstable.NewReader
// directly (outside of a Pebble instance) and want to enable block caching.
//
// These shims are NOT needed when using Pebble normally - Pebble manages
// its own block cache internally.
//
// Example usage:
//
//	c := pebble.NewCache(128 << 20)
//	defer c.Unref()
//	h := c.NewHandle()
//	defer h.Close()
//
//	opts := sstable.ReaderOptions{}
//	sstable.SetCacheOptions(&opts, h, fileNum)
//
//	r, err := sstable.NewReader(ctx, readable, opts)
//	if err != nil {
//		// handle error
//	}
//	defer r.Close()
//
//	// When done with the file, evict its blocks from the cache:
//	sstable.CacheHandleEvictFile(h, fileNum)

// CacheHandle is an alias for cache.Handle, exported for external callers
// who need to configure block caching with sstable.ReaderOptions.
type CacheHandle = cache.Handle

// SetCacheOptions configures block caching on ReaderOptions using the provided
// cache handle and file number. The file number is caller-assigned and must be
// unique within the cache handle's namespace to avoid collisions.
func SetCacheOptions(opts *ReaderOptions, handle *CacheHandle, fileNum uint64) {
	opts.CacheOpts = sstableinternal.CacheOptions{
		CacheHandle: handle,
		FileNum:     base.DiskFileNum(fileNum),
	}
}

// CacheHandleEvictFile evicts all cached blocks for the specified file number
// from the cache handle. Call this when an sstable is no longer needed to free
// cache space.
func CacheHandleEvictFile(handle *CacheHandle, fileNum uint64) {
	handle.EvictFile(base.DiskFileNum(fileNum))
}
