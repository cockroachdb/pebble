// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/sstableinternal"
)

// SetCacheOptions configures the block cache for a reader.
func SetCacheOptions(opts *ReaderOptions, handle *cache.Handle, fileNum uint64) {
	if opts == nil || handle == nil {
		return
	}
	opts.CacheOpts = sstableinternal.CacheOptions{
		CacheHandle: handle,
		FileNum:     base.DiskFileNum(fileNum),
	}
}
