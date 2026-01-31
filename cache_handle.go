// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
)

// CacheHandle exports the cache.Handle type.
type CacheHandle = cache.Handle

// CacheHandleEvictFile evicts all cached blocks for the provided file number.
func CacheHandleEvictFile(h *CacheHandle, fileNum uint64) {
	if h == nil {
		return
	}
	h.EvictFile(base.DiskFileNum(fileNum))
}
