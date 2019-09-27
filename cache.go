// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import "github.com/cockroachdb/pebble/internal/cache"

// Cache exports the cache.Cache type.
type Cache = cache.Cache

// NewCache creates a new cache of the specified size. Memory for the cache is
// allocated on demand, not during initialization.
func NewCache(size int64) *cache.Cache {
	return cache.New(size)
}
