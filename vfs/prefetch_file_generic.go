// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// +build !linux

package vfs

// Prefetch, on supported platforms, signals the OS to fetch the next size
// bytes after offset into cache. Any subsequent reads in that range will
// not issue disk IO.
func (p *prefetchFile) Prefetch(offset uint64, size uint64) error {
	// No-op.
	return nil
}
