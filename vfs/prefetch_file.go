// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

type prefetchFile struct {
	File
	fd uintptr
}

// Prefetch is an interface for a file that supports prefetching/readahead.
type Prefetch interface {
	Prefetch(offset uint64, size uint64) error
}

// NewPrefetchFile wraps a readable file and adds a Prefetch() method on
// supported platforms.
func NewPrefetchFile(f File) File {
	if f == nil {
		return nil
	}

	s := &prefetchFile{
		File: f,
	}

	type fd interface {
		Fd() uintptr
	}
	if d, ok := f.(fd); ok {
		s.fd = d.Fd()
	}
	return s
}
