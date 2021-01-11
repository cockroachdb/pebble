// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

// fileWithFd is an interface for a file with an Fd() method. A lot of
// File related optimizations (eg. Prefetch(), WAL recycling) rely on the
// existence of the Fd method to return a raw file descriptor.
type fileWithFd interface {
	Fd() uintptr
}

// fdFileWrapper is a File wrapper that also exposes an Fd() method. Used to
// wrap file wrappers that could unintentionally hide the Fd() method exposed by
// the root File.
type fdFileWrapper struct {
	File

	// All methods usually pass through to File above, except for Fd(), which
	// bypasses it and gets called directly on the inner file.
	inner fileWithFd
}

func (f *fdFileWrapper) Fd() uintptr {
	return f.inner.Fd()
}

// WithFd takes an inner (unwrapped) and an outer (wrapped) vfs.File,
// and returns an fdFileWrapper if the inner file has an Fd() method. Use this
// method to fix the hiding of the Fd() method and the unintentional
func WithFd(inner, outer File) File {
	if f, ok := inner.(fileWithFd); ok {
		return &fdFileWrapper{
			File:  outer,
			inner: f,
		}
	}
	return outer
}
