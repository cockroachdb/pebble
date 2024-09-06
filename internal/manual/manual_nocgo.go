// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build !cgo
// +build !cgo

package manual

// Provides versions of New and Free when cgo is not available (e.g. cross
// compilation).

// New allocates a slice of size n.
func New(purpose Purpose, n int) []byte {
	recordAlloc(purpose, n)
	return make([]byte, n)
}

// Free frees the specified slice. It has to be exactly the slice that was
// returned by New.
func Free(purpose Purpose, b []byte) {
	recordFree(purpose, cap(b))
}
