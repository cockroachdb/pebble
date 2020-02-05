// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// +build !cgo

package cache

// Provides versions of manualNew and manualFree when cgo is not available
// (e.g. cross compilation).

// manualNew allocates a slice of size n.
func manualNew(n int) []byte {
	return make([]byte, n)
}

// manualFree frees the specified slice.
func manualFree(b []byte) {
}
