// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package errors

// CorruptionError is a marker error that indicates data in a file (WAL,
// MANIFEST, sstable) isn't in the expected format.
type CorruptionError struct {
	Err error
}

// Unwrap returns the error that describes the corruption.
func (i CorruptionError) Unwrap() error {
	return i.Err
}

func (i CorruptionError) Error() string {
	return i.Err.Error()
}
