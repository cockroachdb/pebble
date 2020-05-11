// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import "github.com/cockroachdb/errors"

// ErrNotFound means that a get or delete call did not find the requested key.
var ErrNotFound = errors.New("pebble: not found")

// CorruptionError is a marker error that indicates data in a file (WAL,
// MANIFEST, sstable) isn't in the expected format.
type CorruptionError struct {
	err error
}

// NewCorruptionError creates a CorruptionError by wrapping the given error.
func NewCorruptionError(err error) error {
	if errors.As(err, &CorruptionError{}) {
		return err
	}
	return CorruptionError{err: err}
}

// CorruptionErrorf formats according to a format specifier and returns the
// string as a value that is a CorruptionError.
func CorruptionErrorf(format string, args ...interface{}) error {
	return CorruptionError{err: errors.Newf(format, args...)}
}

// Unwrap returns the error that describes the corruption.
func (i CorruptionError) Unwrap() error {
	return i.err
}

func (i CorruptionError) Error() string {
	return i.err.Error()
}
