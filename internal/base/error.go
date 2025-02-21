// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/invariants"
)

// ErrNotFound means that a get or delete call did not find the requested key.
var ErrNotFound = errors.New("pebble: not found")

// ErrCorruption is a marker to indicate that data in a file (WAL, MANIFEST,
// sstable) isn't in the expected format (or the file is missing).
var ErrCorruption = errors.New("pebble: corruption")

// MarkCorruptionError marks given error as a corruption error.
func MarkCorruptionError(err error) error {
	if errors.Is(err, ErrCorruption) {
		return err
	}
	return errors.Mark(err, ErrCorruption)
}

// IsCorruptionError returns true if the given error indicates corruption.
func IsCorruptionError(err error) bool {
	return errors.Is(err, ErrCorruption)
}

// CorruptionErrorf formats according to a format specifier and returns
// the string as an error value that is marked as a corruption error.
func CorruptionErrorf(format string, args ...interface{}) error {
	return errors.Mark(errors.Newf(format, args...), ErrCorruption)
}

// AssertionFailedf creates an assertion error and panics in invariants.Enabled
// builds. It should only be used when it indicates a bug.
func AssertionFailedf(format string, args ...interface{}) error {
	err := errors.AssertionFailedf(format, args...)
	if invariants.Enabled {
		panic(err)
	}
	return err
}

// CatchErrorPanic runs a function and catches any panic that contains an
// error, returning that error. Used in tests, in particular to catch panics
// threw by AssertionFailedf.
func CatchErrorPanic(f func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = e
			} else {
				panic(r)
			}
		}
	}()
	return f()
}
