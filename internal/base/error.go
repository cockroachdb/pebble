// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import (
	"slices"

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

// CorruptBlockData is a wrapper error type that carries the raw block data
// associated with a corruption error. It wraps the cause error transparently
// (Error() delegates to cause), does not implement SafeDetailer, and is not
// registered with the error encoding system, ensuring the raw data does not
// leak to Sentry or over the wire.
type CorruptBlockData struct {
	cause error
	Data  []byte
}

func (e *CorruptBlockData) Error() string { return e.cause.Error() }
func (e *CorruptBlockData) Unwrap() error { return e.cause }

// AttachCorruptBlockData wraps an error with a copy of the raw block data.
func AttachCorruptBlockData(err error, data []byte) error {
	return &CorruptBlockData{cause: err, Data: slices.Clone(data)}
}

// ExtractCorruptBlockData extracts the raw block data from the error chain, if
// present.
func ExtractCorruptBlockData(err error) []byte {
	var e *CorruptBlockData
	if errors.As(err, &e) {
		return e.Data
	}
	return nil
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
