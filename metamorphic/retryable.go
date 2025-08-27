// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs/errorfs"
)

// A RetryPolicy determines what error values should trigger a retry of an
// operation.
type RetryPolicy func(error) bool

var (
	// NeverRetry implements a RetryPolicy that never retries.
	NeverRetry = func(error) bool { return false }
	// RetryInjected implements a RetryPolicy that retries whenever an
	// errorfs.ErrInjected error is returned.
	RetryInjected RetryPolicy = func(err error) bool {
		return errors.Is(err, errorfs.ErrInjected)
	}
)

func withRetries(fn func() error, retryPolicy RetryPolicy) error {
	for {
		if err := fn(); !retryPolicy(err) {
			return err
		}
	}
}

// retryableIter holds an iterator and the state necessary to reset it to its
// state after the last successful operation. This allows us to retry failed
// iterator operations by running them again on a non-error iterator with the
// same pre-operation state.
type retryableIter struct {
	iter      *pebble.Iterator
	lastKey   []byte
	needRetry RetryPolicy
}

func (i *retryableIter) withRetry(fn func()) {
	for {
		fn()
		if !i.needRetry(i.iter.Error()) {
			break
		}
		for i.needRetry(i.iter.Error()) {
			i.iter.SeekGE(i.lastKey)
		}
	}

	i.lastKey = i.lastKey[:0]
	if i.iter.Valid() {
		i.lastKey = append(i.lastKey, i.iter.Key()...)
	}
}

func (i *retryableIter) Close() error {
	return i.iter.Close()
}

func (i *retryableIter) Error() error {
	return i.iter.Error()
}

func (i *retryableIter) First() bool {
	var valid bool
	i.withRetry(func() {
		valid = i.iter.First()
	})
	return valid
}

func (i *retryableIter) Key() []byte {
	return i.iter.Key()
}

func (i *retryableIter) RangeKeyChanged() bool {
	return i.iter.RangeKeyChanged()
}

func (i *retryableIter) HasPointAndRange() (bool, bool) {
	return i.iter.HasPointAndRange()
}

func (i *retryableIter) RangeBounds() ([]byte, []byte) {
	return i.iter.RangeBounds()
}

func (i *retryableIter) RangeKeys() []pebble.RangeKeyData {
	return i.iter.RangeKeys()
}

func (i *retryableIter) Last() bool {
	var valid bool
	i.withRetry(func() { valid = i.iter.Last() })
	return valid
}

func (i *retryableIter) Next() bool {
	var valid bool
	i.withRetry(func() {
		valid = i.iter.Next()
	})
	return valid
}

func (i *retryableIter) NextWithLimit(limit []byte) pebble.IterValidityState {
	var validity pebble.IterValidityState
	i.withRetry(func() {
		validity = i.iter.NextWithLimit(limit)
	})
	return validity

}

func (i *retryableIter) NextPrefix() bool {
	var valid bool
	i.withRetry(func() {
		valid = i.iter.NextPrefix()
	})
	return valid
}

func (i *retryableIter) Prev() bool {
	var valid bool
	i.withRetry(func() {
		valid = i.iter.Prev()
	})
	return valid
}

func (i *retryableIter) PrevWithLimit(limit []byte) pebble.IterValidityState {
	var validity pebble.IterValidityState
	i.withRetry(func() {
		validity = i.iter.PrevWithLimit(limit)
	})
	return validity
}

func (i *retryableIter) SeekGE(key []byte) bool {
	var valid bool
	i.withRetry(func() { valid = i.iter.SeekGE(key) })
	return valid
}

func (i *retryableIter) SeekGEWithLimit(key []byte, limit []byte) pebble.IterValidityState {
	var validity pebble.IterValidityState
	i.withRetry(func() { validity = i.iter.SeekGEWithLimit(key, limit) })
	return validity
}

func (i *retryableIter) SeekLT(key []byte) bool {
	var valid bool
	i.withRetry(func() { valid = i.iter.SeekLT(key) })
	return valid
}

func (i *retryableIter) SeekLTWithLimit(key []byte, limit []byte) pebble.IterValidityState {
	var validity pebble.IterValidityState
	i.withRetry(func() { validity = i.iter.SeekLTWithLimit(key, limit) })
	return validity
}

func (i *retryableIter) SeekPrefixGE(key []byte) bool {
	var valid bool
	i.withRetry(func() { valid = i.iter.SeekPrefixGE(key) })
	return valid
}

func (i *retryableIter) SetBounds(lower, upper []byte) {
	i.iter.SetBounds(lower, upper)
}

func (i *retryableIter) SetOptions(opts *pebble.IterOptions) {
	i.iter.SetOptions(opts)
}

func (i *retryableIter) Valid() bool {
	return i.iter.Valid()
}

func (i *retryableIter) Value() []byte {
	return i.iter.Value()
}
