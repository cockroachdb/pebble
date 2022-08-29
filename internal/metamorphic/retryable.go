// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"bytes"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/errorfs"
	"github.com/cockroachdb/pebble/internal/testkeys"
)

// withRetries executes fn, retrying it whenever an errorfs.ErrInjected error
// is returned.  It returns the first nil or non-errorfs.ErrInjected error
// returned by fn.
func withRetries(fn func() error) error {
	for {
		if err := fn(); !errors.Is(err, errorfs.ErrInjected) {
			return err
		}
	}
}

// retryableIter holds an iterator and the state necessary to reset it to its
// state after the last successful operation. This allows us to retry failed
// iterator operations by running them again on a non-error iterator with the
// same pre-operation state.
type retryableIter struct {
	iter    *pebble.Iterator
	lastKey []byte

	// When filterMax is >0, this iterator filters out keys with suffixes
	// outside of the range [filterMin, filterMax). Keys without suffixes are
	// surfaced. This is used to ensure determinism regardless of whether
	// block-property filters filter keys or not.
	filterMin, filterMax uint64

	// rangeKeyChangeGuess is only used if the iterator has a filter set. A single
	// operation on the retryableIter may result in many operations on
	// retryableIter.iter if we need to skip filtered keys. Thus, the value of
	// retryableIter.iter.RangeKeyChanged() will not necessarily indicate if the
	// range key actually changed.
	//
	// Since one call to a positioning operation may lead to multiple
	// positioning operations, we set rangeKeyChangeGuess to false, iff every single
	// positioning operation returned iter.RangeKeyChanged() == false.
	//
	// rangeKeyChangeGuess == true implies that at least one of the many iterator
	// operations returned RangeKeyChanged to true, but we may have false
	// positives. We can't assume that the range key actually changed if
	// iter.RangeKeyChanged() returns true after one of the positioning
	// operations. Consider a db with two range keys, which are also in the same
	// block, a-f, g-h where the iterator's filter excludes g-h. If the iterator
	// is positioned on a-f, then a call to SeekLT(z), will position the iterator
	// over g-h, but the retryableIter will call Prev and the iterator will be
	// positioned back over a-f. In this case the range key hasn't changed, but
	// one of the positioning operations will return iter.RangeKeyChanged() ==
	// true.
	rangeKeyChangeGuess bool

	// rangeKeyChanged is the true accurate value of whether the range key has
	// changed from the perspective of a client of the retryableIter. It is used
	// to determine if rangeKeyChangeGuess is a false positive. It is computed
	// by comparing the range key at the current position with the range key
	// at the previous position.
	rangeKeyChanged bool

	// When a filter is set on the iterator, one positioning op from the
	// perspective of a client of the retryableIter, may result in multiple
	// intermediary positioning ops. This bool is set if the current positioning
	// op is intermediate.
	intermediatePosition bool

	rkeyBuff []byte
}

func (i *retryableIter) shouldFilter() bool {
	if i.filterMax == 0 {
		return false
	}
	k := i.iter.Key()
	n := testkeys.Comparer.Split(k)
	if n == len(k) {
		// No suffix, don't filter it.
		return false
	}
	v, err := testkeys.ParseSuffix(k[n:])
	if err != nil {
		panic(err)
	}
	ts := uint64(v)
	return ts < i.filterMin || ts >= i.filterMax
}

func (i *retryableIter) needRetry() bool {
	return errors.Is(i.iter.Error(), errorfs.ErrInjected)
}

func (i *retryableIter) withRetry(fn func()) {
	for {
		fn()
		if !i.needRetry() {
			break
		}
		for i.needRetry() {
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

// A call to an iterator positioning function may result in sub calls to other
// iterator positioning functions. We need to run some code in the top level
// call, so we use withPosition to reduce code duplication in the positioning
// functions.
func (i *retryableIter) withPosition(fn func()) {
	// For the top level op, i.intermediatePosition must always be false.
	intermediate := i.intermediatePosition
	// Any subcalls to positioning ops will be intermediate.
	i.intermediatePosition = true
	defer func() {
		i.intermediatePosition = intermediate
	}()

	if !intermediate {
		// Clear out the previous value stored in the buff.
		i.rkeyBuff = i.rkeyBuff[:0]
		if _, hasRange := i.iter.HasPointAndRange(); hasRange {
			// This is a top level positioning op. We should determine if the iter
			// is positioned over a range key to later determine if the range key
			// changed.
			startTmp, _ := i.iter.RangeBounds()
			i.rkeyBuff = append(i.rkeyBuff, startTmp...)

		}
		// Set this to false. Any positioning op can set this to true.
		i.rangeKeyChangeGuess = false
	}

	fn()

	if !intermediate {
		// Check if the range key changed.
		var newStartKey []byte
		if _, hasRange := i.iter.HasPointAndRange(); hasRange {
			newStartKey, _ = i.iter.RangeBounds()
		}

		i.rangeKeyChanged = !bytes.Equal(newStartKey, i.rkeyBuff)
	}
}

func (i *retryableIter) updateRangeKeyChangedGuess() {
	i.rangeKeyChangeGuess = i.rangeKeyChangeGuess || i.iter.RangeKeyChanged()
}

func (i *retryableIter) First() bool {
	var valid bool
	i.withPosition(func() {
		i.withRetry(func() {
			valid = i.iter.First()
		})
		i.updateRangeKeyChangedGuess()
		if valid && i.shouldFilter() {
			valid = i.Next()
		}
	})
	return valid
}

func (i *retryableIter) Key() []byte {
	return i.iter.Key()
}

func (i *retryableIter) RangeKeyChanged() bool {
	if i.filterMax == 0 {
		return i.iter.RangeKeyChanged()
	}

	if !i.rangeKeyChangeGuess {
		// false negatives shouldn't be possible so just return.
		return false
	}

	// i.rangeKeyChangeGuess is true. This may be a false positive, so just
	// return i.rangeKeyChanged which will always be correct.
	return i.rangeKeyChanged
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
	i.withPosition(func() {
		i.withRetry(func() { valid = i.iter.Last() })
		i.updateRangeKeyChangedGuess()
		if valid && i.shouldFilter() {
			valid = i.Prev()
		}
	})
	return valid
}

func (i *retryableIter) Next() bool {
	var valid bool
	i.withPosition(func() {
		i.withRetry(func() {
			valid = i.iter.Next()
			i.updateRangeKeyChangedGuess()
			for valid && i.shouldFilter() {
				valid = i.iter.Next()
				i.updateRangeKeyChangedGuess()
			}
		})
	})
	return valid
}

func (i *retryableIter) NextWithLimit(limit []byte) pebble.IterValidityState {
	var validity pebble.IterValidityState
	i.withPosition(func() {
		i.withRetry(func() {
			validity = i.iter.NextWithLimit(limit)
			i.updateRangeKeyChangedGuess()
			for validity == pebble.IterValid && i.shouldFilter() {
				validity = i.iter.NextWithLimit(limit)
				i.updateRangeKeyChangedGuess()
			}
		})
	})
	return validity

}

func (i *retryableIter) Prev() bool {
	var valid bool
	i.withPosition(func() {
		i.withRetry(func() {
			valid = i.iter.Prev()
			i.updateRangeKeyChangedGuess()
			for valid && i.shouldFilter() {
				valid = i.iter.Prev()
				i.updateRangeKeyChangedGuess()
			}
		})
	})
	return valid
}

func (i *retryableIter) PrevWithLimit(limit []byte) pebble.IterValidityState {
	var validity pebble.IterValidityState
	i.withPosition(func() {
		i.withRetry(func() {
			validity = i.iter.PrevWithLimit(limit)
			i.updateRangeKeyChangedGuess()
			for validity == pebble.IterValid && i.shouldFilter() {
				validity = i.iter.PrevWithLimit(limit)
				i.updateRangeKeyChangedGuess()
			}
		})
	})
	return validity
}

func (i *retryableIter) SeekGE(key []byte) bool {
	var valid bool
	i.withPosition(func() {
		i.withRetry(func() { valid = i.iter.SeekGE(key) })
		i.updateRangeKeyChangedGuess()
		if valid && i.shouldFilter() {
			valid = i.Next()
		}
	})
	return valid
}

func (i *retryableIter) SeekGEWithLimit(key []byte, limit []byte) pebble.IterValidityState {
	var validity pebble.IterValidityState
	i.withPosition(func() {
		i.withRetry(func() { validity = i.iter.SeekGEWithLimit(key, limit) })
		i.updateRangeKeyChangedGuess()
		if validity == pebble.IterValid && i.shouldFilter() {
			validity = i.NextWithLimit(limit)
		}
	})
	return validity
}

func (i *retryableIter) SeekLT(key []byte) bool {
	var valid bool
	i.withPosition(func() {
		i.withRetry(func() { valid = i.iter.SeekLT(key) })
		i.updateRangeKeyChangedGuess()
		if valid && i.shouldFilter() {
			valid = i.Prev()
		}
	})
	return valid
}

func (i *retryableIter) SeekLTWithLimit(key []byte, limit []byte) pebble.IterValidityState {
	var validity pebble.IterValidityState
	i.withPosition(func() {
		i.withRetry(func() { validity = i.iter.SeekLTWithLimit(key, limit) })
		i.updateRangeKeyChangedGuess()
		if validity == pebble.IterValid && i.shouldFilter() {
			validity = i.PrevWithLimit(limit)
		}
	})
	return validity
}

func (i *retryableIter) SeekPrefixGE(key []byte) bool {
	var valid bool
	i.withPosition(func() {
		i.withRetry(func() { valid = i.iter.SeekPrefixGE(key) })
		i.updateRangeKeyChangedGuess()
		if valid && i.shouldFilter() {
			valid = i.Next()
		}
	})
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
