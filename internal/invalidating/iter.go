// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package invalidating

import "github.com/cockroachdb/pebble/internal/base"

// iter tests unsafe key/value slice reuse by modifying the last
// returned key/value to all 1s.
type iter struct {
	iter        base.InternalIterator
	lastKey     *base.InternalKey
	lastValue   []byte
	ignoreKinds [base.InternalKeyKindMax + 1]bool
	err         error
}

// Option configures the behavior of an invalidating iterator.
type Option interface {
	apply(*iter)
}

type funcOpt func(*iter)

func (f funcOpt) apply(i *iter) { f(i) }

// IgnoreKinds constructs an Option that configures an invalidating iterator to
// skip trashing k/v pairs with the provided key kinds. Some iterators provided
// key stability guarantees for specific key kinds.
func IgnoreKinds(kinds ...base.InternalKeyKind) Option {
	return funcOpt(func(i *iter) {
		for _, kind := range kinds {
			i.ignoreKinds[kind] = true
		}
	})
}

// NewIter constructs a new invalidating iterator that wraps the provided
// iterator, trashing buffers for previously returned keys.
func NewIter(originalIterator base.InternalIterator, opts ...Option) base.InternalIterator {
	i := &iter{iter: originalIterator}
	for _, opt := range opts {
		opt.apply(i)
	}
	return i
}

func (i *iter) update(
	key *base.InternalKey, value base.LazyValue,
) (*base.InternalKey, base.LazyValue) {
	i.trashLastKV()

	v, _, err := value.Value(nil)
	if err != nil {
		i.err = err
		key = nil
	}
	if key == nil {
		i.lastKey = nil
		i.lastValue = nil
		return nil, base.LazyValue{}
	}

	i.lastKey = &base.InternalKey{}
	*i.lastKey = key.Clone()
	i.lastValue = make([]byte, len(v))
	copy(i.lastValue, v)
	return i.lastKey, base.MakeInPlaceValue(i.lastValue)
}

func (i *iter) trashLastKV() {
	if i.lastKey == nil {
		return
	}
	if i.ignoreKinds[i.lastKey.Kind()] {
		return
	}

	if i.lastKey != nil {
		for j := range i.lastKey.UserKey {
			i.lastKey.UserKey[j] = 0xff
		}
		i.lastKey.Trailer = 0xffffffffffffffff
	}
	for j := range i.lastValue {
		i.lastValue[j] = 0xff
	}
}

func (i *iter) SeekGE(key []byte, flags base.SeekGEFlags) (*base.InternalKey, base.LazyValue) {
	return i.update(i.iter.SeekGE(key, flags))
}

func (i *iter) SeekPrefixGE(
	prefix, key []byte, flags base.SeekGEFlags,
) (*base.InternalKey, base.LazyValue) {
	return i.update(i.iter.SeekPrefixGE(prefix, key, flags))
}

func (i *iter) SeekLT(key []byte, flags base.SeekLTFlags) (*base.InternalKey, base.LazyValue) {
	return i.update(i.iter.SeekLT(key, flags))
}

func (i *iter) First() (*base.InternalKey, base.LazyValue) {
	return i.update(i.iter.First())
}

func (i *iter) Last() (*base.InternalKey, base.LazyValue) {
	return i.update(i.iter.Last())
}

func (i *iter) Next() (*base.InternalKey, base.LazyValue) {
	return i.update(i.iter.Next())
}

func (i *iter) Prev() (*base.InternalKey, base.LazyValue) {
	return i.update(i.iter.Prev())
}

func (i *iter) NextPrefix(succKey []byte) (*base.InternalKey, base.LazyValue) {
	return i.update(i.iter.NextPrefix(succKey))
}

func (i *iter) Error() error {
	if err := i.iter.Error(); err != nil {
		return err
	}
	return i.err
}

func (i *iter) Close() error {
	return i.iter.Close()
}

func (i *iter) SetBounds(lower, upper []byte) {
	i.iter.SetBounds(lower, upper)
}

func (i *iter) String() string {
	return i.iter.String()
}
