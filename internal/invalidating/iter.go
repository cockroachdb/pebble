// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package invalidating

import (
	"context"
	"slices"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/treesteps"
)

// MaybeWrapIfInvariants wraps some iterators with an invalidating iterator.
// MaybeWrapIfInvariants does nothing in non-invariant builds.
func MaybeWrapIfInvariants(iter base.InternalIterator) base.InternalIterator {
	if invariants.Enabled && invariants.Sometimes(10) {
		return NewIter(iter)
	}
	return iter
}

// iter tests unsafe key/value slice reuse by modifying the last
// returned key/value to all 1s.
type iter struct {
	iter        base.InternalIterator
	lastKV      *base.InternalKV
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
func NewIter(originalIterator base.InternalIterator, opts ...Option) base.TopLevelIterator {
	i := &iter{iter: originalIterator}
	for _, opt := range opts {
		opt.apply(i)
	}
	return i
}

func (i *iter) update(kv *base.InternalKV) *base.InternalKV {
	i.trashLastKV()
	if kv == nil {
		i.lastKV = nil
		return nil
	}

	lv := kv.LazyValue()
	copiedLV := base.LazyValue{
		ValueOrHandle: slices.Clone(lv.ValueOrHandle),
	}
	if lv.Fetcher != nil {
		fetcher := new(base.LazyFetcher)
		*fetcher = *lv.Fetcher
		copiedLV.Fetcher = fetcher
	}
	i.lastKV = &base.InternalKV{
		K: kv.K.Clone(),
		V: base.MakeLazyValue(copiedLV),
	}
	return i.lastKV
}

func (i *iter) trashLastKV() {
	if i.lastKV == nil {
		return
	}
	if i.ignoreKinds[i.lastKV.Kind()] {
		return
	}

	if i.lastKV != nil {
		for j := range i.lastKV.K.UserKey {
			i.lastKV.K.UserKey[j] = 0xff
		}
		i.lastKV.K.Trailer = 0xffffffffffffffff
	}
	lv := i.lastKV.LazyValue()
	for j := range lv.ValueOrHandle {
		lv.ValueOrHandle[j] = 0xff
	}
	if lv.Fetcher != nil {
		// Not all the LazyFetcher fields are visible, so we zero out the last
		// value's Fetcher struct entirely.
		*lv.Fetcher = base.LazyFetcher{}
	}
}

func (i *iter) SeekGE(key []byte, flags base.SeekGEFlags) *base.InternalKV {
	return i.update(i.iter.SeekGE(key, flags))
}

func (i *iter) SeekPrefixGE(prefix, key []byte, flags base.SeekGEFlags) *base.InternalKV {
	return i.update(i.iter.SeekPrefixGE(prefix, key, flags))
}

func (i *iter) SeekPrefixGEStrict(prefix, key []byte, flags base.SeekGEFlags) *base.InternalKV {
	return i.update(i.iter.SeekPrefixGE(prefix, key, flags))
}

func (i *iter) SeekLT(key []byte, flags base.SeekLTFlags) *base.InternalKV {
	return i.update(i.iter.SeekLT(key, flags))
}

func (i *iter) First() *base.InternalKV {
	return i.update(i.iter.First())
}

func (i *iter) Last() *base.InternalKV {
	return i.update(i.iter.Last())
}

func (i *iter) Next() *base.InternalKV {
	return i.update(i.iter.Next())
}

func (i *iter) Prev() *base.InternalKV {
	return i.update(i.iter.Prev())
}

func (i *iter) NextPrefix(succKey []byte) *base.InternalKV {
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

func (i *iter) SetContext(ctx context.Context) {
	i.iter.SetContext(ctx)
}

// TreeStepsNode is part of the InternalIterator interface.
func (i *iter) TreeStepsNode() treesteps.NodeInfo {
	// Pass through; this node will not be visible in the tree.
	return i.iter.TreeStepsNode()
}

func (i *iter) String() string {
	return i.iter.String()
}
