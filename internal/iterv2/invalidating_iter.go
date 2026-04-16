// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package iterv2

import (
	"context"
	"slices"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/treesteps"
)

// InvalidatingIter wraps an Iter and catches unsafe reuse of key/value/span
// buffers by copying returned data and trashing the previous copy before each
// operation.
type InvalidatingIter struct {
	inner  Iter
	lastKV *base.InternalKV
	span   Span
}

var _ Iter = (*InvalidatingIter)(nil)

// NewInvalidating creates an InvalidatingIter wrapping inner.
func NewInvalidating(inner Iter) *InvalidatingIter {
	return &InvalidatingIter{inner: inner}
}

// MaybeWrapInInvalidating wraps the iterator in an InvalidatingIter when
// invariants are enabled (randomly). It does nothing in non-invariant builds.
func MaybeWrapInInvalidating(iter Iter) Iter {
	if invariants.Enabled && invariants.Sometimes(10) {
		return NewInvalidating(iter)
	}
	return iter
}

func (i *InvalidatingIter) update(kv *base.InternalKV) *base.InternalKV {
	i.trashLast()

	// Copy the inner span.
	innerSpan := i.inner.Span()
	i.span.BoundaryType = innerSpan.BoundaryType
	i.span.Boundary = slices.Clone(innerSpan.Boundary)
	i.span.Keys = slices.Clone(innerSpan.Keys)
	for j, k := range i.span.Keys {
		i.span.Keys[j] = keyspan.Key{
			Trailer: k.Trailer,
			Suffix:  slices.Clone(k.Suffix),
			Value:   slices.Clone(k.Value),
		}
	}

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

func (i *InvalidatingIter) trashLast() {
	trash := func(slice []byte) {
		for j := range slice {
			slice[j] = 0xcc
		}
	}

	if i.lastKV != nil {
		trash(i.lastKV.K.UserKey)
		i.lastKV.K.Trailer = 0xcccccccccccccccc
		lv := i.lastKV.LazyValue()
		trash(lv.ValueOrHandle)
		if lv.Fetcher != nil {
			*lv.Fetcher = base.LazyFetcher{}
		}
	}

	// Trash the span copy.
	trash(i.span.Boundary)
	for j := range i.span.Keys {
		trash(i.span.Keys[j].Suffix)
		trash(i.span.Keys[j].Value)
		i.span.Keys[j].Trailer = 0xcccccccccccccccc
	}
	i.span.BoundaryType = BoundaryNone
}

// Span implements Iter.
func (i *InvalidatingIter) Span() *Span {
	return &i.span
}

// SeekGE implements Iter.
func (i *InvalidatingIter) SeekGE(key []byte, flags base.SeekGEFlags) *base.InternalKV {
	return i.update(i.inner.SeekGE(key, flags))
}

// SeekPrefixGE implements Iter.
func (i *InvalidatingIter) SeekPrefixGE(
	prefix, key []byte, flags base.SeekGEFlags,
) *base.InternalKV {
	return i.update(i.inner.SeekPrefixGE(prefix, key, flags))
}

// SeekLT implements Iter.
func (i *InvalidatingIter) SeekLT(key []byte, flags base.SeekLTFlags) *base.InternalKV {
	return i.update(i.inner.SeekLT(key, flags))
}

// First implements Iter.
func (i *InvalidatingIter) First() *base.InternalKV {
	return i.update(i.inner.First())
}

// Last implements Iter.
func (i *InvalidatingIter) Last() *base.InternalKV {
	return i.update(i.inner.Last())
}

// Next implements Iter.
func (i *InvalidatingIter) Next() *base.InternalKV {
	return i.update(i.inner.Next())
}

// NextPrefix implements Iter.
func (i *InvalidatingIter) NextPrefix(succKey []byte) *base.InternalKV {
	return i.update(i.inner.NextPrefix(succKey))
}

// Prev implements Iter.
func (i *InvalidatingIter) Prev() *base.InternalKV {
	return i.update(i.inner.Prev())
}

// Error implements Iter.
func (i *InvalidatingIter) Error() error {
	return i.inner.Error()
}

// Close implements Iter.
func (i *InvalidatingIter) Close() error {
	return i.inner.Close()
}

// SetBounds implements Iter.
func (i *InvalidatingIter) SetBounds(lower, upper []byte) {
	i.inner.SetBounds(lower, upper)
}

// SetContext implements Iter.
func (i *InvalidatingIter) SetContext(ctx context.Context) {
	i.inner.SetContext(ctx)
}

// TreeStepsNode implements treesteps.Node.
func (i *InvalidatingIter) TreeStepsNode() treesteps.NodeInfo {
	return i.inner.TreeStepsNode()
}

// String implements fmt.Stringer.
func (i *InvalidatingIter) String() string {
	return i.inner.String()
}
