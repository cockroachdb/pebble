// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
)

type prefixReplacingIterator struct {
	i         Iterator
	cmp       base.Compare
	src, dst  []byte
	arg, arg2 []byte
	res       InternalKey
	err       error
}

var errInputPrefixMismatch = errors.New("key argument does not have prefix required for replacement")
var errOutputPrefixMismatch = errors.New("key returned does not have prefix required for replacement")

var _ Iterator = (*prefixReplacingIterator)(nil)

// newPrefixReplacingIterator wraps an iterator over keys that have prefix `src`
// in an iterator that will make them appear to have prefix `dst`. Every key
// passed as an argument to methods on this iterator must have prefix `dst`, and
// every key produced by the underlying iterator must have prefix `src`.
//
// INVARIANT: len(dst) > 0.
func newPrefixReplacingIterator(i Iterator, src, dst []byte, cmp base.Compare) Iterator {
	if invariants.Enabled && len(dst) == 0 {
		panic("newPrefixReplacingIterator called without synthetic prefix")
	}
	return &prefixReplacingIterator{
		i:   i,
		cmp: cmp,
		src: src, dst: dst,
		arg: append([]byte{}, src...), arg2: append([]byte{}, src...),
		res: InternalKey{UserKey: append([]byte{}, dst...)},
	}
}

func (p *prefixReplacingIterator) SetContext(ctx context.Context) {
	p.i.SetContext(ctx)
}

func (p *prefixReplacingIterator) rewriteArg(key []byte) []byte {
	if !bytes.HasPrefix(key, p.dst) {
		p.err = errInputPrefixMismatch
		return key
	}
	p.arg = append(p.arg[:len(p.src)], key[len(p.dst):]...)
	return p.arg
}

func (p *prefixReplacingIterator) rewriteArg2(key []byte) []byte {
	if !bytes.HasPrefix(key, p.dst) {
		p.err = errInputPrefixMismatch
		return key
	}
	p.arg2 = append(p.arg2[:len(p.src)], key[len(p.dst):]...)
	return p.arg2
}

func (p *prefixReplacingIterator) rewriteResult(
	k *InternalKey, v base.LazyValue,
) (*InternalKey, base.LazyValue) {
	if k == nil {
		return k, v
	}
	if !bytes.HasPrefix(k.UserKey, p.src) {
		p.err = errOutputPrefixMismatch
		if invariants.Enabled {
			panic(p.err)
		}
		return nil, base.LazyValue{}
	}
	p.res.Trailer = k.Trailer
	p.res.UserKey = append(p.res.UserKey[:len(p.dst)], k.UserKey[len(p.src):]...)
	return &p.res, v
}

// SeekGE implements the Iterator interface.
func (p *prefixReplacingIterator) SeekGE(
	key []byte, flags base.SeekGEFlags,
) (*InternalKey, base.LazyValue) {
	return p.rewriteResult(p.i.SeekGE(p.rewriteArg(key), flags))
}

// SeekPrefixGE implements the Iterator interface.
func (p *prefixReplacingIterator) SeekPrefixGE(
	prefix, key []byte, flags base.SeekGEFlags,
) (*InternalKey, base.LazyValue) {
	return p.rewriteResult(p.i.SeekPrefixGE(p.rewriteArg2(prefix), p.rewriteArg(key), flags))
}

// SeekLT implements the Iterator interface.
func (p *prefixReplacingIterator) SeekLT(
	key []byte, flags base.SeekLTFlags,
) (*InternalKey, base.LazyValue) {
	cmp := p.cmp(key, p.dst)
	if cmp < 0 {
		// Exhaust the iterator by Prev()ing before the First key.
		p.i.First()
		return p.rewriteResult(p.i.Prev())
	}
	return p.rewriteResult(p.i.SeekLT(p.rewriteArg(key), flags))
}

// First implements the Iterator interface.
func (p *prefixReplacingIterator) First() (*InternalKey, base.LazyValue) {
	return p.rewriteResult(p.i.First())
}

// Last implements the Iterator interface.
func (p *prefixReplacingIterator) Last() (*InternalKey, base.LazyValue) {
	return p.rewriteResult(p.i.Last())
}

// Next implements the Iterator interface.
func (p *prefixReplacingIterator) Next() (*InternalKey, base.LazyValue) {
	return p.rewriteResult(p.i.Next())
}

// NextPrefix implements the Iterator interface.
func (p *prefixReplacingIterator) NextPrefix(succKey []byte) (*InternalKey, base.LazyValue) {
	return p.rewriteResult(p.i.NextPrefix(p.rewriteArg(succKey)))
}

// Prev implements the Iterator interface.
func (p *prefixReplacingIterator) Prev() (*InternalKey, base.LazyValue) {
	return p.rewriteResult(p.i.Prev())
}

// Error implements the Iterator interface.
func (p *prefixReplacingIterator) Error() error {
	if p.err != nil {
		return p.err
	}
	return p.i.Error()
}

// Close implements the Iterator interface.
func (p *prefixReplacingIterator) Close() error {
	return p.i.Close()
}

// SetBounds implements the Iterator interface.
func (p *prefixReplacingIterator) SetBounds(lower, upper []byte) {
	// Check if the underlying iterator requires un-rewritten bounds, i.e. if it
	// is going to rewrite them itself or pass them to something e.g. vState that
	// will rewrite them.
	if x, ok := p.i.(interface{ SetBoundsWithSyntheticPrefix() bool }); ok && x.SetBoundsWithSyntheticPrefix() {
		p.i.SetBounds(lower, upper)
		return
	}
	p.i.SetBounds(p.rewriteArg(lower), p.rewriteArg2(upper))
}

func (p *prefixReplacingIterator) MaybeFilteredKeys() bool {
	return p.i.MaybeFilteredKeys()
}

// String implements the Iterator interface.
func (p *prefixReplacingIterator) String() string {
	return fmt.Sprintf("%s [%s->%s]", p.i.String(), hex.EncodeToString(p.src), hex.EncodeToString(p.dst))
}

func (p *prefixReplacingIterator) SetCloseHook(fn func(i Iterator) error) {
	p.i.SetCloseHook(fn)
}

type prefixReplacingFragmentIterator struct {
	i          keyspan.FragmentIterator
	err        error
	src, dst   []byte
	arg        []byte
	out1, out2 []byte
}

// newPrefixReplacingFragmentIterator wraps a FragmentIterator over some reader
// that contains range keys in some key span to make those range keys appear to
// be remapped into some other key-span.
func newPrefixReplacingFragmentIterator(
	i keyspan.FragmentIterator, src, dst []byte,
) keyspan.FragmentIterator {
	return &prefixReplacingFragmentIterator{
		i:   i,
		src: src, dst: dst,
		arg:  append([]byte{}, src...),
		out1: append([]byte(nil), dst...),
		out2: append([]byte(nil), dst...),
	}
}

func (p *prefixReplacingFragmentIterator) rewriteArg(key []byte) []byte {
	if !bytes.HasPrefix(key, p.dst) {
		p.err = errInputPrefixMismatch
		return key
	}
	p.arg = append(p.arg[:len(p.src)], key[len(p.dst):]...)
	return p.arg
}

func (p *prefixReplacingFragmentIterator) rewriteSpan(sp *keyspan.Span) *keyspan.Span {
	if !bytes.HasPrefix(sp.Start, p.src) || !bytes.HasPrefix(sp.End, p.src) {
		p.err = errInputPrefixMismatch
		return sp
	}
	sp.Start = append(p.out1[:len(p.dst)], sp.Start[len(p.src):]...)
	sp.End = append(p.out2[:len(p.dst)], sp.End[len(p.src):]...)
	return sp
}

// SeekGE implements the FragmentIterator interface.
func (p *prefixReplacingFragmentIterator) SeekGE(key []byte) *keyspan.Span {
	return p.rewriteSpan(p.i.SeekGE(p.rewriteArg(key)))
}

// SeekLT implements the FragmentIterator interface.
func (p *prefixReplacingFragmentIterator) SeekLT(key []byte) *keyspan.Span {
	return p.rewriteSpan(p.i.SeekLT(p.rewriteArg(key)))
}

// First implements the FragmentIterator interface.
func (p *prefixReplacingFragmentIterator) First() *keyspan.Span {
	return p.rewriteSpan(p.i.First())
}

// Last implements the FragmentIterator interface.
func (p *prefixReplacingFragmentIterator) Last() *keyspan.Span {
	return p.rewriteSpan(p.i.Last())
}

// Close implements the FragmentIterator interface.
func (p *prefixReplacingFragmentIterator) Next() *keyspan.Span {
	return p.rewriteSpan(p.i.Next())
}

// Prev implements the FragmentIterator interface.
func (p *prefixReplacingFragmentIterator) Prev() *keyspan.Span {
	return p.rewriteSpan(p.i.Prev())
}

// Error implements the FragmentIterator interface.
func (p *prefixReplacingFragmentIterator) Error() error {
	if p.err != nil {
		return p.err
	}
	return p.i.Error()
}

// Close implements the FragmentIterator interface.
func (p *prefixReplacingFragmentIterator) Close() error {
	return p.i.Close()
}

// WrapChildren implements FragmentIterator.
func (p *prefixReplacingFragmentIterator) WrapChildren(wrap keyspan.WrapFn) {
	p.i = wrap(p.i)
}
