// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"slices"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
)

type prefixReplacingIterator struct {
	i               Iterator
	cmp             base.Compare
	contentPrefix   []byte
	syntheticPrefix []byte

	// keyInRange is a valid key in the logical range that has the syntheticPrefix.
	// When an argument to a seek function does not have the syntheticPrefix,
	// keyInRange is used to determine if the argument key is before or after the
	// range of keys produced by the iterator.
	keyInRange []byte

	// arg and arg2 are buffers that are used to avoid allocations when rewriting
	// keys that are provided as arguments. They always start with contentPrefix.
	arg, arg2 []byte

	// res is used to avoid allocations when rewriting result keys. It always
	// starts with syntheticPrefix.
	res InternalKey
	err error
	// empty is set after a seek operation that returns no keys.
	empty bool
}

func errInputPrefixMismatch() error {
	return errors.AssertionFailedf("key argument does not have prefix required for replacement")
}

func errOutputPrefixMismatch() error {
	return errors.AssertionFailedf("key returned does not have prefix required for replacement")
}

var _ Iterator = (*prefixReplacingIterator)(nil)

// newPrefixReplacingIterator wraps an iterator over keys that have
// `contentPrefix` in an iterator that will make them appear to have
// `syntheticPrefix`. Every key produced by the underlying iterator must have
// `contentPrefix`.
//
// keyInRange is a valid key that starts with syntheticPrefix. When a seek
// function is called with a key that does not start with syntheticPrefix,
// keyInRange is used to determine if the key is before or after the synthetic
// prefix range.
//
// INVARIANT: len(syntheticPrefix) > 0 && keyInRange stars with syntheticPrefix.
func newPrefixReplacingIterator(
	i Iterator, contentPrefix, syntheticPrefix []byte, keyInRange []byte, cmp base.Compare,
) Iterator {
	if invariants.Enabled {
		if len(syntheticPrefix) == 0 {
			panic("newPrefixReplacingIterator called without synthetic prefix")
		}
		if !bytes.HasPrefix(keyInRange, syntheticPrefix) {
			panic(fmt.Sprintf("keyInRange %q does not have synthetic prefix %q", keyInRange, syntheticPrefix))
		}
	}
	return &prefixReplacingIterator{
		i:               i,
		cmp:             cmp,
		contentPrefix:   contentPrefix,
		syntheticPrefix: syntheticPrefix,
		keyInRange:      keyInRange,
		arg:             slices.Clone(contentPrefix),
		arg2:            slices.Clone(contentPrefix),
		res:             InternalKey{UserKey: slices.Clone(syntheticPrefix)},
	}
}

func (p *prefixReplacingIterator) SetContext(ctx context.Context) {
	p.i.SetContext(ctx)
}

func (p *prefixReplacingIterator) rewriteArg(key []byte) []byte {
	p.arg = append(p.arg[:len(p.contentPrefix)], key[len(p.syntheticPrefix):]...)
	return p.arg
}

func (p *prefixReplacingIterator) rewriteArg2(key []byte) []byte {
	p.arg2 = append(p.arg2[:len(p.contentPrefix)], key[len(p.syntheticPrefix):]...)
	return p.arg2
}

func (p *prefixReplacingIterator) rewriteResult(
	k *InternalKey, v base.LazyValue,
) (*InternalKey, base.LazyValue) {
	if k == nil {
		return k, v
	}
	if !bytes.HasPrefix(k.UserKey, p.contentPrefix) {
		p.err = errOutputPrefixMismatch()
		if invariants.Enabled {
			panic(p.err)
		}
		return nil, base.LazyValue{}
	}
	p.res.Trailer = k.Trailer
	p.res.UserKey = append(p.res.UserKey[:len(p.syntheticPrefix)], k.UserKey[len(p.contentPrefix):]...)
	return &p.res, v
}

// SeekGE implements the Iterator interface.
func (p *prefixReplacingIterator) SeekGE(
	key []byte, flags base.SeekGEFlags,
) (*InternalKey, base.LazyValue) {
	p.empty = false
	if !bytes.HasPrefix(key, p.syntheticPrefix) {
		if p.cmp(key, p.keyInRange) > 0 {
			p.empty = true
			return nil, base.LazyValue{}
		}
		// Key must be before the range; use First instead.
		return p.rewriteResult(p.i.First())
	}
	return p.rewriteResult(p.i.SeekGE(p.rewriteArg(key), flags))
}

// SeekPrefixGE implements the Iterator interface.
func (p *prefixReplacingIterator) SeekPrefixGE(
	prefix, key []byte, flags base.SeekGEFlags,
) (*InternalKey, base.LazyValue) {
	p.empty = false
	if invariants.Enabled && !bytes.HasPrefix(key, prefix) {
		panic(fmt.Sprintf("key %q does not have prefix %q", key, prefix))
	}
	if !bytes.HasPrefix(prefix, p.syntheticPrefix) {
		// We never produce keys with this prefix; we can return nil.
		p.empty = true
		return nil, base.LazyValue{}
	}
	return p.rewriteResult(p.i.SeekPrefixGE(p.rewriteArg2(prefix), p.rewriteArg(key), flags))
}

// SeekLT implements the Iterator interface.
func (p *prefixReplacingIterator) SeekLT(
	key []byte, flags base.SeekLTFlags,
) (*InternalKey, base.LazyValue) {
	p.empty = false
	if !bytes.HasPrefix(key, p.syntheticPrefix) {
		if p.cmp(key, p.keyInRange) < 0 {
			p.empty = true
			return nil, base.LazyValue{}
		}
		// Key must be after the range. Use Last instead.
		return p.rewriteResult(p.i.Last())
	}
	return p.rewriteResult(p.i.SeekLT(p.rewriteArg(key), flags))
}

// First implements the Iterator interface.
func (p *prefixReplacingIterator) First() (*InternalKey, base.LazyValue) {
	p.empty = false
	return p.rewriteResult(p.i.First())
}

// Last implements the Iterator interface.
func (p *prefixReplacingIterator) Last() (*InternalKey, base.LazyValue) {
	p.empty = false
	return p.rewriteResult(p.i.Last())
}

// Next implements the Iterator interface.
func (p *prefixReplacingIterator) Next() (*InternalKey, base.LazyValue) {
	if p.empty {
		return nil, base.LazyValue{}
	}
	return p.rewriteResult(p.i.Next())
}

// NextPrefix implements the Iterator interface.
func (p *prefixReplacingIterator) NextPrefix(succKey []byte) (*InternalKey, base.LazyValue) {
	if p.empty {
		return nil, base.LazyValue{}
	}
	return p.rewriteResult(p.i.NextPrefix(p.rewriteArg(succKey)))
}

// Prev implements the Iterator interface.
func (p *prefixReplacingIterator) Prev() (*InternalKey, base.LazyValue) {
	if p.empty {
		return nil, base.LazyValue{}
	}
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
	if lower != nil {
		lower = append([]byte{}, p.rewriteArg(lower)...)
	}
	if upper != nil {
		upper = append([]byte{}, p.rewriteArg(upper)...)
	}
	p.i.SetBounds(lower, upper)
}

func (p *prefixReplacingIterator) MaybeFilteredKeys() bool {
	return p.i.MaybeFilteredKeys()
}

// String implements the Iterator interface.
func (p *prefixReplacingIterator) String() string {
	return fmt.Sprintf("%s [%s->%s]", p.i.String(), hex.EncodeToString(p.contentPrefix), hex.EncodeToString(p.syntheticPrefix))
}

func (p *prefixReplacingIterator) SetCloseHook(fn func(i Iterator) error) {
	p.i.SetCloseHook(fn)
}

type prefixReplacingFragmentIterator struct {
	i   keyspan.FragmentIterator
	cmp base.Compare

	contentPrefix   []byte
	syntheticPrefix []byte

	// keyInRange is a valid key in the logical range that has the syntheticPrefix.
	// When an argument to a seek function does not have the syntheticPrefix,
	// keyInRange is used to determine if the argument key is before or after the
	// range of keys produced by the iterator.
	keyInRange []byte

	arg        []byte
	out1, out2 []byte
}

// newPrefixReplacingFragmentIterator wraps a FragmentIterator over some reader
// that contains range keys in some key span to make those range keys appear to
// be remapped into some other key-span.
func newPrefixReplacingFragmentIterator(
	i keyspan.FragmentIterator,
	contentPrefix, syntheticPrefix []byte,
	keyInRange []byte,
	cmp base.Compare,
) keyspan.FragmentIterator {
	return &prefixReplacingFragmentIterator{
		i:               i,
		cmp:             cmp,
		contentPrefix:   contentPrefix,
		syntheticPrefix: syntheticPrefix,
		keyInRange:      keyInRange,
		arg:             slices.Clone(contentPrefix),
		out1:            slices.Clone(syntheticPrefix),
		out2:            slices.Clone(syntheticPrefix),
	}
}

func (p *prefixReplacingFragmentIterator) rewriteArg(key []byte) ([]byte, error) {
	if !bytes.HasPrefix(key, p.syntheticPrefix) {
		return nil, errInputPrefixMismatch()
	}
	p.arg = append(p.arg[:len(p.contentPrefix)], key[len(p.syntheticPrefix):]...)
	return p.arg, nil
}

func (p *prefixReplacingFragmentIterator) rewriteSpan(
	sp *keyspan.Span, err error,
) (*keyspan.Span, error) {
	if sp == nil {
		return sp, err
	}
	if !bytes.HasPrefix(sp.Start, p.contentPrefix) || !bytes.HasPrefix(sp.End, p.contentPrefix) {
		return nil, errOutputPrefixMismatch()
	}
	sp.Start = append(p.out1[:len(p.syntheticPrefix)], sp.Start[len(p.contentPrefix):]...)
	sp.End = append(p.out2[:len(p.syntheticPrefix)], sp.End[len(p.contentPrefix):]...)
	return sp, nil
}

// SeekGE implements the FragmentIterator interface.
func (p *prefixReplacingFragmentIterator) SeekGE(key []byte) (*keyspan.Span, error) {
	if !bytes.HasPrefix(key, p.syntheticPrefix) {
		if p.cmp(key, p.keyInRange) > 0 {
			return nil, nil
		}
		// Key must be before the range; use First instead.
		return p.First()
	}
	rewrittenKey, err := p.rewriteArg(key)
	if err != nil {
		return nil, err
	}
	return p.rewriteSpan(p.i.SeekGE(rewrittenKey))
}

// SeekLT implements the FragmentIterator interface.
func (p *prefixReplacingFragmentIterator) SeekLT(key []byte) (*keyspan.Span, error) {
	if !bytes.HasPrefix(key, p.syntheticPrefix) {
		if p.cmp(key, p.keyInRange) < 0 {
			return nil, nil
		}
		// Key must be after the range; use Last instead.
		return p.Last()
	}
	rewrittenKey, err := p.rewriteArg(key)
	if err != nil {
		return nil, err
	}
	return p.rewriteSpan(p.i.SeekLT(rewrittenKey))
}

// First implements the FragmentIterator interface.
func (p *prefixReplacingFragmentIterator) First() (*keyspan.Span, error) {
	return p.rewriteSpan(p.i.First())
}

// Last implements the FragmentIterator interface.
func (p *prefixReplacingFragmentIterator) Last() (*keyspan.Span, error) {
	return p.rewriteSpan(p.i.Last())
}

// Close implements the FragmentIterator interface.
func (p *prefixReplacingFragmentIterator) Next() (*keyspan.Span, error) {
	return p.rewriteSpan(p.i.Next())
}

// Prev implements the FragmentIterator interface.
func (p *prefixReplacingFragmentIterator) Prev() (*keyspan.Span, error) {
	return p.rewriteSpan(p.i.Prev())
}

// Close implements the FragmentIterator interface.
func (p *prefixReplacingFragmentIterator) Close() error {
	return p.i.Close()
}

// WrapChildren implements FragmentIterator.
func (p *prefixReplacingFragmentIterator) WrapChildren(wrap keyspan.WrapFn) {
	p.i = wrap(p.i)
}
