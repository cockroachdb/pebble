// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package iterv2

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/treesteps"
)

// iterState tracks the positioning state of an iterator.
type iterState int8

const (
	iterUnpositioned    iterState = iota // initial state, or after SetBounds
	iterFwdValid                         // last forward op returned non-nil
	iterFwdExhausted                     // last forward op returned nil
	iterBwdValid                         // last backward op returned non-nil
	iterBwdExhausted                     // last backward op returned nil
	iterPrefixValid                      // SeekPrefixGE returned non-nil
	iterPrefixExhausted                  // prefix mode, returned nil
)

// IllegalOpError is the error returned when an illegal operation is performed.
type IllegalOpError struct {
	msg string
}

func (e *IllegalOpError) Error() string { return e.msg }

func illegalOpf(format string, args ...interface{}) *IllegalOpError {
	return &IllegalOpError{msg: fmt.Sprintf(format, args...)}
}

// OpCheckIter wraps an Iter and validates that operations are legal according
// to the InternalIterator and Iter contracts. On any illegal operation,
// OpCheckIter panics with an *IllegalOpError without altering the state of the
// iterator (this is used to generate sequences of valid operations in random
// tests).
type OpCheckIter struct {
	inner            Iter
	cmp              *base.Comparer
	state            iterState
	atBoundary       bool   // true if the last returned key was a boundary key
	lastKey          []byte // copy of the last returned user key (for NextPrefix validation)
	lower            []byte
	upper            []byte
	lastSeekIsPrefix bool // true if last seek was SeekPrefixGE
	// trySeekBound is used to validate TrySeekUsingNext. The zero value (Key=nil,
	// Boundary=Exclusive) indicates that TrySeekUsingNext is not legal. The
	// special value (Key=nil, Boundary=Inclusive) indicates that any key is legal
	// (which is the case after a First() call).
	trySeekBound base.UserKeyBoundary
}

var _ Iter = (*OpCheckIter)(nil)

// NewOpCheckIter creates an OpCheckIter wrapping inner.
func NewOpCheckIter(inner Iter, cmp *base.Comparer, lower, upper []byte) *OpCheckIter {
	return &OpCheckIter{inner: inner, cmp: cmp, lower: lower, upper: upper}
}

// trackKV updates atBoundary and lastKey from the result of an iterator
// operation.
func (c *OpCheckIter) trackKV(kv *base.InternalKV) {
	if kv != nil {
		c.atBoundary = kv.K.Kind() == base.InternalKeyKindSpanBoundary
		c.lastKey = append(c.lastKey[:0], kv.K.UserKey...)
	} else {
		c.atBoundary = false
		c.lastKey = c.lastKey[:0]
	}
}

func (c *OpCheckIter) fwdTransition(kv *base.InternalKV) *base.InternalKV {
	c.trackKV(kv)
	if kv != nil {
		c.state = iterFwdValid
	} else {
		c.state = iterFwdExhausted
	}
	return kv
}

func (c *OpCheckIter) bwdTransition(kv *base.InternalKV) *base.InternalKV {
	c.trackKV(kv)
	if kv != nil {
		c.state = iterBwdValid
	} else {
		c.state = iterBwdExhausted
	}
	return kv
}

func (c *OpCheckIter) prefixTransition(kv *base.InternalKV) *base.InternalKV {
	c.trackKV(kv)
	if kv != nil {
		c.state = iterPrefixValid
	} else {
		c.state = iterPrefixExhausted
	}
	return kv
}

// checkTrySeekUsingNext validates the TrySeekUsingNext flag on SeekGE/SeekPrefixGE.
// isPrefix indicates whether the current seek is a SeekPrefixGE.
func (c *OpCheckIter) checkTrySeekUsingNext(opName string, key []byte, isPrefix bool) {
	if isPrefix != c.lastSeekIsPrefix {
		panic(illegalOpf("%s(%s, TrySeekUsingNext): cannot mix SeekGE and SeekPrefixGE",
			opName, c.cmp.FormatKey(key)))
	}
	if c.trySeekBound.Key == nil {
		// Kind=Inclusive indicates any seek key is ok.
		if c.trySeekBound.Kind == base.Exclusive {
			panic(illegalOpf("%s(%s, TrySeekUsingNext): no valid seek boundary",
				opName, c.cmp.FormatKey(key)))
		}
	} else if cmpVal := c.cmp.Compare(key, c.trySeekBound.Key); cmpVal < 0 || (cmpVal == 0 && c.trySeekBound.Kind == base.Exclusive) {
		panic(illegalOpf("%s(%s, TrySeekUsingNext): key must be %s boundary key %s",
			opName, c.cmp.FormatKey(key),
			map[base.BoundaryKind]string{base.Inclusive: ">=", base.Exclusive: ">"}[c.trySeekBound.Kind],
			c.cmp.FormatKey(c.trySeekBound.Key)))
	}
}

// SeekGE implements Iter.
func (c *OpCheckIter) SeekGE(key []byte, flags base.SeekGEFlags) *base.InternalKV {
	if c.lower != nil && c.cmp.Compare(key, c.lower) < 0 {
		panic(illegalOpf("SeekGE(%s) with lower bound %s", c.cmp.FormatKey(key), c.lower))
	}
	if flags.TrySeekUsingNext() {
		c.checkTrySeekUsingNext("SeekGE", key, false /* isPrefix */)
	}
	c.lastSeekIsPrefix = false
	c.trySeekBound = base.UserKeyBoundary{Key: append(c.trySeekBound.Key[:0], key...), Kind: base.Inclusive}
	return c.fwdTransition(c.inner.SeekGE(key, flags))
}

// SeekPrefixGE implements Iter.
func (c *OpCheckIter) SeekPrefixGE(prefix, key []byte, flags base.SeekGEFlags) *base.InternalKV {
	if c.lower != nil && c.cmp.Compare(key, c.lower) < 0 {
		panic(illegalOpf("SeekPrefixGE(%s) with lower bound %s", c.cmp.FormatKey(key), c.lower))
	}
	if !bytes.Equal(prefix, c.cmp.Split.Prefix(key)) {
		panic(illegalOpf("SeekPrefixGE(%s) incorrect prefix %s", c.cmp.FormatKey(key), c.cmp.FormatKey(prefix)))
	}
	if flags.TrySeekUsingNext() {
		c.checkTrySeekUsingNext("SeekPrefixGE", key, true /* isPrefix */)
	}
	c.lastSeekIsPrefix = true
	c.trySeekBound = base.UserKeyBoundary{Key: append(c.trySeekBound.Key[:0], key...), Kind: base.Inclusive}
	return c.prefixTransition(c.inner.SeekPrefixGE(prefix, key, flags))
}

// SeekLT implements Iter.
func (c *OpCheckIter) SeekLT(key []byte, flags base.SeekLTFlags) *base.InternalKV {
	if c.upper != nil && c.cmp.Compare(key, c.upper) > 0 {
		panic(illegalOpf("SeekLT(%s) with upper bound %s", c.cmp.FormatKey(key), c.upper))
	}
	c.lastSeekIsPrefix = false
	c.trySeekBound = base.UserKeyBoundary{}
	return c.bwdTransition(c.inner.SeekLT(key, flags))
}

// First implements Iter.
func (c *OpCheckIter) First() *base.InternalKV {
	if c.lower != nil {
		panic(illegalOpf("First with lower bound set; use SeekGE instead"))
	}
	c.lastSeekIsPrefix = false
	c.trySeekBound = base.UserKeyBoundary{Kind: base.Inclusive}
	return c.fwdTransition(c.inner.First())
}

// Last implements Iter.
func (c *OpCheckIter) Last() *base.InternalKV {
	if c.upper != nil {
		panic(illegalOpf("Last with upper bound set; use SeekLT instead"))
	}
	c.lastSeekIsPrefix = false
	c.trySeekBound = base.UserKeyBoundary{}
	return c.bwdTransition(c.inner.Last())
}

// Next implements Iter.
func (c *OpCheckIter) Next() *base.InternalKV {
	switch c.state {
	case iterUnpositioned:
		panic(illegalOpf("Next called on unpositioned iterator"))
	case iterFwdExhausted:
		panic(illegalOpf("Next called on forward-exhausted iterator"))
	case iterPrefixExhausted:
		panic(illegalOpf("Next called on prefix-exhausted iterator"))
	}

	if len(c.lastKey) > 0 {
		c.trySeekBound = base.UserKeyBoundary{Key: append(c.trySeekBound.Key[:0], c.lastKey...), Kind: base.Exclusive}
	} else {
		// lastKey is empty, meaning the previous operation returned nil (which must
		// be backward exhaustion). After Next, the iterator is positioned at the
		// first key, so any forward seek is valid.
		c.trySeekBound = base.UserKeyBoundary{Kind: base.Inclusive}
	}

	switch c.state {
	case iterPrefixValid:
		return c.prefixTransition(c.inner.Next())
	default:
		return c.fwdTransition(c.inner.Next())
	}
}

// NextPrefix implements Iter.
func (c *OpCheckIter) NextPrefix(succKey []byte) *base.InternalKV {
	if c.state != iterFwdValid {
		panic(illegalOpf("NextPrefix called in state %d; requires forward-valid", c.state))
	}
	if c.atBoundary {
		panic(illegalOpf("NextPrefix called at boundary key"))
	}
	// Validate that succKey is the ImmediateSuccessor of the current key's prefix.
	prefix := c.cmp.Split.Prefix(c.lastKey)
	expectedSuccKey := c.cmp.ImmediateSuccessor(nil, prefix)
	if c.cmp.Compare(succKey, expectedSuccKey) != 0 {
		panic(illegalOpf("NextPrefix(%s) with incorrect succKey; expected %s (prefix of %s)",
			c.cmp.FormatKey(succKey), c.cmp.FormatKey(expectedSuccKey), c.cmp.FormatKey(c.lastKey)))
	}
	c.trySeekBound = base.UserKeyBoundary{Key: append(c.trySeekBound.Key[:0], succKey...), Kind: base.Inclusive}
	return c.fwdTransition(c.inner.NextPrefix(succKey))
}

// Prev implements Iter.
func (c *OpCheckIter) Prev() *base.InternalKV {
	switch c.state {
	case iterUnpositioned:
		panic(illegalOpf("Prev called on unpositioned iterator"))
	case iterBwdExhausted:
		panic(illegalOpf("Prev called on backward-exhausted iterator"))
	case iterPrefixValid:
		panic(illegalOpf("Prev called in prefix iteration mode"))
	case iterPrefixExhausted:
		panic(illegalOpf("Prev called on prefix-exhausted iterator"))
	default:
		c.trySeekBound = base.UserKeyBoundary{}
		return c.bwdTransition(c.inner.Prev())
	}
}

// Span implements Iter.
func (c *OpCheckIter) Span() *Span {
	return c.inner.Span()
}

// Error implements Iter.
func (c *OpCheckIter) Error() error {
	return c.inner.Error()
}

// Close implements Iter.
func (c *OpCheckIter) Close() error {
	return c.inner.Close()
}

// SetBounds implements Iter.
func (c *OpCheckIter) SetBounds(lower, upper []byte) {
	c.state = iterUnpositioned
	c.atBoundary = false
	c.lastKey = c.lastKey[:0]
	c.lower = lower
	c.upper = upper
	c.trySeekBound = base.UserKeyBoundary{}
	c.inner.SetBounds(lower, upper)
}

// SetContext implements Iter.
func (c *OpCheckIter) SetContext(ctx context.Context) {
	c.inner.SetContext(ctx)
}

// String implements fmt.Stringer.
func (c *OpCheckIter) String() string {
	return fmt.Sprintf("op-check(%s)", c.inner.String())
}

// TreeStepsNode implements treesteps.Node.
func (c *OpCheckIter) TreeStepsNode() treesteps.NodeInfo {
	return treesteps.NodeInfof(c, "OpCheckIter")
}
