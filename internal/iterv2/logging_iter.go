// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package iterv2

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/treesteps"
)

// LoggingIter wraps an Iter and logs every positioning operation and its result
// to an internal buffer, useful for debugging and testing.
type LoggingIter struct {
	inner Iter
	buf   strings.Builder
}

var _ Iter = (*LoggingIter)(nil)

// NewLoggingIter creates a LoggingIter wrapping inner.
func NewLoggingIter(inner Iter) *LoggingIter {
	return &LoggingIter{inner: inner}
}

func (i *LoggingIter) logResult(kv *base.InternalKV) {
	if kv == nil {
		fmt.Fprint(&i.buf, ".")
	} else {
		fmt.Fprint(&i.buf, kv.K.String())
	}
	fmt.Fprint(&i.buf, "   ")
	fmt.Fprint(&i.buf, i.inner.Span().String())
	fmt.Fprintln(&i.buf)
}

// SeekGE implements Iter.
func (i *LoggingIter) SeekGE(key []byte, flags base.SeekGEFlags) *base.InternalKV {
	kv := i.inner.SeekGE(key, flags)
	fmt.Fprintf(&i.buf, "SeekGE(%q, %d) = ", key, flags)
	i.logResult(kv)
	return kv
}

// SeekPrefixGE implements Iter.
func (i *LoggingIter) SeekPrefixGE(prefix, key []byte, flags base.SeekGEFlags) *base.InternalKV {
	kv := i.inner.SeekPrefixGE(prefix, key, flags)
	fmt.Fprintf(&i.buf, "SeekPrefixGE(%q, %q, %d) = ", prefix, key, flags)
	i.logResult(kv)
	return kv
}

// SeekLT implements Iter.
func (i *LoggingIter) SeekLT(key []byte, flags base.SeekLTFlags) *base.InternalKV {
	kv := i.inner.SeekLT(key, flags)
	fmt.Fprintf(&i.buf, "SeekLT(%q, %d) = ", key, flags)
	i.logResult(kv)
	return kv
}

// First implements Iter.
func (i *LoggingIter) First() *base.InternalKV {
	kv := i.inner.First()
	fmt.Fprintf(&i.buf, "First() = ")
	i.logResult(kv)
	return kv
}

// Last implements Iter.
func (i *LoggingIter) Last() *base.InternalKV {
	kv := i.inner.Last()
	fmt.Fprintf(&i.buf, "Last() = ")
	i.logResult(kv)
	return kv
}

// Next implements Iter.
func (i *LoggingIter) Next() *base.InternalKV {
	kv := i.inner.Next()
	fmt.Fprintf(&i.buf, "Next() = ")
	i.logResult(kv)
	return kv
}

// NextPrefix implements Iter.
func (i *LoggingIter) NextPrefix(succKey []byte) *base.InternalKV {
	kv := i.inner.NextPrefix(succKey)
	fmt.Fprintf(&i.buf, "NextPrefix(%q) = ", succKey)
	i.logResult(kv)
	return kv
}

// Prev implements Iter.
func (i *LoggingIter) Prev() *base.InternalKV {
	kv := i.inner.Prev()
	fmt.Fprintf(&i.buf, "Prev() = ")
	i.logResult(kv)
	return kv
}

// Span implements Iter.
func (i *LoggingIter) Span() *Span {
	return i.inner.Span()
}

// Error implements Iter.
func (i *LoggingIter) Error() error {
	return i.inner.Error()
}

// Close implements Iter.
func (i *LoggingIter) Close() error {
	return i.inner.Close()
}

// SetBounds implements Iter.
func (i *LoggingIter) SetBounds(lower, upper []byte) {
	fmt.Fprintf(&i.buf, "SetBounds(%q, %q)\n", lower, upper)
	i.inner.SetBounds(lower, upper)
}

// SetContext implements Iter.
func (i *LoggingIter) SetContext(ctx context.Context) {
	i.inner.SetContext(ctx)
}

// String returns the accumulated log contents.
func (i *LoggingIter) String() string {
	return i.buf.String()
}

// TreeStepsNode implements treesteps.Node.
func (i *LoggingIter) TreeStepsNode() treesteps.NodeInfo {
	return i.inner.TreeStepsNode()
}
