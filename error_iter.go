// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
)

type errorIter struct {
	err error
}

// errorIter implements the base.InternalIterator interface.
var _ internalIterator = (*errorIter)(nil)

func (c *errorIter) SeekGE(key []byte, flags base.SeekGEFlags) *base.InternalKV {
	return nil
}

func (c *errorIter) SeekPrefixGE(prefix, key []byte, flags base.SeekGEFlags) *base.InternalKV {
	return c.SeekPrefixGEStrict(prefix, key, flags)
}

func (c *errorIter) SeekPrefixGEStrict(
	prefix, key []byte, flags base.SeekGEFlags,
) *base.InternalKV {
	return nil
}

func (c *errorIter) SeekLT(key []byte, flags base.SeekLTFlags) *base.InternalKV {
	return nil
}

func (c *errorIter) First() *base.InternalKV {
	return nil
}

func (c *errorIter) Last() *base.InternalKV {
	return nil
}

func (c *errorIter) Next() *base.InternalKV {
	return nil
}

func (c *errorIter) Prev() *base.InternalKV {
	return nil
}

func (c *errorIter) NextPrefix([]byte) *base.InternalKV {
	return nil
}

func (c *errorIter) Error() error {
	return c.err
}

func (c *errorIter) Close() error {
	return c.err
}

func (c *errorIter) String() string {
	return "error"
}

func (c *errorIter) SetBounds(lower, upper []byte) {}

func (c *errorIter) SetContext(_ context.Context) {}

type errorKeyspanIter struct {
	err error
}

// errorKeyspanIter implements the keyspan.FragmentIterator interface.
var _ keyspan.FragmentIterator = (*errorKeyspanIter)(nil)

func (i *errorKeyspanIter) SeekGE(key []byte) (*keyspan.Span, error) { return nil, i.err }
func (i *errorKeyspanIter) SeekLT(key []byte) (*keyspan.Span, error) { return nil, i.err }
func (i *errorKeyspanIter) First() (*keyspan.Span, error)            { return nil, i.err }
func (i *errorKeyspanIter) Last() (*keyspan.Span, error)             { return nil, i.err }
func (i *errorKeyspanIter) Next() (*keyspan.Span, error)             { return nil, i.err }
func (i *errorKeyspanIter) Prev() (*keyspan.Span, error)             { return nil, i.err }
func (i *errorKeyspanIter) Close() error                             { return i.err }
func (*errorKeyspanIter) String() string                             { return "error" }
func (*errorKeyspanIter) WrapChildren(wrap keyspan.WrapFn)           {}
