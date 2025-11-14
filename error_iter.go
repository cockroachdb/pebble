// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/treesteps"
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

func (c *errorIter) TreeStepsNode() treesteps.NodeInfo {
	return treesteps.NodeInfof(c, "%T(%p)", c, c)
}

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
func (i *errorKeyspanIter) SetContext(ctx context.Context)           {}
func (i *errorKeyspanIter) Close()                                   {}
func (*errorKeyspanIter) String() string                             { return "error" }
func (*errorKeyspanIter) WrapChildren(wrap keyspan.WrapFn)           {}
func (i *errorKeyspanIter) TreeStepsNode() treesteps.NodeInfo {
	return treesteps.NodeInfof(i, "%T(%p)", i, i)
}
