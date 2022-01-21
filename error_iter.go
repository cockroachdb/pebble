// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
)

type errorIter struct {
	err error
}

// errorIter implements the base.InternalIterator interface.
var _ base.InternalIterator = (*errorIter)(nil)

// errorIter implements the keyspan.FragmentIterator interface.
var _ keyspan.FragmentIterator = (*errorIter)(nil)

func newErrorIter(err error) *errorIter {
	return &errorIter{err: err}
}

func (c *errorIter) SeekGE(key []byte, trySeekUsingNext bool) (*InternalKey, []byte) {
	return nil, nil
}

func (c *errorIter) SeekPrefixGE(
	prefix, key []byte, trySeekUsingNext bool,
) (*base.InternalKey, []byte) {
	return nil, nil
}

func (c *errorIter) SeekLT(key []byte) (*InternalKey, []byte) {
	return nil, nil
}

func (c *errorIter) First() (*InternalKey, []byte) {
	return nil, nil
}

func (c *errorIter) Last() (*InternalKey, []byte) {
	return nil, nil
}

func (c *errorIter) Next() (*InternalKey, []byte) {
	return nil, nil
}

func (c *errorIter) NextPrefix(int) (*InternalKey, []byte) {
	return nil, nil
}

func (c *errorIter) Prev() (*InternalKey, []byte) {
	return nil, nil
}

func (c *errorIter) End() []byte {
	return nil
}

func (c *errorIter) Current() keyspan.Span {
	return keyspan.Span{}
}

func (c *errorIter) Valid() bool {
	return false
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
