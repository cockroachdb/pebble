// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

type errorIter struct {
	err error
}

var _ internalIterator = (*errorIter)(nil)

func newErrorIter(err error) *errorIter {
	return &errorIter{err: err}
}

func (c *errorIter) SeekGE(key []byte) (*InternalKey, []byte) {
	return nil, nil
}

func (c *errorIter) SeekPrefixGE(prefix, key []byte) (*InternalKey, []byte) {
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

func (c *errorIter) Prev() (*InternalKey, []byte) {
	return nil, nil
}

func (c *errorIter) Key() *InternalKey {
	return nil
}

func (c *errorIter) Value() []byte {
	return nil
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

func (c *errorIter) SetBounds(lower, upper []byte) {}
