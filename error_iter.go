// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import "github.com/petermattis/pebble/db"

type errorIter struct {
	err error
}

var _ internalIterator = (*errorIter)(nil)

func newErrorIter(err error) *errorIter {
	return &errorIter{err: err}
}

func (c *errorIter) SeekGE(key []byte) (*db.InternalKey, []byte) {
	return nil, nil
}

func (c *errorIter) SeekPrefixGE(prefix, key []byte) (*db.InternalKey, []byte) {
	return nil, nil
}

func (c *errorIter) SeekLT(key []byte) (*db.InternalKey, []byte) {
	return nil, nil
}

func (c *errorIter) First() (*db.InternalKey, []byte) {
	return nil, nil
}

func (c *errorIter) Last() (*db.InternalKey, []byte) {
	return nil, nil
}

func (c *errorIter) Next() (*db.InternalKey, []byte) {
	return nil, nil
}

func (c *errorIter) Prev() (*db.InternalKey, []byte) {
	return nil, nil
}

func (c *errorIter) Key() *db.InternalKey {
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
