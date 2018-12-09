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

func (c *errorIter) SeekGE(key []byte) bool {
	return false
}

func (c *errorIter) SeekLT(key []byte) bool {
	return false
}

func (c *errorIter) First() bool {
	return false
}

func (c *errorIter) Last() bool {
	return false
}

func (c *errorIter) Next() bool {
	return false
}

func (c *errorIter) Prev() bool {
	return false
}

func (c *errorIter) Key() db.InternalKey {
	return db.InvalidInternalKey
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
