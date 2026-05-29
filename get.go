// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"io"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
)

// Get gets the value for the given key. It returns ErrNotFound if the DB does
// not contain the key.
//
// The caller should not modify the contents of the returned slice, but it is
// safe to modify the contents of the argument after Get returns. The returned
// slice will remain valid until the returned Closer is closed. On success, the
// caller MUST call closer.Close() or a memory leak will occur.
func (d *DB) Get(key []byte) ([]byte, io.Closer, error) {
	return d.getInternal(key, nil /* batch */, nil /* snapshot */)
}

func (d *DB) getInternal(key []byte, b *Batch, s *Snapshot) ([]byte, io.Closer, error) {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}
	var seqNum base.SeqNum
	if s != nil {
		seqNum = s.seqNum
	} else {
		seqNum = d.mu.versions.visibleSeqNum.Load()
	}
	iter := d.newIter(
		context.Background(),
		b,
		newIterOpts{snapshot: snapshotIterOpts{seqNum: seqNum}},
		&IterOptions{Category: categoryGet},
	)
	if !iter.SeekPrefixGE(key) || !d.opts.Comparer.Equal(iter.Key(), key) {
		if err := iter.Close(); err != nil {
			return nil, nil, err
		}
		return nil, nil, ErrNotFound
	}
	val, err := iter.ValueAndErr()
	if err != nil {
		return nil, nil, errors.CombineErrors(err, iter.Close())
	}
	return val, iter, nil
}
