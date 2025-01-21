// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import "github.com/cockroachdb/pebble/v2/internal/base"

// SeekLE seeks to the span that contains or is before the target key. If an
// error occurs while seeking iter, a nil span and non-nil error is returned.
func SeekLE(cmp base.Compare, iter FragmentIterator, key []byte) (*Span, error) {
	// Seek to the smallest span that contains a key ≥ key. If some span
	// contains the key `key`, SeekGE will return it.
	iterSpan, err := iter.SeekGE(key)
	if err != nil {
		return nil, err
	}
	if iterSpan != nil && cmp(key, iterSpan.Start) >= 0 {
		return iterSpan, nil
	}
	// No span covers exactly `key`. Step backwards to move onto the largest
	// span < key.
	return iter.Prev()
}
