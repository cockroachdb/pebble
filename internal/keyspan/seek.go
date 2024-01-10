// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import "github.com/cockroachdb/pebble/internal/base"

// SeekLE seeks to the span that contains or is before the target key. If an
// error occurs while seeking iter, a nil span and non-nil error is returned.
func SeekLE(cmp base.Compare, iter FragmentIterator, key []byte) (*Span, error) {
	// Seek to the smallest span that contains a key ≥ key. If some span
	// contains the key `key`, SeekGE will return it.
	iterSpan := iter.SeekGE(key)
	if iterSpan == nil {
		if err := iter.Error(); err != nil {
			return nil, err
		}
		// Fallthrough to Prev()-ing.
	} else if cmp(key, iterSpan.Start) >= 0 {
		return iterSpan, nil
	}

	// No span covers exactly `key`. Step backwards to move onto the largest
	// span < key.
	iterSpan = iter.Prev()
	if iterSpan == nil {
		// NB: iter.Error() may be nil or non-nil.
		return iterSpan, iter.Error()
	}
	return iterSpan, nil
}
