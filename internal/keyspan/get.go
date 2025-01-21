// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import "github.com/cockroachdb/pebble/v2/internal/base"

// Get returns the newest span that contains the target key. If no span contains
// the target key, an empty span is returned. The iterator must contain
// fragmented spans: no span may overlap another.
//
// If an error occurs while seeking iter, a nil span and non-nil error is
// returned.
func Get(cmp base.Compare, iter FragmentIterator, key []byte) (*Span, error) {
	// NB: FragmentIterator.SeekGE moves the iterator to the first span covering
	// a key greater than or equal to the given key. This is equivalent to
	// seeking to the first span with an end key greater than the given key.
	iterSpan, err := iter.SeekGE(key)
	switch {
	case err != nil:
		return nil, err
	case iterSpan != nil && cmp(iterSpan.Start, key) > 0:
		return nil, nil
	default:
		return iterSpan, nil
	}
}
