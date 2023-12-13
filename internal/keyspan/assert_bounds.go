// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
)

// AssertUserKeyBounds wraps an iterator and asserts that all spans are within
// the given bounds [lower, upper).
//
// Any panic will include the 'context' string.
func AssertUserKeyBounds(
	iter FragmentIterator, lower, upper []byte, cmp base.Compare, context string,
) FragmentIterator {
	return AssertBounds(iter, base.MakeSearchKey(lower), upper, cmp, context)
}

// AssertBounds wraps an iterator and asserts that all spans are within the
// given bounds [lower.UserKey, upper), and that all keys in a span that starts
// exactly at lower.UserKey are >= lower.
//
// The asymmetry here is due to fragment spans having exclusive end user keys.
//
// Any panic will include the 'context' string.
func AssertBounds(
	iter FragmentIterator, lower base.InternalKey, upper []byte, cmp base.Compare, context string,
) FragmentIterator {
	if context != "" {
		context = "; " + context
	}
	// We use a no-op filtering function (to reuse the Filter iterator code).
	filterFn := func(in *Span, out *Span) (keep bool) {
		switch startCmp := cmp(in.Start, lower.UserKey); {
		case startCmp < 0:
			panic(errors.AssertionFailedf("lower bound %q violated by span %s%s", lower.UserKey, in, context))
		case startCmp == 0:
			if len(in.Keys) > 0 && in.SmallestKey().Trailer > lower.Trailer {
				panic(errors.AssertionFailedf("lower bound %s violated by key %s%s", lower, in.SmallestKey(), context))
			}
		}
		if cmp(in.End, upper) > 0 {
			panic(errors.AssertionFailedf("upper bound %q violated by span %s%s", upper, in, context))
		}
		*out = *in
		return true
	}
	return Filter(iter, filterFn, cmp)
}
