// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangedel

import (
	"sync"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
)

// Encode takes a Span containing only range deletions. It invokes the provided
// closure with the encoded internal keys that represent the Span's state. The
// keys and values passed to emit are only valid until the closure returns.  If
// emit returns an error, Encode stops and returns the error.
func Encode(s keyspan.Span, emit func(k base.InternalKey, v []byte) error) error {
	for _, k := range s.Keys {
		if k.Kind() != base.InternalKeyKindRangeDelete {
			return base.CorruptionErrorf("pebble: rangedel.Encode cannot encode %s key", k.Kind())
		}
		ik := base.InternalKey{
			UserKey: s.Start,
			Trailer: k.Trailer,
		}
		if err := emit(ik, s.End); err != nil {
			return err
		}
	}
	return nil
}

// Decode takes an internal key pair encoding a range deletion and returns a
// decoded keyspan containing the key. If keysDst is provided, the key will be
// appended to keysDst, avoiding an allocation.
func Decode(ik base.InternalKey, v []byte, keysDst []keyspan.Key) keyspan.Span {
	return keyspan.Span{
		Start: ik.UserKey,
		End:   v,
		Keys: append(keysDst, keyspan.Key{
			Trailer: ik.Trailer,
		}),
	}
}

// DecodeIntoSpan decodes an internal key pair encoding a range deletion and
// appends a key to the given span. The start and end keys must match those in
// the span.
func DecodeIntoSpan(cmp base.Compare, ik base.InternalKey, v []byte, s *keyspan.Span) error {
	// This function should only be called when ik.UserKey matches the Start of
	// the span we already have. If this is not the case, it is a bug in the
	// calling code.
	if invariants.Enabled && cmp(s.Start, ik.UserKey) != 0 {
		return base.AssertionFailedf("DecodeIntoSpan called with different start key")
	}
	// The value can come from disk or from the user, so we want to check the end
	// key in all builds.
	if cmp(s.End, v) != 0 {
		return base.CorruptionErrorf("pebble: corrupt range key fragmentation")
	}
	s.Keys = append(s.Keys, keyspan.Key{Trailer: ik.Trailer})
	return nil
}

// Interleave takes a point iterator and a range deletion iterator, returning an
// iterator that interleaves range deletion boundary keys at the maximal
// sequence number among the stream of point keys.
//
// In addition, Interleave returns a function that may be used to retrieve the
// range tombstone overlapping the current iterator position, if any. If range
// deletion iterator is nil, the returned function is nil.
//
// The returned iterator must only be closed once.
func Interleave(
	comparer *base.Comparer, iter base.InternalIterator, rangeDelIter keyspan.FragmentIterator,
) (base.InternalIterator, func() *keyspan.Span) {
	// If there is no range deletion iterator, don't bother using an interleaving
	// iterator. We can return iter verbatim and a func that unconditionally
	// returns nil.
	if rangeDelIter == nil {
		return iter, nil
	}

	ii := interleavingIterPool.Get().(*interleavingIter)
	ii.Init(comparer, iter, rangeDelIter, keyspan.InterleavingIterOpts{
		InterleaveEndKeys: true,
	})
	return ii, ii.Span
}

var interleavingIterPool = sync.Pool{
	New: func() interface{} {
		return &interleavingIter{}
	},
}

type interleavingIter struct {
	keyspan.InterleavingIter
}

// Close closes the interleaving iterator and returns the interleaving iterator
// to the pool.
func (i *interleavingIter) Close() error {
	err := i.InterleavingIter.Close()
	*i = interleavingIter{}
	interleavingIterPool.Put(i)
	return err
}
