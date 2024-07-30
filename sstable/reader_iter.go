// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"fmt"
	"os"
	"sync"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
)

// Iterator iterates over an entire table of data.
type Iterator interface {
	base.InternalIterator

	// NextPrefix implements (base.InternalIterator).NextPrefix.
	NextPrefix(succKey []byte) *base.InternalKV

	SetCloseHook(fn func(i Iterator) error)
}

// Iterator positioning optimizations and singleLevelIterator and
// twoLevelIterator:
//
// An iterator is absolute positioned using one of the Seek or First or Last
// calls. After absolute positioning, there can be relative positioning done
// by stepping using Prev or Next.
//
// We implement optimizations below where an absolute positioning call can in
// some cases use the current position to do less work. To understand these,
// we first define some terms. An iterator is bounds-exhausted if the bounds
// (upper of lower) have been reached. An iterator is data-exhausted if it has
// the reached the end of the data (forward or reverse) in the sstable. A
// singleLevelIterator only knows a local-data-exhausted property since when
// it is used as part of a twoLevelIterator, the twoLevelIterator can step to
// the next lower-level index block.
//
// The bounds-exhausted property is tracked by
// singleLevelIterator.exhaustedBounds being +1 (upper bound reached) or -1
// (lower bound reached). The same field is reused by twoLevelIterator. Either
// may notice the exhaustion of the bound and set it. Note that if
// singleLevelIterator sets this property, it is not a local property (since
// the bound has been reached regardless of whether this is in the context of
// the twoLevelIterator or not).
//
// The data-exhausted property is tracked in a more subtle manner. We define
// two predicates:
// - partial-local-data-exhausted (PLDE):
//   i.data.isDataInvalidated() || !i.data.valid()
// - partial-global-data-exhausted (PGDE):
//   i.index.isDataInvalidated() || !i.index.valid() || i.data.isDataInvalidated() ||
//   !i.data.valid()
//
// PLDE is defined for a singleLevelIterator. PGDE is defined for a
// twoLevelIterator. Oddly, in our code below the singleLevelIterator does not
// know when it is part of a twoLevelIterator so it does not know when its
// property is local or global.
//
// Now to define data-exhausted:
// - Prerequisite: we must know that the iterator has been positioned and
//   i.err is nil.
// - bounds-exhausted must not be true:
//   If bounds-exhausted is true, we have incomplete knowledge of
//   data-exhausted since PLDE or PGDE could be true because we could have
//   chosen not to load index block or data block and figured out that the
//   bound is exhausted (due to block property filters filtering out index and
//   data blocks and going past the bound on the top level index block). Note
//   that if we tried to separate out the BPF case from others we could
//   develop more knowledge here.
// - PGDE is true for twoLevelIterator. PLDE is true if it is a standalone
//   singleLevelIterator. !PLDE or !PGDE of course imply that data-exhausted
//   is not true.
//
// An implication of the above is that if we are going to somehow utilize
// knowledge of data-exhausted in an optimization, we must not forget the
// existing value of bounds-exhausted since by forgetting the latter we can
// erroneously think that data-exhausted is true. Bug #2036 was due to this
// forgetting.
//
// Now to the two categories of optimizations we currently have:
// - Monotonic bounds optimization that reuse prior iterator position when
//   doing seek: These only work with !data-exhausted. We could choose to make
//   these work with data-exhausted but have not bothered because in the
//   context of a DB if data-exhausted were true, the DB would move to the
//   next file in the level. Note that this behavior of moving to the next
//   file is not necessarily true for L0 files, so there could be some benefit
//   in the future in this optimization. See the WARNING-data-exhausted
//   comments if trying to optimize this in the future.
// - TrySeekUsingNext optimizations: these work regardless of exhaustion
//   state.
//
// Implementation detail: In the code PLDE only checks that
// i.data.isDataInvalidated(). This narrower check is safe, since this is a
// subset of the set expressed by the OR expression. Also, it is not a
// de-optimization since whenever we exhaust the iterator we explicitly call
// i.data.invalidate(). PGDE checks i.index.isDataInvalidated() &&
// i.data.isDataInvalidated(). Again, this narrower check is safe, and not a
// de-optimization since whenever we exhaust the iterator we explicitly call
// i.index.invalidate() and i.data.invalidate(). The && is questionable -- for
// now this is a bit of defensive code. We should seriously consider removing
// it, since defensive code suggests we are not confident about our invariants
// (and if we are not confident, we need more invariant assertions, not
// defensive code).
//
// TODO(sumeer): remove the aforementioned defensive code.

var singleLevelIterPool = sync.Pool{
	New: func() interface{} {
		i := &singleLevelIterator{}
		// Note: this is a no-op if invariants are disabled or race is enabled.
		invariants.SetFinalizer(i, checkSingleLevelIterator)
		return i
	},
}

var twoLevelIterPool = sync.Pool{
	New: func() interface{} {
		i := &twoLevelIterator{}
		// Note: this is a no-op if invariants are disabled or race is enabled.
		invariants.SetFinalizer(i, checkTwoLevelIterator)
		return i
	},
}

func checkSingleLevelIterator(obj interface{}) {
	i := obj.(*singleLevelIterator)
	if p := i.data.Handle().Get(); p != nil {
		fmt.Fprintf(os.Stderr, "singleLevelIterator.data.handle is not nil: %p\n", p)
		os.Exit(1)
	}
	if p := i.index.Handle().Get(); p != nil {
		fmt.Fprintf(os.Stderr, "singleLevelIterator.index.handle is not nil: %p\n", p)
		os.Exit(1)
	}
}

func checkTwoLevelIterator(obj interface{}) {
	i := obj.(*twoLevelIterator)
	if p := i.data.Handle().Get(); p != nil {
		fmt.Fprintf(os.Stderr, "singleLevelIterator.data.handle is not nil: %p\n", p)
		os.Exit(1)
	}
	if p := i.index.Handle().Get(); p != nil {
		fmt.Fprintf(os.Stderr, "singleLevelIterator.index.handle is not nil: %p\n", p)
		os.Exit(1)
	}
}
