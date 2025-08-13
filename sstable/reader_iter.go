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
	"github.com/cockroachdb/pebble/sstable/blockiter"
	"github.com/cockroachdb/pebble/sstable/colblk"
	"github.com/cockroachdb/pebble/sstable/rowblk"
)

// dataBlockIterator extends the blockiter.Data interface with a constraint
// that the implementing type be a pointer to a type I.
//
// dataBlockIterator requires that the type be a pointer to its type parameter,
// D, to allow sstable iterators embed the block iterator within its struct. See
// this example from the Go generics proposal:
// https://go.googlesource.com/proposal/+/refs/heads/master/design/43651-type-parameters.md#pointer-method-example
type dataBlockIterator[D any] interface {
	blockiter.Data

	*D // non-interface type constraint element
}

// indexBlockIterator extends the blockiter.Index interface with a constraint
// that the implementing type be a pointer to a type I.
//
// indexBlockIterator requires that the type be a pointer to its type parameter,
// I, to allow sstable iterators embed the block iterator within its struct. See
// this example from the Go generics proposal:
// https://go.googlesource.com/proposal/+/refs/heads/master/design/43651-type-parameters.md#pointer-method-example
type indexBlockIterator[I any] interface {
	blockiter.Index

	*I // non-interface type constraint element
}

// Iterator iterates over an entire table of data.
type Iterator interface {
	base.InternalIterator

	// NextPrefix implements (base.InternalIterator).NextPrefix.
	NextPrefix(succKey []byte) *base.InternalKV

	// SetCloseHook sets a function that will be called when the iterator is
	// closed. This is used by the file cache to release the reference count on
	// the open sstable.Reader when the iterator is closed.
	SetCloseHook(func())
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
//   i.data.IsDataInvalidated() || !i.data.Valid()
// - partial-global-data-exhausted (PGDE):
//   i.index.IsDataInvalidated() || !i.index.Valid() || i.data.IsDataInvalidated() ||
//   !i.data.Valid()
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
// i.data.IsDataInvalidated(). This narrower check is safe, since this is a
// subset of the set expressed by the OR expression. Also, it is not a
// de-optimization since whenever we exhaust the iterator we explicitly call
// i.data.Invalidate(). PGDE checks i.index.IsDataInvalidated() &&
// i.data.IsDataInvalidated(). Again, this narrower check is safe, and not a
// de-optimization since whenever we exhaust the iterator we explicitly call
// i.index.Invalidate() and i.data.Invalidate(). The && is questionable -- for
// now this is a bit of defensive code. We should seriously consider removing
// it, since defensive code suggests we are not confident about our invariants
// (and if we are not confident, we need more invariant assertions, not
// defensive code).
//
// TODO(sumeer): remove the aforementioned defensive code.

type (
	singleLevelIteratorRowBlocks    = singleLevelIterator[rowblk.IndexIter, *rowblk.IndexIter, rowblk.Iter, *rowblk.Iter]
	twoLevelIteratorRowBlocks       = twoLevelIterator[rowblk.IndexIter, *rowblk.IndexIter, rowblk.Iter, *rowblk.Iter]
	singleLevelIteratorColumnBlocks = singleLevelIterator[colblk.IndexIter, *colblk.IndexIter, colblk.DataBlockIter, *colblk.DataBlockIter]
	twoLevelIteratorColumnBlocks    = twoLevelIterator[colblk.IndexIter, *colblk.IndexIter, colblk.DataBlockIter, *colblk.DataBlockIter]
)

var (
	singleLevelIterRowBlockPool    sync.Pool // *singleLevelIteratorRowBlocks
	twoLevelIterRowBlockPool       sync.Pool // *twoLevelIteratorRowBlocks
	singleLevelIterColumnBlockPool sync.Pool // *singleLevelIteratorColumnBlocks
	twoLevelIterColumnBlockPool    sync.Pool // *twoLevelIteratorColumnBlocks
)

func init() {
	singleLevelIterRowBlockPool = sync.Pool{
		New: func() interface{} {
			i := &singleLevelIteratorRowBlocks{pool: &singleLevelIterRowBlockPool}
			if invariants.UseFinalizers {
				invariants.SetFinalizer(i, checkSingleLevelIterator[rowblk.IndexIter, *rowblk.IndexIter, rowblk.Iter, *rowblk.Iter])
			}
			return i
		},
	}
	twoLevelIterRowBlockPool = sync.Pool{
		New: func() interface{} {
			i := &twoLevelIteratorRowBlocks{pool: &twoLevelIterRowBlockPool}
			if invariants.UseFinalizers {
				invariants.SetFinalizer(i, checkTwoLevelIterator[rowblk.IndexIter, *rowblk.IndexIter, rowblk.Iter, *rowblk.Iter])
			}
			return i
		},
	}
	singleLevelIterColumnBlockPool = sync.Pool{
		New: func() interface{} {
			i := &singleLevelIteratorColumnBlocks{
				pool: &singleLevelIterColumnBlockPool,
			}
			if invariants.UseFinalizers {
				invariants.SetFinalizer(i, checkSingleLevelIterator[colblk.IndexIter, *colblk.IndexIter, colblk.DataBlockIter, *colblk.DataBlockIter])
			}
			return i
		},
	}
	twoLevelIterColumnBlockPool = sync.Pool{
		New: func() interface{} {
			i := &twoLevelIteratorColumnBlocks{
				pool: &twoLevelIterColumnBlockPool,
			}
			if invariants.UseFinalizers {
				invariants.SetFinalizer(i, checkTwoLevelIterator[colblk.IndexIter, *colblk.IndexIter, colblk.DataBlockIter, *colblk.DataBlockIter])
			}
			return i
		},
	}
}

func checkSingleLevelIterator[I any, PI indexBlockIterator[I], D any, PD dataBlockIterator[D]](
	obj interface{},
) {
	i := obj.(*singleLevelIterator[I, PI, D, PD])
	if h := PD(&i.data).Handle(); h.Valid() {
		fmt.Fprintf(os.Stderr, "singleLevelIterator.data.handle is not nil: %#v\n", h)
		os.Exit(1)
	}
	if h := PI(&i.index).Handle(); h.Valid() {
		fmt.Fprintf(os.Stderr, "singleLevelIterator.index.handle is not nil: %#v\n", h)
		os.Exit(1)
	}
}

func checkTwoLevelIterator[I any, PI indexBlockIterator[I], D any, PD dataBlockIterator[D]](
	obj interface{},
) {
	i := obj.(*twoLevelIterator[I, PI, D, PD])
	if h := PD(&i.secondLevel.data).Handle(); h.Valid() {
		fmt.Fprintf(os.Stderr, "singleLevelIterator.data.handle is not nil: %#v\n", h)
		os.Exit(1)
	}
	if h := PI(&i.secondLevel.index).Handle(); h.Valid() {
		fmt.Fprintf(os.Stderr, "singleLevelIterator.index.handle is not nil: %#v\n", h)
		os.Exit(1)
	}
}
