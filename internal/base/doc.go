// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package base defines fundamental types used across Pebble, including keys,
// iterators, etc.
//
// # Iterators
//
// The [InternalIterator] interface defines the iterator interface implemented
// by all iterators over point keys. Internal iterators are composed to form an
// "iterator stack," resulting in a single internal iterator (see mergingIter in
// the pebble package) that yields a merged view of the LSM.
//
// The SeekGE and SeekPrefixGE positioning methods take a set of flags
// [SeekGEFlags] allowing the caller to provide additional context to iterator
// implementations. The TrySeekUsingNext flag is set when the caller has
// knowledge that no action has been performed to move this iterator beyond the
// first key that would be found if this iterator were to honestly do the
// intended seek. This allows a class of optimizations where an internal
// iterator may avoid a full naive repositioning if the iterator is already
// at a proximate position. This also means every caller (including intermediary
// internal iterators within the iterator stack) must preserve this
// relationship.
//
// For example, if a range deletion deletes the remainder of a prefix, the
// merging iterator may be able to elide a SeekPrefixGE on level iterators
// beneath the range deletion. However in doing so, a TrySeekUsingNext flag
// passed by the merging iterator's client no longer transitively holds for
// subsequent seeks of child level iterators in all cases. The merging iterator
// assumes responsibility for ensuring that SeekPrefixGE is propagated to its
// consitutent iterators only when valid.
//
// Description of TrySeekUsingNext mechanics across the iterator stack:
//
// As the top-level entry point of user seeks, the [pebble.Iterator] is
// responsible for detecting when consecutive seeks move monotonically forward.
// It saves seek keys and compares consecutive seek keys to decide whether to
// propagate the TrySeekUsingNext flag to its [InternalIterator].
//
// The [pebble.Iterator] also has its own SeekPrefixGE optimization: Above the
// [InternalIterator] interface, the [pebble.Iterator]'s SeekGE method detects
// consecutive seeks to monotonically increasing keys and examines the current
// key. If the iterator is already positioned appropriately (at a key ≥ the seek
// key), it elides the entire seek of the internal iterator.
//
// The pebble mergingIter does not perform any TrySeekUsingNext optimization
// itself, but it must preserve the TrySeekUsingNext contract in its calls to
// its child iterators because it passes the TrySeekUsingNext flag as-is to its
// child iterators. It can do this because it always translates calls to its
// SeekGE and SeekPrefixGE methods as equivalent calls to every child iterator.
// However there are a few subtleties:
//
//   - In some cases the calls made to child iterators may only be equivalent
//     within the context of the iterator's visible sequence number. For example,
//     if a range deletion tombstone is present on a level, seek keys propagated
//     to lower-levelled child iterators may be adjusted without violating the
//     transitivity of the TrySeekUsingNext flag and its invariants so long as
//     the mergingIter is always reading state at the same visible sequence
//     number.
//   - The mergingIter takes care to avoid ever advancing a child iterator that's
//     already positioned beyond the current iteration prefix.
//   - When propagating TrySeekUsingNext to its child iterators, the mergingIter
//     must propagate it to all child iterators or none. This is required because
//     of the mergingIter's handling of range deletions. Unequal application of
//     TrySeekUsingNext may cause range deletions that have already been skipped
//     over in a level to go unseen, despite being relevant to other levels that
//     do not use TrySeekUsingNext.
//
// The pebble levelIter makes use of the TrySeekUsingNext flag to avoid a naive
// seek among a level's file metadatas. When TrySeekUsingNext is passed by the
// caller, the relevant key must fall within the current file or later.
//
// In-memory iterators arenaskl.Iterator and batchskl.Iterator make use of the
// TrySeekUsingNext flag, attempting a fixed number of Nexts before falling back
// to performing a seek using skiplist structures.
//
// The sstable iterators use the TrySeekUsingNext flag to avoid naive seeks
// through a table's index structures:
//   - If an iterator is already exhausted, either because there are no
//     subsequent point keys or because the upper bound has been reached, the
//     iterator uses TrySeekUsingNext to avoid any repositioning at all.
//   - Otherwise, a TrySeekUsingNext flag causes the sstable Iterator to Next
//     forward a capped number of times, stopping as soon as a key ≥ the seek key
//     is discovered.
package base
