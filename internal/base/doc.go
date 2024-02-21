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
// passed by the merging iterator's client no longer transitively holds for the
// level iterators in all cases. The merging iterator assumes responsibility for
// ensuring that SeekPrefixGE is propagated to its consitutent iterators only
// when valid.
//
// Instances of TrySeekUsingNext optimizations and interactions:
//
// The [pebble.Iterator] has a SeekPrefixGE optimization: Above the
// [InternalIterator] interface, the [pebble.Iterator]'s SeekGE method detects
// consecutive seeks to monotonically increasing keys and examines the current
// key. If the iterator is already positioned appropriately (at a key ≥ the seek
// key), it elides the entire seek of the internal iterator.
//
// The pebble mergingIter does not perform any TrySeekUsingNext optimization
// itself, but it must preserve the TrySeekUsingNext contract in its calls to
// its child iterators because it passes the TrySeekUsingNext flag as-is to its
// child iterators. It can do this because it always translates calls to its SeekGE and
// SeekPrefixGE methods as equivalent calls to every child iterator. However
// there are a few subtleties:
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
//     of the handling of range deletions. Unequal application of TrySeekUsingNext
//     may cause range deletions that have already been skipped over in a level to
//     go unseen, despite being relevant to other levels that do not use
//     TrySeekUsingNext.
package base
