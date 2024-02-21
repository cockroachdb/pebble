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
// implementations.
//
// ## TrySeekUsingNext
//
// The TrySeekUsingNext flag is set when the caller has knowledge that no action
// has been performed to move this iterator beyond the first key that would be
// found if this iterator were to honestly do the intended seek. This allows a
// class of optimizations where an internal iterator may avoid a full naive
// repositioning if the iterator is already at a proximate position.
//
// Let [s] be the seek key of an InternalIterator.Seek[Prefix]GE operation with
// TrySeekSeekUsingNext()=true on an internal iterator positioned at the key k_i
// among k_0, k_1, ..., k_n keys known to the internal iterator. We maintain the
// following universal invariants:
//
// U1: For all the internal iterators' keys k_j st j<i [all keys before its
// current key k_i], one or more of the following hold:
//
//   - (a) k_j < s
//   - (b) k_j is invisible at the iterator's sequence number
//   - (c) k_j is deleted by a visible range tombstone
//   - (d) k_j is deleted by a visible point tombstone
//   - (e) k_j is excluded by a block property filter, range key masking, etc.
//
// This contract must hold for every call passing TrySeekUsingNext, including
// calls within the interior of the iterator stack. It's the responsibility of
// each caller to preserve this relationship. Intuitively, the caller is
// promising that nothing behind the iterator's current position is relevant and
// the callee may search in the forward direction only. Note that there is no
// universal responsibility on the callee's behavior outside the ordinary seek
// operation's contract, and the callee may freely ignore the flag entirely.
//
// In addition to the universal invariants, the merging iterator and level
// iterator impose additional invariants on TrySeekUsingNext due to their
// responsibilities of applying range deletions and surfacing files' range
// deletions respectively.
//
// Let [s] be the seek key of a Seek[Prefix]GE operation on a merging iterator,
// and [s2] be the seek key of the resulting Seek[Prefix]GE operation on a level
// iterator at level l_i among levels l_0, l_1, ..., l_n, positioned at the file
// f_i among files f_0, f_1, ..., f_n and the key k_i among keys k_0, k_1, ...,
// k_n known to the internal iterator. We maintain the following merging
// iterator invariants:
//
// M1: Cascading: If TrySeekUsingNext is propagated to the level iterator at
// level l_i, TrySeekUsingNext must be propagated to all the merging iterator's
// iterators at levels j > i.
// M2: File monotonicity: If TrySeekUsingNext is propagated to a level iterator,
// the level iterator must not return a key from a file f_j where j < i, even if
// file f_j includes a key k_j such that s2 ≤ k_j < k_i.
//
// Together, these invariants ensure that any range deletions relevant to
// lower-levelled keys are either in currently open files or future files.
//
// Description of TrySeekUsingNext mechanics across the iterator stack:
//
// As the top-level entry point of user seeks, the [pebble.Iterator] is
// responsible for detecting when consecutive user-initiated seeks move
// monotonically forward. It saves seek keys and compares consecutive seek keys
// to decide whether to propagate the TrySeekUsingNext flag to its
// [InternalIterator].
//
// The [pebble.Iterator] also has its own TrySeekUsingNext optimization in
// SeekGE: Above the [InternalIterator] interface, the [pebble.Iterator]'s
// SeekGE method detects consecutive seeks to monotonically increasing keys and
// examines the current key. If the iterator is already positioned appropriately
// (at a key ≥ the seek key), it elides the entire seek of the internal
// iterator.
//
// The pebble mergingIter does not perform any TrySeekUsingNext optimization
// itself, but it must preserve the universal U1 invariant, as well as the M1
// invariant specific to the mergingIter. It does both by always translating
// calls to its SeekGE and SeekPrefixGE methods as equivalent calls to every
// child iterator. There are subtleties:
//
//   - The mergingIter takes care to avoid ever advancing a child iterator
//     that's already positioned beyond the current iteration prefix. During
//     prefix iteration, some levels may omit keys that don't match the
//     prefix. Meanwhile the merging iterator sometimes skips keys (eg, due to
//     visibility filtering). If we did not guard against iterating beyond the
//     iteration prefix, this key skipping could move some iterators beyond the
//     keys that were omitted due to prefix mismatch. A subsequent
//     TrySeekUsingNext could surface the omitted keys, but not relevant range
//     deletions that deleted them.
//
// The pebble levelIter makes use of the TrySeekUsingNext flag to avoid a naive
// seek within the level's B-Tree of files. When TrySeekUsingNext is passed by
// the caller, the relevant key must fall within the current file or a later
// file. The search space is reduced from (-∞,+∞) to [current file, +∞). If the
// current file's bounds overlap the key, the levelIter propagates the
// TrySeekUsingNext to the current sstable iterator. If the levelIter must
// advance to a new file, it drops the flag because the new file's sstable
// iterator is still unpositioned.
//
// In-memory iterators arenaskl.Iterator and batchskl.Iterator make use of the
// TrySeekUsingNext flag, attempting a fixed number of Nexts before falling back
// to performing a seek using skiplist structures.
//
// The sstable iterators use the TrySeekUsingNext flag to avoid naive seeks
// through a table's index structures. See the long comment in
// sstable/reader_iter.go for more details:
//   - If an iterator is already exhausted, either because there are no
//     subsequent point keys or because the upper bound has been reached, the
//     iterator uses TrySeekUsingNext to avoid any repositioning at all.
//   - Otherwise, a TrySeekUsingNext flag causes the sstable Iterator to Next
//     forward a capped number of times, stopping as soon as a key ≥ the seek key
//     is discovered.
//   - The sstable iterator does not always position itself in response to a
//     SeekPrefixGE even when TrySeekUsingNext()=false, because bloom filters may
//     indicate the prefix does not exist within the file. The sstable iterator
//     takes care to remember when it didn't position itself, so that a
//     subsequent seek using TrySeekUsingNext does NOT try to reuse the current
//     iterator position.
package base

// TODO(sumeer): Come back to this comment and incorporate some of the comments
// from PR #3329.
