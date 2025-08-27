// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangekeystack

import (
	"bytes"

	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/internal/invariants"
	"github.com/cockroachdb/pebble/v2/internal/keyspan"
	"github.com/cockroachdb/pebble/v2/internal/keyspan/keyspanimpl"
	"github.com/cockroachdb/pebble/v2/internal/manifest"
	"github.com/cockroachdb/pebble/v2/internal/rangekey"
)

// UserIteratorConfig holds state for constructing the range key iterator stack
// for user iteration. The range key iterator must merge range key spans across
// the levels of the LSM. This merging is performed by a keyspanimpl.MergingIter
// on-the-fly. The UserIteratorConfig implements keyspan.Transformer, evaluating
// range-key semantics and shadowing, so the spans returned by a MergingIter are
// fully resolved.
//
// The MergingIter is wrapped by a BoundedIter, which elides spans that are
// outside the iterator bounds (or the current prefix's bounds, during prefix
// iteration mode).
//
// To provide determinisim during iteration, the BoundedIter is wrapped by a
// DefragmentingIter that defragments abutting spans with identical
// user-observable state.
//
// At the top-level an InterleavingIter interleaves range keys with point keys
// and performs truncation to iterator bounds.
//
// Below is an abbreviated diagram illustrating the mechanics of a SeekGE.
//
//	               InterleavingIter.SeekGE
//	                       │
//	            DefragmentingIter.SeekGE
//	                       │
//	               BoundedIter.SeekGE
//	                       │
//	      ╭────────────────┴───────────────╮
//	      │                                ├── defragmentBwd*
//	MergingIter.SeekGE                     │
//	      │                                ╰── defragmentFwd
//	      ╰─╶╶ per level╶╶ ─╮
//	                        │
//	                        │
//	                        ├── <?>.SeekLT
//	                        │
//	                        ╰── <?>.Next
type UserIteratorConfig struct {
	snapshot     base.SeqNum
	comparer     *base.Comparer
	miter        keyspanimpl.MergingIter
	biter        keyspan.BoundedIter
	diter        keyspan.DefragmentingIter
	liters       [manifest.NumLevels]keyspanimpl.LevelIter
	litersUsed   int
	internalKeys bool
	bufs         *Buffers
}

// Buffers holds various buffers used for range key iteration. They're exposed
// so that they may be pooled and reused between iterators.
type Buffers struct {
	merging       keyspanimpl.MergingBuffers
	defragmenting keyspan.DefragmentingBuffers
	sortBuf       []keyspan.Key
}

// PrepareForReuse discards any excessively large buffers.
func (bufs *Buffers) PrepareForReuse() {
	bufs.merging.PrepareForReuse()
	bufs.defragmenting.PrepareForReuse()
}

// Init initializes the range key iterator stack for user iteration. The
// resulting fragment iterator applies range key semantics, defragments spans
// according to their user-observable state and, if !internalKeys, removes all
// Keys other than RangeKeySets describing the current state of range keys. The
// resulting spans contain Keys sorted by suffix (unless internalKeys is true,
// in which case they remain sorted by trailer descending).
//
// The snapshot sequence number parameter determines which keys are visible. Any
// keys not visible at the provided snapshot are ignored.
func (ui *UserIteratorConfig) Init(
	comparer *base.Comparer,
	snapshot base.SeqNum,
	lower, upper []byte,
	hasPrefix *bool,
	prefix *[]byte,
	internalKeys bool,
	bufs *Buffers,
	iters ...keyspan.FragmentIterator,
) keyspan.FragmentIterator {
	ui.snapshot = snapshot
	ui.comparer = comparer
	ui.internalKeys = internalKeys
	ui.miter.Init(comparer, ui, &bufs.merging, iters...)
	ui.biter.Init(comparer.Compare, comparer.Split, &ui.miter, lower, upper, hasPrefix, prefix)
	if internalKeys {
		ui.diter.Init(comparer, &ui.biter, keyspan.DefragmentInternal, keyspan.StaticDefragmentReducer, &bufs.defragmenting)
	} else {
		ui.diter.Init(comparer, &ui.biter, ui, keyspan.StaticDefragmentReducer, &bufs.defragmenting)
	}
	ui.litersUsed = 0
	ui.bufs = bufs
	return &ui.diter
}

// AddLevel adds a new level to the bottom of the iterator stack. AddLevel
// must be called after Init and before any other method on the iterator.
func (ui *UserIteratorConfig) AddLevel(iter keyspan.FragmentIterator) {
	ui.miter.AddLevel(iter)
}

// NewLevelIter returns a pointer to a newly allocated or reused
// keyspanimpl.LevelIter. The caller is responsible for calling Init() on this
// instance.
func (ui *UserIteratorConfig) NewLevelIter() *keyspanimpl.LevelIter {
	if ui.litersUsed >= len(ui.liters) {
		return &keyspanimpl.LevelIter{}
	}
	ui.litersUsed++
	return &ui.liters[ui.litersUsed-1]
}

// SetBounds propagates bounds to the iterator stack. The fragment iterator
// interface ordinarily doesn't enforce bounds, so this is exposed as an
// explicit method on the user iterator config.
func (ui *UserIteratorConfig) SetBounds(lower, upper []byte) {
	ui.biter.SetBounds(lower, upper)
}

// Transform implements the keyspan.Transformer interface for use with a
// keyspanimpl.MergingIter. It transforms spans by resolving range keys at the
// provided snapshot sequence number. Shadowing of keys is resolved (eg, removal
// of unset keys, removal of keys overwritten by a set at the same suffix, etc)
// and then non-RangeKeySet keys are removed. The resulting transformed spans
// only contain RangeKeySets describing the state visible at the provided
// sequence number, and hold their Keys sorted by Suffix (except if internalKeys
// is true, then keys remain sorted by trailer.
func (ui *UserIteratorConfig) Transform(
	suffixCmp base.CompareRangeSuffixes, s keyspan.Span, dst *keyspan.Span,
) error {
	// Apply shadowing of keys.
	dst.Start = s.Start
	dst.End = s.End
	ui.bufs.sortBuf = rangekey.CoalesceInto(suffixCmp, ui.bufs.sortBuf[:0], ui.snapshot, s.Keys)
	if ui.internalKeys {
		if s.KeysOrder != keyspan.ByTrailerDesc {
			panic("unexpected key ordering in UserIteratorTransform with internalKeys = true")
		}
		dst.Keys = ui.bufs.sortBuf
		keyspan.SortKeysByTrailer(dst.Keys)
		return nil
	}
	// During user iteration over range keys, unsets and deletes don't matter. This
	// step helps logical defragmentation during iteration.
	keys := ui.bufs.sortBuf
	dst.Keys = dst.Keys[:0]
	for i := range keys {
		switch keys[i].Kind() {
		case base.InternalKeyKindRangeKeySet:
			if invariants.Enabled && len(dst.Keys) > 0 && suffixCmp(dst.Keys[len(dst.Keys)-1].Suffix, keys[i].Suffix) > 0 {
				panic("pebble: keys unexpectedly not in ascending suffix order")
			}
			dst.Keys = append(dst.Keys, keys[i])
		case base.InternalKeyKindRangeKeyUnset:
			if invariants.Enabled && len(dst.Keys) > 0 && suffixCmp(dst.Keys[len(dst.Keys)-1].Suffix, keys[i].Suffix) > 0 {
				panic("pebble: keys unexpectedly not in ascending suffix order")
			}
			// Skip.
			continue
		case base.InternalKeyKindRangeKeyDelete:
			// Skip.
			continue
		default:
			return base.CorruptionErrorf("pebble: unrecognized range key kind %s", keys[i].Kind())
		}
	}
	// coalesce results in dst.Keys being sorted by Suffix.
	dst.KeysOrder = keyspan.BySuffixAsc
	return nil
}

// ShouldDefragment implements the DefragmentMethod interface and configures a
// DefragmentingIter to defragment spans of range keys if their user-visible
// state is identical. This defragmenting method assumes the provided spans have
// already been transformed through (UserIterationConfig).Transform, so all
// RangeKeySets are user-visible sets and are already in Suffix order. This
// defragmenter checks for equality between set suffixes and values (ignoring
// sequence numbers). It's intended for use during user iteration, when the
// wrapped keyspan iterator is merging spans across all levels of the LSM.
func (ui *UserIteratorConfig) ShouldDefragment(
	suffixCmp base.CompareRangeSuffixes, a, b *keyspan.Span,
) bool {
	// This method is not called with internalKeys = true.
	if ui.internalKeys {
		panic("unexpected call to ShouldDefragment with internalKeys = true")
	}
	// This implementation must only be used on spans that have transformed by
	// ui.Transform. The transform applies shadowing, removes all keys besides
	// the resulting Sets and sorts the keys by suffix. Since shadowing has been
	// applied, each Set must set a unique suffix. If the two spans are
	// equivalent, they must have the same number of range key sets.
	if len(a.Keys) != len(b.Keys) || len(a.Keys) == 0 {
		return false
	}
	if a.KeysOrder != keyspan.BySuffixAsc || b.KeysOrder != keyspan.BySuffixAsc {
		panic("pebble: range key span's keys unexpectedly not in ascending suffix order")
	}

	ret := true
	for i := range a.Keys {
		if invariants.Enabled {
			if a.Keys[i].Kind() != base.InternalKeyKindRangeKeySet ||
				b.Keys[i].Kind() != base.InternalKeyKindRangeKeySet {
				panic("pebble: unexpected non-RangeKeySet during defragmentation")
			}
			if i > 0 && (suffixCmp(a.Keys[i].Suffix, a.Keys[i-1].Suffix) < 0 ||
				suffixCmp(b.Keys[i].Suffix, b.Keys[i-1].Suffix) < 0) {
				panic("pebble: range keys not ordered by suffix during defragmentation")
			}
		}
		if suffixCmp(a.Keys[i].Suffix, b.Keys[i].Suffix) != 0 {
			ret = false
			break
		}
		if !bytes.Equal(a.Keys[i].Value, b.Keys[i].Value) {
			ret = false
			break
		}
	}
	return ret
}
