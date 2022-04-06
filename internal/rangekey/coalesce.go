// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangekey

import (
	"bytes"
	"sort"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
)

// InitUserIteration initializes the range key iterator stack for user
// iteration. The resulting fragment iterator applies range key semantics,
// defragments spans according to their user-observable state and removes all
// Keys other than RangeKeySets describing the current state of range keys.
//
// The snapshot sequence number parameter determines which keys are visible. Any
// keys not visible at the provided snapshot are ignored.
func InitUserIteration(
	cmp base.Compare,
	snapshot uint64,
	miter *keyspan.MergingIter,
	diter *keyspan.DefragmentingIter,
	levelIters ...keyspan.FragmentIterator,
) keyspan.FragmentIterator {
	miter.Init(cmp, userIterationTransform(snapshot), levelIters...)
	diter.Init(cmp, miter, userIterationDefragmenter(), keyspan.StaticDefragmentReducer)
	return diter
}

// userIterationTransform returns a keyspan.Transform for use with a
// keyspan.MergingIter that transforms spans by resolving range keys at the
// provided snapshot sequence number. Shadowing of keys is resolved (eg, removal
// of unset keys, removal of keys overwritten by a set at the same suffix, etc)
// and then non-RangeKeySet keys are removed. The resulting transformed spans
// only contain RangeKeySets describing the state visible at the provided
// sequence number.
func userIterationTransform(snapshot uint64) keyspan.Transform {
	return func(cmp base.Compare, s keyspan.Span, dst *keyspan.Span) error {
		// Apply shadowing of keys.
		if err := Coalesce(cmp, s.Visible(snapshot), dst); err != nil {
			return err
		}

		// During user iteration over range keys, unsets and deletes don't
		// matter. Remove them. This step helps logical defragmentation during
		// iteration.
		keys := dst.Keys
		dst.Keys = dst.Keys[:0]
		for i := range keys {
			switch keys[i].Kind() {
			case base.InternalKeyKindRangeKeySet:
				dst.Keys = append(dst.Keys, keys[i])
			case base.InternalKeyKindRangeKeyUnset:
				// Skip.
				continue
			case base.InternalKeyKindRangeKeyDelete:
				// Skip.
				continue
			default:
				return base.CorruptionErrorf("pebble: unrecognized range key kind %s", keys[i].Kind())
			}
		}
		return nil
	}
}

// userIterationDefragmenter constructs a DefragmentMethod that configures a
// DefragmentingIter to defragment spans of range keys if their user-visible
// state is identical. This defragmenting method assumes the provided spans have
// already been transformed through UserIterationTransform, so all RangeKeySets
// are user-visible sets. This defragmenter checks for equality between set
// suffixes and values (ignoring sequence numbers). It's intended for use during
// user iteration, when the wrapped keyspan iterator is merging spans across all
// levels of the LSM.
//
// The returned defragmenting method is stateful, and must not be used on
// multiple DefragmentingIters concurrently.
func userIterationDefragmenter() keyspan.DefragmentMethod {
	var bufA keysBySuffix
	var bufB keysBySuffix
	return func(cmp base.Compare, a, b keyspan.Span) bool {
		// UserIterationDefragmenter must only be used on spans that have
		// transformed by UserIterationTransform. The transform applies
		// shadowing and removes all keys besides the resulting Sets. Since
		// shadowing has been applied, each Set must set a unique suffix. If the
		// two spans are equivalent, they must have the same number of range key
		// sets.
		if len(a.Keys) != len(b.Keys) {
			return false
		}

		// The keys in both spans must be all sets. In order to be equivalent
		// during user iteration, they must set identical suffix-value mappings.
		// They do not need to have the same sequence number.
		//
		// Currently, the Keys are sorted by Trailer (as per the keyspan.Span
		// and keyspan.FragmentIterator interfaces). Collect all the keys in
		// both spans into buffers and sort them by suffix for comparison.
		bufA.cmp = cmp
		bufA.keys = append(bufA.keys[:0], a.Keys...)
		sort.Sort(bufA)
		bufB.cmp = cmp
		bufB.keys = append(bufB.keys[:0], b.Keys...)
		sort.Sort(bufB)

		for i := range bufA.keys {
			if bufA.keys[i].Kind() != base.InternalKeyKindRangeKeySet ||
				bufB.keys[i].Kind() != base.InternalKeyKindRangeKeySet {
				panic("pebble: unexpected non-RangeKeySet during defragmentation")
			}
			if cmp(bufA.keys[i].Suffix, bufB.keys[i].Suffix) != 0 {
				return false
			}
			if !bytes.Equal(bufA.keys[i].Value, bufB.keys[i].Value) {
				return false
			}
		}
		return true
	}
}

// Coalesce imposes range key semantics and coalesces range keys with the same
// bounds. Coalesce drops any keys shadowed by more recent sets, unsets or
// deletes. Coalesce modifies the provided span's Keys slice, reslicing the
// slice to remove dropped keys.
//
// Coalescence has subtle behavior with respect to sequence numbers. Coalesce
// depends on a keyspan.Span's Keys being sorted in sequence number descending
// order. The first key has the largest sequence number. The returned coalesced
// span includes only the largest sequence number. All other sequence numbers
// are forgotten. When a compaction constructs output range keys from a
// coalesced span, it produces at most one RANGEKEYSET, one RANGEKEYUNSET and
// one RANGEKEYDEL. Each one of these keys adopt the largest sequence number.
//
// This has the potentially surprising effect of 'promoting' a key to a higher
// sequence number. This is okay, because:
//   - There are no other overlapping keys within the coalesced span of
//     sequence numbers (otherwise they would be in the compaction, due to
//     the LSM invariant).
//   - Range key sequence numbers are never compared to point key sequence
//     numbers. Range keys and point keys have parallel existences.
//   - Compactions only coalesce within snapshot stripes.
//
// Additionally, internal range keys at the same sequence number have subtle
// mechanics:
//   * RANGEKEYSETs shadow RANGEKEYUNSETs of the same suffix.
//   * RANGEKEYDELs only apply to keys at lower sequence numbers.
// This is required for ingestion. Ingested sstables are assigned a single
// sequence number for the file, at which all of the file's keys are visible.
// The RANGEKEYSET, RANGEKEYUNSET and RANGEKEYDEL key kinds are ordered such
// that among keys with equal sequence numbers (thus ordered by their kinds) the
// keys do not affect one another. Ingested sstables are expected to be
// consistent with respect to the set/unset suffixes: A given suffix should be
// set or unset but not both.
func Coalesce(cmp base.Compare, span keyspan.Span, dst *keyspan.Span) error {
	// TODO(jackson): Currently, Coalesce doesn't actually perform the sequence
	// number promotion described in the comment above.

	keysBySuffix := keysBySuffix{
		cmp:  cmp,
		keys: dst.Keys[:0],
	}
	var deleted bool
	for i := 0; i < len(span.Keys) && !deleted; i++ {
		k := span.Keys[i]
		if invariants.Enabled && i > 0 && k.Trailer > span.Keys[i-1].Trailer {
			panic("pebble: invariant violation: span keys unordered")
		}

		// NB: Within a given sequence number, keys are ordered as:
		//   RangeKeySet > RangeKeyUnset > RangeKeyDelete
		// This is significant, because this ensures that none of the range keys
		// sharing a sequence number shadow each other.
		switch k.Kind() {
		case base.InternalKeyKindRangeKeySet:
			n := len(keysBySuffix.keys)

			if keysBySuffix.get(n, k.Suffix) < n {
				// This suffix is already set or unset at a higher sequence
				// number. Skip.
				continue
			}
			keysBySuffix.keys = append(keysBySuffix.keys, k)
			sort.Sort(keysBySuffix)
		case base.InternalKeyKindRangeKeyUnset:
			n := len(keysBySuffix.keys)

			if keysBySuffix.get(n, k.Suffix) < n {
				// This suffix is already set or unset at a higher sequence
				// number. Skip.
				continue
			}
			keysBySuffix.keys = append(keysBySuffix.keys, k)
			sort.Sort(keysBySuffix)
		case base.InternalKeyKindRangeKeyDelete:
			// All remaining range keys in this span have been deleted by this
			// RangeKeyDelete. There's no need to continue looping, because all
			// the remaining keys are shadowed by this one. The for loop
			// condition will terminate when it sees the last key is a
			// range key deletion.
			keysBySuffix.keys = append(keysBySuffix.keys, k)
			deleted = true
		default:
			return base.CorruptionErrorf("pebble: unexpected range key kind %s", k.Kind())
		}
	}

	// Update the span with the (potentially reduced) keys slice, and re-sort it
	// by Trailer.
	*dst = keyspan.Span{
		Start: span.Start,
		End:   span.End,
		Keys:  keysBySuffix.keys,
	}
	keyspan.SortKeys(dst.Keys)
	return nil
}

// SortBySuffix sorts the provided keys by suffix.
func SortBySuffix(cmp base.Compare, keys []keyspan.Key) {
	bySuffix := keysBySuffix{
		cmp:  cmp,
		keys: keys,
	}
	sort.Sort(bySuffix)
}

type keysBySuffix struct {
	cmp  base.Compare
	keys []keyspan.Key
}

// get searches for suffix among the first n keys in keys. If the suffix is
// found, it returns the index of the item with the suffix. If the suffix is not
// found, it returns n.
func (s keysBySuffix) get(n int, suffix []byte) (i int) {
	// Binary search for the suffix to see if there's an existing key with the
	// suffix. Only binary search among the first n items. get is called while
	// appending new keys with suffixes that may sort before existing keys.
	// The n parameter indicates what portion of the keys slice is sorted and
	// may contain relevant keys.

	i = sort.Search(n, func(i int) bool {
		return s.cmp(s.keys[i].Suffix, suffix) >= 0
	})
	if i < n && s.cmp(s.keys[i].Suffix, suffix) == 0 {
		return i
	}
	return n
}

func (s keysBySuffix) Len() int           { return len(s.keys) }
func (s keysBySuffix) Less(i, j int) bool { return s.cmp(s.keys[i].Suffix, s.keys[j].Suffix) < 0 }
func (s keysBySuffix) Swap(i, j int)      { s.keys[i], s.keys[j] = s.keys[j], s.keys[i] }
