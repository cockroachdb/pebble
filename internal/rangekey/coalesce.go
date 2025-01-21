// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangekey

import (
	"math"
	"slices"

	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/internal/invariants"
	"github.com/cockroachdb/pebble/v2/internal/keyspan"
)

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
//   - RANGEKEYSETs shadow RANGEKEYUNSETs of the same suffix.
//   - RANGEKEYDELs only apply to keys at lower sequence numbers.
//
// This is required for ingestion. Ingested sstables are assigned a single
// sequence number for the file, at which all of the file's keys are visible.
// The RANGEKEYSET, RANGEKEYUNSET and RANGEKEYDEL key kinds are ordered such
// that among keys with equal sequence numbers (thus ordered by their kinds) the
// keys do not affect one another. Ingested sstables are expected to be
// consistent with respect to the set/unset suffixes: A given suffix should be
// set or unset but not both.
//
// The resulting dst Keys slice is sorted by InternalKeyTrailer.
func Coalesce(suffixCmp base.CompareRangeSuffixes, keys []keyspan.Key, dst *[]keyspan.Key) {
	// TODO(jackson): Currently, Coalesce doesn't actually perform the sequence
	// number promotion described in the comment above.
	*dst = CoalesceInto(suffixCmp, (*dst)[:0], math.MaxUint64, keys)
	// Update the span with the (potentially reduced) keys slice. coalesce left
	// the keys in *dst sorted by suffix. Re-sort them by trailer.
	keyspan.SortKeysByTrailer(*dst)
}

// CoalesceInto is a variant of Coalesce which outputs the results into dst
// without sorting them.
func CoalesceInto(
	suffixCmp base.CompareRangeSuffixes, dst []keyspan.Key, snapshot base.SeqNum, keys []keyspan.Key,
) []keyspan.Key {
	dst = dst[:0]
	// First, enforce visibility and RangeKeyDelete mechanics. We only need to
	// consider the prefix of keys before and including the first
	// RangeKeyDelete. We also must skip any keys that aren't visible at the
	// provided snapshot sequence number.
	//
	// NB: Within a given sequence number, keys are ordered as:
	//   RangeKeySet > RangeKeyUnset > RangeKeyDelete
	// This is significant, because this ensures that a Set or Unset sharing a
	// sequence number with a Delete do not shadow each other.
	deleteIdx := -1
	for i := range keys {
		if invariants.Enabled && i > 0 && keys[i].Trailer > keys[i-1].Trailer {
			panic("pebble: invariant violation: span keys unordered")
		}
		if !keys[i].VisibleAt(snapshot) {
			continue
		}
		// Once a RangeKeyDelete is observed, we know it shadows all subsequent
		// keys and we can break early. We don't add the RangeKeyDelete key to
		// keysBySuffix.keys yet, because we don't want a suffix-less key
		// that appeared earlier in the slice to elide it. It'll be added back
		// in at the end.
		if keys[i].Kind() == base.InternalKeyKindRangeKeyDelete {
			deleteIdx = i
			break
		}
		dst = append(dst, keys[i])
	}

	// Sort the accumulated keys by suffix. There may be duplicates within a
	// suffix, in which case the one with a larger trailer survives.
	//
	// We use a stable sort so that the first key with a given suffix is the one
	// that with the highest InternalKeyTrailer (because the input `keys` was sorted by
	// trailer descending).
	slices.SortStableFunc(dst, func(a, b keyspan.Key) int {
		return suffixCmp(a.Suffix, b.Suffix)
	})

	// Grab a handle of the full sorted slice, before reslicing
	// dst to accumulate the final coalesced keys.
	sorted := dst
	dst = dst[:0]

	var (
		// prevSuffix is updated on each iteration of the below loop, and
		// compared by the subsequent iteration to determine whether adjacent
		// keys are defined at the same suffix.
		prevSuffix []byte
		// shadowing is set to true once any Key is shadowed by another key.
		// When it's set to true—or after the loop if no keys are shadowed—the
		// keysBySuffix.keys slice is resliced to contain the prefix of
		// unshadowed keys. This avoids copying them incrementally in the common
		// case of no shadowing.
		shadowing bool
	)
	for i := range sorted {
		if i > 0 && suffixCmp(prevSuffix, sorted[i].Suffix) == 0 {
			// Skip; this key is shadowed by the predecessor that had a larger
			// InternalKeyTrailer. If this is the first shadowed key, set shadowing=true
			// and reslice keysBySuffix.keys to hold the entire unshadowed
			// prefix.
			if !shadowing {
				dst = dst[:i]
				shadowing = true
			}
			continue
		}
		prevSuffix = sorted[i].Suffix
		if shadowing {
			dst = append(dst, sorted[i])
		}
	}
	// If there was no shadowing, dst.keys is untouched. We can simply set it to
	// the existing `sorted` slice (also backed by dst).
	if !shadowing {
		dst = sorted
	}
	// If the original input `keys` slice contained a RangeKeyDelete, add it.
	if deleteIdx >= 0 {
		dst = append(dst, keys[deleteIdx])
	}
	return dst
}

// ForeignSSTTransformer implements a keyspan.Transformer for range keys in
// shared ingested sstables. It is largely similar to the Transform function
// implemented in UserIteratorConfig in that it calls coalesce to remove range
// keys shadowed by other range keys, but also retains the range key that does
// the shadowing. In addition, it elides RangeKey unsets/dels in L6 as they are
// inapplicable when reading from a different Pebble instance. Finally, it
// returns keys sorted in trailer order, not suffix order, as that's what the
// rest of the iterator stack expects.
type ForeignSSTTransformer struct {
	Equal   base.Equal
	SeqNum  base.SeqNum
	sortBuf []keyspan.Key
}

// Transform implements the Transformer interface.
func (f *ForeignSSTTransformer) Transform(
	suffixCmp base.CompareRangeSuffixes, s keyspan.Span, dst *keyspan.Span,
) error {
	// Apply shadowing of keys.
	dst.Start = s.Start
	dst.End = s.End
	f.sortBuf = f.sortBuf[:0]
	f.sortBuf = CoalesceInto(suffixCmp, f.sortBuf, math.MaxUint64, s.Keys)
	keys := f.sortBuf
	dst.Keys = dst.Keys[:0]
	for i := range keys {
		switch keys[i].Kind() {
		case base.InternalKeyKindRangeKeySet:
			if invariants.Enabled && len(dst.Keys) > 0 && suffixCmp(dst.Keys[len(dst.Keys)-1].Suffix, keys[i].Suffix) > 0 {
				panic("pebble: keys unexpectedly not in ascending suffix order")
			}
		case base.InternalKeyKindRangeKeyUnset:
			if invariants.Enabled && len(dst.Keys) > 0 && suffixCmp(dst.Keys[len(dst.Keys)-1].Suffix, keys[i].Suffix) > 0 {
				panic("pebble: keys unexpectedly not in ascending suffix order")
			}
		case base.InternalKeyKindRangeKeyDelete:
			// Nothing to do.
		default:
			return base.CorruptionErrorf("pebble: unrecognized range key kind %s", keys[i].Kind())
		}
		dst.Keys = append(dst.Keys, keyspan.Key{
			Trailer: base.MakeTrailer(f.SeqNum, keys[i].Kind()),
			Suffix:  keys[i].Suffix,
			Value:   keys[i].Value,
		})
	}
	// coalesce results in dst.Keys being sorted by Suffix. Change it back to
	// ByTrailerDesc, as that's what the iterator stack will expect.
	keyspan.SortKeysByTrailer(dst.Keys)
	dst.KeysOrder = keyspan.ByTrailerDesc
	return nil
}
