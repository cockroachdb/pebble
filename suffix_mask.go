// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// SuffixMask: design overview
//
// A SuffixMask is a `[Lower, Upper)` range over the comparer's suffix
// ordering. When attached to a table's metadata, the iterator runtime
// hides every key in that table whose suffix falls in the range. Keys
// with no suffix are never hidden. The mask is invisible to readers —
// it surfaces as if the masked keys had been point-deleted.
//
// `DeleteSuffixRange` is the public API that attaches masks. For each
// SSTable overlapping the user-key span, it picks one of two actions
// based on how the file relates to the span and the suffix range:
//
//	excise — Whole-file content is in the mask range (today, only the
//	         `SyntheticSuffix` case detects this). The overlapping
//	         portion is deleted; outside portions become virtual SSTs
//	         retaining the parent's existing mask list (read-only).
//	mask   — Some keys fall in the mask range. The new mask is attached
//	         to the (possibly virtual) inside portion. If the file
//	         straddles the span, `applySuffixMaskToStraddlingTable`
//	         splits it into up to three virtual tables and the mask
//	         applies only to the inside one.
//
// Multi-mask: `TableMetadata.SuffixMasks` is an ordered list, not a
// single range, so repeated `DeleteSuffixRange` calls accumulate.
// On insert we compare only with the last entry (`expandSuffixMask`):
// if the new range is contiguous, we expand in place; otherwise we
// append. We never sort or reshape the list — the per-row filter
// scans it linearly, which is fast for the realistic small N.
//
// Aliasing discipline: `SuffixMasks` and the byte slices it contains
// are immutable by convention. Any DSR path that needs to extend an
// inherited list clones it first (`slices.Clone`). The convention is
// documented on each field; this file's helpers honor it.
//
// Compaction interaction: compactions iterate input files via the
// standard iterator stack, which applies the per-row mask filter. The
// output file is therefore post-filter and must NOT carry the mask
// forward; see `TestSuffixMaskClearedAfterCompaction`.

package pebble

import (
	"context"
	"slices"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/sstable"
)

// DeleteSuffixRange deletes all point keys and range key entries within the
// given key span whose suffixes fall within the range [lower, upper) as
// determined by ComparePointSuffixes. Lower must be <= upper per the
// comparer; lower is inclusive, upper is exclusive. Keys with no suffix
// are not affected. The affected keys are hidden from all future iterators
// and eventually dropped during compaction.
//
// Internally, the deletion is implemented by attaching a suffix mask to the
// metadata of overlapping SSTables. For SSTables fully contained within the
// span, the mask is applied directly. For SSTables straddling the span
// boundary, the table is split into virtual tables so the mask applies only
// to the portion within the span.
//
// If the memtable overlaps the span, it is flushed before applying the mask
// to ensure all in-memory data is in SSTs.
//
// Durability: on successful return, the resulting manifest edit (if any) is
// durably recorded. If no SSTables overlap `span`, the call is a no-op and
// returns nil without forcing a manifest fsync. Concurrent writers that race
// with this call are not guaranteed to be masked; if the caller needs that
// guarantee it must serialize with its own writers.
//
// Repeated DeleteSuffixRange calls are supported: a per-table SuffixMasks
// list accumulates masks over time. Contiguous masks are merged in place;
// disjoint masks are appended.
func (d *DB) DeleteSuffixRange(ctx context.Context, span KeyRange, lower, upper []byte) error {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}
	if d.opts.ReadOnly {
		return ErrReadOnly
	}
	if len(lower) == 0 {
		return errors.New("DeleteSuffixRange: lower bound is empty")
	}
	if len(upper) == 0 {
		return errors.New("DeleteSuffixRange: upper bound is empty")
	}
	if !span.Valid() {
		return errors.New("invalid key range")
	}
	if d.FormatMajorVersion() < FormatSuffixMask {
		return errors.Newf(
			"DeleteSuffixRange requires at least format major version %d",
			FormatSuffixMask,
		)
	}

	// Flush memtables that overlap the DSR span, and extend the overlap
	// check with the protected ranges of any pre-file-only EFOS whose
	// protected ranges overlap our span. The extension forces a flush that
	// will transition such EFOSes to file-only — pinning a pre-DSR
	// `*Version` — before our mask attaches. Without this, an EFOS reader
	// in its protected range would observe our mask via the seqnum-
	// filtered current-LSM read path that pre-file-only EFOSes use, and
	// the EFOS contract that protected-range reads remain consistent
	// across non-additive mutations would be violated.
	//
	// Classic `Snapshot` observers cannot be protected the same way (no
	// protected ranges, no transition mechanism); their post-DSR reads
	// will observe the masked LSM, matching the documented
	// excise-with-classic-snapshot behavior. See the `DB.NewSnapshot`
	// docstring.
	if err := d.flushIfOverlapping(span, func() []bounded {
		return exciseOverlapBounds(d.cmp, &d.mu.snapshots.snapshotList, span, base.SeqNumMax)
	}); err != nil {
		return err
	}

	suffixMask := sstable.SuffixMask{Lower: lower, Upper: upper}

	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.mu.versions.UpdateVersionLocked(func() (versionUpdate, error) {
		// Force any pre-file-only EFOS that can transition (no blocking
		// memtable content in its protected ranges) to transition with the
		// CURRENT (pre-DSR) version pinned. `flushIfOverlapping` above
		// already handles the case where EFOS-protected memtable content
		// blocks transition by forcing a flush; the flush completion's
		// own call to this same routine transitions the EFOS. This
		// explicit invocation closes the gap for EFOSes where the overlap
		// check found no memtable content (so no flush was triggered)
		// and for any EFOS whose pre-DSR transition would otherwise be
		// deferred to an unrelated future flush — at which point the
		// pinned version would include our mask.
		d.maybeTransitionSnapshotsToFileOnlyLocked()

		current := d.mu.versions.currentVersion()
		ve := &manifest.VersionEdit{
			DeletedTables: make(map[manifest.DeletedTableEntry]*manifest.TableMetadata),
		}

		bounds := span.UserKeyBounds()

		suffixCmp := d.opts.Comparer.ComparePointSuffixes
		for level := 0; level < manifest.NumLevels; level++ {
			overlaps := current.Overlaps(level, bounds)
			iter := overlaps.Iter()
			for m := iter.First(); m != nil; m = iter.Next() {
				// L0 Overlaps expands its result to include the transitive
				// closure of all files reachable through pairwise overlap,
				// which can include files that don't overlap our original
				// span. Skip those.
				mBounds := m.UserKeyBounds()
				if !bounds.Overlaps(d.cmp, mBounds) {
					continue
				}
				// SyntheticSuffix files have a largely uniform mask answer:
				// point keys all have effective suffix = synth; RangeKeySet
				// entries with non-empty original suffix get effective = synth;
				// RangeKeySet entries with empty original suffix retain empty
				// (never masked); RangeKeyDelete entries have no per-key suffix
				// (also never masked). RangeKeyUnset cannot appear on a
				// SyntheticSuffix file (see the assertion in
				// `rowblk_fragment_iter.go::applySpanTransforms`).
				//
				// Three cases based on (synth-in-mask, has-range-keys):
				//
				//   1. synth NOT in mask: no row in the file matches the mask.
				//      Skip the file entirely (no version edit).
				//   2. synth IN mask AND no range keys: every point key is
				//      uniformly masked. Excise the file's overlap; a per-row
				//      mask would be functionally equivalent but more expensive
				//      at iteration time.
				//   3. synth IN mask AND has range keys: cannot safely excise —
				//      excising would drop RangeKeyDelete entries (suffixless
				//      per the DSR contract) and any empty-suffix RangeKeySet
				//      entries (effective suffix retains empty, also never
				//      masked). Fall through to per-row mask attachment; the
				//      per-row paths in `rowblk_iter.go`,
				//      `colblk/data_block.go`, and `rowblk_fragment_iter.go`
				//      collectively honor SyntheticSuffix and skip
				//      empty-effective-suffix entries.
				//
				// TODO(dt): track "file contains an empty-suffix RangeKeySet"
				// and "file contains a RangeKeyDelete" as bits on
				// `TableMetadata` set at writer/ingest time. With those bits
				// we could distinguish the genuinely unsafe case from the
				// (synth-in-mask + range-keys-but-no-suffixless-entries) case,
				// where excise would be safe and is cheaper. Today we fall
				// through conservatively whenever HasRangeKeys is true.
				if m.SyntheticPrefixAndSuffix.HasSuffix() {
					ss := m.SyntheticPrefixAndSuffix.Suffix()
					synthInMask := len(ss) > 0 &&
						suffixCmp(ss, suffixMask.Lower) >= 0 &&
						suffixCmp(ss, suffixMask.Upper) < 0
					if !synthInMask {
						continue
					}
					if !m.HasRangeKeys {
						if err := d.exciseOverlap(ctx, ve, m, level, span); err != nil {
							return versionUpdate{}, err
						}
						continue
					}
					// Fall through to per-row mask attachment below.
				}

				// Build the new SuffixMasks list for the (virtual) target
				// table. We clone-on-extend to honor the aliasing discipline
				// documented on TableMetadata.SuffixMasks. Defer cloning until
				// we know we're producing a new list: the contained-in-last
				// case below short-circuits with no version edit at all.
				var newMasks []sstable.SuffixMask
				if n := len(m.SuffixMasks); n > 0 {
					merged, ok := expandSuffixMask(suffixCmp, m.SuffixMasks[n-1], suffixMask)
					if ok {
						// If the merge is a no-op (the new mask is fully
						// contained in the existing last mask), skip the
						// file entirely — there is no version edit to
						// apply. Note this is independent of n; only the
						// last mask is considered because masks are kept
						// in a list whose last entry is the most-recently
						// extended one.
						if suffixCmp(merged.Lower, m.SuffixMasks[n-1].Lower) == 0 &&
							suffixCmp(merged.Upper, m.SuffixMasks[n-1].Upper) == 0 {
							continue
						}
						newMasks = slices.Clone(m.SuffixMasks)
						newMasks[n-1] = merged
					} else {
						newMasks = slices.Clone(m.SuffixMasks)
						newMasks = append(newMasks, suffixMask)
					}
				} else {
					newMasks = []sstable.SuffixMask{suffixMask}
				}

				// span.End is exclusive; a naive `cmp(span.End, m.Largest().UserKey) >= 0`
				// would treat a file whose largest key has the same user key as
				// `span.End` (with an inclusive boundary) as fully contained,
				// even though that key falls *outside* the span. Use the
				// half-open bounds containment check.
				if bounds.ContainsBounds(d.cmp, mBounds) {
					d.applySuffixMaskToTable(ve, m, level, newMasks)
				} else {
					if err := d.applySuffixMaskToStraddlingTable(ctx, ve, m, level, span, newMasks); err != nil {
						return versionUpdate{}, err
					}
				}
			}
		}

		if len(ve.DeletedTables) == 0 && len(ve.NewTables) == 0 {
			return versionUpdate{}, nil
		}

		if invariants.Enabled {
			for _, e := range ve.NewTables {
				if err := e.Meta.Validate(d.cmp, d.opts.Comparer.FormatKey); err != nil {
					return versionUpdate{}, errors.AssertionFailedf(
						"DeleteSuffixRange constructed invalid table %s: %v", e.Meta.TableNum, err)
				}
			}
		}

		return versionUpdate{
			VE: ve,
			InProgressCompactionsFn: func() []compactionInfo {
				return d.getInProgressCompactionInfoLocked(nil)
			},
		}, nil
	})
	if err != nil {
		return err
	}
	d.updateReadStateLocked(d.opts.DebugCheck)
	return nil
}

// applySuffixMaskToTable applies the suffix masks to a table that is fully
// contained within the target span.
func (d *DB) applySuffixMaskToTable(
	ve *manifest.VersionEdit, m *manifest.TableMetadata, level int, suffixMasks []sstable.SuffixMask,
) {
	newMeta := &manifest.TableMetadata{
		TableNum:                 d.mu.versions.getNextTableNum(),
		Size:                     m.Size,
		CreationTime:             m.CreationTime,
		SeqNums:                  m.SeqNums,
		LargestSeqNumAbsolute:    m.LargestSeqNumAbsolute,
		Virtual:                  true,
		SyntheticPrefixAndSuffix: m.SyntheticPrefixAndSuffix,
		SuffixMasks:              suffixMasks,
		BlobReferenceDepth:       m.BlobReferenceDepth,
		BlobReferences:           m.BlobReferences,
	}
	if m.HasPointKeys {
		newMeta.ExtendPointKeyBounds(
			d.cmp, m.PointKeyBounds.Smallest(), m.PointKeyBounds.Largest())
	}
	if m.HasRangeKeys {
		newMeta.ExtendRangeKeyBounds(
			d.cmp, m.RangeKeyKinds, m.RangeKeyBounds.Smallest(), m.RangeKeyBounds.Largest())
	}
	newMeta.AttachVirtualBacking(m.TableBacking)

	ve.DeletedTables[manifest.DeletedTableEntry{
		Level:   level,
		FileNum: m.TableNum,
	}] = m
	if !m.Virtual {
		ve.CreatedBackingTables = append(ve.CreatedBackingTables, m.TableBacking)
	}
	ve.NewTables = append(ve.NewTables, manifest.NewTableEntry{Level: level, Meta: newMeta})
}

// applySuffixMaskToStraddlingTable handles a table that straddles the span
// boundary. It splits the table into up to three virtual tables so that the
// new masks list is applied only to the portion within the span. The portions
// outside the span retain m's existing masks (if any): `exciseTable` shares
// (read-only) `m.SuffixMasks` with the outside virtual tables, per the
// aliasing discipline documented on `TableMetadata.SuffixMasks`.
func (d *DB) applySuffixMaskToStraddlingTable(
	ctx context.Context,
	ve *manifest.VersionEdit,
	m *manifest.TableMetadata,
	level int,
	span KeyRange,
	tableMasks []sstable.SuffixMask,
) error {
	exciseBounds := span.UserKeyBounds()

	// exciseTable is used here to split the table: it returns the portions
	// of m outside the span, which we keep unmasked. We separately construct
	// a virtual table for the portion inside the span with the masks applied.
	leftTable, rightTable, err := d.exciseTable(ctx, exciseBounds, m, level, tightExciseBoundsIfLocal)
	if err != nil {
		return err
	}

	ve.DeletedTables[manifest.DeletedTableEntry{
		Level:   level,
		FileNum: m.TableNum,
	}] = m
	if !m.Virtual {
		ve.CreatedBackingTables = append(ve.CreatedBackingTables, m.TableBacking)
	}

	// Add outside portions; exciseTable carried m's existing masks (if any)
	// onto them, which is what we want — only the inside portion gets the
	// expanded masks via middleTable below.
	if leftTable != nil {
		ve.NewTables = append(ve.NewTables, manifest.NewTableEntry{Level: level, Meta: leftTable})
	}
	if rightTable != nil {
		ve.NewTables = append(ve.NewTables, manifest.NewTableEntry{Level: level, Meta: rightTable})
	}

	// Construct the inside portion with loose bounds clamped to the span.
	middleTable := &manifest.TableMetadata{
		TableNum:                 d.mu.versions.getNextTableNum(),
		CreationTime:             m.CreationTime,
		SeqNums:                  m.SeqNums,
		LargestSeqNumAbsolute:    m.LargestSeqNumAbsolute,
		Virtual:                  true,
		SyntheticPrefixAndSuffix: m.SyntheticPrefixAndSuffix,
		SuffixMasks:              tableMasks,
		BlobReferenceDepth:       m.BlobReferenceDepth,
	}
	looseMiddleTableBounds(d.cmp, m, middleTable, exciseBounds)
	if !middleTable.HasPointKeys && !middleTable.HasRangeKeys {
		return nil
	}

	var used uint64
	if leftTable != nil {
		used += leftTable.Size
	}
	if rightTable != nil {
		used += rightTable.Size
	}
	middleTable.Size = middleTableSize(m.Size, used)
	middleTable.AttachVirtualBacking(m.TableBacking)
	determineSuffixMaskBlobReferences(m.BlobReferences, m.Size, middleTable, d.FormatMajorVersion())
	ve.NewTables = append(ve.NewTables, manifest.NewTableEntry{Level: level, Meta: middleTable})
	return nil
}

// smallestInternalKeyAt returns the smallest possible (in InternalCompare
// order) non-sentinel internal key at the given user key, with a kind suitable
// for the bound it represents. The trailer pairs `SeqNumMax-1` (the largest
// non-sentinel sequence number) with the provided kind; because internal keys
// at equal user keys sort by trailer DESCENDING, this yields the smallest such
// key with the given kind.
//
// Use this when synthesizing the inclusive smallest bound of a virtual table
// at a user key. A naive `(userKey, 0, kind)` would sort AFTER any real range
// tombstone at the same user key (whose trailer encodes a real non-zero
// sequence number), tripping the lower-bound check in `keyspan.AssertBounds`
// against `PointKeyBounds.Smallest()`.
//
// `SeqNumMax` itself cannot be used: it marks an exclusive sentinel, and
// `UserKeyBoundsFromInternal` panics on a sentinel smallest. The caller is
// responsible for passing a kind that is valid for the bound type (e.g.
// `InternalKeyKindMaxForSSTable` for a point bound, `InternalKeyKindRangeKeyMax`
// for a range-key bound; see `isValidPointBoundKeyKind` and
// `isValidRangeKeyBoundKeyKind` in `internal/manifest/table_metadata.go`).
func smallestInternalKeyAt(userKey []byte, kind base.InternalKeyKind) base.InternalKey {
	return base.MakeInternalKey(userKey, base.SeqNumMax-1, kind)
}

// middleTableSize returns the size to attribute to the middle (masked)
// virtual table produced by `applySuffixMaskToStraddlingTable`. The left and
// right virtual tables already carry size estimates; the middle table gets
// whatever's left of the original. If the left+right estimates exceed the
// original (loose bounds make this possible), we clamp to 1 since a zero
// size would imply the table is empty and risks underflow elsewhere.
func middleTableSize(originalSize, used uint64) uint64 {
	if used >= originalSize {
		return 1
	}
	return originalSize - used
}

// looseMiddleTableBounds sets loose bounds on middleTable for the portion of
// originalTable that lies within the given bounds.
func looseMiddleTableBounds(
	cmp Compare, originalTable, middleTable *manifest.TableMetadata, bounds base.UserKeyBounds,
) {
	if originalTable.HasPointKeys {
		// Always synthesize the smallest with the maximum non-sentinel
		// trailer. Using the originalTable's PointKeyBounds.Smallest()
		// directly is unsafe: that key may have a small trailer (e.g.
		// seqnum=0, kind=DELSIZED), and a real range tombstone at the
		// same user key with a larger trailer would violate the
		// rangeDelIter's lower-bound assertion in invariants builds.
		// The bound is loose either way; using the largest trailer is
		// strictly safer.
		smallestUserKey := originalTable.PointKeyBounds.Smallest().UserKey
		if cmp(smallestUserKey, bounds.Start) < 0 {
			smallestUserKey = bounds.Start
		}
		smallest := smallestInternalKeyAt(smallestUserKey, base.InternalKeyKindMaxForSSTable)
		largest := originalTable.PointKeyBounds.Largest()
		if largest.IsUpperBoundFor(cmp, bounds.End.Key) {
			largest = base.MakeRangeDeleteSentinelKey(bounds.End.Key)
		}
		if base.InternalCompare(cmp, smallest, largest) < 0 {
			middleTable.ExtendPointKeyBounds(cmp, smallest, largest)
		}
	}
	if originalTable.HasRangeKeys {
		// Same rationale as the point-key bound above.
		smallestUserKey := originalTable.RangeKeyBounds.Smallest().UserKey
		if cmp(smallestUserKey, bounds.Start) < 0 {
			smallestUserKey = bounds.Start
		}
		smallest := smallestInternalKeyAt(smallestUserKey, base.InternalKeyKindRangeKeyMax)
		largest := originalTable.RangeKeyBounds.Largest()
		if largest.IsUpperBoundFor(cmp, bounds.End.Key) {
			// `bounds` always has Kind == Exclusive when produced from
			// KeyRange.UserKeyBounds(), the only construction path today.
			largest = base.MakeExclusiveSentinelKey(largest.Kind(), bounds.End.Key)
		}
		if base.InternalCompare(cmp, smallest, largest) < 0 {
			middleTable.ExtendRangeKeyBounds(cmp, originalTable.RangeKeyKinds, smallest, largest)
		}
	}
}

// exciseOverlap deletes the portion of m that overlaps with the span. If m is
// fully contained, it is deleted entirely. If m straddles the span boundary,
// the outside portions are kept as virtual tables. This is used for files with
// SyntheticSuffix whose suffix falls within the mask range — every key in the
// file is masked, so no per-row filtering is needed.
func (d *DB) exciseOverlap(
	ctx context.Context,
	ve *manifest.VersionEdit,
	m *manifest.TableMetadata,
	level int,
	span KeyRange,
) error {
	// See the analogous check in DeleteSuffixRange; span.End is exclusive so
	// the half-open bounds containment is the correct comparison.
	spanBounds := span.UserKeyBounds()
	mBounds := m.UserKeyBounds()
	fullyContained := spanBounds.ContainsBounds(d.cmp, mBounds)

	ve.DeletedTables[manifest.DeletedTableEntry{
		Level:   level,
		FileNum: m.TableNum,
	}] = m
	if !m.Virtual {
		ve.CreatedBackingTables = append(ve.CreatedBackingTables, m.TableBacking)
	}

	if fullyContained {
		return nil
	}

	// Straddling: keep the outside portions.
	exciseBounds := span.UserKeyBounds()
	leftTable, rightTable, err := d.exciseTable(ctx, exciseBounds, m, level, tightExciseBoundsIfLocal)
	if err != nil {
		return err
	}
	if leftTable != nil {
		ve.NewTables = append(ve.NewTables, manifest.NewTableEntry{Level: level, Meta: leftTable})
	}
	if rightTable != nil {
		ve.NewTables = append(ve.NewTables, manifest.NewTableEntry{Level: level, Meta: rightTable})
	}
	return nil
}

// expandSuffixMask attempts to merge two SuffixMasks into a single contiguous
// range. It returns (merged, true) when the union of the two input ranges is
// itself a single contiguous range, or (zero, false) when the two ranges are
// disjoint (i.e. have a gap between them) and cannot be merged.
//
// Both masks must be valid: suffixCmp(Lower, Upper) < 0. Bounds are compared
// using the provided suffixCmp. Using bytes.Compare here would be incorrect
// for any suffix encoding where byte order disagrees with the comparer's
// suffix order (e.g., testkeys' variable-length decimal "@N" suffixes:
// "@9" < "@99" in bytes but "@9" > "@99" in suffix order).
func expandSuffixMask(
	suffixCmp base.ComparePointSuffixes, a, b sstable.SuffixMask,
) (sstable.SuffixMask, bool) {
	// Two half-open intervals [L1, U1) and [L2, U2) (with L < U in suffix
	// order) overlap or are adjacent iff max(L1, L2) <= min(U1, U2). If
	// max(L1, L2) > min(U1, U2), there is a gap and the union is not a
	// single contiguous range.
	maxLower := a.Lower
	if suffixCmp(b.Lower, maxLower) > 0 {
		maxLower = b.Lower
	}
	minUpper := a.Upper
	if suffixCmp(b.Upper, minUpper) < 0 {
		minUpper = b.Upper
	}
	if suffixCmp(maxLower, minUpper) > 0 {
		return sstable.SuffixMask{}, false
	}
	// Merged = [min(Lower), max(Upper)] in suffix order.
	lower := a.Lower
	if suffixCmp(b.Lower, lower) < 0 {
		lower = b.Lower
	}
	upper := a.Upper
	if suffixCmp(b.Upper, upper) > 0 {
		upper = b.Upper
	}
	return sstable.SuffixMask{Lower: lower, Upper: upper}, true
}

func determineSuffixMaskBlobReferences(
	originalRefs manifest.BlobReferences,
	originalSize uint64,
	table *manifest.TableMetadata,
	fmv FormatMajorVersion,
) {
	if len(originalRefs) == 0 {
		return
	}
	determineExcisedTableBlobReferences(originalRefs, originalSize, table, fmv)
}
