// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/iterv2"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/treesteps"
)

// levelIterV2 provides a merged view of the sstables in a level.
//
// levelIterV2 implements the iterv2.Iter interface, exposing both point keys
// and span information. Each file's point keys and range deletions are wrapped
// in an iterv2.InterleavingIter which partitions the file's key space into
// contiguous spans. The level iterator stitches these per-file spans together.
//
// # Keyspace partitioning
//
// For a level with files F1, F2, ..., Fn (ordered by smallest key), the level
// iterator assigns each file's InterleavingIter the range:
//
//   - startKey = Fi.SmallestUserKey
//   - endKey   = Fi+1.SmallestUserKey  (nil for the last file)
//
// This means each InterleavingIter covers not just its file's key range but
// also any gap between it and the next file. The InterleavingIter will expose
// this gap as an empty span (no range deletion keys). This way, the level
// iterator's keyspace is fully covered by a sequence of per-file
// InterleavingIters without any explicit inter-file gap handling.
//
// Example with three files F1=[a,d], F2=[h,k], F3=[p,s]:
//
//	file bounds:    a-----d              h-----k              p------s
//	iter ranges:  a--------------------h--------------------p-------------------)
//	              | InterleavingIter 1 | InterleavingIter 2 | InterleavingIter 3
//	              |     startKey=a     |     startKey=h     |     startKey=p
//	              |      endKey=h      |      endKey=p      |     endKey=nil
//	              |                    |                    |
//	              | file  |    gap     | file  |    gap     |  file |
//	              | a b c d            | h i j k            | p q r s
//	                                   ^                    ^
//	                                   |                    |
//	                          boundary |           boundary |
//	                           emitted /            emitted /
//	                              at h                 at p
//
// # Spurious boundaries and lazy file loading
//
// The boundary emitted at file boundaries (each file's SmallestUserKey) is
// potentially spurious: it does not necessarily correspond to a change in range
// deletion coverage within this level. Two adjacent files might have no range
// deletions at all, yet a boundary is still emitted between them.
//
// This is by design and is critical for performance when levelIterV2 is used as
// a child of mergingIterV2. The boundary key goes in the merging iter's heap,
// ensuring the level iter only opens the next file if iteration gets that far
// (and if it doesn't become shadowed by a tombstone).
type levelIterV2 struct {
	// The context is stored here since (a) iterators are expected to be
	// short-lived (since they pin sstables), and (b) plumbing a context into
	// every method is very painful (and can hurt performance).
	ctx      context.Context
	logger   Logger
	comparer *Comparer
	// The lower/upper bounds for iteration as specified at creation or the most
	// recent call to SetBounds.
	lower []byte
	upper []byte
	// The point iterator options for the currently open table. If
	// tableOpts.{Lower,Upper}Bound are nil, the corresponding iteration boundary
	// does not lie within the table's bounds. Note that "bounds" here refers to
	// the original table bounds (SmallestUserKey to LargestUserKey), not the
	// SmallestUserKey to next file's SmallestUserKey extended bounds for the
	// InterleavedIters.
	tableOpts IterOptions
	// The layer this levelIterV2 is initialized for. This can be either
	// a level L1+, an L0 sublevel, or a flushable ingests layer.
	layer manifest.Layer
	// iter is the per-file InterleavingIter, reused across file loads. It wraps
	// the file's point iterator and range deletion iterator and partitions the
	// file's key space into contiguous spans. Whether a file is currently loaded
	// is indicated by iterFile being non-nil.
	iter iterv2.InterleavingIter
	// iterFile is the file currently loaded into iter, or nil if no file is
	// loaded.
	//
	// Note: iterFile may still be non-nil when err != nil (e.g. when
	// iterHasError detects an error from the underlying iterator). It gets
	// cleaned up by the next loadFile call or by Close.
	iterFile *manifest.TableMetadata
	// newIters constructs point and range deletion iterators for a table.
	newIters tableNewIters
	// files iterates over the level's tables. Its current position is related
	// to iterFile as follows:
	//
	//  - When atSyntheticBoundary is false and iterFile is non-nil,
	//    files.Current() == iterFile.
	//
	//  - When atSyntheticBoundary is true, files.Current() is the file that the
	//    next step in the current iteration direction will load. It may differ
	//    from iterFile (which may still reference a previously loaded file, or
	//    be nil). In the forward direction, files.Current() is the file whose
	//    SmallestUserKey was used as the boundary (or nil if the boundary is at
	//    the upper bound). In the backward direction, files.Current() is the
	//    file to load next going backward (or nil if the boundary is at the
	//    lower bound).
	//
	//  - On error from loadFile (err != nil), iterFile is nil (closeFileIter
	//    cleared it) and files may be positioned at the file that failed to
	//    load. On error from iterHasError, iterFile may still reference the
	//    file whose iterator reported the error.
	files manifest.LevelIterator

	// loadRangeDels controls whether range deletions are loaded from files.
	// When true, range deletions are interleaved with point keys via the
	// InterleavingIter. When false, an empty span iterator is used, resulting
	// in a single empty span covering the file's key range.
	loadRangeDels bool

	// currentSpan tracks the current span for the Span() method. When positioned
	// within a file (atSyntheticBoundary is false), it mirrors the per-file
	// InterleavingIter's span. When atSyntheticBoundary is true, it was set by
	// emitBoundary() and represents the synthetic gap boundary between files or
	// between bounds and files (Keys is always nil). When exhausted or
	// unpositioned, it is zeroed out.
	currentSpan iterv2.Span

	// internalOpts holds the internal iterator options to pass to the table
	// cache when constructing new table iterators.
	internalOpts internalIterOpts

	// Scratch space for the obsolete keys filter, when there are no other block
	// property filters specified. See the performance note where
	// IterOptions.PointKeyFilters is declared.
	filtersBuf [1]BlockPropertyFilter

	err error
	// dir indicates the current iteration direction: +1 for forward, -1 for
	// backward, 0 when unpositioned.
	dir int8
	// atSyntheticBoundary indicates that the last returned key was a boundary
	// key synthesized by the level iterator itself (via emitBoundary), rather
	// than returned by the per-file InterleavingIter. Normally, boundaries
	// between files are emitted by the InterleavingIter (whose startKey is set to
	// the file's SmallestUserKey and endKey is set to the next file's
	// SmallestUserKey). The level iterator only synthesizes boundary keys in edge
	// cases: gaps between iteration bounds and file ranges, or when First() emits
	// the first file's start boundary. When set, Next/Prev will load the adjacent
	// file rather than advancing within the current file's iterator.
	atSyntheticBoundary bool
	// prefixExhausted is set to true once we emit a boundary key that does not
	// match the prefix. Further calls to Next() will return nil.
	prefixExhausted bool
	// prefix holds the iteration prefix when the most recent absolute
	// positioning method was a SeekPrefixGE.
	prefix []byte

	// kv is scratch space for constructing the InternalKV returned by
	// emitBoundary.
	kv base.InternalKV
}

var _ iterv2.Iter = (*levelIterV2)(nil)

// newLevelIterV2 returns a levelIterV2. It is permissible to pass a nil split
// parameter if the caller is never going to call SeekPrefixGE.
func newLevelIterV2(
	ctx context.Context,
	opts IterOptions,
	comparer *Comparer,
	newIters tableNewIters,
	files manifest.LevelIterator,
	layer manifest.Layer,
	internalOpts internalIterOpts,
) *levelIterV2 {
	l := &levelIterV2{}
	l.init(ctx, opts, comparer, newIters, files, layer, internalOpts)
	return l
}

func (l *levelIterV2) init(
	ctx context.Context,
	opts IterOptions,
	comparer *Comparer,
	newIters tableNewIters,
	files manifest.LevelIterator,
	layer manifest.Layer,
	internalOpts internalIterOpts,
) {
	l.ctx = ctx
	l.err = nil
	l.layer = layer
	l.logger = opts.getLogger()
	l.prefix = nil
	l.lower = opts.LowerBound
	l.upper = opts.UpperBound
	l.loadRangeDels = true
	l.tableOpts.PointKeyFilters = opts.PointKeyFilters
	if len(opts.PointKeyFilters) == 0 {
		l.tableOpts.PointKeyFilters = l.filtersBuf[:0:1]
	}
	// TODO(radu): investigate maximum suffix property.
	l.tableOpts.MaximumSuffixProperty = nil // opts.MaximumSuffixProperty
	l.tableOpts.UseL6Filters = opts.UseL6Filters
	l.tableOpts.Category = opts.Category
	l.tableOpts.layer = l.layer
	l.tableOpts.snapshotForHideObsoletePoints = opts.snapshotForHideObsoletePoints
	l.comparer = comparer
	l.iterFile = nil
	l.newIters = newIters
	l.files = files
	l.dir = 0
	l.atSyntheticBoundary = false
	l.internalOpts = internalOpts
	l.currentSpan = iterv2.Span{}
}

// findFileGE returns the earliest file whose largest key is >= key.
func (l *levelIterV2) findFileGE(key []byte, flags base.SeekGEFlags) *manifest.TableMetadata {
	if invariants.Enabled && flags.RelativeSeek() {
		panic(errors.AssertionFailedf("levelIterV2 does not support RelativeSeek"))
	}
	// TODO(radu): do better for TrySeekUsingNext.
	m := l.files.SeekGE(l.comparer.Compare, key)
	if invariants.Enabled && m != nil && !m.HasPointKeys {
		panic(errors.AssertionFailedf("file has no point keys"))
	}
	if m == nil || !l.fileWithinUpper(m) {
		return nil
	}
	return m
}

func (l *levelIterV2) findFileLT(key []byte, flags base.SeekLTFlags) *manifest.TableMetadata {
	// Find the last file whose smallest key is < key.
	if invariants.Enabled && flags.RelativeSeek() {
		panic(errors.AssertionFailedf("levelIterV2 does not support RelativeSeek"))
	}
	m := l.files.SeekLT(l.comparer.Compare, key)
	if invariants.Enabled && m != nil && !m.HasPointKeys {
		panic(errors.AssertionFailedf("file has no point keys"))
	}
	if m == nil || !l.fileWithinLower() {
		return nil
	}
	return m
}

// Init the point iteration bounds for the current table. The table bounds are assumed
// to intersect the level iterator's current bounds.
//
// A bound is set to non-nil only if it is inside the table's point key bounds.
func (l *levelIterV2) initTableBounds(f *manifest.TableMetadata) {
	l.tableOpts.LowerBound = nil
	l.tableOpts.UpperBound = nil
	if l.lower != nil {
		if l.comparer.Compare(l.lower, f.PointKeyBounds.SmallestUserKey()) > 0 {
			// The lower bound falls within the table's key range, so the table
			// iterator needs to enforce it.
			l.tableOpts.LowerBound = l.lower
		}
	}
	if l.upper != nil {
		if l.comparer.Compare(l.upper, f.PointKeyBounds.LargestUserKey()) <= 0 {
			// The upper bound falls within the table's key range (at or before
			// the largest key), so the table iterator needs to enforce it.
			l.tableOpts.UpperBound = l.upper
		}
	}
}

// fileEndKey returns the endKey for the file at the current l.files position:
// the next point-key file's SmallestUserKey, or nil for the last file
// (unbounded end). Requires l.files to be positioned at the file in question
// (reads l.files.PeekNext()).
func (l *levelIterV2) fileEndKey() []byte {
	if next := l.files.PeekNext(); next != nil {
		return next.PointKeyBounds.SmallestUserKey()
	}
	return nil
}

// updateCurrentSpan copies the per-file InterleavingIter's current span into
// l.currentSpan.
func (l *levelIterV2) updateCurrentSpan() {
	l.currentSpan = *l.iter.Span()
}

// loadFile ensures that l.iter is loaded for the given file.
func (l *levelIterV2) loadFile(file *manifest.TableMetadata) loadFileReturnIndicator {
	if l.err != nil {
		return noFileLoaded
	}
	if l.iterFile == file {
		// We don't bother comparing the file bounds with the iteration bounds when we have
		// an already open iterator. It is possible that the iter may not be relevant given the
		// current iteration bounds, but it knows those bounds, so it will enforce them.
		return fileAlreadyLoaded
	}

	// Close the current iter.
	if err := l.closeFileIter(); err != nil {
		return noFileLoaded
	}

	l.initTableBounds(file)
	iterKinds := iterPointKeys
	if l.loadRangeDels {
		iterKinds |= iterRangeDeletions
	}

	var iters iterSet
	iters, l.err = l.newIters(l.ctx, file, &l.tableOpts, l.internalOpts, iterKinds)
	if l.err != nil {
		return noFileLoaded
	}

	// Determine the span iterator: real range deletion iter or nil.
	var spanIter keyspan.FragmentIterator
	if l.loadRangeDels && iters.rangeDeletion != nil {
		spanIter = iters.rangeDeletion
	}
	startKey := file.PointKeyBounds.SmallestUserKey()
	endKey := l.fileEndKey()

	// The InterleavingIter operates over [startKey, endKey), which can extend
	// well past the file's LargestUserKey (into the gap before the next file).
	// We need to clip it to the iteration bounds [lower, upper):
	//
	//  - tableOpts.UpperBound is set by initTableBounds only when upper falls
	//    within the file's point key bounds. But upper may also fall in the gap
	//    between the file and the next file (past LargestUserKey but before
	//    endKey); in that case, we pass l.upper directly.
	//
	//  - No equivalent logic is needed for lowerBound because startKey IS the
	//    file's SmallestUserKey: l.lower either falls within the file (handled
	//    by tableOpts.LowerBound) or is before startKey (no clipping needed).
	upperBound := l.tableOpts.UpperBound
	if upperBound == nil && l.upper != nil && (endKey == nil || l.comparer.Compare(l.upper, endKey) < 0) {
		upperBound = l.upper
	}
	lowerBound := l.tableOpts.LowerBound
	// Initialize the per-file InterleavingIter.
	l.iter.Init(l.comparer, iters.Point(), spanIter, startKey, endKey, lowerBound, upperBound)
	l.iterFile = file

	if treesteps.Enabled && treesteps.IsRecording(l) {
		treesteps.NodeUpdated(l, fmt.Sprintf("file %s loaded", l.iterFile.TableNum))
	}
	return newFileLoaded
}

func (l *levelIterV2) SeekGE(key []byte, flags base.SeekGEFlags) (kv *base.InternalKV) {
	if treesteps.Enabled && treesteps.IsRecording(l) {
		op := treesteps.StartOpf(l, "SeekGE(%q, %d)", key, flags)
		defer func() {
			op.Finishf("= %s", kv.String())
		}()
	}
	if invariants.Enabled && l.lower != nil && l.comparer.Compare(key, l.lower) < 0 {
		panic(errors.AssertionFailedf("levelIterV2 SeekGE to key %q violates lower bound %q", key, l.lower))
	}

	l.reset(+1)

	// TODO(radu): revisit TrySeekUsingNext support for v2.
	flags = flags.DisableTrySeekUsingNext()

	file := l.findFileGE(key, flags)
	if file == nil || l.comparer.Compare(key, file.PointKeyBounds.SmallestUserKey()) < 0 {
		// The seek key is either past all files (file == nil) or in a gap before
		// the returned file. Emit a boundary at the next file's start (or upper
		// bound).
		//
		// The upper-bound check is needed when file == nil (key past all files).
		// When file != nil, fileWithinUpper guarantees file.SmallestUserKey <
		// upper, and key < file.SmallestUserKey implies key < upper.
		if l.upper != nil && l.comparer.Compare(key, l.upper) >= 0 {
			l.invalidateSpan()
			return nil
		}
		return l.maybeEmitBoundaryFwd(file)
	}
	if l.loadFile(file) == noFileLoaded {
		return nil
	}
	// INVARIANT: l.iter is the iterator over file; key >= file.PointKeyBounds.SmallestUserKey().
	if kv := l.iter.SeekGE(key, base.SeekGEFlagsNone); kv != nil {
		l.updateCurrentSpan()
		return kv
	}
	if l.iterHasError() {
		return nil
	}
	// Check for the case where the key overlaps the file but is actually beyond
	// the upper bound. Conceptually, it would be easy to check this upfront. But
	// we know that in this particular case, iter.SeekGE must return nil (because
	// l.iter has the same upper bound), so we do the check here and avoid it in
	// the common path (where iter.SeekGE return something).
	if l.upper != nil && l.comparer.Compare(key, l.upper) >= 0 {
		l.invalidateSpan()
		return nil
	}
	return l.maybeEmitBoundaryFwd(l.files.Next())
}

// SeekPrefixGE implements InternalIterator.SeekPrefixGE, documented above.
func (l *levelIterV2) SeekPrefixGE(
	prefix, key []byte, flags base.SeekGEFlags,
) (kv *base.InternalKV) {
	if treesteps.Enabled && treesteps.IsRecording(l) {
		op := treesteps.StartOpf(l, "SeekPrefixGE(%q, %q, %d)", prefix, key, flags)
		defer func() {
			op.Finishf("= %s", kv.String())
		}()
	}
	if invariants.Enabled && l.lower != nil && l.comparer.Compare(key, l.lower) < 0 {
		panic(errors.AssertionFailedf("levelIterV2 SeekPrefixGE to key %q violates lower bound %q", key, l.lower))
	}
	l.reset(+1)
	l.prefix = prefix

	// TODO(radu): revisit TrySeekUsingNext support for v2.
	flags = flags.DisableTrySeekUsingNext()

	file := l.findFileGE(key, flags)
	if file == nil || l.comparer.Compare(key, file.PointKeyBounds.SmallestUserKey()) < 0 {
		if l.upper != nil && l.comparer.Compare(key, l.upper) >= 0 {
			l.invalidateSpan()
			return nil
		}
		return l.maybeEmitBoundaryFwd(file)
	}
	if l.loadFile(file) == noFileLoaded {
		return nil
	}
	if kv := l.iter.SeekPrefixGE(prefix, key, base.SeekGEFlagsNone); kv != nil {
		if kv.K.Kind() == base.InternalKeyKindSpanBoundary && !l.matchesPrefix(kv.K.UserKey) {
			l.prefixExhausted = true
		}
		l.updateCurrentSpan()
		return kv
	}
	if l.iterHasError() {
		return nil
	}
	// Check for the case where the key overlaps the file but is actually beyond
	// the upper bound. Conceptually, it would be easy to check this upfront. But
	// we know that in this particular case, iter.SeekGE must return nil (because
	// l.iter has the same upper bound), so we do the check here and avoid it in
	// the common path (where iter.SeekGE return something).
	if l.upper != nil && l.comparer.Compare(key, l.upper) >= 0 {
		l.invalidateSpan()
		return nil
	}
	return l.maybeEmitBoundaryFwd(l.files.Next())
}

func (l *levelIterV2) SeekLT(key []byte, flags base.SeekLTFlags) (kv *base.InternalKV) {
	if treesteps.Enabled && treesteps.IsRecording(l) {
		op := treesteps.StartOpf(l, "SeekLT(%q, %d)", key, flags)
		defer func() {
			op.Finishf("= %s", kv.String())
		}()
	}
	if invariants.Enabled && l.upper != nil && l.comparer.Compare(key, l.upper) > 0 {
		panic(errors.AssertionFailedf("levelIterV2 SeekLT to key %q violates upper bound %q", key, l.upper))
	}

	l.reset(-1)

	file := l.findFileLT(key, flags)
	if file == nil {
		if l.lower != nil && l.comparer.Compare(key, l.lower) <= 0 {
			l.invalidateSpan()
			return nil
		}
		return l.maybeEmitBoundaryBwd(nil)
	}
	if l.loadFile(file) == noFileLoaded {
		l.invalidateSpan()
		return nil
	}
	if kv := l.iter.SeekLT(key, base.SeekLTFlagsNone); kv != nil {
		l.updateCurrentSpan()
		return kv
	}
	if l.iterHasError() {
		return nil
	}
	// Check for the case where the key overlaps the file but is actually below
	// the lower bound. Conceptually, it would be easy to check this upfront. But
	// we know that in this particular case, iter.SeekLT must return nil (because
	// l.iter has the same lower bound), so we do the check here and avoid it in
	// the common path (where iter.SeekLT return something).
	if l.lower != nil && l.comparer.Compare(key, l.lower) <= 0 {
		l.invalidateSpan()
		return nil
	}
	return l.maybeEmitBoundaryBwd(l.files.Prev())
}

func (l *levelIterV2) First() (kv *base.InternalKV) {
	if treesteps.Enabled && treesteps.IsRecording(l) {
		op := treesteps.StartOpf(l, "First()")
		defer func() {
			op.Finishf("= %s", kv.String())
		}()
	}
	if invariants.Enabled && l.lower != nil {
		panic(errors.AssertionFailedf("levelIterV2 First called while lower bound %q is set", l.lower))
	}

	l.reset(+1)

	file := l.files.First()
	if file == nil || !l.fileWithinUpper(file) {
		return l.maybeEmitBoundaryFwd(nil)
	}
	return l.emitBoundary(file.PointKeyBounds.SmallestUserKey(), +1)
}

func (l *levelIterV2) Last() (kv *base.InternalKV) {
	if treesteps.Enabled && treesteps.IsRecording(l) {
		op := treesteps.StartOpf(l, "Last()")
		defer func() {
			op.Finishf("= %s", kv.String())
		}()
	}
	if invariants.Enabled && l.upper != nil {
		panic(errors.AssertionFailedf("levelIterV2 Last called while upper bound %q is set", l.upper))
	}

	l.reset(-1)

	file := l.files.Last()
	if file == nil || !l.fileWithinLower() {
		return l.maybeEmitBoundaryBwd(nil)
	}
	if l.loadFile(file) == noFileLoaded {
		l.invalidateSpan()
		return nil
	}
	kv = l.iter.Last()
	if kv == nil && l.iterHasError() {
		return nil
	}
	l.updateCurrentSpan()
	return kv
}

func (l *levelIterV2) Next() (kv *base.InternalKV) {
	if treesteps.Enabled && treesteps.IsRecording(l) {
		op := treesteps.StartOpf(l, "Next()")
		defer func() {
			op.Finishf("= %s", kv.String())
		}()
	}
	if l.err != nil {
		return nil
	}
	if l.dir != +1 {
		// Direction change. We cannot be in prefix-iteration mode in this case.
		if !l.currentSpan.Valid() {
			// Exhausted or never positioned. Re-seek from the start of the range.
			if l.lower != nil {
				return l.SeekGE(l.lower, base.SeekGEFlagsNone)
			}
			return l.First()
		}
		if l.atSyntheticBoundary {
			// At a level-iter-synthesized boundary. We can't simply switch
			// direction because files/iterFile state may not match; re-seek.
			// TODO(radu): we can probably do better here.
			return l.SeekGE(l.currentSpan.Boundary, base.SeekGEFlagsNone)
		}
		// Positioned within a file's InterleavingIter; flip direction.
		l.dir = +1
	}

	if l.prefix != nil && l.prefixExhausted {
		l.invalidateSpan()
		return nil
	}

	if !l.atSyntheticBoundary {
		if invariants.Enabled && l.iterFile != l.files.Current() {
			panic(errors.AssertionFailedf("files.Current() (%v) != iterFile (%v)", l.files.Current(), l.iterFile))
		}
		if kv := l.iter.Next(); kv != nil {
			if l.prefix != nil && kv.K.Kind() == base.InternalKeyKindSpanBoundary && !l.matchesPrefix(kv.K.UserKey) {
				// This is the last key we return.
				l.prefixExhausted = true
			}
			l.updateCurrentSpan()
			return kv
		}
		if l.iterHasError() {
			return nil
		}
	}

	var nextFile *manifest.TableMetadata
	if l.atSyntheticBoundary {
		nextFile = l.files.Current()
		l.atSyntheticBoundary = false
	} else {
		nextFile = l.files.Next()
	}
	if nextFile == nil || !l.fileWithinUpper(nextFile) {
		l.invalidateSpan()
		return nil
	}
	if l.loadFile(nextFile) == noFileLoaded {
		return nil
	}
	if l.prefix != nil {
		// Here we know we have to start at the beginning of the file; however, if
		// we use First() we would need to implement the prefix mode semantics
		// ourselves. If we have a long run of same-prefix keys, the one seek won't
		// make much difference. If we have a short run, then it's rare that it
		// would cross a file boundary.
		kv = l.iter.SeekPrefixGE(l.prefix, l.prefix, base.SeekGEFlagsNone)
	} else {
		kv = l.iter.First()
	}
	if kv == nil && l.iterHasError() {
		return nil
	}
	if kv != nil && l.prefix != nil && kv.K.Kind() == base.InternalKeyKindSpanBoundary && !l.matchesPrefix(kv.K.UserKey) {
		// This is the last key we return.
		l.prefixExhausted = true
	}
	l.updateCurrentSpan()
	return kv
}

func (l *levelIterV2) NextPrefix(succKey []byte) (kv *base.InternalKV) {
	if treesteps.Enabled && treesteps.IsRecording(l) {
		op := treesteps.StartOpf(l, "NextPrefix(%q)", succKey)
		defer func() {
			op.Finishf("= %s", kv.String())
		}()
	}
	panic(errors.AssertionFailedf("NextPrefix not implemented"))
}

func (l *levelIterV2) Prev() (kv *base.InternalKV) {
	if treesteps.Enabled && treesteps.IsRecording(l) {
		op := treesteps.StartOpf(l, "Prev()")
		defer func() {
			op.Finishf("= %s", kv.String())
		}()
	}
	if l.err != nil {
		return nil
	}
	if invariants.Enabled && l.prefix != nil {
		panic(errors.AssertionFailedf("Prev called in prefix iteration mode"))
	}
	if l.dir != -1 {
		// Direction change.
		if !l.currentSpan.Valid() {
			// Exhausted or never positioned. Re-seek from the end of the range.
			if l.upper != nil {
				return l.SeekLT(l.upper, base.SeekLTFlagsNone)
			}
			return l.Last()
		}
		if l.atSyntheticBoundary {
			// At a level-iter-synthesized boundary. We can't simply switch
			// direction because files/iterFile state may not match; re-seek.
			// TODO(radu): we can probably do better here.
			return l.SeekLT(l.currentSpan.Boundary, base.SeekLTFlagsNone)
		}
		// Positioned within a file's InterleavingIter; flip direction.
		l.dir = -1
	}

	if !l.atSyntheticBoundary {
		if invariants.Enabled && l.iterFile != l.files.Current() {
			panic(errors.AssertionFailedf("files.Current() (%v) != iterFile (%v)", l.files.Current(), l.iterFile))
		}
		if kv := l.iter.Prev(); kv != nil {
			l.updateCurrentSpan()
			return kv
		}
		if l.iterHasError() {
			return nil
		}
	}

	var prevFile *manifest.TableMetadata
	if l.atSyntheticBoundary {
		prevFile = l.files.Current()
		l.atSyntheticBoundary = false
	} else {
		prevFile = l.files.Prev()
		if prevFile == nil {
			// We're before all files. After files.Prev() returns nil,
			// files.PeekNext() returns the first file. If there's a gap between
			// l.lower and the first file's start, emit a BoundaryStart at l.lower
			// to represent that empty span. After emitting, files.Current() is
			// nil, so a subsequent Prev() will exhaust.
			if l.lower != nil {
				if firstFile := l.files.PeekNext(); firstFile != nil && l.comparer.Compare(l.lower, firstFile.PointKeyBounds.SmallestUserKey()) < 0 {
					return l.emitBoundary(l.lower, -1)
				}
			}
			l.invalidateSpan()
			return nil
		}
	}
	if prevFile == nil || !l.fileWithinLower() {
		l.invalidateSpan()
		return nil
	}
	if l.loadFile(prevFile) == noFileLoaded {
		return nil
	}
	kv = l.iter.Last()
	if kv == nil && l.iterHasError() {
		return nil
	}
	l.updateCurrentSpan()
	return kv
}

// reset is called at the start of every absolute positioning method (SeekGE,
// SeekLT, First, Last) and by setError. It clears error, direction, prefix,
// and span state but does NOT close the current file iterator; callers that
// need a file close (e.g. SetBounds) must call closeFileIter separately.
func (l *levelIterV2) reset(dir int8) {
	l.err = nil // clear cached iteration error
	l.dir = dir
	l.atSyntheticBoundary = false
	l.prefixExhausted = false
	l.prefix = nil
	l.invalidateSpan()
}

func (l *levelIterV2) invalidateSpan() {
	l.currentSpan = iterv2.Span{}
}

// Span implements iterv2.Iter. It returns the current span. When positioned,
// BoundaryType is not BoundaryNone. When exhausted or unpositioned, the span
// has zero-value fields.
func (l *levelIterV2) Span() *iterv2.Span {
	return &l.currentSpan
}

func (l *levelIterV2) Error() error {
	if l.err != nil {
		return l.err
	}
	if l.iterFile != nil {
		return l.iter.Error()
	}
	return nil
}

func (l *levelIterV2) closeFileIter() error {
	if l.iterFile != nil {
		if err := l.iter.Close(); err != nil && l.err == nil {
			l.err = err
		}
		l.iterFile = nil
	}
	return l.err
}
func (l *levelIterV2) Close() error {
	_ = l.closeFileIter()
	l.invalidateSpan()
	return l.err
}

func (l *levelIterV2) SetBounds(lower, upper []byte) {
	l.reset(0)
	l.lower = lower
	l.upper = upper

	// TODO(radu): if the file is still covered by the iterator, keep it open and
	// call SetBounds with the new bounds.
	// closeFileIter() will set levelIterV2.err if an error occurs.
	_ = l.closeFileIter()
}

func (l *levelIterV2) SetContext(ctx context.Context) {
	l.ctx = ctx
	if l.iterFile != nil {
		// TODO(sumeer): this is losing the ctx = objiotracing.WithLevel(ctx,
		// manifest.LevelToInt(opts.level)) that happens in table_cache.go.
		l.iter.SetContext(ctx)
	}
}

// TreeStepsNode is part of the InternalIterator interface.
func (l *levelIterV2) TreeStepsNode() treesteps.NodeInfo {
	info := treesteps.NodeInfof(l, "levelIterV2 %s", l.layer)
	if l.iterFile != nil {
		info.AddPropf("file", "%s", l.iterFile.TableNum)
	}
	info.AddChildren(&l.iter)
	return info
}

func (l *levelIterV2) String() string {
	if l.iterFile != nil {
		return fmt.Sprintf("%s: fileNum=%s", l.layer, l.iterFile.TableNum.String())
	}
	return fmt.Sprintf("%s: fileNum=<nil>", l.layer)
}

// maybeEmitBoundaryFwd emits a forward boundary key. Called when the current
// position is in a gap before nextFile, or past all files (nextFile == nil).
// When nextFile is non-nil, files must be positioned at nextFile; emits
// nextFile.SmallestUserKey as a BoundaryEnd. When nextFile is nil, emits
// l.upper as a BoundaryEnd (or exhausts if no upper bound). After emitting,
// files.Current() is the file to load on the next Next().
func (l *levelIterV2) maybeEmitBoundaryFwd(nextFile *manifest.TableMetadata) *base.InternalKV {
	if nextFile == nil {
		if l.upper != nil {
			return l.emitBoundary(l.upper, +1)
		}
		l.invalidateSpan()
		return nil
	}
	return l.emitBoundary(nextFile.PointKeyBounds.SmallestUserKey(), +1)
}

// maybeEmitBoundaryBwd emits a backward boundary key. Called when the current
// file's InterleavingIter is exhausted going backward. prevFile is the file
// before the current one (from files.Prev()), or nil if before all files. When
// prevFile is non-nil, files is positioned at prevFile; emits l.fileEndKey()
// (the SmallestUserKey of the file after prevFile, i.e. the boundary between
// prevFile's extended range and the current file's range) as a BoundaryStart.
// When prevFile is nil, emits l.lower as a BoundaryStart (or exhausts if no
// lower bound). After emitting, files.Current() is the file to load on the
// next Prev().
func (l *levelIterV2) maybeEmitBoundaryBwd(prevFile *manifest.TableMetadata) *base.InternalKV {
	if prevFile == nil {
		if l.lower != nil {
			return l.emitBoundary(l.lower, -1)
		}
		l.invalidateSpan()
		return nil
	}
	return l.emitBoundary(l.fileEndKey(), -1)
}

// emitBoundary synthesizes a SpanBoundary key at userKey and sets
// atSyntheticBoundary = true. The currentSpan is set to an empty span (no
// Keys) with BoundaryEnd (forward) or BoundaryStart (backward). After this
// call, files.Current() is the file the next step in dir should load (see the
// files field comment for details).
func (l *levelIterV2) emitBoundary(userKey []byte, dir int8) *base.InternalKV {
	l.atSyntheticBoundary = true
	l.kv = base.InternalKV{
		K: base.MakeInternalKey(userKey, base.SeqNumMax, base.InternalKeyKindSpanBoundary),
	}
	l.currentSpan = iterv2.Span{
		BoundaryType: iterv2.BoundaryEnd,
		Boundary:     userKey,
	}
	if dir == -1 {
		l.currentSpan.BoundaryType = iterv2.BoundaryStart
	}
	if l.prefix != nil && !l.matchesPrefix(userKey) {
		l.prefixExhausted = true
	}
	return &l.kv
}

func (l *levelIterV2) fileWithinUpper(m *manifest.TableMetadata) bool {
	return l.upper == nil || l.comparer.Compare(m.PointKeyBounds.SmallestUserKey(), l.upper) < 0
}

// fileWithinLower returns true if the file at l.files.Current() is relevant for
// backward iteration. Specifically, it checks whether the file's extended range
// (from SmallestUserKey to the next file's SmallestUserKey) extends past
// l.lower, i.e. fileEndKey() > lower.
func (l *levelIterV2) fileWithinLower() bool {
	if l.lower != nil {
		if fileEndKey := l.fileEndKey(); fileEndKey != nil {
			return l.comparer.Compare(fileEndKey, l.lower) > 0
		}
	}
	return true
}

func (l *levelIterV2) iterHasError() bool {
	if err := l.iter.Error(); err != nil {
		l.setError(err)
		return true
	}
	return false
}

func (l *levelIterV2) setError(err error) {
	l.reset(0)
	l.err = err
}

func (l *levelIterV2) matchesPrefix(key []byte) bool {
	return l.comparer.HasPrefix(key, l.prefix)
}
