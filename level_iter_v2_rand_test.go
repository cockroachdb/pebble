// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/iterv2"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

func TestLevelIterV2Rand(t *testing.T) {
	// To reproduce a test failure, use:
	// runLevelIterV2RandomTest(t, <seed>)
	for range 200 {
		runLevelIterV2RandomTest(t, rand.Uint64())
	}
}

func runLevelIterV2RandomTest(t *testing.T, seed uint64) {
	rng := rand.New(rand.NewPCG(0, seed))
	cmp := testkeys.Comparer
	cfg := iterv2.RandKeyConfig(rng)
	points := iterv2.RandPointKeys(rng, cfg, 50)
	spans := iterv2.RandSpans(rng, cfg, 10)

	// Deduplicate span keys by trailer since RandSpans can generate duplicate
	// trailers within a span, which the sstable writer rejects.
	for i := range spans {
		spans[i].Keys = slices.CompactFunc(spans[i].Keys, func(a, b keyspan.Key) bool {
			return a.Trailer == b.Trailer
		})
	}

	numFiles := 2 + rng.IntN(5) // 2 to 6
	boundaries := pickFileBoundaries(rng, cmp, cfg, points, spans, numFiles)

	// Create sstables for each file range [boundaries[i], boundaries[i+1]).
	mem := vfs.NewMem()
	var readers []*sstable.Reader
	var metas []*manifest.TableMetadata
	var filePointKeys [][]base.InternalKV
	var fileSpans [][]keyspan.Span
	defer func() {
		for _, r := range readers {
			r.Close()
		}
	}()

	for i := 0; i+1 < len(boundaries); i++ {
		lo := boundaries[i]
		hi := boundaries[i+1]
		pts := filterPointKeys(cmp.Compare, points, lo, hi)
		clipped := clipSpans(cmp.Compare, spans, lo, hi)
		if len(pts) == 0 && len(clipped) == 0 {
			continue
		}
		r, m := createSSTable(t, mem, cmp, base.TableNum(len(readers)), pts, clipped)
		readers = append(readers, r)
		metas = append(metas, m)
		filePointKeys = append(filePointKeys, pts)
		fileSpans = append(fileSpans, clipped)
	}

	if len(metas) == 0 {
		// No data generated; skip.
		return
	}

	// Build newIters callback following the pattern in levelIterTest.newIters.
	newIters := func(
		ctx context.Context,
		file *manifest.TableMetadata,
		opts *IterOptions,
		iio internalIterOpts,
		kinds iterKinds,
	) (iterSet, error) {
		transforms := file.IterTransforms()
		var set iterSet
		if kinds.Point() {
			iter, err := readers[file.TableNum].NewPointIter(ctx, sstable.IterOptions{
				Lower:          opts.GetLowerBound(),
				Upper:          opts.GetUpperBound(),
				Transforms:     transforms,
				Env:            iio.readEnv,
				ReaderProvider: sstable.MakeTrivialReaderProvider(readers[file.TableNum]),
			})
			if err != nil {
				return iterSet{}, err
			}
			set.point = iter
		}
		if kinds.RangeDeletion() {
			rangeDelIter, err := readers[file.TableNum].NewRawRangeDelIter(
				ctx, file.FragmentIterTransforms(), sstable.NoReadEnv,
			)
			if err != nil {
				return iterSet{}, err
			}
			set.rangeDeletion = rangeDelIter
		}
		return set, nil
	}

	// Optionally generate initial bounds.
	var lower, upper []byte
	if rng.IntN(2) == 0 {
		lower = []byte("a")
	}
	if rng.IntN(2) == 0 {
		upper = []byte("z~")
	}

	// Build the levelIterV2.
	slice := manifest.NewLevelSliceKeySorted(cmp.Compare, metas)
	li := newLevelIterV2(
		context.Background(), IterOptions{
			LowerBound: lower,
			UpperBound: upper,
		}, cmp, newIters,
		slice.Iter(), manifest.Level(1), internalIterOpts{},
	)
	li.loadRangeDels = true
	defer li.Close()

	// Build reference TestIter.
	var fileBoundarySpans []keyspan.Span
	for i := 0; i < len(metas); i++ {
		bKey := metas[i].PointKeyBounds.SmallestUserKey()
		fileBoundarySpans = append(fileBoundarySpans, keyspan.Span{
			Start: bKey,
			End:   bKey,
		})
	}

	// On failure, log seed and configuration.
	defer func() {
		if t.Failed() {
			t.Logf("seed: %d", seed)
			t.Logf("cfg: %+v", cfg)
			t.Logf("initial bounds: %q %q", lower, upper)
			for i, m := range metas {
				p := make([]base.InternalKey, len(filePointKeys[i]))
				for j := range p {
					p[j] = filePointKeys[i][j].K
				}
				t.Logf("file %d (%s - %s):", i, m.PointKeyBounds.Smallest(), m.PointKeyBounds.Largest())
				t.Logf("  points: %v", p)
				t.Logf("  spans: %v", fileSpans[i])
			}
		}
	}()

	allSpans := append(slices.Clone(spans), fileBoundarySpans...)
	slices.SortFunc(allSpans, func(a, b keyspan.Span) int {
		return cmp.Compare(a.Start, b.Start)
	})
	ops := iterv2.AllTestOps
	// TODO(radu): implement NextPrefix.
	ops[iterv2.TestOpNextPrefix] = 0
	iterv2.CheckIter(t, rng, cmp, cfg, ops /* iterv2.AllTestOps*/, points, allSpans, li, nil, nil, lower, upper, 500)
}

// pickFileBoundaries generates sorted, deduplicated file boundaries
// including startKey and endKey.
func pickFileBoundaries(
	rng *rand.Rand,
	cmp *base.Comparer,
	cfg iterv2.KeyGenConfig,
	points []base.InternalKV,
	spans []keyspan.Span,
	numFiles int,
) [][]byte {
	// Collect candidate user keys from points and spans.
	var candidates [][]byte
	for _, p := range points {
		candidates = append(candidates, p.K.UserKey)
	}
	for _, s := range spans {
		candidates = append(candidates, s.Start, s.End)
	}

	var first, last []byte
	if len(points) > 0 {
		first = points[0].K.UserKey
		last = cmp.ImmediateSuccessor(nil, cmp.Split.Prefix(points[len(points)-1].K.UserKey))
	}
	if len(spans) > 0 {
		if first == nil || cmp.Compare(spans[0].Start, first) < 0 {
			first = spans[0].Start
		}
		if last == nil || cmp.Compare(spans[len(spans)-1].End, last) > 0 {
			last = spans[len(spans)-1].End
		}
	}
	if first == nil || rng.IntN(2) == 0 {
		first = []byte("a")
	}
	if last == nil || rng.IntN(2) == 0 {
		last = []byte("z~")
	}

	var boundaries [][]byte
	boundaries = append(boundaries, first)
	// Generate numFiles-1 internal boundary keys.
	for range numFiles - 1 {
		var key []byte
		if len(candidates) > 0 && rng.IntN(5) < 4 {
			// 80% of the time pick from existing keys.
			key = slices.Clone(candidates[rng.IntN(len(candidates))])
		} else {
			key = cfg.RandKey(rng)
		}
		boundaries = append(boundaries, key)
	}
	boundaries = append(boundaries, last)

	// Sort and deduplicate.
	slices.SortFunc(boundaries, func(a, b []byte) int {
		return cmp.Compare(a, b)
	})
	boundaries = slices.CompactFunc(boundaries, func(a, b []byte) bool {
		return cmp.Compare(a, b) == 0
	})
	return boundaries
}

// filterPointKeys returns the point keys with UserKey in [lo, hi).
func filterPointKeys(
	cmp base.Compare, pointKeys []base.InternalKV, lo, hi []byte,
) []base.InternalKV {
	var result []base.InternalKV
	for _, pk := range pointKeys {
		if cmp(pk.K.UserKey, lo) >= 0 && cmp(pk.K.UserKey, hi) < 0 {
			result = append(result, pk)
		}
	}
	return result
}

// clipSpans returns spans overlapping [lo, hi), clipped to that range.
func clipSpans(cmp base.Compare, spans []keyspan.Span, lo, hi []byte) []keyspan.Span {
	var result []keyspan.Span
	for _, s := range spans {
		if cmp(s.Start, hi) >= 0 || cmp(s.End, lo) <= 0 {
			continue
		}
		clipped := s
		if cmp(clipped.Start, lo) < 0 {
			clipped.Start = lo
		}
		if cmp(clipped.End, hi) > 0 {
			clipped.End = hi
		}
		if cmp(clipped.Start, clipped.End) < 0 {
			result = append(result, clipped)
		}
	}
	return result
}

// createSSTable writes an sstable with the given point keys and range deletion
// spans, then opens and returns the reader and table metadata.
func createSSTable(
	t *testing.T,
	mem vfs.FS,
	cmp *base.Comparer,
	tableNum base.TableNum,
	points []base.InternalKV,
	spans []keyspan.Span,
) (*sstable.Reader, *manifest.TableMetadata) {
	t.Helper()

	name := fmt.Sprint(tableNum)
	f, err := mem.Create(name, vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatal(err)
	}

	w := sstable.NewRawWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{
		Comparer:    cmp,
		TableFormat: sstable.TableFormatMinSupported,
	})

	// Add point keys (must be in sorted order, which they already are).
	for _, pk := range points {
		if err := w.Add(pk.K, pk.V.InPlaceValue(), false /* forceObsolete */, base.KVMeta{}); err != nil {
			t.Fatal(err)
		}
	}

	// Encode range deletion spans (must be in sorted order, which they already are).
	for _, s := range spans {
		if err := w.EncodeSpan(s); err != nil {
			t.Fatal(err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	meta, err := w.Metadata()
	if err != nil {
		t.Fatal(err)
	}

	rf, err := mem.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	readable, err := objstorage.NewSimpleReadable(rf)
	if err != nil {
		t.Fatal(err)
	}
	r, err := sstable.NewReader(context.Background(), readable, sstable.ReaderOptions{
		Comparer: cmp,
	})
	if err != nil {
		t.Fatal(err)
	}

	m := &manifest.TableMetadata{TableNum: tableNum}
	if meta.HasPointKeys {
		m.ExtendPointKeyBounds(cmp.Compare, meta.SmallestPoint, meta.LargestPoint)
	}
	if meta.HasRangeDelKeys {
		m.ExtendPointKeyBounds(cmp.Compare, meta.SmallestRangeDel, meta.LargestRangeDel)
	}
	if meta.HasRangeKeys {
		m.ExtendRangeKeyBounds(cmp.Compare, meta.SmallestRangeKey, meta.LargestRangeKey)
	}
	m.InitPhysicalBacking()
	return r, m
}
