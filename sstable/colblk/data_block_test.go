// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/itertest"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/internal/treeprinter"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/blockiter"
)

var testKeysSchema = DefaultKeySchema(testkeys.Comparer, 16)

// dataBlockIterInternalIterator wraps a DataBlockIter as a base.InternalIterator
// for use in itertest.RunInternalIterCmd.
type dataBlockIterInternalIterator struct {
	*DataBlockIter
}

func (d *dataBlockIterInternalIterator) SeekPrefixGE(
	prefix, key []byte, flags base.SeekGEFlags,
) *base.InternalKV {
	panic("SeekPrefixGE not supported on data block iterators")
}

func (d *dataBlockIterInternalIterator) SetBounds(lower, upper []byte) {}
func (d *dataBlockIterInternalIterator) SetContext(_ context.Context)  {}
func (d *dataBlockIterInternalIterator) String() string                { return "data-block-iter" }

func TestDataBlock(t *testing.T) {
	var buf bytes.Buffer
	var w DataBlockEncoder
	var r DataBlockDecoder
	var bd BlockDecoder
	var v DataBlockValidator
	var it DataBlockIter
	tieringConfig := NoTieringColumns()
	rw := NewDataBlockRewriter(&testKeysSchema, testkeys.Comparer.EnsureDefaults(), tieringConfig)
	var sizes []int
	it.InitOnce(&testKeysSchema, testkeys.Comparer,
		getInternalValuer(func([]byte) base.InternalValue {
			return base.MakeInPlaceValue([]byte("mock external value"))
		}), tieringConfig)

	datadriven.Walk(t, "testdata/data_block", func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
			buf.Reset()
			switch td.Cmd {
			case "init":
				var bundleSize int
				if td.MaybeScanArgs(t, "bundle-size", &bundleSize) {
					s := DefaultKeySchema(testkeys.Comparer, bundleSize)
					w.Init(&s, NoTieringColumns())
				} else {
					w.Init(&testKeysSchema, NoTieringColumns())
				}
				fmt.Fprint(&buf, &w)
				sizes = sizes[:0]
				return buf.String()
			case "write", "write-block":
				// write-block does init/write/finish in a single command, and doesn't
				// print anything.
				if td.Cmd == "write-block" {
					w.Init(&testKeysSchema, NoTieringColumns())
				}
				var prevKey base.InternalKey
				for line := range crstrings.LinesSeq(td.Input) {
					line, isObsolete := strings.CutSuffix(line, "obsolete")

					j := strings.IndexRune(line, ':')
					ik := base.ParseInternalKey(line[:j])

					kcmp := w.KeyWriter.ComparePrev(ik.UserKey)
					valueString := line[j+1:]
					vp := block.InPlaceValuePrefix(kcmp.PrefixEqual())
					if strings.HasPrefix(valueString, "valueHandle") {
						vp = block.ValueBlockHandlePrefix(kcmp.PrefixEqual(), 0)
					} else if strings.HasPrefix(valueString, "blobHandle") {
						vp = block.BlobValueHandlePrefix(kcmp.PrefixEqual(), 0)
					}
					if kcmp.UserKeyComparison == 0 && prevKey.Kind() != base.InternalKeyKindMerge {
						isObsolete = true
					}
					v := []byte(line[j+1:])
					w.Add(ik, v, vp, kcmp, isObsolete, base.KVMeta{})
					prevKey = ik
					sizes = append(sizes, w.Size())
				}
				if td.Cmd == "write-block" {
					block, _ := w.Finish(w.Rows(), w.Size())
					bd = r.Init(&testKeysSchema, block)
					return ""
				}
				fmt.Fprint(&buf, &w)
				return buf.String()
			case "rewrite":
				var from, to string
				td.ScanArgs(t, "from", &from)
				td.ScanArgs(t, "to", &to)
				start, end, rewrittenBlock, err := rw.RewriteSuffixes(bd.Data(), []byte(from), []byte(to))
				if err != nil {
					return fmt.Sprintf("error: %s", err)
				}
				bd = r.Init(&testKeysSchema, rewrittenBlock)
				f := binfmt.New(bd.Data()).LineWidth(20)
				tp := treeprinter.New()
				r.Describe(f, tp, bd)
				fmt.Fprintf(&buf, "Start: %s\nEnd: %s\n%s",
					start.Pretty(testkeys.Comparer.FormatKey),
					end.Pretty(testkeys.Comparer.FormatKey),
					tp.String())
				return buf.String()
			case "finish":
				rows := w.Rows()
				td.MaybeScanArgs(t, "rows", &rows)
				block, lastKey := w.Finish(rows, sizes[rows-1])
				bd = r.Init(&testKeysSchema, block)
				f := binfmt.New(bd.Data()).LineWidth(20)
				tp := treeprinter.New()
				r.Describe(f, tp, bd)
				fmt.Fprintf(&buf, "LastKey: %s\n%s", lastKey.Pretty(testkeys.Comparer.FormatKey), tp.String())
				if err := v.Validate(block, testkeys.Comparer, &testKeysSchema); err != nil {
					fmt.Fprintln(&buf, err)
				}
				return buf.String()
			case "iter":
				var seqNum uint64
				var syntheticPrefix, syntheticSuffix string
				td.MaybeScanArgs(t, "synthetic-seq-num", &seqNum)
				td.MaybeScanArgs(t, "synthetic-prefix", &syntheticPrefix)
				td.MaybeScanArgs(t, "synthetic-suffix", &syntheticSuffix)
				masks := parseSuffixMaskArgs(t, td)
				transforms := blockiter.Transforms{
					SyntheticSeqNum:          blockiter.SyntheticSeqNum(seqNum),
					HideObsoletePoints:       td.HasArg("hide-obsolete-points"),
					SyntheticPrefixAndSuffix: blockiter.MakeSyntheticPrefixAndSuffix([]byte(syntheticPrefix), []byte(syntheticSuffix)),
					SuffixMasks:              masks,
				}
				if err := it.Init(&r, bd, transforms, tieringConfig); err != nil {
					return err.Error()
				}

				o := []itertest.IterOpt{itertest.ShowCommands}
				if td.HasArg("verbose") {
					o = append(o, itertest.Verbose)
				}
				if td.HasArg("invalidated") {
					it.Invalidate()
				}
				return itertest.RunInternalIterCmd(t, td, &dataBlockIterInternalIterator{&it}, o...)
			default:
				return fmt.Sprintf("unknown command: %s", td.Cmd)
			}
		})
	})
}

func BenchmarkDataBlockWriter(b *testing.B) {
	for _, prefixSize := range []int{8, 32, 128} {
		for _, valueSize := range []int{8, 128, 1024} {
			b.Run(fmt.Sprintf("prefix=%d,value=%d", prefixSize, valueSize), func(b *testing.B) {
				benchmarkDataBlockWriter(b, prefixSize, valueSize)
			})
		}
	}
}

func benchmarkDataBlockWriter(b *testing.B, prefixSize, valueSize int) {
	const targetBlockSize = 32 << 10
	seed := uint64(time.Now().UnixNano())
	rng := rand.New(rand.NewPCG(0, seed))
	keys, values := makeTestKeyRandomKVs(rng, prefixSize, valueSize, targetBlockSize)

	var w DataBlockEncoder
	w.Init(&testKeysSchema, NoTieringColumns())
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w.Reset()
		var j int
		for w.Size() < targetBlockSize {
			ik := base.MakeInternalKey(keys[j], base.SeqNum(rng.Uint64N(uint64(base.SeqNumMax))), base.InternalKeyKindSet)
			kcmp := w.KeyWriter.ComparePrev(ik.UserKey)
			vp := block.InPlaceValuePrefix(kcmp.PrefixEqual())
			w.Add(ik, values[j], vp, kcmp, false /* isObsolete */, base.KVMeta{})
			j++
		}
		w.Finish(w.Rows(), w.Size())
	}
}

func makeTestKeyRandomKVs(
	rng *rand.Rand, prefixSize, valueSize int, aggregateSize int,
) (keys, vals [][]byte) {
	keys = make([][]byte, aggregateSize/valueSize+1)
	vals = make([][]byte, len(keys))
	for i := range keys {
		keys[i] = randTestKey(rng, make([]byte, prefixSize+testkeys.MaxSuffixLen), prefixSize)
		vals[i] = make([]byte, valueSize)
		for j := range vals[i] {
			vals[i][j] = byte(rng.Uint32())
		}
	}
	slices.SortFunc(keys, bytes.Compare)
	return keys, vals
}

func randTestKey(rng *rand.Rand, buf []byte, prefixLen int) []byte {
	suffix := rng.Int64N(100)
	sl := testkeys.SuffixLen(suffix)
	buf = buf[0 : prefixLen+sl]
	for i := 0; i < prefixLen; i++ {
		buf[i] = byte(rng.IntN(26) + 'a')
	}
	testkeys.WriteSuffix(buf[prefixLen:], suffix)
	return buf
}

type getInternalValuer func([]byte) base.InternalValue

func (g getInternalValuer) GetInternalValueForPrefixAndValueHandle(
	handle []byte,
) base.InternalValue {
	return g(handle)
}

// TestDataBlockIterSeekPrefixGENextWithSamePrefix exercises
// DataBlockIter.SeekPrefixGE and DataBlockIter.NextWithSamePrefix against an
// independently-derived expected behavior (linear scan of the keys with
// byte-level prefix comparison) under random transforms.
func TestDataBlockIterSeekPrefixGENextWithSamePrefix(t *testing.T) {
	const targetBlockSize = 32 << 10
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))
	// Use a moderate prefix size to ensure shared prefixes.
	keys, values := makeTestKeyRandomKVs(rng, 4, 8, targetBlockSize)
	// makeTestKeyRandomKVs sorts via bytes.Compare, but the KeySchema uses
	// testkeys.Comparer which orders suffixes in reverse numeric order. Re-sort
	// in the comparer's order so the block is well-formed.
	slices.SortFunc(keys, testkeys.Comparer.Compare)

	// Build a data block. Each row is marked obsolete with a per-block
	// probability picked uniformly at random; this lets coverage span from no
	// obsolete rows to all rows obsolete across seeds.
	obsoleteProb := rng.Float64()
	var w DataBlockEncoder
	w.Init(&testKeysSchema, NoTieringColumns())
	var nRows, nObsolete int
	var blockKeys [][]byte
	var blockObsolete []bool
	for j := 0; w.Size() < targetBlockSize; j++ {
		ik := base.MakeInternalKey(keys[j], base.SeqNum(rng.Uint64N(uint64(base.SeqNumMax))), base.InternalKeyKindSet)
		kcmp := w.KeyWriter.ComparePrev(ik.UserKey)
		vp := block.InPlaceValuePrefix(kcmp.PrefixEqual())
		isObsolete := rng.Float64() < obsoleteProb
		w.Add(ik, values[j], vp, kcmp, isObsolete, base.KVMeta{})
		nRows++
		if isObsolete {
			nObsolete++
		}
		blockKeys = append(blockKeys, append([]byte(nil), ik.UserKey...))
		blockObsolete = append(blockObsolete, isObsolete)
	}
	t.Logf("rows: %d, obsolete: %d (probability %.3f)", nRows, nObsolete, obsoleteProb)
	blockData, _ := w.Finish(w.Rows(), w.Size())

	var r DataBlockDecoder
	bd := r.Init(&testKeysSchema, blockData)

	split := testkeys.Comparer.Split
	cmp := testkeys.Comparer.Compare

	// applyTransforms returns the visible UserKey for the given row, accounting
	// for synthetic prefix and synthetic suffix (but not seq num).
	applyTransforms := func(row int, transforms blockiter.Transforms) []byte {
		k := blockKeys[row]
		out := append([]byte(nil), transforms.SyntheticPrefixAndSuffix.Prefix()...)
		if transforms.HasSyntheticSuffix() {
			n := split(k)
			out = append(out, k[:n]...)
			out = append(out, transforms.SyntheticPrefixAndSuffix.Suffix()...)
		} else {
			out = append(out, k...)
		}
		return out
	}

	// rowVisible reports whether row r is visible under transforms (i.e. not
	// hidden via HideObsoletePoints).
	rowVisible := func(r int, transforms blockiter.Transforms) bool {
		return !(transforms.HideObsoletePoints && blockObsolete[r])
	}

	// expectedSeekPrefixGE returns the expected (key, prefixDidNotMatch) for
	// SeekPrefixGE(seekKey) against the linear scan ground truth. expectedRow
	// is the row the iterator should be positioned at, or -1 if no key ≥
	// seekKey. expectedKey is the key the iterator's KV() should reflect at
	// that row (set even when prefixDidNotMatch is true, since the iterator
	// is still positioned).
	expectedSeekPrefixGE := func(seekKey []byte, transforms blockiter.Transforms) (
		expectedKey []byte, prefixDidNotMatch bool, expectedRow int,
	) {
		seekPrefixLen := split(seekKey)
		seekPrefix := seekKey[:seekPrefixLen]
		for r := 0; r < nRows; r++ {
			if !rowVisible(r, transforms) {
				continue
			}
			k := applyTransforms(r, transforms)
			if cmp(k, seekKey) >= 0 {
				kPrefixLen := split(k)
				kPrefix := k[:kPrefixLen]
				if bytes.Equal(seekPrefix, kPrefix) {
					return k, false, r
				}
				return k, true, r
			}
		}
		return nil, false, -1
	}

	// expectedNextWithSamePrefix returns the expected (kv key, prefixExhausted)
	// for NextWithSamePrefix from currentRow under transforms. expectedRow is
	// the row the iterator should be positioned at after the call:
	//   - the new same-prefix row when prefixExhausted=false
	//   - the new different-prefix row when prefixExhausted=true (NOTE: new
	//     semantics — iterator is left positioned at the new-prefix row)
	//   - -1 when there are no more visible rows.
	// expectedKey is the key the iterator's KV() should reflect at expectedRow
	// (set even when prefixExhausted is true, since the iterator is still
	// positioned).
	expectedNextWithSamePrefix := func(currentRow int, transforms blockiter.Transforms) (
		expectedKey []byte, prefixExhausted bool, expectedRow int,
	) {
		curKey := applyTransforms(currentRow, transforms)
		curPrefixLen := split(curKey)
		curPrefix := curKey[:curPrefixLen]
		for r := currentRow + 1; r < nRows; r++ {
			if !rowVisible(r, transforms) {
				continue
			}
			k := applyTransforms(r, transforms)
			kPrefixLen := split(k)
			if kPrefixLen != curPrefixLen ||
				!bytes.Equal(k[:kPrefixLen], curPrefix) {
				return k, true, r
			}
			return k, false, r
		}
		// No more visible keys after currentRow → past end.
		return nil, false, -1
	}

	// randomTransforms returns a random Transforms.
	randomTransforms := func() blockiter.Transforms {
		var tr blockiter.Transforms
		if rng.IntN(2) == 0 {
			tr.SyntheticSeqNum = blockiter.SyntheticSeqNum(1 + rng.Uint64N(1000))
		}
		tr.HideObsoletePoints = rng.IntN(2) == 0
		var sp blockiter.SyntheticPrefix
		var ss blockiter.SyntheticSuffix
		switch rng.IntN(4) {
		case 0:
			// none
		case 1:
			sp = randSyntheticPrefix(rng)
		case 2:
			ss = blockiter.SyntheticSuffix("@1000")
		case 3:
			sp = randSyntheticPrefix(rng)
			ss = blockiter.SyntheticSuffix("@1000")
		}
		tr.SyntheticPrefixAndSuffix = blockiter.MakeSyntheticPrefixAndSuffix(sp, ss)
		return tr
	}

	// rowOf returns the row index for the given kv (using the iterator's
	// internal row state).
	rowOf := func(it *DataBlockIter) int {
		return it.row
	}

	const numIters = 20
	const numOps = 200
	for iter := 0; iter < numIters; iter++ {
		transforms := randomTransforms()
		sp := transforms.SyntheticPrefixAndSuffix.Prefix()
		t.Run(fmt.Sprintf("iter%d", iter), func(t *testing.T) {
			t.Logf("transforms: SyntheticSeqNum=%d HideObsoletePoints=%v SyntheticPrefix=%q SyntheticSuffix=%q",
				transforms.SyntheticSeqNum, transforms.HideObsoletePoints,
				sp, transforms.SyntheticPrefixAndSuffix.Suffix())

			var it DataBlockIter
			it.InitOnce(&testKeysSchema, testkeys.Comparer,
				getInternalValuer(func([]byte) base.InternalValue {
					return base.MakeInPlaceValue(nil)
				}), NoTieringColumns())
			if err := it.Init(&r, bd, transforms, NoTieringColumns()); err != nil {
				t.Fatal(err)
			}
			defer it.Close()

			// Each op picks a random seek key, calls SeekPrefixGE, then walks
			// some NextWithSamePrefix calls.
			for op := 0; op < numOps; op++ {
				seekKey := append([]byte(nil), keys[rng.IntN(len(keys))]...)
				if sp.IsSet() {
					seekKey = append(append([]byte(nil), sp...), seekKey...)
				}
				expKey, expDidNotMatch, expRow := expectedSeekPrefixGE(seekKey, transforms)

				kv, didNotMatch := it.SeekPrefixGE(seekKey, base.SeekGEFlagsNone)
				if expRow == -1 {
					if kv != nil || didNotMatch {
						t.Fatalf("SeekPrefixGE(%q): expected (nil, false), got (%v, %v)",
							seekKey, kv, didNotMatch)
					}
					continue
				}
				if didNotMatch != expDidNotMatch {
					t.Fatalf("SeekPrefixGE(%q): didNotMatch=%v, want %v",
						seekKey, didNotMatch, expDidNotMatch)
				}
				if expDidNotMatch {
					if kv != nil {
						t.Fatalf("SeekPrefixGE(%q): expected nil kv with didNotMatch=true, got %s",
							seekKey, kv)
					}
					// Iterator should be positioned at expRow; its KV() should
					// reflect expKey.
					if rowOf(&it) != expRow {
						t.Fatalf("SeekPrefixGE(%q): iterator row=%d, want %d",
							seekKey, rowOf(&it), expRow)
					}
					if got := it.KV().K.UserKey; !bytes.Equal(got, expKey) {
						t.Fatalf("SeekPrefixGE(%q): KV()=%q, want %q",
							seekKey, got, expKey)
					}
				} else {
					if kv == nil || !bytes.Equal(kv.K.UserKey, expKey) {
						t.Fatalf("SeekPrefixGE(%q): kv=%v, want key=%q",
							seekKey, kv, expKey)
					}
					if rowOf(&it) != expRow {
						t.Fatalf("SeekPrefixGE(%q): iterator row=%d, want %d",
							seekKey, rowOf(&it), expRow)
					}
				}

				// Now walk a few NextWithSamePrefix calls.
				if expDidNotMatch {
					// When SeekPrefixGE returned (nil, true), continuing with
					// NextWithSamePrefix would iterate within the (different)
					// prefix at the current row. Skip — this is well-defined
					// but not exciting to test.
					continue
				}
				curRow := expRow
				for step := 0; step < 5; step++ {
					expKey2, expExhausted, expRow2 := expectedNextWithSamePrefix(curRow, transforms)
					kv2, exhausted := it.NextWithSamePrefix()
					if expRow2 == -1 && !expExhausted {
						// Past end.
						if kv2 != nil || exhausted {
							t.Fatalf("NextWithSamePrefix from row %d: expected (nil, false), got (%v, %v)",
								curRow, kv2, exhausted)
						}
						break
					}
					if exhausted != expExhausted {
						t.Fatalf("NextWithSamePrefix from row %d: exhausted=%v, want %v",
							curRow, exhausted, expExhausted)
					}
					if expExhausted {
						if kv2 != nil {
							t.Fatalf("NextWithSamePrefix from row %d: expected nil kv with exhausted=true, got %s",
								curRow, kv2)
						}
						// Per the new spec, the iterator IS positioned at the
						// new-prefix row. Verify both row index and that lazy
						// KV() materializes the expected key.
						if rowOf(&it) != expRow2 {
							t.Fatalf("NextWithSamePrefix from row %d: iter row=%d, want %d",
								curRow, rowOf(&it), expRow2)
						}
						if got := it.KV().K.UserKey; !bytes.Equal(got, expKey2) {
							t.Fatalf("NextWithSamePrefix from row %d: KV()=%q, want %q",
								curRow, got, expKey2)
						}
						break
					}
					if kv2 == nil || !bytes.Equal(kv2.K.UserKey, expKey2) {
						t.Fatalf("NextWithSamePrefix from row %d: kv=%v, want key=%q",
							curRow, kv2, expKey2)
					}
					if rowOf(&it) != expRow2 {
						t.Fatalf("NextWithSamePrefix from row %d: iter row=%d, want %d",
							curRow, rowOf(&it), expRow2)
					}
					curRow = expRow2
				}
			}
		})
	}
}

func randSyntheticPrefix(rng *rand.Rand) blockiter.SyntheticPrefix {
	n := 1 + rng.IntN(4)
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = byte(rng.IntN(26) + 'A')
	}
	return blockiter.SyntheticPrefix(buf)
}

func BenchmarkDataBlockDecoderInit(b *testing.B) {
	const targetBlockSize = 32 << 10
	seed := uint64(20250919)
	rng := rand.New(rand.NewPCG(0, seed))
	keys, values := makeTestKeyRandomKVs(rng, 8, 8, targetBlockSize)

	var w DataBlockEncoder
	w.Init(&testKeysSchema, NoTieringColumns())
	for j := 0; w.Size() < targetBlockSize; j++ {
		ik := base.MakeInternalKey(keys[j], base.SeqNum(rng.Uint64N(uint64(base.SeqNumMax))), base.InternalKeyKindSet)
		kcmp := w.KeyWriter.ComparePrev(ik.UserKey)
		vp := block.InPlaceValuePrefix(kcmp.PrefixEqual())
		w.Add(ik, values[j], vp, kcmp, false /* isObsolete */, base.KVMeta{})
	}
	finished, _ := w.Finish(w.Rows(), w.Size())

	var md block.Metadata

	b.ResetTimer()
	for range b.N {
		InitDataBlockMetadata(&testKeysSchema, &md, finished)
	}
}

// TestDataBlockIterSuffixMaskOracle exercises every DataBlockIter positioning
// method under random combinations of SuffixMask and HideObsoletePoints,
// comparing against an oracle that filters the raw key list in user space.
//
// The oracle is the natural specification for "hide keys whose suffix is in
// [Lower, Upper)" — anything more clever would just reimplement the iterator
// (and likely the same bug). This test would have caught the obsolete-then-
// mask interleave bug, would catch any positioning method that forgets to
// call the hidden-row skip, and would catch any divergence between the row-
// level mask predicate and ComparePointSuffixes.
func TestDataBlockIterSuffixMaskOracle(t *testing.T) {
	const targetBlockSize = 4 << 10
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	// Generate keys with a small prefix space so many keys share prefixes
	// (exercises NextPrefix/SeekPrefixGE more thoroughly).
	keys, values := makeTestKeyRandomKVs(rng, 2, 8, targetBlockSize)
	slices.SortFunc(keys, testkeys.Comparer.Compare)

	// Build a block where ~1/4 of rows are marked obsolete.
	var w DataBlockEncoder
	w.Init(&testKeysSchema, NoTieringColumns())
	var blockKeys [][]byte
	var blockObsolete []bool
	for j := 0; w.Size() < targetBlockSize && j < len(keys); j++ {
		ik := base.MakeInternalKey(keys[j], base.SeqNum(j+1), base.InternalKeyKindSet)
		kcmp := w.KeyWriter.ComparePrev(ik.UserKey)
		vp := block.InPlaceValuePrefix(kcmp.PrefixEqual())
		isObsolete := rng.IntN(4) == 0
		w.Add(ik, values[j], vp, kcmp, isObsolete, base.KVMeta{})
		blockKeys = append(blockKeys, append([]byte(nil), ik.UserKey...))
		blockObsolete = append(blockObsolete, isObsolete)
	}
	blockData, _ := w.Finish(w.Rows(), w.Size())
	t.Logf("rows: %d", len(blockKeys))

	var r DataBlockDecoder
	bd := r.Init(&testKeysSchema, blockData)
	split := testkeys.Comparer.Split
	cmp := testkeys.Comparer.Compare
	suffixCmp := testkeys.Comparer.ComparePointSuffixes

	// suffixOf returns the suffix bytes of a key (empty if suffixless).
	suffixOf := func(k []byte) []byte {
		n := split(k)
		return k[n:]
	}

	// randMask returns a random non-zero SuffixMask. Returns the zero value
	// if there are not enough distinct suffixes to construct one.
	randMask := func() (blockiter.SuffixMask, bool) {
		// Pick two suffixes from existing keys' suffixes so the mask is
		// likely to actually filter some rows. The testkeys comparer orders
		// larger numeric suffixes first, so Lower must compare <= Upper per
		// the comparer (i.e. Lower's numeric suffix is larger).
		var suffixes [][]byte
		for _, k := range blockKeys {
			if s := suffixOf(k); len(s) > 0 {
				suffixes = append(suffixes, s)
			}
		}
		if len(suffixes) < 2 {
			return blockiter.SuffixMask{}, false
		}
		a := suffixes[rng.IntN(len(suffixes))]
		b := suffixes[rng.IntN(len(suffixes))]
		if suffixCmp(a, b) > 0 {
			a, b = b, a
		}
		return blockiter.SuffixMask{Lower: a, Upper: b}, true
	}

	// randMasks returns 0..3 random SuffixMasks.
	randMasks := func() []blockiter.SuffixMask {
		n := rng.IntN(4) // 0..3
		var out []blockiter.SuffixMask
		for j := 0; j < n; j++ {
			if m, ok := randMask(); ok {
				out = append(out, m)
			}
		}
		return out
	}

	// visibleRows returns the indices of rows visible under transforms, in
	// block order. This is the oracle.
	visibleRows := func(tr blockiter.Transforms) []int {
		var out []int
		for i, k := range blockKeys {
			if tr.HideObsoletePoints && blockObsolete[i] {
				continue
			}
			if masked := func() bool {
				if len(tr.SuffixMasks) == 0 {
					return false
				}
				s := suffixOf(k)
				if len(s) == 0 {
					return false
				}
				for _, m := range tr.SuffixMasks {
					if suffixCmp(s, m.Lower) >= 0 && suffixCmp(s, m.Upper) < 0 {
						return true
					}
				}
				return false
			}(); masked {
				continue
			}
			out = append(out, i)
		}
		return out
	}

	const trials = 25
	for trial := 0; trial < trials; trial++ {
		tr := blockiter.Transforms{
			HideObsoletePoints: rng.IntN(2) == 0,
			SuffixMasks:        randMasks(),
		}
		t.Run(fmt.Sprintf("trial%d", trial), func(t *testing.T) {
			t.Logf("hideObsolete=%v numMasks=%d", tr.HideObsoletePoints, len(tr.SuffixMasks))
			for i, m := range tr.SuffixMasks {
				t.Logf("  mask[%d]=[%x,%x)", i, m.Lower, m.Upper)
			}

			visible := visibleRows(tr)
			t.Logf("visible rows: %d / %d", len(visible), len(blockKeys))

			newIter := func() *DataBlockIter {
				it := &DataBlockIter{}
				it.InitOnce(&testKeysSchema, testkeys.Comparer,
					getInternalValuer(func([]byte) base.InternalValue {
						return base.MakeInPlaceValue(nil)
					}), NoTieringColumns())
				if err := it.Init(&r, bd, tr, NoTieringColumns()); err != nil {
					t.Fatal(err)
				}
				return it
			}

			// Forward traversal: First/Next must yield exactly the visible rows
			// in order.
			t.Run("forward", func(t *testing.T) {
				it := newIter()
				defer it.Close()
				var got []int
				for kv := it.First(); kv != nil; kv = it.Next() {
					got = append(got, it.row)
				}
				if !slices.Equal(got, visible) {
					t.Fatalf("forward got rows %v, want %v", got, visible)
				}
			})

			// Backward traversal: Last/Prev must yield visible rows in reverse.
			t.Run("backward", func(t *testing.T) {
				it := newIter()
				defer it.Close()
				var got []int
				for kv := it.Last(); kv != nil; kv = it.Prev() {
					got = append(got, it.row)
				}
				want := slices.Clone(visible)
				slices.Reverse(want)
				if !slices.Equal(got, want) {
					t.Fatalf("backward got rows %v, want %v", got, want)
				}
			})

			// SeekGE then Next: for each block key, SeekGE(k) must land at the
			// first visible row whose key is >= k.
			t.Run("seek-ge", func(t *testing.T) {
				it := newIter()
				defer it.Close()
				for _, seekKey := range blockKeys {
					var want []int
					for _, r := range visible {
						if cmp(blockKeys[r], seekKey) >= 0 {
							want = append(want, r)
						}
					}
					var got []int
					for kv := it.SeekGE(seekKey, base.SeekGEFlagsNone); kv != nil; kv = it.Next() {
						got = append(got, it.row)
					}
					if !slices.Equal(got, want) {
						t.Fatalf("SeekGE(%q): got %v, want %v", seekKey, got, want)
					}
				}
			})

			// SeekLT then Prev: for each block key, SeekLT(k) must land at the
			// last visible row whose key is < k.
			t.Run("seek-lt", func(t *testing.T) {
				it := newIter()
				defer it.Close()
				for _, seekKey := range blockKeys {
					var want []int
					for _, r := range visible {
						if cmp(blockKeys[r], seekKey) < 0 {
							want = append([]int{r}, want...) // reverse order
						}
					}
					var got []int
					for kv := it.SeekLT(seekKey, base.SeekLTFlagsNone); kv != nil; kv = it.Prev() {
						got = append(got, it.row)
					}
					if !slices.Equal(got, want) {
						t.Fatalf("SeekLT(%q): got %v, want %v", seekKey, got, want)
					}
				}
			})

			// NextPrefix: First then NextPrefix repeatedly must yield the first
			// visible row of each distinct prefix in block order.
			t.Run("next-prefix", func(t *testing.T) {
				it := newIter()
				defer it.Close()
				var want []int
				var lastPrefix []byte
				for _, r := range visible {
					p := blockKeys[r][:split(blockKeys[r])]
					if lastPrefix == nil || !bytes.Equal(p, lastPrefix) {
						want = append(want, r)
						lastPrefix = p
					}
				}
				var got []int
				kv := it.First()
				for kv != nil {
					got = append(got, it.row)
					kv = it.NextPrefix(nil)
				}
				if !slices.Equal(got, want) {
					t.Fatalf("NextPrefix got %v, want %v", got, want)
				}
			})
		})
	}
}

// TestDataBlockIterSuffixMaskSyntheticOracle exercises the synth-aware per-row
// mask check in `isSuffixMasked`: under a SyntheticSuffix transform, the
// effective suffix of any row with a non-empty stored suffix is the synthetic
// one (empty stored suffixes retain empty and are never masked, per the DSR
// contract).
//
// The block under test mixes suffixless rows with rows at known suffix values.
// For each trial the test picks a random synthetic suffix and a random mask
// set, computes the visible rows with the oracle, and verifies First/Next and
// Last/Prev produce exactly those rows. Seek positioning under synth
// substitution is intentionally not exercised here — the synth substitution
// changes the user-visible keys' sort order in ways that make a row-index-
// based oracle awkward; correctness of the per-row mask predicate is the
// concern this test pins, and forward/backward suffice.
func TestDataBlockIterSuffixMaskSyntheticOracle(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	// Build a block by hand: a few rows per prefix, mixing suffixless rows
	// and rows at @10, @50, @99. Some rows are obsolete.
	type kv struct {
		key      []byte
		obsolete bool
	}
	mk := func(prefix string, suffix int64) []byte {
		buf := make([]byte, len(prefix)+testkeys.MaxSuffixLen)
		copy(buf, prefix)
		if suffix < 0 {
			return buf[:len(prefix)]
		}
		n := testkeys.WriteSuffix(buf[len(prefix):], suffix)
		return buf[:len(prefix)+n]
	}
	rows := []kv{
		{mk("aa", -1), false}, // suffixless
		{mk("aa", 99), true},  // obsolete
		{mk("aa", 50), false},
		{mk("aa", 10), false},
		{mk("bb", -1), false}, // suffixless
		{mk("bb", 99), false},
		{mk("bb", 10), true}, // obsolete
		{mk("cc", 50), false},
		{mk("cc", 10), false},
		{mk("dd", -1), false}, // suffixless
	}
	// Sort by key per the testkeys comparer (larger numeric suffix sorts
	// first within a prefix; suffixless sorts last).
	slices.SortFunc(rows, func(a, b kv) int { return testkeys.Comparer.Compare(a.key, b.key) })

	var w DataBlockEncoder
	w.Init(&testKeysSchema, NoTieringColumns())
	var blockKeys [][]byte
	var blockObsolete []bool
	for j, r := range rows {
		ik := base.MakeInternalKey(r.key, base.SeqNum(j+1), base.InternalKeyKindSet)
		kcmp := w.KeyWriter.ComparePrev(ik.UserKey)
		vp := block.InPlaceValuePrefix(kcmp.PrefixEqual())
		w.Add(ik, []byte("v"), vp, kcmp, r.obsolete, base.KVMeta{})
		blockKeys = append(blockKeys, append([]byte(nil), ik.UserKey...))
		blockObsolete = append(blockObsolete, r.obsolete)
	}
	blockData, _ := w.Finish(w.Rows(), w.Size())

	var r DataBlockDecoder
	bd := r.Init(&testKeysSchema, blockData)
	split := testkeys.Comparer.Split
	suffixCmp := testkeys.Comparer.ComparePointSuffixes

	suffixOf := func(k []byte) []byte {
		n := split(k)
		return k[n:]
	}

	// All stored non-empty suffixes are @1..@99 (well, @10/@50/@99 here).
	// A valid synthetic suffix must sort STRICTLY BEFORE every stored
	// non-empty suffix per the comparer. testkeys orders larger numeric
	// suffix first, so @100..@200 all qualify. Sample within that range.
	randSynth := func() []byte {
		n := int64(100) + rng.Int64N(101) // @100..@200
		buf := make([]byte, testkeys.SuffixLen(n))
		testkeys.WriteSuffix(buf, n)
		return buf
	}

	// Mask construction reuses the existing pattern: pick two suffixes from
	// the universe of {stored suffixes, the chosen synth}. Lower must
	// compare <= Upper per the comparer.
	randMask := func(synth []byte) (blockiter.SuffixMask, bool) {
		var suffixes [][]byte
		for _, k := range blockKeys {
			if s := suffixOf(k); len(s) > 0 {
				suffixes = append(suffixes, s)
			}
		}
		if len(synth) > 0 {
			suffixes = append(suffixes, synth)
		}
		if len(suffixes) < 2 {
			return blockiter.SuffixMask{}, false
		}
		a := suffixes[rng.IntN(len(suffixes))]
		b := suffixes[rng.IntN(len(suffixes))]
		if suffixCmp(a, b) > 0 {
			a, b = b, a
		}
		return blockiter.SuffixMask{Lower: a, Upper: b}, true
	}

	randMasks := func(synth []byte) []blockiter.SuffixMask {
		n := rng.IntN(3) + 1 // 1..3 masks (at least one to actually filter)
		var out []blockiter.SuffixMask
		for j := 0; j < n; j++ {
			if m, ok := randMask(synth); ok {
				out = append(out, m)
			}
		}
		return out
	}

	// visibleRows: the oracle. A row is visible iff:
	//   - not hidden by HideObsoletePoints, AND
	//   - its EFFECTIVE suffix is either empty or not covered by any mask.
	// Effective suffix is the synth iff stored is non-empty; otherwise empty.
	visibleRows := func(tr blockiter.Transforms) []int {
		synth := tr.SyntheticSuffix()
		var out []int
		for i, k := range blockKeys {
			if tr.HideObsoletePoints && blockObsolete[i] {
				continue
			}
			stored := suffixOf(k)
			effective := stored
			if tr.HasSyntheticSuffix() && len(stored) > 0 {
				effective = synth
			}
			masked := false
			if len(effective) > 0 {
				for _, m := range tr.SuffixMasks {
					if suffixCmp(effective, m.Lower) >= 0 && suffixCmp(effective, m.Upper) < 0 {
						masked = true
						break
					}
				}
			}
			if !masked {
				out = append(out, i)
			}
		}
		return out
	}

	const trials = 25
	for trial := 0; trial < trials; trial++ {
		synth := randSynth()
		tr := blockiter.Transforms{
			HideObsoletePoints:       rng.IntN(2) == 0,
			SyntheticPrefixAndSuffix: blockiter.MakeSyntheticPrefixAndSuffix(nil, synth),
			SuffixMasks:              randMasks(synth),
		}
		t.Run(fmt.Sprintf("trial%d", trial), func(t *testing.T) {
			t.Logf("synth=%s hideObsolete=%v numMasks=%d",
				synth, tr.HideObsoletePoints, len(tr.SuffixMasks))
			for i, m := range tr.SuffixMasks {
				t.Logf("  mask[%d]=[%s,%s)", i, m.Lower, m.Upper)
			}

			visible := visibleRows(tr)
			t.Logf("visible rows: %d / %d", len(visible), len(blockKeys))

			newIter := func() *DataBlockIter {
				it := &DataBlockIter{}
				it.InitOnce(&testKeysSchema, testkeys.Comparer,
					getInternalValuer(func([]byte) base.InternalValue {
						return base.MakeInPlaceValue(nil)
					}), NoTieringColumns())
				if err := it.Init(&r, bd, tr, NoTieringColumns()); err != nil {
					t.Fatal(err)
				}
				return it
			}

			t.Run("forward", func(t *testing.T) {
				it := newIter()
				defer it.Close()
				var got []int
				for kv := it.First(); kv != nil; kv = it.Next() {
					got = append(got, it.row)
				}
				if !slices.Equal(got, visible) {
					t.Fatalf("forward got rows %v, want %v", got, visible)
				}
			})

			t.Run("backward", func(t *testing.T) {
				it := newIter()
				defer it.Close()
				var got []int
				for kv := it.Last(); kv != nil; kv = it.Prev() {
					got = append(got, it.row)
				}
				want := slices.Clone(visible)
				slices.Reverse(want)
				if !slices.Equal(got, want) {
					t.Fatalf("backward got rows %v, want %v", got, want)
				}
			})
		})
	}
}
