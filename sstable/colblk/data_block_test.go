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
				transforms := blockiter.Transforms{
					SyntheticSeqNum:          blockiter.SyntheticSeqNum(seqNum),
					HideObsoletePoints:       td.HasArg("hide-obsolete-points"),
					SyntheticPrefixAndSuffix: blockiter.MakeSyntheticPrefixAndSuffix([]byte(syntheticPrefix), []byte(syntheticSuffix)),
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
