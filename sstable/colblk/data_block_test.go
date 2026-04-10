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

func TestDataBlockIterPrefixMatched(t *testing.T) {
	const targetBlockSize = 32 << 10
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))
	// Use a moderate prefix size to ensure shared prefixes.
	keys, values := makeTestKeyRandomKVs(rng, 4, 8, targetBlockSize)

	// Build a data block.
	var w DataBlockEncoder
	w.Init(&testKeysSchema, NoTieringColumns())
	var nRows int
	for j := 0; w.Size() < targetBlockSize; j++ {
		ik := base.MakeInternalKey(keys[j], base.SeqNum(rng.Uint64N(uint64(base.SeqNumMax))), base.InternalKeyKindSet)
		kcmp := w.KeyWriter.ComparePrev(ik.UserKey)
		vp := block.InPlaceValuePrefix(kcmp.PrefixEqual())
		w.Add(ik, values[j], vp, kcmp, false /* isObsolete */, base.KVMeta{})
		nRows++
	}
	blockData, _ := w.Finish(w.Rows(), w.Size())

	var r DataBlockDecoder
	bd := r.Init(&testKeysSchema, blockData)

	split := testkeys.Comparer.Split
	prefix := func(key []byte) []byte {
		return key[:split(key)]
	}

	// Test with NoTransforms (should return Yes/No).
	t.Run("NoTransforms", func(t *testing.T) {
		var it DataBlockIter
		it.InitOnce(&testKeysSchema, testkeys.Comparer,
			getInternalValuer(func([]byte) base.InternalValue {
				return base.MakeInPlaceValue(nil)
			}), NoTieringColumns())
		if err := it.Init(&r, bd, blockiter.Transforms{}, NoTieringColumns()); err != nil {
			t.Fatal(err)
		}
		defer it.Close()

		const numOps = 1000
		var prevPrefix []byte
		var valid bool
		for op := 0; op < numOps; op++ {
			if rng.IntN(3) == 0 || !valid {
				// SeekGE with a random key.
				seekKey := keys[rng.IntN(len(keys))]
				kv := it.SeekGE(seekKey, base.SeekGEFlagsNone)
				if kv != nil {
					pm := it.PrefixMatched()
					seekPfx := prefix(seekKey)
					resultPfx := prefix(kv.K.UserKey)
					eq := bytes.Equal(seekPfx, resultPfx)
					if eq && pm != blockiter.PrefixMatchYes {
						t.Fatalf("SeekGE(%q) -> %q: expected PrefixMatchYes, got %d", seekKey, kv.K.UserKey, pm)
					}
					if !eq && pm != blockiter.PrefixMatchNo {
						t.Fatalf("SeekGE(%q) -> %q: expected PrefixMatchNo, got %d", seekKey, kv.K.UserKey, pm)
					}
					prevPrefix = append(prevPrefix[:0], resultPfx...)
					valid = true
				} else {
					valid = false
				}
			} else {
				// Next.
				kv := it.Next()
				if kv != nil {
					pm := it.PrefixMatched()
					resultPfx := prefix(kv.K.UserKey)
					eq := bytes.Equal(prevPrefix, resultPfx)
					if eq && pm != blockiter.PrefixMatchYes {
						t.Fatalf("Next -> %q (prev prefix %q): expected PrefixMatchYes, got %d", kv.K.UserKey, prevPrefix, pm)
					}
					if !eq && pm != blockiter.PrefixMatchNo {
						t.Fatalf("Next -> %q (prev prefix %q): expected PrefixMatchNo, got %d", kv.K.UserKey, prevPrefix, pm)
					}
					prevPrefix = append(prevPrefix[:0], resultPfx...)
				} else {
					valid = false
				}
			}
		}
	})

	// Test with transforms active (should always return Unknown).
	t.Run("WithTransforms", func(t *testing.T) {
		var it DataBlockIter
		it.InitOnce(&testKeysSchema, testkeys.Comparer,
			getInternalValuer(func([]byte) base.InternalValue {
				return base.MakeInPlaceValue(nil)
			}), NoTieringColumns())
		transforms := blockiter.Transforms{
			SyntheticSeqNum: 42,
		}
		if err := it.Init(&r, bd, transforms, NoTieringColumns()); err != nil {
			t.Fatal(err)
		}
		defer it.Close()

		kv := it.First()
		if kv != nil {
			pm := it.PrefixMatched()
			if pm != blockiter.PrefixMatchUnknown {
				t.Fatalf("First with transforms: expected PrefixMatchUnknown, got %d", pm)
			}
		}
		kv = it.Next()
		if kv != nil {
			pm := it.PrefixMatched()
			if pm != blockiter.PrefixMatchUnknown {
				t.Fatalf("Next with transforms: expected PrefixMatchUnknown, got %d", pm)
			}
		}
		kv = it.SeekGE(keys[0], base.SeekGEFlagsNone)
		if kv != nil {
			pm := it.PrefixMatched()
			if pm != blockiter.PrefixMatchUnknown {
				t.Fatalf("SeekGE with transforms: expected PrefixMatchUnknown, got %d", pm)
			}
		}
	})
}
