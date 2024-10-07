// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/itertest"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/sstable/block"
	"golang.org/x/exp/rand"
)

var testKeysSchema = DefaultKeySchema(testkeys.Comparer, 16)

func TestDataBlock(t *testing.T) {
	var buf bytes.Buffer
	var w DataBlockEncoder
	var r DataBlockDecoder
	var it DataBlockIter
	rw := NewDataBlockRewriter(testKeysSchema, testkeys.Comparer.Compare, testkeys.Comparer.Split)
	var sizes []int
	it.InitOnce(testKeysSchema, testkeys.Comparer.Compare, testkeys.Comparer.Split, getLazyValuer(func([]byte) base.LazyValue {
		return base.LazyValue{ValueOrHandle: []byte("mock external value")}
	}))

	datadriven.Walk(t, "testdata/data_block", func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
			buf.Reset()
			switch td.Cmd {
			case "init":
				var bundleSize int
				if td.MaybeScanArgs(t, "bundle-size", &bundleSize) {
					w.Init(DefaultKeySchema(testkeys.Comparer, bundleSize))
				} else {
					w.Init(testKeysSchema)
				}
				fmt.Fprint(&buf, &w)
				sizes = sizes[:0]
				return buf.String()
			case "write", "write-block":
				// write-block does init/write/finish in a single command, and doesn't
				// print anything.
				if td.Cmd == "write-block" {
					w.Init(testKeysSchema)
				}
				for _, line := range strings.Split(td.Input, "\n") {
					line, isObsolete := strings.CutSuffix(line, "obsolete")

					j := strings.IndexRune(line, ':')
					ik := base.ParseInternalKey(line[:j])

					kcmp := w.KeyWriter.ComparePrev(ik.UserKey)
					valueString := line[j+1:]
					vp := block.InPlaceValuePrefix(kcmp.PrefixEqual())
					if strings.HasPrefix(valueString, "valueHandle") {
						vp = block.ValueHandlePrefix(kcmp.PrefixEqual(), 0)
					}
					v := []byte(line[j+1:])
					w.Add(ik, v, vp, kcmp, isObsolete)
					sizes = append(sizes, w.Size())
				}
				if td.Cmd == "write-block" {
					block, _ := w.Finish(w.Rows(), w.Size())
					r.Init(testKeysSchema, block)
					return ""
				}
				fmt.Fprint(&buf, &w)
				return buf.String()
			case "rewrite":
				var from, to string
				td.ScanArgs(t, "from", &from)
				td.ScanArgs(t, "to", &to)
				start, end, rewrittenBlock, err := rw.RewriteSuffixes(r.r.data, []byte(from), []byte(to))
				if err != nil {
					return fmt.Sprintf("error: %s", err)
				}
				r.Init(testKeysSchema, rewrittenBlock)
				f := binfmt.New(r.r.data).LineWidth(20)
				r.Describe(f)
				fmt.Fprintf(&buf, "Start: %s\nEnd: %s\n%s",
					start.Pretty(testkeys.Comparer.FormatKey),
					end.Pretty(testkeys.Comparer.FormatKey),
					f.String())
				return buf.String()
			case "finish":
				rows := w.Rows()
				td.MaybeScanArgs(t, "rows", &rows)
				block, lastKey := w.Finish(rows, sizes[rows-1])
				r.Init(testKeysSchema, block)
				f := binfmt.New(r.r.data).LineWidth(20)
				r.Describe(f)
				fmt.Fprintf(&buf, "LastKey: %s\n%s", lastKey.Pretty(testkeys.Comparer.FormatKey), f.String())
				return buf.String()
			case "iter":
				var seqNum uint64
				var syntheticPrefix, syntheticSuffix string
				td.MaybeScanArgs(t, "synthetic-seq-num", &seqNum)
				td.MaybeScanArgs(t, "synthetic-prefix", &syntheticPrefix)
				td.MaybeScanArgs(t, "synthetic-suffix", &syntheticSuffix)
				transforms := block.IterTransforms{
					SyntheticSeqNum:    block.SyntheticSeqNum(seqNum),
					HideObsoletePoints: td.HasArg("hide-obsolete-points"),
					SyntheticPrefix:    []byte(syntheticPrefix),
					SyntheticSuffix:    []byte(syntheticSuffix),
				}
				if err := it.Init(&r, transforms); err != nil {
					return err.Error()
				}

				o := []itertest.IterOpt{itertest.ShowCommands}
				if td.HasArg("verbose") {
					o = append(o, itertest.Verbose)
				}
				if td.HasArg("invalidated") {
					it.Invalidate()
				}
				return itertest.RunInternalIterCmd(t, td, &it, o...)
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
	rng := rand.New(rand.NewSource(seed))
	keys, values := makeTestKeyRandomKVs(rng, prefixSize, valueSize, targetBlockSize)

	var w DataBlockEncoder
	w.Init(testKeysSchema)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w.Reset()
		var j int
		for w.Size() < targetBlockSize {
			ik := base.MakeInternalKey(keys[j], base.SeqNum(rng.Uint64n(uint64(base.SeqNumMax))), base.InternalKeyKindSet)
			kcmp := w.KeyWriter.ComparePrev(ik.UserKey)
			vp := block.InPlaceValuePrefix(kcmp.PrefixEqual())
			w.Add(ik, values[j], vp, kcmp, false /* isObsolete */)
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
		rng.Read(vals[i])
	}
	slices.SortFunc(keys, bytes.Compare)
	return keys, vals
}

func randTestKey(rng *rand.Rand, buf []byte, prefixLen int) []byte {
	suffix := rng.Int63n(100)
	sl := testkeys.SuffixLen(suffix)
	buf = buf[0 : prefixLen+sl]
	for i := 0; i < prefixLen; i++ {
		buf[i] = byte(rng.Intn(26) + 'a')
	}
	testkeys.WriteSuffix(buf[prefixLen:], suffix)
	return buf
}

type getLazyValuer func([]byte) base.LazyValue

func (g getLazyValuer) GetLazyValueForPrefixAndValueHandle(handle []byte) base.LazyValue {
	return g(handle)
}
