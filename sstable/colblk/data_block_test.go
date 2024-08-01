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
	"golang.org/x/exp/rand"
)

var testKeysSchema = DefaultKeySchema(testkeys.Comparer, 16)

func TestDataBlock(t *testing.T) {
	var buf bytes.Buffer
	var w DataBlockWriter
	var r DataBlockReader
	var it DataBlockIter
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
				return buf.String()
			case "write":
				for _, line := range strings.Split(td.Input, "\n") {
					j := strings.IndexRune(line, ':')
					ik := base.ParsePrettyInternalKey(line[:j])

					kcmp := w.KeyWriter.ComparePrev(ik.UserKey)
					v := []byte(line[j+1:])
					w.Add(ik, v, kcmp)
				}
				fmt.Fprint(&buf, &w)
				return buf.String()
			case "finish":
				block := w.Finish()
				r.Init(testKeysSchema, block)
				f := binfmt.New(r.r.data).LineWidth(20)
				r.r.headerToBinFormatter(f)
				for i := 0; i < int(r.r.header.Columns); i++ {
					r.r.columnToBinFormatter(f, i, int(r.r.header.Rows))
				}
				return f.String()
			case "iter":
				it.Init(&r, testKeysSchema.NewKeySeeker())
				return itertest.RunInternalIterCmd(t, td, &it)
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

	var w DataBlockWriter
	w.Init(testKeysSchema)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w.Reset()
		var j int
		for w.Size() < targetBlockSize {
			ik := base.MakeInternalKey(keys[j], base.SeqNum(rng.Uint64n(uint64(base.SeqNumMax))), base.InternalKeyKindSet)
			kcmp := w.KeyWriter.ComparePrev(ik.UserKey)
			w.Add(ik, values[j], kcmp)
			j++
		}
		w.Finish()
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
