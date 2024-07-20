// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"golang.org/x/exp/rand"
)

func TestKeyspanBlock(t *testing.T) {
	var buf bytes.Buffer
	var kr KeyspanReader
	var w KeyspanBlockWriter
	datadriven.RunTest(t, "testdata/keyspan_block", func(t *testing.T, td *datadriven.TestData) string {
		buf.Reset()
		switch td.Cmd {
		case "init":
			w.Init(bytes.Equal)
			fmt.Fprint(&buf, &w)
			return buf.String()
		case "reset":
			w.Reset()
			fmt.Fprint(&buf, &w)
			return buf.String()
		case "add":
			for _, line := range strings.Split(td.Input, "\n") {
				s := keyspan.ParseSpan(line)
				w.AddSpan(&s)
			}
			fmt.Fprint(&buf, &w)
			return buf.String()
		case "finish":
			block := w.Finish()
			kr.Init(block)
			fmt.Fprint(&buf, kr.DebugString())
			return buf.String()
		case "iter":
			var iter KeyspanIter
			iter.Init(base.DefaultComparer.Compare, &kr)
			return keyspan.RunFragmentIteratorCmd(&iter, td.Input, nil)
		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func BenchmarkKeyspanBlock_RangeDeletions(b *testing.B) {
	for _, numSpans := range []int{1, 10, 100} {
		for _, keysPerSpan := range []int{1, 2, 5} {
			for _, keySize := range []int{10, 128} {
				b.Run(fmt.Sprintf("numSpans=%d/keysPerSpan=%d/keySize=%d", numSpans, keysPerSpan, keySize), func(b *testing.B) {
					benchmarkKeyspanBlockRangeDeletions(b, numSpans, keysPerSpan, keySize)
				})
			}
		}
	}
}

func benchmarkKeyspanBlockRangeDeletions(b *testing.B, numSpans, keysPerSpan, keySize int) {
	var w KeyspanBlockWriter
	w.Init(bytes.Equal)
	format := fmt.Sprintf("%%0%dd", keySize)
	s := keyspan.Span{
		Start: make([]byte, 0, keySize),
		End:   make([]byte, 0, keySize),
		Keys:  make([]keyspan.Key, 0, keysPerSpan),
	}
	keys := make([][]byte, numSpans+1)
	for k := range keys {
		keys[k] = fmt.Appendf([]byte{}, format, k)
	}
	for i := 0; i < numSpans; i++ {
		s = keyspan.Span{
			Start: keys[i],
			End:   keys[i+1],
			Keys:  s.Keys[:0],
		}
		for j := 0; j < keysPerSpan; j++ {
			t := base.MakeTrailer(base.SeqNum(j), base.InternalKeyKindRangeDelete)
			s.Keys = append(s.Keys, keyspan.Key{Trailer: t})
		}
		w.AddSpan(&s)
	}
	block := w.Finish()
	avgRowSize := float64(w.Size()) / float64(numSpans*keysPerSpan)

	var kr KeyspanReader
	kr.Init(block)

	var it KeyspanIter
	it.Init(base.DefaultComparer.Compare, &kr)
	b.Run("SeekGE", func(b *testing.B) {
		rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

		b.ResetTimer()
		var sum uint64
		for i := 0; i < b.N; i++ {
			if s, _ := it.SeekGE(keys[rng.Intn(len(keys))]); s != nil {
				for _, k := range s.Keys {
					sum += uint64(k.Trailer)
				}
			}
		}
		b.StopTimer()
		b.ReportMetric(avgRowSize, "bytes/row")
		fmt.Fprint(io.Discard, sum)
	})
	b.Run("Next", func(b *testing.B) {
		_, _ = it.First()
		b.ResetTimer()
		var sum uint64
		for i := 0; i < b.N; i++ {
			s, _ := it.Next()
			if s == nil {
				s, _ = it.First()
			}
			for _, k := range s.Keys {
				sum += uint64(k.Trailer)
			}
		}
		b.StopTimer()
		b.ReportMetric(avgRowSize, "bytes/row")
		fmt.Fprint(io.Discard, sum)
	})
}
