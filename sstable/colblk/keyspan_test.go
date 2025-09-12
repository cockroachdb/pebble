// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/blockiter"
	"github.com/stretchr/testify/require"
)

func TestKeyspanBlock(t *testing.T) {
	var buf bytes.Buffer
	var kr KeyspanDecoder
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
				w.AddSpan(keyspan.ParseSpan(line))
			}
			fmt.Fprint(&buf, &w)
			return buf.String()
		case "finish":
			sm, la := w.UnsafeBoundaryKeys()
			fmt.Fprintf(&buf, "Boundaries: %s — %s\n", sm.Pretty(testkeys.Comparer.FormatKey), la.Pretty(testkeys.Comparer.FormatKey))
			block := w.Finish()
			kr.Init(block)
			fmt.Fprint(&buf, kr.DebugString())
			return buf.String()
		case "iter":
			var iter keyspanIter
			var syntheticSeqNum uint64
			var syntheticPrefix, syntheticSuffix string
			td.MaybeScanArgs(t, "synthetic-seq-num", &syntheticSeqNum)
			td.MaybeScanArgs(t, "synthetic-prefix", &syntheticPrefix)
			td.MaybeScanArgs(t, "synthetic-suffix", &syntheticSuffix)
			transforms := blockiter.FragmentTransforms{
				SyntheticSeqNum:          blockiter.SyntheticSeqNum(syntheticSeqNum),
				SyntheticPrefixAndSuffix: blockiter.MakeSyntheticPrefixAndSuffix([]byte(syntheticPrefix), []byte(syntheticSuffix)),
			}
			iter.init(base.DefaultComparer.Compare, &kr, transforms)
			return keyspan.RunFragmentIteratorCmd(&iter, td.Input, nil)
		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

// TestKeyspanBlockPooling exercises the NewKeyspanIter constructor of a
// KeyspanIter and the Close behavior that retains keyspan iters within a pool.
func TestKeyspanBlockPooling(t *testing.T) {
	var w KeyspanBlockWriter
	w.Init(testkeys.Comparer.Equal)
	s1 := keyspan.ParseSpan("b-c:{(#100,RANGEDEL) (#20,RANGEDEL) (#0,RANGEDEL)}")
	s2 := keyspan.ParseSpan("c-d:{(#100,RANGEDEL) (#0,RANGEDEL)}")
	w.AddSpan(s1)
	w.AddSpan(s2)
	b := w.Finish()

	c := cache.New(10 << 10)
	defer c.Unref()
	ch := c.NewHandle()
	defer ch.Close()
	v := block.Alloc(len(b), nil)
	copy(v.BlockData(), b)
	d := (*KeyspanDecoder)(unsafe.Pointer(v.BlockMetadata()))
	d.Init(v.BlockData())
	v.SetInCacheForTesting(ch, base.DiskFileNum(1), 0)

	getBlockAndIterate := func() {
		cv := ch.Get(base.DiskFileNum(1), 0, cache.CategorySSTableData)
		require.NotNil(t, cv)
		it := NewKeyspanIter(testkeys.Comparer.Compare, block.CacheBufferHandle(cv), blockiter.NoFragmentTransforms)
		defer it.Close()
		s, err := it.First()
		require.NoError(t, err)
		require.NotNil(t, s)
		require.Equal(t, s1.String(), s.String())
		s, err = it.Next()
		require.NoError(t, err)
		require.NotNil(t, s)
		require.Equal(t, s2.String(), s.String())
		s, err = it.Next()
		require.NoError(t, err)
		require.Nil(t, s)
	}

	const workers = 8
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				getBlockAndIterate()
			}
		}()
	}
	wg.Wait()
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
		w.AddSpan(s)
	}
	avgRowSize := float64(w.Size()) / float64(numSpans*keysPerSpan)

	var kr KeyspanDecoder
	kr.Init(w.Finish())

	var it KeyspanIter
	it.init(base.DefaultComparer.Compare, &kr, blockiter.NoFragmentTransforms)
	b.Run("SeekGE", func(b *testing.B) {
		rng := rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))

		b.ResetTimer()
		var sum uint64
		for i := 0; i < b.N; i++ {
			if s, _ := it.SeekGE(keys[rng.IntN(len(keys))]); s != nil {
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
