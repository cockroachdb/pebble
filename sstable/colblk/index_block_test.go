// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"unsafe"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/blockiter"
	"github.com/stretchr/testify/require"
)

func TestIndexBlock(t *testing.T) {
	var decoder IndexBlockDecoder
	var buf bytes.Buffer
	datadriven.RunTest(t, "testdata/index_block", func(t *testing.T, d *datadriven.TestData) string {
		buf.Reset()
		switch d.Cmd {
		case "build":
			var w IndexBlockWriter
			w.Init()
			for _, line := range strings.Split(d.Input, "\n") {
				fields := strings.Fields(line)
				var err error
				var h block.Handle
				h.Offset, err = strconv.ParseUint(fields[1], 10, 64)
				require.NoError(t, err)
				h.Length, err = strconv.ParseUint(fields[2], 10, 64)
				require.NoError(t, err)
				var bp []byte
				if len(fields) > 3 {
					bp = []byte(fields[3])
				}
				w.AddBlockHandle([]byte(fields[0]), h, bp)
			}

			rows := w.Rows()
			d.MaybeScanArgs(t, "rows", &rows)
			data := w.Finish(rows)
			fmt.Fprintf(&buf, "UnsafeSeparator(%d) = %q\n", rows-1, w.UnsafeSeparator(rows-1))
			decoder.Init(data)
			fmt.Fprint(&buf, decoder.DebugString())
			return buf.String()
		case "iter":
			var syntheticPrefix, syntheticSuffix string
			d.MaybeScanArgs(t, "synthetic-prefix", &syntheticPrefix)
			d.MaybeScanArgs(t, "synthetic-suffix", &syntheticSuffix)
			transforms := blockiter.Transforms{
				SyntheticPrefixAndSuffix: blockiter.MakeSyntheticPrefixAndSuffix([]byte(syntheticPrefix), []byte(syntheticSuffix)),
			}
			var it IndexIter
			it.InitWithDecoder(testkeys.Comparer, &decoder, transforms)
			for _, line := range strings.Split(d.Input, "\n") {
				fields := strings.Fields(line)
				var valid bool
				switch fields[0] {
				case "seek-ge":
					valid = it.SeekGE([]byte(fields[1]))
				case "first":
					valid = it.First()
				case "last":
					valid = it.Last()
				case "next":
					valid = it.Next()
				case "prev":
					valid = it.Prev()
				case "is-valid":
					fmt.Fprintf(&buf, "Valid()=%t\n", it.Valid())
					continue
				case "invalidate":
					it.Invalidate()
				default:
					panic(fmt.Sprintf("unknown command: %s", fields[0]))
				}
				if valid {
					var bp string
					bhp, err := it.BlockHandleWithProperties()
					if err != nil {
						fmt.Fprintf(&buf, "<err invalid bh: %s>", err)
						continue
					}
					if len(bhp.Props) > 0 {
						bp = fmt.Sprintf(" props=%q", bhp.Props)
					}
					fmt.Fprintf(&buf, "separator: %s  block %d: %d-%d%s\n",
						testkeys.Comparer.FormatKey(it.Separator()), it.row, bhp.Offset, bhp.Offset+bhp.Length, bp)
				} else {
					fmt.Fprintln(&buf, ".")
				}
			}
			return buf.String()
		default:
			panic(fmt.Sprintf("unknown command: %s", d.Cmd))
		}
	})
}

// TestIndexIterInitHandle exercises initializing an IndexIter through
// InitHandle.
func TestIndexIterInitHandle(t *testing.T) {
	var w IndexBlockWriter
	w.Init()
	bh1 := block.Handle{Offset: 0, Length: 2000}
	bh2 := block.Handle{Offset: 2008, Length: 1000}
	w.AddBlockHandle([]byte("a"), bh1, nil)
	w.AddBlockHandle([]byte("b"), bh2, nil)
	blockData := w.Finish(w.Rows())

	c := cache.New(10 << 10)
	defer c.Unref()
	ch := c.NewHandle()
	defer ch.Close()

	{
		v := block.Alloc(len(blockData), nil)
		copy(v.BlockData(), blockData)
		d := (*IndexBlockDecoder)(unsafe.Pointer(v.BlockMetadata()))
		d.Init(v.BlockData())
		v.SetInCacheForTesting(ch, base.DiskFileNum(1), 0)
	}

	getBlockAndIterate := func(it *IndexIter) {
		cv := ch.Get(base.DiskFileNum(1), 0, cache.CategorySSTableData)
		require.NotNil(t, cv)
		require.NoError(t, it.InitHandle(testkeys.Comparer, block.CacheBufferHandle(cv), blockiter.NoTransforms))
		defer it.Close()
		require.True(t, it.First())
		bh, err := it.BlockHandleWithProperties()
		require.NoError(t, err)
		require.Equal(t, bh1, bh.Handle)
		require.True(t, it.Next())
		bh, err = it.BlockHandleWithProperties()
		require.NoError(t, err)
		require.Equal(t, bh2, bh.Handle)
		require.False(t, it.Next())
		require.False(t, it.IsDataInvalidated())
		it.Invalidate()
		require.True(t, it.IsDataInvalidated())
	}

	const workers = 8
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			var iter IndexIter
			defer wg.Done()
			for i := 0; i < 10; i++ {
				getBlockAndIterate(&iter)
			}
		}()
	}
	wg.Wait()
}
