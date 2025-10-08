// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rowblk

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/rangedel"
	"github.com/cockroachdb/pebble/internal/rangekey"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/blockiter"
	"github.com/stretchr/testify/require"
)

func TestBlockFragmentIterator(t *testing.T) {
	comparer := testkeys.Comparer
	var cacheVal *cache.Value
	c := cache.New(2048)
	cacheHandle := c.NewHandle()
	defer func() {
		cacheHandle.Close()
		c.Unref()
		if cacheVal != nil {
			cache.Free(cacheVal)
		}
	}()

	datadriven.RunTest(t, "testdata/rowblk_fragment_iter", func(t *testing.T, d *datadriven.TestData) string {
		var buf strings.Builder
		switch d.Cmd {
		case "build":
			var spans []keyspan.Span
			fragmenter := keyspan.Fragmenter{
				Cmp:    comparer.Compare,
				Format: comparer.FormatKey,
				Emit: func(s keyspan.Span) {
					spans = append(spans, s)
				},
			}
			for _, l := range strings.Split(d.Input, "\n") {
				if l == "" {
					continue
				}
				span := keyspan.ParseSpan(l)
				fragmenter.Add(span)
			}
			fragmenter.Finish()
			// Range del or range key blocks always use restart interval 1.
			w := Writer{RestartInterval: 1}
			emitFn := func(k base.InternalKey, v []byte) error {
				return w.Add(k, v)
			}
			for _, s := range spans {
				if s.Keys[0].Kind() == base.InternalKeyKindRangeDelete {
					rangedel.Encode(s, emitFn)
				} else {
					rangekey.Encode(s, emitFn)
				}
			}
			blockData := w.Finish()

			// Evict and free the previous block.
			cacheHandle.EvictFile(0)
			if cacheVal != nil {
				cache.Free(cacheVal)
			}
			cacheVal = cache.Alloc(block.MetadataSize + len(blockData))
			copy(cacheVal.RawBuffer()[block.MetadataSize:], blockData)
			cacheHandle.Set(0, 0, cacheVal)

			for _, s := range spans {
				buf.WriteString(s.String() + "\n")
			}

		case "iter":
			var transforms blockiter.FragmentTransforms
			var seqNum uint64
			d.MaybeScanArgs(t, "synthetic-seq-num", &seqNum)
			transforms.SyntheticSeqNum = blockiter.SyntheticSeqNum(seqNum)
			var syntheticPrefix, syntheticSuffix string
			d.MaybeScanArgs(t, "synthetic-prefix", &syntheticPrefix)
			d.MaybeScanArgs(t, "synthetic-suffix", &syntheticSuffix)
			transforms.SyntheticPrefixAndSuffix = blockiter.MakeSyntheticPrefixAndSuffix([]byte(syntheticPrefix), []byte(syntheticSuffix))
			if d.HasArg("invariants-only") && !invariants.Enabled {
				// Skip testcase.
				return d.Expected
			}

			blockHandle := block.CacheBufferHandle(cacheHandle.Get(0, 0, base.MakeLevel(0), cache.CategorySSTableData))
			require.True(t, blockHandle.Valid())
			i, err := NewFragmentIter(0, comparer, blockHandle, transforms)
			defer i.Close()
			require.NoError(t, err)

			for _, l := range strings.Split(d.Input, "\n") {
				if l == "" {
					continue
				}
				func() {
					defer func() {
						if r := recover(); r != nil {
							fmt.Fprintf(&buf, "panic: %v\n", r)
						}
					}()
					var span *keyspan.Span
					var err error
					fields := strings.Fields(l)
					switch fields[0] {
					case "first":
						span, err = i.First()
					case "last":
						span, err = i.Last()
					case "next":
						span, err = i.Next()
					case "prev":
						span, err = i.Prev()
					case "seek-ge":
						span, err = i.SeekGE([]byte(fields[1]))
					case "seek-lt":
						span, err = i.SeekLT([]byte(fields[1]))
					default:
						d.Fatalf(t, "unknown iter command %q", fields[0])
					}
					require.NoError(t, err)
					fmt.Fprintf(&buf, "%8s:  %v\n", fields[0], span)
				}()
			}

		default:
			d.Fatalf(t, "unknown command %s", d.Cmd)
		}
		return buf.String()
	})
}
