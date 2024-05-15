// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspanimpl

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/stretchr/testify/require"
)

func TestLevelIterEquivalence(t *testing.T) {
	type level [][]keyspan.Span
	testCases := []struct {
		name   string
		levels []level
	}{
		{
			"single level, no gaps, no overlaps",
			[]level{
				{
					{
						{
							Start: []byte("a"),
							End:   []byte("b"),
							Keys: []keyspan.Key{{
								Trailer: base.MakeTrailer(2, base.InternalKeyKindRangeKeySet),
								Suffix:  nil,
								Value:   []byte("foo"),
							}},
						},
						{
							Start: []byte("b"),
							End:   []byte("c"),
							Keys: []keyspan.Key{{
								Trailer: base.MakeTrailer(2, base.InternalKeyKindRangeKeySet),
								Suffix:  nil,
								Value:   []byte("foo"),
							}},
						},
						{
							Start: []byte("c"),
							End:   []byte("d"),
							Keys: []keyspan.Key{{
								Trailer: base.MakeTrailer(2, base.InternalKeyKindRangeKeySet),
								Suffix:  nil,
								Value:   []byte("foo"),
							}},
						},
					},
					{
						{
							Start: []byte("d"),
							End:   []byte("e"),
							Keys: []keyspan.Key{{
								Trailer: base.MakeTrailer(2, base.InternalKeyKindRangeKeySet),
								Suffix:  nil,
								Value:   []byte("foo"),
							}},
						},
						{
							Start: []byte("e"),
							End:   []byte("f"),
							Keys: []keyspan.Key{{
								Trailer: base.MakeTrailer(2, base.InternalKeyKindRangeKeySet),
								Suffix:  nil,
								Value:   []byte("foo"),
							}},
						},
						{
							Start: []byte("f"),
							End:   []byte("g"),
							Keys: []keyspan.Key{{
								Trailer: base.MakeTrailer(2, base.InternalKeyKindRangeKeySet),
								Suffix:  nil,
								Value:   []byte("foo"),
							}},
						},
					},
				},
			},
		},
		{
			"single level, overlapping fragments",
			[]level{
				{
					{
						{
							Start: []byte("a"),
							End:   []byte("b"),
							Keys: []keyspan.Key{
								{
									Trailer: base.MakeTrailer(4, base.InternalKeyKindRangeKeySet),
									Suffix:  nil,
									Value:   []byte("bar"),
								},
								{
									Trailer: base.MakeTrailer(2, base.InternalKeyKindRangeKeySet),
									Suffix:  nil,
									Value:   []byte("foo"),
								},
							},
						},
						{
							Start: []byte("b"),
							End:   []byte("c"),
							Keys: []keyspan.Key{
								{
									Trailer: base.MakeTrailer(4, base.InternalKeyKindRangeKeySet),
									Suffix:  nil,
									Value:   []byte("bar"),
								},
								{
									Trailer: base.MakeTrailer(2, base.InternalKeyKindRangeKeySet),
									Suffix:  nil,
									Value:   []byte("foo"),
								},
							},
						},
						{
							Start: []byte("c"),
							End:   []byte("d"),
							Keys: []keyspan.Key{{
								Trailer: base.MakeTrailer(2, base.InternalKeyKindRangeKeySet),
								Suffix:  nil,
								Value:   []byte("foo"),
							}},
						},
					},
					{
						{
							Start: []byte("d"),
							End:   []byte("e"),
							Keys: []keyspan.Key{{
								Trailer: base.MakeTrailer(2, base.InternalKeyKindRangeKeySet),
								Suffix:  nil,
								Value:   []byte("foo"),
							}},
						},
						{
							Start: []byte("e"),
							End:   []byte("f"),
							Keys: []keyspan.Key{{
								Trailer: base.MakeTrailer(2, base.InternalKeyKindRangeKeySet),
								Suffix:  nil,
								Value:   []byte("foo"),
							}},
						},
						{
							Start: []byte("f"),
							End:   []byte("g"),
							Keys: []keyspan.Key{{
								Trailer: base.MakeTrailer(2, base.InternalKeyKindRangeKeySet),
								Suffix:  nil,
								Value:   []byte("foo"),
							}},
						},
					},
				},
			},
		},
		{
			"single level, gaps between files and range keys",
			[]level{
				{
					{
						{
							Start: []byte("a"),
							End:   []byte("b"),
							Keys: []keyspan.Key{{
								Trailer: base.MakeTrailer(2, base.InternalKeyKindRangeKeySet),
								Suffix:  nil,
								Value:   []byte("foo"),
							}},
						},
						{
							Start: []byte("c"),
							End:   []byte("d"),
							Keys: []keyspan.Key{{
								Trailer: base.MakeTrailer(2, base.InternalKeyKindRangeKeySet),
								Suffix:  nil,
								Value:   []byte("foo"),
							}},
						},
						{
							Start: []byte("e"),
							End:   []byte("f"),
							Keys: []keyspan.Key{{
								Trailer: base.MakeTrailer(2, base.InternalKeyKindRangeKeySet),
								Suffix:  nil,
								Value:   []byte("foo"),
							}},
						},
					},
					{
						{
							Start: []byte("g"),
							End:   []byte("h"),
							Keys: []keyspan.Key{{
								Trailer: base.MakeTrailer(2, base.InternalKeyKindRangeKeySet),
								Suffix:  nil,
								Value:   []byte("foo"),
							}},
						},
						{
							Start: []byte("i"),
							End:   []byte("j"),
							Keys: []keyspan.Key{{
								Trailer: base.MakeTrailer(2, base.InternalKeyKindRangeKeySet),
								Suffix:  nil,
								Value:   []byte("foo"),
							}},
						},
						{
							Start: []byte("k"),
							End:   []byte("l"),
							Keys: []keyspan.Key{{
								Trailer: base.MakeTrailer(2, base.InternalKeyKindRangeKeySet),
								Suffix:  nil,
								Value:   []byte("foo"),
							}},
						},
					},
				},
			},
		},
		{
			"two levels, one with overlapping unset",
			[]level{
				{
					{
						{
							Start: []byte("a"),
							End:   []byte("h"),
							Keys: []keyspan.Key{{
								Trailer: base.MakeTrailer(2, base.InternalKeyKindRangeKeySet),
								Suffix:  nil,
								Value:   []byte("foo"),
							}},
						},
					},
					{
						{
							Start: []byte("l"),
							End:   []byte("u"),
							Keys: []keyspan.Key{{
								Trailer: base.MakeTrailer(2, base.InternalKeyKindRangeKeyUnset),
								Suffix:  nil,
								Value:   nil,
							}},
						},
					},
				},
				{
					{
						{
							Start: []byte("e"),
							End:   []byte("r"),
							Keys: []keyspan.Key{{
								Trailer: base.MakeTrailer(1, base.InternalKeyKindRangeKeySet),
								Suffix:  nil,
								Value:   []byte("foo"),
							}},
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		var fileIters []keyspan.FragmentIterator
		var levelIters []keyspan.FragmentIterator
		var iter1, iter2 MergingIter
		for j, level := range tc.levels {
			j := j // Copy for use in closures down below.
			var levelIter LevelIter
			var metas []*manifest.FileMetadata
			for k, file := range level {
				fileIters = append(fileIters, keyspan.NewIter(base.DefaultComparer.Compare, file))
				meta := &manifest.FileMetadata{
					FileNum:               base.FileNum(k + 1),
					Size:                  1024,
					SmallestSeqNum:        2,
					LargestSeqNum:         2,
					LargestSeqNumAbsolute: 2,
					SmallestRangeKey:      base.MakeInternalKey(file[0].Start, file[0].SmallestKey().SeqNum(), file[0].SmallestKey().Kind()),
					LargestRangeKey:       base.MakeExclusiveSentinelKey(file[len(file)-1].LargestKey().Kind(), file[len(file)-1].End),
					HasPointKeys:          false,
					HasRangeKeys:          true,
				}
				meta.InitPhysicalBacking()
				meta.ExtendRangeKeyBounds(base.DefaultComparer.Compare, meta.SmallestRangeKey, meta.LargestRangeKey)
				metas = append(metas, meta)
			}

			tableNewIters := func(file *manifest.FileMetadata, iterOptions keyspan.SpanIterOptions) (keyspan.FragmentIterator, error) {
				return keyspan.NewIter(base.DefaultComparer.Compare, tc.levels[j][file.FileNum-1]), nil
			}
			// Add all the fileMetadatas to L6.
			b := &manifest.BulkVersionEdit{}
			amap := make(map[base.FileNum]*manifest.FileMetadata)
			for i := range metas {
				amap[metas[i].FileNum] = metas[i]
			}
			b.Added[6] = amap
			v, err := b.Apply(nil, base.DefaultComparer, 0, 0)
			require.NoError(t, err)
			levelIter.Init(
				keyspan.SpanIterOptions{}, base.DefaultComparer.Compare, tableNewIters,
				v.Levels[6].Iter(), 0, manifest.KeyTypeRange,
			)
			levelIters = append(levelIters, &levelIter)
		}

		iter1.Init(base.DefaultComparer, keyspan.VisibleTransform(base.InternalKeySeqNumMax), new(MergingBuffers), fileIters...)
		iter2.Init(base.DefaultComparer, keyspan.VisibleTransform(base.InternalKeySeqNumMax), new(MergingBuffers), levelIters...)

		// Check iter1 and iter2 for equivalence.
		s1, err1 := iter1.First()
		s2, err2 := iter2.First()
		require.NoError(t, err1)
		require.NoError(t, err2)
		require.Equal(t, s1, s2, "failed on test case %q", tc.name)
		valid := true
		for valid {
			s1, err1 = iter1.Next()
			require.NoError(t, err1)
			for {
				s2, err2 = iter2.Next()
				require.NoError(t, err2)
				// The level iter could produce empty spans that straddle between
				// files. Ignore those.
				if s2 == nil || !s2.Empty() {
					break
				}
			}

			require.Equal(t, s1, s2, "failed on test case %q", tc.name)
			valid = s1 != nil && s2 != nil
		}
	}
}

func TestLevelIter(t *testing.T) {
	var level [][]keyspan.Span
	var rangedels [][]keyspan.Span
	var metas []*manifest.FileMetadata
	var iter keyspan.FragmentIterator
	var extraInfo func() string

	datadriven.RunTest(t, "testdata/level_iter", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			level = level[:0]
			metas = metas[:0]
			rangedels = rangedels[:0]
			if iter != nil {
				iter.Close()
				iter = nil
			}
			var pointKeys []base.InternalKey
			var currentRangeDels []keyspan.Span
			var currentFile []keyspan.Span
			for _, key := range strings.Split(d.Input, "\n") {
				if strings.HasPrefix(key, "file") {
					// Skip the very first file creation.
					if len(level) != 0 || len(currentFile) != 0 {
						meta := &manifest.FileMetadata{
							FileNum: base.FileNum(len(level) + 1),
						}
						if len(currentFile) > 0 {
							smallest := base.MakeInternalKey(currentFile[0].Start, currentFile[0].SmallestKey().SeqNum(), currentFile[0].SmallestKey().Kind())
							largest := base.MakeExclusiveSentinelKey(currentFile[len(currentFile)-1].LargestKey().Kind(), currentFile[len(currentFile)-1].End)
							meta.ExtendRangeKeyBounds(base.DefaultComparer.Compare, smallest, largest)
						}
						if len(pointKeys) != 0 {
							meta.ExtendPointKeyBounds(base.DefaultComparer.Compare, pointKeys[0], pointKeys[len(pointKeys)-1])
						}
						meta.InitPhysicalBacking()
						level = append(level, currentFile)
						metas = append(metas, meta)
						rangedels = append(rangedels, currentRangeDels)
						currentRangeDels = nil
						currentFile = nil
						pointKeys = nil
					}
					continue
				}
				key = strings.TrimSpace(key)
				if strings.HasPrefix(key, "point:") {
					key = strings.TrimPrefix(key, "point:")
					j := strings.Index(key, ":")
					ikey := base.ParseInternalKey(key[:j])
					pointKeys = append(pointKeys, ikey)
					if ikey.Kind() == base.InternalKeyKindRangeDelete {
						currentRangeDels = append(currentRangeDels, keyspan.Span{
							Start: ikey.UserKey, End: []byte(key[j+1:]), Keys: []keyspan.Key{{Trailer: ikey.Trailer}}})
					}
					continue
				}
				span := keyspan.ParseSpan(key)
				currentFile = append(currentFile, span)
			}
			meta := &manifest.FileMetadata{
				FileNum: base.FileNum(len(level) + 1),
			}
			meta.InitPhysicalBacking()
			level = append(level, currentFile)
			rangedels = append(rangedels, currentRangeDels)
			if len(currentFile) > 0 {
				smallest := base.MakeInternalKey(currentFile[0].Start, currentFile[0].SmallestKey().SeqNum(), currentFile[0].SmallestKey().Kind())
				largest := base.MakeExclusiveSentinelKey(currentFile[len(currentFile)-1].LargestKey().Kind(), currentFile[len(currentFile)-1].End)
				meta.ExtendRangeKeyBounds(base.DefaultComparer.Compare, smallest, largest)
			}
			if len(pointKeys) != 0 {
				meta.ExtendPointKeyBounds(base.DefaultComparer.Compare, pointKeys[0], pointKeys[len(pointKeys)-1])
			}
			metas = append(metas, meta)
			return ""
		case "num-files":
			return fmt.Sprintf("%d", len(level))
		case "close-iter":
			_ = iter.Close()
			iter = nil
			return "ok"
		case "iter":
			keyType := manifest.KeyTypeRange
			for _, arg := range d.CmdArgs {
				if strings.Contains(arg.Key, "rangedel") {
					keyType = manifest.KeyTypePoint
				}
			}
			if iter == nil {
				var lastFileNum base.FileNum
				tableNewIters := func(file *manifest.FileMetadata, _ keyspan.SpanIterOptions) (keyspan.FragmentIterator, error) {
					keyType := keyType
					spans := level[file.FileNum-1]
					if keyType == manifest.KeyTypePoint {
						spans = rangedels[file.FileNum-1]
					}
					lastFileNum = file.FileNum
					return keyspan.NewIter(base.DefaultComparer.Compare, spans), nil
				}
				b := &manifest.BulkVersionEdit{}
				amap := make(map[base.FileNum]*manifest.FileMetadata)
				for i := range metas {
					amap[metas[i].FileNum] = metas[i]
				}
				b.Added[6] = amap
				v, err := b.Apply(nil, base.DefaultComparer, 0, 0)
				require.NoError(t, err)
				iter = NewLevelIter(
					keyspan.SpanIterOptions{}, base.DefaultComparer.Compare,
					tableNewIters, v.Levels[6].Iter(), 6, keyType,
				)
				extraInfo = func() string {
					return fmt.Sprintf("file = %s.sst", lastFileNum)
				}
			}

			return keyspan.RunFragmentIteratorCmd(iter, d.Input, extraInfo)

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})

	if iter != nil {
		iter.Close()
	}
}
