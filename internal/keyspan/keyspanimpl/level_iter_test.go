// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspanimpl

import (
	"context"
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
			var metas []*manifest.TableMetadata
			for k, file := range level {
				fileIters = append(fileIters, keyspan.NewIter(base.DefaultComparer.Compare, file))
				meta := &manifest.TableMetadata{
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

			tableNewIters := func(ctx context.Context, file *manifest.TableMetadata, iterOptions keyspan.SpanIterOptions) (keyspan.FragmentIterator, error) {
				return keyspan.NewIter(base.DefaultComparer.Compare, tc.levels[j][file.FileNum-1]), nil
			}
			// Add all the fileMetadatas to L6.
			b := &manifest.BulkVersionEdit{}
			amap := make(map[base.FileNum]*manifest.TableMetadata)
			for i := range metas {
				amap[metas[i].FileNum] = metas[i]
			}
			b.AddedTables[6] = amap
			l0Organizer := manifest.NewL0Organizer(base.DefaultComparer, 0 /* flushSplitBytes */)
			emptyVersion := manifest.NewInitialVersion(base.DefaultComparer, l0Organizer)
			v, err := b.Apply(emptyVersion, l0Organizer, 0)
			require.NoError(t, err)
			levelIter.Init(
				context.Background(),
				keyspan.SpanIterOptions{}, base.DefaultComparer.Compare, tableNewIters,
				v.Levels[6].Iter(), manifest.Level(0), manifest.KeyTypeRange,
			)
			levelIters = append(levelIters, &levelIter)
		}

		iter1.Init(base.DefaultComparer, keyspan.VisibleTransform(base.SeqNumMax), new(MergingBuffers), fileIters...)
		iter2.Init(base.DefaultComparer, keyspan.VisibleTransform(base.SeqNumMax), new(MergingBuffers), levelIters...)

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
	var cmp = base.DefaultComparer.Compare
	type file struct {
		meta      *manifest.TableMetadata
		rangeDels []keyspan.Span
		rangeKeys []keyspan.Span
	}
	var files []file

	datadriven.RunTest(t, "testdata/level_iter", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			files = nil
			for _, key := range strings.Split(d.Input, "\n") {
				if strings.HasPrefix(key, "file") {
					meta := &manifest.TableMetadata{
						FileNum: base.FileNum(len(files) + 1),
					}
					meta.InitPhysicalBacking()

					fields := strings.Fields(key)
					for _, f := range fields[1:] {
						arg, val, ok := strings.Cut(f, "=")
						if !ok {
							d.Fatalf(t, "invalid line %q", key)
						}
						switch arg {
						case "point-key-bounds":
							start, end := base.ParseInternalKeyRange(val)
							meta.ExtendPointKeyBounds(cmp, start, end)
						case "range-key-bounds":
							start, end := base.ParseInternalKeyRange(val)
							meta.ExtendRangeKeyBounds(cmp, start, end)
						default:
							d.Fatalf(t, "unknown argument %q", arg)
						}
					}
					files = append(files, file{meta: meta})
					continue
				}
				span := keyspan.ParseSpan(key)
				f := &files[len(files)-1]
				smallest := base.MakeInternalKey(span.Start, span.SmallestKey().SeqNum(), span.SmallestKey().Kind())
				largest := base.MakeExclusiveSentinelKey(span.LargestKey().Kind(), span.End)
				if smallest.Kind() == base.InternalKeyKindRangeDelete {
					f.rangeDels = append(f.rangeDels, span)
					f.meta.ExtendPointKeyBounds(cmp, smallest, largest)
				} else {
					f.rangeKeys = append(f.rangeKeys, span)
					f.meta.ExtendRangeKeyBounds(cmp, smallest, largest)
				}
			}
			var strs []string
			for i := range files {
				strs = append(strs, files[i].meta.String())
			}
			return strings.Join(strs, "\n")

		case "iter":
			keyType := manifest.KeyTypeRange
			for _, arg := range d.CmdArgs {
				if strings.Contains(arg.Key, "rangedel") {
					keyType = manifest.KeyTypePoint
				}
			}
			tableNewIters := func(ctx context.Context, file *manifest.TableMetadata, _ keyspan.SpanIterOptions) (keyspan.FragmentIterator, error) {
				f := files[file.FileNum-1]
				if keyType == manifest.KeyTypePoint {
					return keyspan.NewIter(cmp, f.rangeDels), nil
				}
				return keyspan.NewIter(cmp, f.rangeKeys), nil
			}
			metas := make([]*manifest.TableMetadata, len(files))
			for i := range files {
				metas[i] = files[i].meta
			}
			lm := manifest.MakeLevelMetadata(cmp, 6, metas)
			iter := NewLevelIter(context.Background(), keyspan.SpanIterOptions{}, cmp, tableNewIters, lm.Iter(), manifest.Level(6), keyType)
			extraInfo := func() string {
				return iter.String()
			}

			defer iter.Close()
			return keyspan.RunFragmentIteratorCmd(iter, d.Input, extraInfo)

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}
