// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/stretchr/testify/require"
)

func TestLevelIterEquivalence(t *testing.T) {
	type level [][]Span
	testCases := []struct {
		name   string
		levels []level
	}{
		{
			"single level, no gaps, no overlaps",
			[]level{
				{
					{
						Span{
							Start: base.MakeInternalKey([]byte("a"), 2, base.InternalKeyKindRangeKeySet),
							End:   []byte("b"),
							Value: []byte("foo"),
						},
						Span{
							Start: base.MakeInternalKey([]byte("b"), 2, base.InternalKeyKindRangeKeySet),
							End:   []byte("c"),
							Value: []byte("foo"),
						},
						Span{
							Start: base.MakeInternalKey([]byte("c"), 2, base.InternalKeyKindRangeKeySet),
							End:   []byte("d"),
							Value: []byte("foo"),
						},
					},
					{
						Span{
							Start: base.MakeInternalKey([]byte("d"), 2, base.InternalKeyKindRangeKeySet),
							End:   []byte("e"),
							Value: []byte("foo"),
						},
						Span{
							Start: base.MakeInternalKey([]byte("e"), 2, base.InternalKeyKindRangeKeySet),
							End:   []byte("f"),
							Value: []byte("foo"),
						},
						Span{
							Start: base.MakeInternalKey([]byte("f"), 2, base.InternalKeyKindRangeKeySet),
							End:   []byte("g"),
							Value: []byte("foo"),
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
						Span{
							Start: base.MakeInternalKey([]byte("a"), 4, base.InternalKeyKindRangeKeySet),
							End:   []byte("b"),
							Value: []byte("bar"),
						},
						Span{
							Start: base.MakeInternalKey([]byte("a"), 2, base.InternalKeyKindRangeKeySet),
							End:   []byte("b"),
							Value: []byte("foo"),
						},
						Span{
							Start: base.MakeInternalKey([]byte("b"), 4, base.InternalKeyKindRangeKeySet),
							End:   []byte("c"),
							Value: []byte("bar"),
						},
						Span{
							Start: base.MakeInternalKey([]byte("b"), 2, base.InternalKeyKindRangeKeySet),
							End:   []byte("c"),
							Value: []byte("foo"),
						},
						Span{
							Start: base.MakeInternalKey([]byte("c"), 2, base.InternalKeyKindRangeKeySet),
							End:   []byte("d"),
							Value: []byte("foo"),
						},
					},
					{
						Span{
							Start: base.MakeInternalKey([]byte("d"), 2, base.InternalKeyKindRangeKeySet),
							End:   []byte("e"),
							Value: []byte("foo"),
						},
						Span{
							Start: base.MakeInternalKey([]byte("e"), 2, base.InternalKeyKindRangeKeySet),
							End:   []byte("f"),
							Value: []byte("foo"),
						},
						Span{
							Start: base.MakeInternalKey([]byte("f"), 2, base.InternalKeyKindRangeKeySet),
							End:   []byte("g"),
							Value: []byte("foo"),
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
						Span{
							Start: base.MakeInternalKey([]byte("a"), 2, base.InternalKeyKindRangeKeySet),
							End:   []byte("b"),
							Value: []byte("foo"),
						},
						Span{
							Start: base.MakeInternalKey([]byte("c"), 2, base.InternalKeyKindRangeKeySet),
							End:   []byte("d"),
							Value: []byte("foo"),
						},
						Span{
							Start: base.MakeInternalKey([]byte("e"), 2, base.InternalKeyKindRangeKeySet),
							End:   []byte("f"),
							Value: []byte("foo"),
						},
					},
					{
						Span{
							Start: base.MakeInternalKey([]byte("g"), 2, base.InternalKeyKindRangeKeySet),
							End:   []byte("h"),
							Value: []byte("foo"),
						},
						Span{
							Start: base.MakeInternalKey([]byte("i"), 2, base.InternalKeyKindRangeKeySet),
							End:   []byte("j"),
							Value: []byte("foo"),
						},
						Span{
							Start: base.MakeInternalKey([]byte("k"), 2, base.InternalKeyKindRangeKeySet),
							End:   []byte("l"),
							Value: []byte("foo"),
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
						Span{
							Start: base.MakeInternalKey([]byte("a"), 2, base.InternalKeyKindRangeKeySet),
							End:   []byte("h"),
							Value: []byte("foo"),
						},
					},
					{
						Span{
							Start: base.MakeInternalKey([]byte("l"), 2, base.InternalKeyKindRangeKeyUnset),
							End:   []byte("u"),
							Value: nil,
						},
					},
				},
				{
					{
						Span{
							Start: base.MakeInternalKey([]byte("e"), 1, base.InternalKeyKindRangeKeySet),
							End:   []byte("r"),
							Value: []byte("foo"),
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		var fileIters []FragmentIterator
		var levelIters []FragmentIterator
		var iter1, iter2 MergingIter
		for j, level := range tc.levels {
			j := j // Copy for use in closures down below.
			var levelIter LevelIter
			var metas []*manifest.FileMetadata
			for k, file := range level {
				fileIters = append(fileIters, NewIter(base.DefaultComparer.Compare, file))
				metas = append(metas, &manifest.FileMetadata{
					FileNum:          base.FileNum(k + 1),
					Size:             1024,
					SmallestSeqNum:   2,
					LargestSeqNum:    2,
					SmallestRangeKey: file[0].Start,
					LargestRangeKey:  file[len(file)-1].Start,
					Smallest:         file[0].Start,
					Largest:          file[len(file)-1].Start,
					HasPointKeys:     false,
					HasRangeKeys:     true,
				})
			}

			tableNewIters := func(file *manifest.FileMetadata, iterOptions *RangeIterOptions) (FragmentIterator, error) {
				return NewIter(base.DefaultComparer.Compare, tc.levels[j][file.FileNum-1]), nil
			}
			// Add all the fileMetadatas to L6.
			b := &manifest.BulkVersionEdit{}
			b.Added[6] = metas
			v, _, err := b.Apply(nil, base.DefaultComparer.Compare, base.DefaultFormatter, 0, 0)
			require.NoError(t, err)
			levelIter.Init(RangeIterOptions{}, base.DefaultComparer.Compare, tableNewIters, v.Levels[6].Iter(), 0, nil)
			levelIters = append(levelIters, &levelIter)
		}

		iter1.Init(base.DefaultComparer.Compare, VisibleTransform(base.InternalKeySeqNumMax), fileIters...)
		iter2.Init(base.DefaultComparer.Compare, VisibleTransform(base.InternalKeySeqNumMax), levelIters...)
		// Check iter1 and iter2 for equivalence.

		require.Equal(t, iter1.First(), iter2.First(), "failed on test case %q", tc.name)
		valid := true
		for valid {
			f1 := iter1.Next()
			f2 := iter2.Next()

			require.Equal(t, f1, f2, "failed on test case %q", tc.name)
			valid = !f1.Empty() && !f2.Empty()
		}
	}
}

type testLogger struct {
	t *testing.T
}

// Infof implements the Logger.Infof interface.
func (t *testLogger) Infof(format string, args ...interface{}) {
	_ = log.Output(2, fmt.Sprintf(format, args...))
}

// Fatalf implements the Logger.Fatalf interface.
func (t *testLogger) Fatalf(format string, args ...interface{}) {
	_ = log.Output(2, fmt.Sprintf(format, args...))
	t.t.Fail()
}

func TestLevelIter(t *testing.T) {
	var level [][]Span
	var metas []*manifest.FileMetadata
	var pointKey *base.InternalKey
	var iter FragmentIterator

	datadriven.RunTest(t, "testdata/level_iter", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			level = level[:0]
			metas = metas[:0]
			if iter != nil {
				iter.Close()
				iter = nil
			}
			var currentFile []Span
			for _, key := range strings.Split(d.Input, "\n") {
				if strings.HasPrefix(key, "file") {
					// Skip the very first file creation.
					if len(level) != 0 || len(currentFile) != 0 {
						meta := &manifest.FileMetadata{
							FileNum: base.FileNum(len(level) + 1),
						}
						if len(currentFile) > 0 {
							meta.ExtendRangeKeyBounds(base.DefaultComparer.Compare, currentFile[0].Start, base.MakeRangeKeySentinelKey(currentFile[len(currentFile)-1].Start.Kind(), currentFile[len(currentFile)-1].End))
						}
						if pointKey != nil {
							meta.ExtendPointKeyBounds(base.DefaultComparer.Compare, *pointKey, *pointKey)
						}
						level = append(level, currentFile)
						metas = append(metas, meta)
						currentFile = nil
						pointKey = nil
					}
					continue
				}
				key = strings.TrimSpace(key)
				j := strings.Index(key, ":")
				ikey := base.ParseInternalKey(key[:j])
				if ikey.Kind() != base.InternalKeyKindRangeKeySet &&
					ikey.Kind() != base.InternalKeyKindRangeKeyUnset &&
					ikey.Kind() != base.InternalKeyKindRangeKeyDelete {
					pointKey = &ikey
					continue
				}
				currentFile = append(currentFile, Span{
					Start: ikey,
					End:   []byte(key[j+1:]),
				})
			}
			meta := &manifest.FileMetadata{
				FileNum: base.FileNum(len(level) + 1),
			}
			level = append(level, currentFile)
			if len(currentFile) > 0 {
				meta.ExtendRangeKeyBounds(base.DefaultComparer.Compare, currentFile[0].Start, base.MakeRangeKeySentinelKey(currentFile[len(currentFile)-1].Start.Kind(), currentFile[len(currentFile)-1].End))
			}
			if pointKey != nil {
				meta.ExtendPointKeyBounds(base.DefaultComparer.Compare, *pointKey, *pointKey)
			}
			metas = append(metas, meta)
			return ""
		case "num-files":
			return fmt.Sprintf("%d", len(level))
		case "iter":
			var extraInfo func() string
			if iter == nil {
				var lastFileNum base.FileNum
				tableNewIters := func(file *manifest.FileMetadata, iterOptions *RangeIterOptions) (FragmentIterator, error) {
					spans := level[file.FileNum-1]
					lastFileNum = file.FileNum
					return NewIter(base.DefaultComparer.Compare, spans), nil
				}
				b := &manifest.BulkVersionEdit{}
				b.Added[6] = metas
				v, _, err := b.Apply(nil, base.DefaultComparer.Compare, base.DefaultFormatter, 0, 0)
				require.NoError(t, err)
				iter = newLevelIter(RangeIterOptions{}, base.DefaultComparer.Compare, tableNewIters, v.Levels[6].Iter(), 6, &testLogger{t})
				extraInfo = func() string {
					return fmt.Sprintf("file = %s.sst", lastFileNum)
				}
			}

			return runFragmentIteratorCmd(iter, d.Input, extraInfo)

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})

	if iter != nil {
		iter.Close()
	}
}
