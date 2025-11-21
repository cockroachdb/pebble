// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package overlap

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/stretchr/testify/require"
)

func TestChecker(t *testing.T) {
	tables := newTestTables()
	byName := make(map[string]*manifest.TableMetadata)

	datadriven.RunTest(t, "testdata/checker", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			tt := testTable{
				name: d.CmdArgs[0].String(),
				meta: &manifest.TableMetadata{
					TableNum: base.FileNum(1 + len(tables.tables)),
				},
			}
			tt.meta.InitPhysicalBacking()
			byName[tt.name] = tt.meta
			var overrideBounds base.UserKeyBounds
			for _, section := range splitLinesInSections(d.Input) {
				fields := strings.Fields(section[0])
				switch fields[0] {
				case "points:":
					for _, key := range section[1:] {
						tt.points = append(tt.points, base.InternalKV{
							K: base.ParseInternalKey(key),
						})
					}

				case "range-dels:":
					for _, span := range section[1:] {
						tt.rangeDels = append(tt.rangeDels, keyspan.ParseSpan(span))
					}

				case "range-keys:":
					for _, span := range section[1:] {
						tt.rangeKeys = append(tt.rangeKeys, keyspan.ParseSpan(span))
					}

				case "override-bounds:":
					overrideBounds = base.ParseUserKeyBounds(strings.Join(fields[1:], " "))

				default:
					d.Fatalf(t, "unknown section %q", fields[0])
				}
			}

			if overrideBounds.Start != nil {
				// Note: this code matches the logic used for external ingestions.
				if len(tt.points) > 0 || len(tt.rangeDels) > 0 {
					endKey := base.MakeInternalKey(overrideBounds.End.Key, 0, 0)
					if overrideBounds.End.Kind == base.Exclusive {
						endKey = base.MakeRangeDeleteSentinelKey(overrideBounds.End.Key)
					}
					tt.meta.ExtendPointKeyBounds(
						bytes.Compare,
						base.MakeInternalKey(overrideBounds.Start, 0, base.InternalKeyKindMax),
						endKey,
					)
				}
				if len(tt.rangeKeys) > 0 {
					tt.meta.ExtendRangeKeyBounds(
						bytes.Compare,
						base.MakeInternalKey(overrideBounds.Start, 0, base.InternalKeyKindRangeKeyMax),
						base.MakeExclusiveSentinelKey(base.InternalKeyKindRangeKeyMin, overrideBounds.End.Key),
					)
				}
			} else {
				if len(tt.points) > 0 {
					tt.meta.ExtendPointKeyBounds(bytes.Compare, tt.points[0].K, tt.points[len(tt.points)-1].K)
				}
				if len(tt.rangeDels) > 0 {
					smallest, largest := boundsFromSpans(tt.rangeDels)
					tt.meta.ExtendPointKeyBounds(bytes.Compare, smallest, largest)
				}
				if len(tt.rangeKeys) > 0 {
					smallest, largest := boundsFromSpans(tt.rangeKeys)
					tt.meta.ExtendRangeKeyBounds(bytes.Compare, smallest, largest)
				}
			}
			tables.tables[tt.meta] = tt

		case "overlap":
			var metas []*manifest.TableMetadata
			lines := strings.Split(d.Input, "\n")
			for _, arg := range d.CmdArgs {
				name := arg.String()
				m := byName[name]
				if m == nil {
					d.Fatalf(t, "unknown table %q", name)
				}
				metas = append(metas, m)
			}
			levelMeta := manifest.MakeLevelMetadata(bytes.Compare, 1 /* level */, metas)
			c := MakeChecker(bytes.Compare, tables)
			var buf strings.Builder
			for _, l := range lines {
				bounds := base.ParseUserKeyBounds(l)
				withLevel, err := c.LevelOverlap(context.Background(), bounds, levelMeta.Slice())
				require.NoError(t, err)
				switch withLevel.Result {
				case None:
					fmt.Fprintf(&buf, "%s: no overlap", bounds)
				case OnlyBoundary:
					splitFile := ""
					if withLevel.SplitFile != nil {
						splitFile = fmt.Sprintf(" (split file: %s)", tables.tables[withLevel.SplitFile].name)
					}
					fmt.Fprintf(&buf, "%s: boundary overlap, no data overlap%s", bounds, splitFile)
				case Data:
					fmt.Fprintf(&buf, "%s: possible data overlap", bounds)
				default:
					d.Fatalf(t, "invalid result %v", withLevel)
				}
				fmt.Fprintf(&buf, "   iterators opened: %s\n", tables.OpenedIterators())
			}
			return buf.String()

		default:
			d.Fatalf(t, "unknown command %q", d.Cmd)
		}
		return ""
	})
}

type testTable struct {
	name      string
	meta      *manifest.TableMetadata
	points    []base.InternalKV
	rangeDels []keyspan.Span
	rangeKeys []keyspan.Span
}

var _ IteratorFactory = (*testTables)(nil)

// testTables implements the IteratorFactory interface for testing purposes.
type testTables struct {
	tables map[*manifest.TableMetadata]testTable
	// openedIterators keeps track of the iterators we opened. It is reset before
	// every overlap check.
	openedIterators []string
}

func newTestTables() *testTables {
	return &testTables{
		tables: make(map[*manifest.TableMetadata]testTable),
	}
}

func (tt *testTables) OpenedIterators() string {
	if len(tt.openedIterators) == 0 {
		return "none"
	}
	res := strings.Join(tt.openedIterators, ", ")
	tt.openedIterators = nil
	return res
}

func (tt *testTables) Points(
	ctx context.Context, m *manifest.TableMetadata,
) (base.InternalIterator, error) {
	t := tt.get(m)
	tt.openedIterators = append(tt.openedIterators, fmt.Sprintf("%s/points", t.name))
	if len(t.points) == 0 && rand.IntN(2) == 0 {
		return nil, nil
	}
	return base.NewFakeIter(t.points), nil
}

func (tt *testTables) RangeDels(
	ctx context.Context, m *manifest.TableMetadata,
) (keyspan.FragmentIterator, error) {
	t := tt.get(m)
	tt.openedIterators = append(tt.openedIterators, fmt.Sprintf("%s/range-del", t.name))
	if len(t.rangeDels) == 0 && rand.IntN(2) == 0 {
		return nil, nil
	}
	return keyspan.NewIter(bytes.Compare, t.rangeDels), nil
}

func (tt *testTables) RangeKeys(
	ctx context.Context, m *manifest.TableMetadata,
) (keyspan.FragmentIterator, error) {
	t := tt.get(m)
	tt.openedIterators = append(tt.openedIterators, fmt.Sprintf("%s/range-key", t.name))
	if len(t.rangeKeys) == 0 && rand.IntN(2) == 0 {
		return nil, nil
	}
	return keyspan.NewIter(bytes.Compare, t.rangeKeys), nil
}

func (tt *testTables) get(m *manifest.TableMetadata) testTable {
	t, ok := tt.tables[m]
	if !ok {
		panic("table not found")
	}
	return t
}

// splitLinesInSections parses a multi-line string where each line that is not
// indented starts a section which also includes all indented lines that follow
// it.
func splitLinesInSections(input string) [][]string {
	var res [][]string
	for l := range crstrings.LinesSeq(input) {
		if l[0] == ' ' || l[0] == '\t' {
			if len(res) == 0 {
				panic(fmt.Sprintf("invalid first line %q", l))
			}
			res[len(res)-1] = append(res[len(res)-1], strings.TrimSpace(l))
		} else {
			res = append(res, []string{l})
		}
	}
	return res
}

func boundsFromSpans(spans []keyspan.Span) (smallest, largest base.InternalKey) {
	smallest = base.InternalKey{
		UserKey: spans[0].Start,
		Trailer: spans[0].Keys[0].Trailer,
	}
	last := spans[len(spans)-1]
	largest = base.MakeExclusiveSentinelKey(last.Keys[len(last.Keys)-1].Kind(), last.End)
	return smallest, largest
}
