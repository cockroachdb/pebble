// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/stretchr/testify/require"
)

func TestInterleavingIter(t *testing.T) {
	runInterleavingIterTest(t, "testdata/interleaving_iter")
}

func TestInterleavingIter_Masking(t *testing.T) {
	runInterleavingIterTest(t, "testdata/interleaving_iter_masking")
}

type maskingHooks struct {
	cmp        base.Compare
	split      base.Split
	threshold  []byte
	maskSuffix []byte
}

func (m *maskingHooks) SpanChanged(s Span) {
	// Find the smallest suffix of a key contained within the Span, excluding
	// suffixes less than m.threshold.
	m.maskSuffix = nil
	if m.threshold == nil || len(s.Keys) == 0 {
		return
	}
	for i := range s.Keys {
		if s.Keys[i].Suffix == nil {
			continue
		}
		if m.cmp(s.Keys[i].Suffix, m.threshold) < 0 {
			continue
		}
		if m.maskSuffix == nil || m.cmp(m.maskSuffix, s.Keys[i].Suffix) > 0 {
			m.maskSuffix = s.Keys[i].Suffix
		}
	}
}

func (m *maskingHooks) SkipPoint(userKey []byte) bool {
	pointSuffix := userKey[m.split(userKey):]
	return m.maskSuffix != nil && len(pointSuffix) > 0 && m.cmp(m.maskSuffix, pointSuffix) < 0
}

func runInterleavingIterTest(t *testing.T, filename string) {
	cmp := testkeys.Comparer.Compare
	var keyspanIter MergingIter
	var pointIter pointIterator
	var iter InterleavingIter
	var buf bytes.Buffer
	hooks := maskingHooks{
		cmp:   testkeys.Comparer.Compare,
		split: testkeys.Comparer.Split,
	}

	formatKey := func(k *base.InternalKey, v []byte) {
		if k == nil {
			fmt.Fprint(&buf, ".")
			return
		}
		fmt.Fprintf(&buf, "PointKey: %s\nSpan: %s\n-", k.String(), iter.Span())
	}

	datadriven.RunTest(t, filename, func(td *datadriven.TestData) string {
		buf.Reset()
		switch td.Cmd {
		case "set-masking-threshold":
			hooks.threshold = []byte(strings.TrimSpace(td.Input))
			return "OK"
		case "define-rangekeys":
			var spans []Span
			lines := strings.Split(strings.TrimSpace(td.Input), "\n")
			for _, line := range lines {
				spans = append(spans, ParseSpan(line))
			}
			keyspanIter.Init(cmp, noopTransform, NewIter(cmp, spans))
			hooks.maskSuffix = nil
			iter.Init(cmp, base.WrapIterWithStats(&pointIter), &keyspanIter, Hooks{
				SpanChanged: hooks.SpanChanged,
				SkipPoint:   hooks.SkipPoint,
			}, nil, nil)
			return "OK"
		case "define-pointkeys":
			var points []base.InternalKey
			lines := strings.Split(strings.TrimSpace(td.Input), "\n")
			for _, line := range lines {
				points = append(points, base.ParseInternalKey(line))
			}
			pointIter = pointIterator{cmp: cmp, keys: points}
			hooks.maskSuffix = nil
			iter.Init(cmp, base.WrapIterWithStats(&pointIter), &keyspanIter, Hooks{
				SpanChanged: hooks.SpanChanged,
				SkipPoint:   hooks.SkipPoint,
			}, nil, nil)
			return "OK"
		case "iter":
			buf.Reset()
			// Clear any previous bounds.
			iter.SetBounds(nil, nil)
			lines := strings.Split(strings.TrimSpace(td.Input), "\n")
			for _, line := range lines {
				bufLen := buf.Len()
				line = strings.TrimSpace(line)
				i := strings.IndexByte(line, ' ')
				iterCmd := line
				if i > 0 {
					iterCmd = string(line[:i])
				}
				switch iterCmd {
				case "first":
					formatKey(iter.First())
				case "last":
					formatKey(iter.Last())
				case "next":
					formatKey(iter.Next())
				case "prev":
					formatKey(iter.Prev())
				case "seek-ge":
					formatKey(iter.SeekGE([]byte(strings.TrimSpace(line[i:])), false /* trySeekUsingNext */))
				case "seek-lt":
					formatKey(iter.SeekLT([]byte(strings.TrimSpace(line[i:]))))
				case "set-bounds":
					bounds := strings.Fields(line[i:])
					if len(bounds) != 2 {
						return fmt.Sprintf("set-bounds expects 2 bounds, got %d", len(bounds))
					}
					l, u := []byte(bounds[0]), []byte(bounds[1])
					if bounds[0] == "." {
						l = nil
					}
					if bounds[1] == "." {
						u = nil
					}
					iter.SetBounds(l, u)
				default:
					return fmt.Sprintf("unrecognized iter command %q", iterCmd)
				}
				require.NoError(t, iter.Error())
				if buf.Len() > bufLen {
					fmt.Fprintln(&buf)
				}
			}
			return strings.TrimSpace(buf.String())
		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}

type pointIterator struct {
	cmp   base.Compare
	keys  []base.InternalKey
	lower []byte
	upper []byte
	index int
}

var _ base.InternalIterator = &pointIterator{}

func (i *pointIterator) SeekGE(key []byte, trySeekUsingNext bool) (*base.InternalKey, []byte) {
	i.index = sort.Search(len(i.keys), func(j int) bool {
		return i.cmp(i.keys[j].UserKey, key) >= 0
	})
	if i.index < 0 || i.index >= len(i.keys) {
		return nil, nil
	}
	if i.upper != nil && i.cmp(i.keys[i.index].UserKey, i.upper) >= 0 {
		return nil, nil
	}
	return &i.keys[i.index], nil
}

func (i *pointIterator) SeekPrefixGE(
	prefix, key []byte, trySeekUsingNext bool,
) (*base.InternalKey, []byte) {
	return i.SeekGE(key, trySeekUsingNext)
}

func (i *pointIterator) SeekLT(key []byte) (*base.InternalKey, []byte) {
	i.index = sort.Search(len(i.keys), func(j int) bool {
		return i.cmp(i.keys[j].UserKey, key) >= 0
	})
	i.index--
	if i.index < 0 || i.index >= len(i.keys) {
		return nil, nil
	}
	if i.lower != nil && i.cmp(i.keys[i.index].UserKey, i.lower) < 0 {
		return nil, nil
	}
	return &i.keys[i.index], nil
}

func (i *pointIterator) First() (*base.InternalKey, []byte) {
	i.index = 0
	if i.index < 0 || i.index >= len(i.keys) {
		return nil, nil
	}
	if i.upper != nil && i.cmp(i.keys[i.index].UserKey, i.upper) >= 0 {
		return nil, nil
	}
	return &i.keys[i.index], nil
}

func (i *pointIterator) Last() (*base.InternalKey, []byte) {
	i.index = len(i.keys) - 1
	if i.index < 0 || i.index >= len(i.keys) {
		return nil, nil
	}
	if i.lower != nil && i.cmp(i.keys[i.index].UserKey, i.lower) < 0 {
		return nil, nil
	}
	return &i.keys[i.index], nil
}

func (i *pointIterator) Next() (*base.InternalKey, []byte) {
	i.index++
	if i.index < 0 || i.index >= len(i.keys) {
		return nil, nil
	}
	if i.upper != nil && i.cmp(i.keys[i.index].UserKey, i.upper) >= 0 {
		return nil, nil
	}
	return &i.keys[i.index], nil
}

func (i *pointIterator) Prev() (*base.InternalKey, []byte) {
	i.index--
	if i.index < 0 || i.index >= len(i.keys) {
		return nil, nil
	}
	if i.lower != nil && i.cmp(i.keys[i.index].UserKey, i.lower) < 0 {
		return nil, nil
	}
	return &i.keys[i.index], nil
}

func (i *pointIterator) Close() error   { return nil }
func (i *pointIterator) Error() error   { return nil }
func (i *pointIterator) String() string { return "test-point-iterator" }
func (i *pointIterator) SetBounds(lower, upper []byte) {
	i.lower, i.upper = lower, upper
}
