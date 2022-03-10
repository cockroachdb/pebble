// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangekey

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/stretchr/testify/require"
)

func TestInterleavingIter(t *testing.T) {
	runInterleavingIterTest(t, "testdata/interleaving_iter")
}

func TestInterleavingIter_Masking(t *testing.T) {
	runInterleavingIterTest(t, "testdata/interleaving_iter_masking")
}

func runInterleavingIterTest(t *testing.T, filename string) {
	cmp := testkeys.Comparer.Compare
	var rangeKeyIter Iter
	var pointIter pointIterator
	var iter InterleavingIter
	var buf bytes.Buffer
	var maskingThreshold []byte

	formatKey := func(k *base.InternalKey, v []byte) {
		if k == nil {
			fmt.Fprint(&buf, ".")
			return
		}
		fmt.Fprintf(&buf, "PointKey: %s\nRangeKey: ", k.String())
		if iter.HasRangeKey() {
			start, end := iter.RangeKeyBounds()
			fmt.Fprintf(&buf, "[%s, %s)", start, end)
			formatRangeKeyItems(&buf, iter.RangeKeys())
		} else {
			fmt.Fprint(&buf, ".")
		}
		fmt.Fprint(&buf, "\n-")
	}

	datadriven.RunTest(t, filename, func(td *datadriven.TestData) string {
		buf.Reset()
		switch td.Cmd {
		case "set-masking-threshold":
			maskingThreshold = []byte(strings.TrimSpace(td.Input))
			iter.Init(cmp, testkeys.Comparer.Split, base.WrapIterWithStats(&pointIter), &rangeKeyIter,
				maskingThreshold)
			return "OK"
		case "define-rangekeys":
			var spans []keyspan.Span
			lines := strings.Split(strings.TrimSpace(td.Input), "\n")
			for _, line := range lines {
				startKey, value := Parse(line)
				endKey, v, ok := DecodeEndKey(startKey.Kind(), value)
				require.True(t, ok)
				spans = append(spans, keyspan.Span{
					Start: startKey,
					End:   endKey,
					Value: v,
				})
			}
			rangeKeyIter.Init(cmp, testkeys.Comparer.FormatKey, base.InternalKeySeqNumMax, keyspan.NewIter(cmp, spans))
			iter.Init(cmp, testkeys.Comparer.Split, base.WrapIterWithStats(&pointIter), &rangeKeyIter,
				maskingThreshold)
			return "OK"
		case "define-pointkeys":
			var points []base.InternalKey
			lines := strings.Split(strings.TrimSpace(td.Input), "\n")
			for _, line := range lines {
				points = append(points, base.ParseInternalKey(line))
			}
			pointIter = pointIterator{cmp: cmp, keys: points}
			iter.Init(cmp, testkeys.Comparer.Split, base.WrapIterWithStats(&pointIter), &rangeKeyIter,
				maskingThreshold)
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
