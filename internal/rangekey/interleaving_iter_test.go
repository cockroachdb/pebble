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
	cmp := testkeys.Comparer.Compare
	var rangeKeyIter Iter
	var pointIter pointIterator
	var iter InterleavingIter
	var buf bytes.Buffer

	formatKey := func(k *base.InternalKey, v []byte) {
		if k == nil {
			fmt.Fprintln(&buf, ".")
			return
		}
		fmt.Fprintf(&buf, "PointKey: %s\nRangeKey: ", k.String())
		formatRangeKeySpan(&buf, iter.RangeKey())
		fmt.Fprint(&buf, "\n-")
	}

	datadriven.RunTest(t, "testdata/interleaving_iter", func(td *datadriven.TestData) string {
		buf.Reset()
		switch td.Cmd {
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
			rangeKeyIter.Init(cmp, testkeys.Comparer.FormatKey, keyspan.NewIter(cmp, spans))
			iter.Init(&pointIter, &rangeKeyIter)
			return "OK"
		case "define-pointkeys":
			var points []base.InternalKey
			lines := strings.Split(strings.TrimSpace(td.Input), "\n")
			for _, line := range lines {
				points = append(points, base.ParseInternalKey(line))
			}
			pointIter = pointIterator{cmp: cmp, keys: points}
			iter.Init(&pointIter, &rangeKeyIter)
			return "OK"
		case "iter":
			buf.Reset()
			lines := strings.Split(strings.TrimSpace(td.Input), "\n")
			for _, line := range lines {
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
					formatKey(iter.SeekGE([]byte(strings.TrimSpace(line[i:]))))
				case "seek-lt":
					formatKey(iter.SeekLT([]byte(strings.TrimSpace(line[i:]))))
				default:
					return fmt.Sprintf("unrecognized iter command %q", iterCmd)
				}
				require.NoError(t, iter.Error())
				if buf.Len() > 0 {
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
	index int
}

var _ base.InternalIterator = &pointIterator{}

func (i *pointIterator) SeekGE(key []byte) (*base.InternalKey, []byte) {
	i.index = sort.Search(len(i.keys), func(j int) bool {
		return i.cmp(i.keys[j].UserKey, key) >= 0
	})
	if i.index < 0 || i.index >= len(i.keys) {
		return nil, nil
	}
	return &i.keys[i.index], nil
}

func (i *pointIterator) SeekPrefixGE(prefix, key []byte, trySeekUsingNext bool) (*base.InternalKey, []byte) {
	return i.SeekGE(key)
}

func (i *pointIterator) SeekLT(key []byte) (*base.InternalKey, []byte) {
	i.index = sort.Search(len(i.keys), func(j int) bool {
		return i.cmp(i.keys[j].UserKey, key) >= 0
	})
	i.index--
	if i.index < 0 || i.index >= len(i.keys) {
		return nil, nil
	}
	return &i.keys[i.index], nil
}

func (i *pointIterator) First() (*base.InternalKey, []byte) {
	i.index = 0
	if i.index < 0 || i.index >= len(i.keys) {
		return nil, nil
	}
	return &i.keys[i.index], nil
}

func (i *pointIterator) Last() (*base.InternalKey, []byte) {
	i.index = len(i.keys) - 1
	if i.index < 0 || i.index >= len(i.keys) {
		return nil, nil
	}
	return &i.keys[i.index], nil
}

func (i *pointIterator) Next() (*base.InternalKey, []byte) {
	i.index++
	if i.index < 0 || i.index >= len(i.keys) {
		return nil, nil
	}
	return &i.keys[i.index], nil
}

func (i *pointIterator) Prev() (*base.InternalKey, []byte) {
	i.index--
	if i.index < 0 || i.index >= len(i.keys) {
		return nil, nil
	}
	return &i.keys[i.index], nil
}

func (i *pointIterator) Close() error                  { return nil }
func (i *pointIterator) Error() error                  { return nil }
func (i *pointIterator) String() string                { return "test-point-iterator" }
func (i *pointIterator) SetBounds(upper, lower []byte) {}
