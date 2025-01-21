// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/internal/testkeys"
	"github.com/cockroachdb/pebble/v2/internal/treeprinter"
	"github.com/stretchr/testify/require"
)

func TestInterleavingIter(t *testing.T) {
	runInterleavingIterTest(t, "testdata/interleaving_iter")
}

func TestInterleavingIter_Masking(t *testing.T) {
	runInterleavingIterTest(t, "testdata/interleaving_iter_masking")
}

type maskingHooks struct {
	log        io.Writer
	suffixCmp  base.CompareRangeSuffixes
	split      base.Split
	threshold  []byte
	maskSuffix []byte
}

func (m *maskingHooks) SpanChanged(s *Span) {
	if m.log != nil {
		if s == nil {
			fmt.Fprintln(m.log, "-- SpanChanged(nil)")
		} else {
			fmt.Fprintf(m.log, "-- SpanChanged(%s)\n", s)
		}
	}

	// Find the smallest suffix of a key contained within the Span, excluding
	// suffixes less than m.threshold.
	m.maskSuffix = nil
	if s == nil || m.threshold == nil || len(s.Keys) == 0 {
		return
	}
	for i := range s.Keys {
		if s.Keys[i].Suffix == nil {
			continue
		}
		if m.suffixCmp(s.Keys[i].Suffix, m.threshold) < 0 {
			continue
		}
		if m.maskSuffix == nil || m.suffixCmp(m.maskSuffix, s.Keys[i].Suffix) > 0 {
			m.maskSuffix = s.Keys[i].Suffix
		}
	}
}

func (m *maskingHooks) SkipPoint(userKey []byte) bool {
	pointSuffix := userKey[m.split(userKey):]
	return m.maskSuffix != nil && len(pointSuffix) > 0 && m.suffixCmp(m.maskSuffix, pointSuffix) < 0
}

func runInterleavingIterTest(t *testing.T, filename string) {
	cmp := testkeys.Comparer.Compare
	var keyspanIter FragmentIterator
	var pointIter pointIterator
	var iter InterleavingIter
	var buf bytes.Buffer
	hooks := maskingHooks{
		log:       &buf,
		suffixCmp: testkeys.Comparer.CompareRangeSuffixes,
		split:     testkeys.Comparer.Split,
	}

	var prevKV *base.InternalKV
	formatKey := func(kv *base.InternalKV) {
		if kv == nil {
			fmt.Fprint(&buf, ".")
			return
		}
		prevKV = kv
		s := iter.Span()
		fmt.Fprintf(&buf, "PointKey: %s\n", kv.K.String())
		if s != nil {
			fmt.Fprintf(&buf, "Span: %s\n-", s)
		} else {
			fmt.Fprintf(&buf, "Span: %s\n-", Span{})
		}
	}

	datadriven.RunTest(t, filename, func(t *testing.T, td *datadriven.TestData) string {
		buf.Reset()
		switch td.Cmd {
		case "define-spans":
			var spans []Span
			lines := strings.Split(strings.TrimSpace(td.Input), "\n")
			for _, line := range lines {
				spans = append(spans, ParseSpan(line))
			}
			keyspanIter = NewIter(cmp, spans)
			return "OK"
		case "define-pointkeys":
			var points []base.InternalKV
			lines := strings.Split(strings.TrimSpace(td.Input), "\n")
			for _, line := range lines {
				points = append(points, base.MakeInternalKV(base.ParseInternalKey(line), nil))
			}
			pointIter = pointIterator{cmp: cmp, kvs: points}
			return "OK"
		case "iter":
			buf.Reset()
			hooks.maskSuffix = nil
			opts := InterleavingIterOpts{Mask: &hooks}
			if cmdArg, ok := td.Arg("masking-threshold"); ok {
				hooks.threshold = []byte(strings.Join(cmdArg.Vals, ""))
			}
			opts.InterleaveEndKeys = td.HasArg("interleave-end-keys")
			iter.Init(testkeys.Comparer, &pointIter, keyspanIter, opts)
			// Clear any previous bounds.
			pointIter.SetBounds(nil, nil)
			prevKV = nil
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
				case "next-prefix":
					prevUserKey := prevKV.K.UserKey
					succKey := testkeys.Comparer.ImmediateSuccessor(nil, prevUserKey[:testkeys.Comparer.Split(prevUserKey)])
					formatKey(iter.NextPrefix(succKey))
				case "prev":
					formatKey(iter.Prev())
				case "seek-ge":
					formatKey(iter.SeekGE([]byte(strings.TrimSpace(line[i:])), base.SeekGEFlagsNone))
				case "seek-prefix-ge":
					key := []byte(strings.TrimSpace(line[i:]))
					prefix := key[:testkeys.Comparer.Split(key)]
					formatKey(iter.SeekPrefixGE(prefix, key, base.SeekGEFlagsNone))
				case "seek-lt":
					formatKey(iter.SeekLT([]byte(strings.TrimSpace(line[i:])), base.SeekLTFlagsNone))
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
	require.NoError(t, iter.Close())
}

type pointIterator struct {
	cmp   base.Compare
	kvs   []base.InternalKV
	lower []byte
	upper []byte
	index int
}

var _ base.InternalIterator = &pointIterator{}

func (i *pointIterator) SeekGE(key []byte, flags base.SeekGEFlags) *base.InternalKV {
	i.index = sort.Search(len(i.kvs), func(j int) bool {
		return i.cmp(i.kvs[j].K.UserKey, key) >= 0
	})
	if i.index < 0 || i.index >= len(i.kvs) {
		return nil
	}
	if i.upper != nil && i.cmp(i.kvs[i.index].K.UserKey, i.upper) >= 0 {
		return nil
	}
	return &i.kvs[i.index]
}

func (i *pointIterator) SeekPrefixGE(prefix, key []byte, flags base.SeekGEFlags) *base.InternalKV {
	return i.SeekGE(key, flags)
}

func (i *pointIterator) SeekLT(key []byte, flags base.SeekLTFlags) *base.InternalKV {
	i.index = sort.Search(len(i.kvs), func(j int) bool {
		return i.cmp(i.kvs[j].K.UserKey, key) >= 0
	})
	i.index--
	if i.index < 0 || i.index >= len(i.kvs) {
		return nil
	}
	if i.lower != nil && i.cmp(i.kvs[i.index].K.UserKey, i.lower) < 0 {
		return nil
	}
	return &i.kvs[i.index]
}

func (i *pointIterator) First() *base.InternalKV {
	i.index = 0
	if i.index < 0 || i.index >= len(i.kvs) {
		return nil
	}
	if i.upper != nil && i.cmp(i.kvs[i.index].K.UserKey, i.upper) >= 0 {
		return nil
	}
	return &i.kvs[i.index]
}

func (i *pointIterator) Last() *base.InternalKV {
	i.index = len(i.kvs) - 1
	if i.index < 0 || i.index >= len(i.kvs) {
		return nil
	}
	if i.lower != nil && i.cmp(i.kvs[i.index].K.UserKey, i.lower) < 0 {
		return nil
	}
	return &i.kvs[i.index]
}

func (i *pointIterator) Next() *base.InternalKV {
	i.index++
	if i.index < 0 || i.index >= len(i.kvs) {
		return nil
	}
	if i.upper != nil && i.cmp(i.kvs[i.index].K.UserKey, i.upper) >= 0 {
		return nil
	}
	return &i.kvs[i.index]
}

func (i *pointIterator) NextPrefix(succKey []byte) *base.InternalKV {
	return i.SeekGE(succKey, base.SeekGEFlagsNone)
}

func (i *pointIterator) Prev() *base.InternalKV {
	i.index--
	if i.index < 0 || i.index >= len(i.kvs) {
		return nil
	}
	if i.lower != nil && i.cmp(i.kvs[i.index].K.UserKey, i.lower) < 0 {
		return nil
	}
	return &i.kvs[i.index]
}

func (i *pointIterator) Close() error   { return nil }
func (i *pointIterator) Error() error   { return nil }
func (i *pointIterator) String() string { return "test-point-iterator" }
func (i *pointIterator) SetBounds(lower, upper []byte) {
	i.lower, i.upper = lower, upper
}
func (i *pointIterator) SetContext(_ context.Context) {}

// DebugTree is part of the InternalIterator interface.
func (i *pointIterator) DebugTree(tp treeprinter.Node) {
	tp.Childf("%T(%p)", i, i)
}
