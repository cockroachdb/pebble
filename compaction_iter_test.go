// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/rangekey"
)

func TestSnapshotIndex(t *testing.T) {
	testCases := []struct {
		snapshots      []uint64
		seq            uint64
		expectedIndex  int
		expectedSeqNum uint64
	}{
		{[]uint64{}, 1, 0, InternalKeySeqNumMax},
		{[]uint64{1}, 0, 0, 1},
		{[]uint64{1}, 1, 1, InternalKeySeqNumMax},
		{[]uint64{1}, 2, 1, InternalKeySeqNumMax},
		{[]uint64{1, 3}, 1, 1, 3},
		{[]uint64{1, 3}, 2, 1, 3},
		{[]uint64{1, 3}, 3, 2, InternalKeySeqNumMax},
		{[]uint64{1, 3}, 4, 2, InternalKeySeqNumMax},
		{[]uint64{1, 3, 3}, 2, 1, 3},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			idx, seqNum := snapshotIndex(c.seq, c.snapshots)
			if c.expectedIndex != idx {
				t.Fatalf("expected %d, but got %d", c.expectedIndex, idx)
			}
			if c.expectedSeqNum != seqNum {
				t.Fatalf("expected %d, but got %d", c.expectedSeqNum, seqNum)
			}
		})
	}
}

type debugMerger struct {
	buf []byte
}

func (m *debugMerger) MergeNewer(value []byte) error {
	m.buf = append(m.buf, value...)
	return nil
}

func (m *debugMerger) MergeOlder(value []byte) error {
	buf := make([]byte, 0, len(m.buf)+len(value))
	buf = append(buf, value...)
	buf = append(buf, m.buf...)
	m.buf = buf
	return nil
}

func (m *debugMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	if includesBase {
		m.buf = append(m.buf, []byte("[base]")...)
	}
	return m.buf, nil, nil
}

func TestCompactionIter(t *testing.T) {
	var merge Merge
	var keys []InternalKey
	var rangeKeys []keyspan.Span
	var vals [][]byte
	var snapshots []uint64
	var elideTombstones bool
	var allowZeroSeqnum bool
	var interleavingIter *keyspan.InterleavingIter

	// The input to the data-driven test is dependent on the format major
	// version we are testing against.
	fileFunc := func(formatVersion FormatMajorVersion) string {
		if formatVersion < FormatSetWithDelete {
			return "testdata/compaction_iter"
		}
		return "testdata/compaction_iter_set_with_del"
	}

	newIter := func(formatVersion FormatMajorVersion) *compactionIter {
		// To adhere to the existing assumption that range deletion blocks in
		// SSTables are not released while iterating, and therefore not
		// susceptible to use-after-free bugs, we skip the zeroing of
		// RangeDelete keys.
		fi := &fakeIter{keys: keys, vals: vals}
		interleavingIter = &keyspan.InterleavingIter{}
		interleavingIter.Init(
			base.DefaultComparer,
			fi,
			keyspan.NewIter(base.DefaultComparer.Compare, rangeKeys),
			nil, nil, nil)
		iter := newInvalidatingIter(interleavingIter)
		iter.ignoreKind(InternalKeyKindRangeDelete)
		if merge == nil {
			merge = func(key, value []byte) (base.ValueMerger, error) {
				m := &debugMerger{}
				m.buf = append(m.buf, value...)
				return m, nil
			}
		}

		return newCompactionIter(
			DefaultComparer.Compare,
			DefaultComparer.Equal,
			DefaultComparer.FormatKey,
			merge,
			iter,
			snapshots,
			&keyspan.Fragmenter{},
			&keyspan.Fragmenter{},
			allowZeroSeqnum,
			func([]byte) bool {
				return elideTombstones
			},
			func(_, _ []byte) bool {
				return elideTombstones
			},
			formatVersion,
		)
	}

	runTest := func(t *testing.T, formatVersion FormatMajorVersion) {
		datadriven.RunTest(t, fileFunc(formatVersion), func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "define":
				merge = nil
				if len(d.CmdArgs) > 0 && d.CmdArgs[0].Key == "merger" &&
					len(d.CmdArgs[0].Vals) > 0 && d.CmdArgs[0].Vals[0] == "deletable" {
					merge = newDeletableSumValueMerger
				}
				keys = keys[:0]
				vals = vals[:0]
				rangeKeys = rangeKeys[:0]
				for _, key := range strings.Split(d.Input, "\n") {
					j := strings.Index(key, ":")
					keys = append(keys, base.ParseInternalKey(key[:j]))
					vals = append(vals, []byte(key[j+1:]))
				}
				return ""

			case "define-range-keys":
				for _, key := range strings.Split(d.Input, "\n") {
					s := keyspan.ParseSpan(strings.TrimSpace(key))
					rangeKeys = append(rangeKeys, s)
				}
				return ""

			case "iter":
				snapshots = snapshots[:0]
				elideTombstones = false
				allowZeroSeqnum = false
				for _, arg := range d.CmdArgs {
					switch arg.Key {
					case "snapshots":
						for _, val := range arg.Vals {
							seqNum, err := strconv.Atoi(val)
							if err != nil {
								return err.Error()
							}
							snapshots = append(snapshots, uint64(seqNum))
						}
					case "elide-tombstones":
						var err error
						elideTombstones, err = strconv.ParseBool(arg.Vals[0])
						if err != nil {
							return err.Error()
						}
					case "allow-zero-seqnum":
						var err error
						allowZeroSeqnum, err = strconv.ParseBool(arg.Vals[0])
						if err != nil {
							return err.Error()
						}
					default:
						return fmt.Sprintf("%s: unknown arg: %s", d.Cmd, arg.Key)
					}
				}
				sort.Slice(snapshots, func(i, j int) bool {
					return snapshots[i] < snapshots[j]
				})

				iter := newIter(formatVersion)
				var b bytes.Buffer
				for _, line := range strings.Split(d.Input, "\n") {
					parts := strings.Fields(line)
					if len(parts) == 0 {
						continue
					}
					switch parts[0] {
					case "first":
						iter.First()
					case "next":
						iter.Next()
					case "tombstones":
						var key []byte
						if len(parts) == 2 {
							key = []byte(parts[1])
						}
						for _, v := range iter.Tombstones(key) {
							for _, k := range v.Keys {
								fmt.Fprintf(&b, "%s-%s#%d\n", v.Start, v.End, k.SeqNum())
							}
						}
						fmt.Fprintf(&b, ".\n")
						continue
					case "range-keys":
						var key []byte
						if len(parts) == 2 {
							key = []byte(parts[1])
						}
						for _, v := range iter.RangeKeys(key) {
							fmt.Fprintf(&b, "%s\n", v)
						}
						fmt.Fprintf(&b, ".\n")
						continue
					default:
						return fmt.Sprintf("unknown op: %s", parts[0])
					}
					if iter.Valid() {
						fmt.Fprintf(&b, "%s:%s\n", iter.Key(), iter.Value())
						if iter.Key().Kind() == InternalKeyKindRangeDelete {
							iter.rangeDelFrag.Add(keyspan.Span{
								Start: append([]byte{}, iter.Key().UserKey...),
								End:   append([]byte{}, iter.Value()...),
								Keys: []keyspan.Key{
									{Trailer: iter.Key().Trailer},
								},
							})
						}
						if rangekey.IsRangeKey(iter.Key().Kind()) {
							iter.rangeKeyFrag.Add(*interleavingIter.Span())
						}
					} else if err := iter.Error(); err != nil {
						fmt.Fprintf(&b, "err=%v\n", err)
					} else {
						fmt.Fprintf(&b, ".\n")
					}
				}
				return b.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	}

	// Rather than testing against all format version, we test against the
	// significant boundaries.
	formatVersions := []FormatMajorVersion{
		FormatMostCompatible,
		FormatSetWithDelete - 1,
		FormatSetWithDelete,
		FormatNewest,
	}
	for _, formatVersion := range formatVersions {
		t.Run(fmt.Sprintf("version-%s", formatVersion), func(t *testing.T) {
			runTest(t, formatVersion)
		})
	}
}
