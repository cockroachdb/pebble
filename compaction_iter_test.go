// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invalidating"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/rangekey"
	"github.com/stretchr/testify/require"
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
	var kvs []base.InternalKV
	var rangeKeys []keyspan.Span
	var rangeDels []keyspan.Span
	var snapshots []uint64
	var elideTombstones bool
	var allowZeroSeqnum bool
	var rangeKeyInterleaving *keyspan.InterleavingIter
	var rangeDelInterleaving *keyspan.InterleavingIter

	// The input to the data-driven test is dependent on the format major
	// version we are testing against.
	fileFunc := func(formatVersion FormatMajorVersion) string {
		if formatVersion < FormatDeleteSizedAndObsolete {
			return "testdata/compaction_iter_set_with_del"
		}
		return "testdata/compaction_iter_delete_sized"
	}

	var ineffectualSingleDeleteKeys []string
	var invariantViolationSingleDeleteKeys []string
	resetSingleDelStats := func() {
		ineffectualSingleDeleteKeys = ineffectualSingleDeleteKeys[:0]
		invariantViolationSingleDeleteKeys = invariantViolationSingleDeleteKeys[:0]
	}
	newIter := func(formatVersion FormatMajorVersion) *compactionIter {
		// To adhere to the existing assumption that range deletion blocks in
		// SSTables are not released while iterating, and therefore not
		// susceptible to use-after-free bugs, we skip the zeroing of
		// RangeDelete keys.
		fi := base.NewFakeIter(kvs)
		rangeDelInterleaving = &keyspan.InterleavingIter{}
		rangeDelInterleaving.Init(
			base.DefaultComparer,
			fi,
			keyspan.NewIter(base.DefaultComparer.Compare, rangeDels),
			keyspan.InterleavingIterOpts{})
		rangeKeyInterleaving = &keyspan.InterleavingIter{}
		rangeKeyInterleaving.Init(
			base.DefaultComparer,
			rangeDelInterleaving,
			keyspan.NewIter(base.DefaultComparer.Compare, rangeKeys),
			keyspan.InterleavingIterOpts{})
		iter := invalidating.NewIter(rangeKeyInterleaving, invalidating.IgnoreKinds(InternalKeyKindRangeDelete))
		if merge == nil {
			merge = func(key, value []byte) (base.ValueMerger, error) {
				m := &debugMerger{}
				m.buf = append(m.buf, value...)
				return m, nil
			}
		}
		resetSingleDelStats()
		return newCompactionIter(
			DefaultComparer.Compare,
			DefaultComparer.Equal,
			merge,
			iter,
			snapshots,
			allowZeroSeqnum,
			func([]byte) bool {
				return elideTombstones
			},
			func(_, _ []byte) bool {
				return elideTombstones
			},
			func(userKey []byte) {
				ineffectualSingleDeleteKeys = append(ineffectualSingleDeleteKeys, string(userKey))
			},
			func(userKey []byte) {
				invariantViolationSingleDeleteKeys = append(invariantViolationSingleDeleteKeys, string(userKey))
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
					merge = base.NewDeletableSumValueMerger
				}
				kvs = kvs[:0]
				rangeKeys = rangeKeys[:0]
				rangeDels = rangeDels[:0]
				rangeDelFragmenter := keyspan.Fragmenter{
					Cmp:    DefaultComparer.Compare,
					Format: DefaultComparer.FormatKey,
					Emit: func(s keyspan.Span) {
						rangeDels = append(rangeDels, s)
					},
				}
				for _, key := range strings.Split(d.Input, "\n") {
					// If the line ends in a '}' assume it's a span.
					if strings.HasSuffix(key, "}") {
						s := keyspan.ParseSpan(strings.TrimSpace(key))
						rangeKeys = append(rangeKeys, s)
						continue
					}

					j := strings.Index(key, ":")
					ik := base.ParseInternalKey(key[:j])
					if rangekey.IsRangeKey(ik.Kind()) {
						panic("range keys must be pre-fragmented and formatted as spans")
					}
					if ik.Kind() == base.InternalKeyKindRangeDelete {
						rangeDelFragmenter.Add(keyspan.Span{
							Start: ik.UserKey,
							End:   []byte(key[j+1:]),
							Keys:  []keyspan.Key{{Trailer: ik.Trailer}},
						})
						continue
					}

					var value []byte
					if strings.HasPrefix(key[j+1:], "varint(") {
						valueStr := strings.TrimSuffix(strings.TrimPrefix(key[j+1:], "varint("), ")")
						v, err := strconv.ParseUint(valueStr, 10, 64)
						require.NoError(t, err)
						value = binary.AppendUvarint([]byte(nil), v)
					} else {
						value = []byte(key[j+1:])
					}
					kvs = append(kvs, base.MakeInternalKV(ik, value))
				}
				rangeDelFragmenter.Finish()
				return ""

			case "iter":
				snapshots = snapshots[:0]
				elideTombstones = false
				allowZeroSeqnum = false
				printSnapshotPinned := false
				printMissizedDels := false
				printForceObsolete := false
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
					case "print-snapshot-pinned":
						printSnapshotPinned = true
					case "print-missized-dels":
						printMissizedDels = true
					case "print-force-obsolete":
						printForceObsolete = true
					default:
						return fmt.Sprintf("%s: unknown arg: %s", d.Cmd, arg.Key)
					}
				}
				slices.Sort(snapshots)

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
						for _, v := range iter.TombstonesUpTo(key) {
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
						for _, v := range iter.RangeKeysUpTo(key) {
							fmt.Fprintf(&b, "%s\n", v)
						}
						fmt.Fprintf(&b, ".\n")
						continue
					default:
						return fmt.Sprintf("unknown op: %s", parts[0])
					}
					if iter.Valid() {
						snapshotPinned := ""
						if printSnapshotPinned {
							snapshotPinned = " (not pinned)"
							if iter.snapshotPinned {
								snapshotPinned = " (pinned)"
							}
						}
						forceObsolete := ""
						if printForceObsolete {
							forceObsolete = " (not force obsolete)"
							if iter.forceObsoleteDueToRangeDel {
								forceObsolete = " (force obsolete)"
							}
						}
						v := string(iter.Value())
						if iter.Key().Kind() == base.InternalKeyKindDeleteSized && len(iter.Value()) > 0 {
							vn, n := binary.Uvarint(iter.Value())
							if n != len(iter.Value()) {
								v = fmt.Sprintf("err: %0x value not a uvarint", iter.Value())
							} else {
								v = fmt.Sprintf("varint(%d)", vn)
							}
						}
						fmt.Fprintf(&b, "%s:%s%s%s", iter.Key(), v, snapshotPinned, forceObsolete)
						if iter.Key().Kind() == InternalKeyKindRangeDelete {
							iter.AddTombstoneSpan(rangeDelInterleaving.Span())
							fmt.Fprintf(&b, "; Span() = %s", *rangeDelInterleaving.Span())
						}
						if rangekey.IsRangeKey(iter.Key().Kind()) {
							iter.AddRangeKeySpan(rangeKeyInterleaving.Span())
						}
						fmt.Fprintln(&b)
					} else if err := iter.Error(); err != nil {
						fmt.Fprintf(&b, "err=%v\n", err)
					} else {
						fmt.Fprintf(&b, ".\n")
					}
				}
				if printMissizedDels {
					fmt.Fprintf(&b, "missized-dels=%d\n", iter.stats.countMissizedDels)
				}
				if len(ineffectualSingleDeleteKeys) > 0 {
					fmt.Fprintf(&b, "ineffectual-single-deletes: %s\n",
						strings.Join(ineffectualSingleDeleteKeys, ","))
				}
				if len(invariantViolationSingleDeleteKeys) > 0 {
					fmt.Fprintf(&b, "invariant-violation-single-deletes: %s\n",
						strings.Join(invariantViolationSingleDeleteKeys, ","))
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
		FormatMinSupported,
		internalFormatNewest,
	}
	for _, formatVersion := range formatVersions {
		t.Run(fmt.Sprintf("version-%s", formatVersion), func(t *testing.T) {
			runTest(t, formatVersion)
		})
	}
}
