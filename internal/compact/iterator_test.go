// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compact

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
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/rangekey"
	"github.com/stretchr/testify/require"
)

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
	var merge base.Merge
	var kvs []base.InternalKV
	var rangeKeys []keyspan.Span
	var rangeDels []keyspan.Span
	var snapshots Snapshots
	var elideTombstones bool
	var isBottommostDataLayer bool
	var ineffectualSingleDeleteKeys []string
	var invariantViolationSingleDeleteKeys []string
	resetSingleDelStats := func() {
		ineffectualSingleDeleteKeys = ineffectualSingleDeleteKeys[:0]
		invariantViolationSingleDeleteKeys = invariantViolationSingleDeleteKeys[:0]
	}
	newIter := func() *Iter {
		resetSingleDelStats()
		if merge == nil {
			merge = func(key, value []byte) (base.ValueMerger, error) {
				m := &debugMerger{}
				m.buf = append(m.buf, value...)
				return m, nil
			}
		}
		elision := NoTombstoneElision()
		if elideTombstones {
			// Elide everything.
			elision = ElideTombstonesOutsideOf(nil)
		}
		cfg := IterConfig{
			Comparer:              base.DefaultComparer,
			Merge:                 merge,
			Snapshots:             snapshots,
			TombstoneElision:      elision,
			RangeKeyElision:       elision,
			IsBottommostDataLayer: isBottommostDataLayer,
			IneffectualSingleDeleteCallback: func(userKey []byte) {
				ineffectualSingleDeleteKeys = append(ineffectualSingleDeleteKeys, string(userKey))
			},
			SingleDeleteInvariantViolationCallback: func(userKey []byte) {
				invariantViolationSingleDeleteKeys = append(invariantViolationSingleDeleteKeys, string(userKey))
			},
		}
		pointIter, rangeDelIter, rangeKeyIter := makeInputIters(kvs, rangeDels, rangeKeys)
		return NewIter(cfg, pointIter, rangeDelIter, rangeKeyIter)
	}

	runTest := func(t *testing.T, file string) {
		datadriven.RunTest(t, file, func(t *testing.T, d *datadriven.TestData) string {
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
					Cmp:    base.DefaultComparer.Compare,
					Format: base.DefaultComparer.FormatKey,
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
				isBottommostDataLayer = false
				printSnapshotPinned := false
				printMissizedDels := false
				printForceObsolete := false
				for _, arg := range d.CmdArgs {
					switch arg.Key {
					case "snapshots":
						for _, val := range arg.Vals {
							snapshots = append(snapshots, base.ParseSeqNum(val))
						}
					case "elide-tombstones":
						var err error
						elideTombstones, err = strconv.ParseBool(arg.Vals[0])
						if err != nil {
							return err.Error()
						}
					case "is-bottommost-layer":
						var err error
						isBottommostDataLayer, err = strconv.ParseBool(arg.Vals[0])
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

				iter := newIter()
				var b bytes.Buffer
				for _, line := range strings.Split(d.Input, "\n") {
					parts := strings.Fields(line)
					if len(parts) == 0 {
						continue
					}
					var key *base.InternalKey
					var value []byte
					switch parts[0] {
					case "first":
						key, value = iter.First()
					case "next":
						key, value = iter.Next()
					default:
						d.Fatalf(t, "unknown iter command: %s", parts[0])
					}
					if key != nil {
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
						v := string(value)
						if key.Kind() == base.InternalKeyKindDeleteSized && len(value) > 0 {
							vn, n := binary.Uvarint(value)
							if n != len(value) {
								v = fmt.Sprintf("err: %0x value not a uvarint", value)
							} else {
								v = fmt.Sprintf("varint(%d)", vn)
							}
						}
						fmt.Fprintf(&b, "%s:%s%s%s", key, v, snapshotPinned, forceObsolete)
						if key.Kind() == base.InternalKeyKindRangeDelete || rangekey.IsRangeKey(key.Kind()) {
							fmt.Fprintf(&b, "; Span() = %s", iter.Span())
						}
						fmt.Fprintln(&b)
					} else if err := iter.Error(); err != nil {
						fmt.Fprintf(&b, "err=%v\n", err)
					} else {
						fmt.Fprintf(&b, ".\n")
					}
				}
				if printMissizedDels {
					fmt.Fprintf(&b, "missized-dels=%d\n", iter.stats.CountMissizedDels)
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
				d.Fatalf(t, "unknown command: %s", d.Cmd)
				return ""
			}
		})
	}

	runTest(t, "testdata/iter")
	runTest(t, "testdata/iter_set_with_del")
	runTest(t, "testdata/iter_delete_sized")
}

// makeInputIters creates the iterators necessthat can be used to create a compaction
// Iter.
func makeInputIters(
	points []base.InternalKV, rangeDels, rangeKeys []keyspan.Span,
) (pointIter base.InternalIterator, rangeDelIter, rangeKeyIter keyspan.FragmentIterator) {
	// To adhere to the existing assumption that range deletion blocks in
	// SSTables are not released while iterating, and therefore not
	// susceptible to use-after-free bugs, we skip the zeroing of
	// RangeDelete keys.
	return base.NewFakeIter(points),
		keyspan.NewIter(base.DefaultComparer.Compare, rangeDels),
		keyspan.NewIter(base.DefaultComparer.Compare, rangeKeys)
}
