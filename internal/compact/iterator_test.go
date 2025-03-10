// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compact

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"testing"
	"unicode"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/rangekey"
	"github.com/cockroachdb/pebble/sstable/valblk"
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
	var allowZeroSeqnum bool
	var preserveBlobRefs bool
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
			Comparer:               base.DefaultComparer,
			Merge:                  merge,
			Snapshots:              snapshots,
			TombstoneElision:       elision,
			RangeKeyElision:        elision,
			AllowZeroSeqNum:        allowZeroSeqnum,
			PreserveBlobReferences: preserveBlobRefs,
			IneffectualSingleDeleteCallback: func(userKey []byte) {
				ineffectualSingleDeleteKeys = append(ineffectualSingleDeleteKeys, string(userKey))
			},
			NondeterministicSingleDeleteCallback: func(userKey []byte) {
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

					var iv base.InternalValue
					if strings.HasPrefix(key[j+1:], "varint(") {
						valueStr := strings.TrimSuffix(strings.TrimPrefix(key[j+1:], "varint("), ")")
						v, err := strconv.ParseUint(valueStr, 10, 64)
						require.NoError(t, err)
						iv = base.MakeInPlaceValue(binary.AppendUvarint([]byte(nil), v))
					} else if strings.HasPrefix(key[j+1:], "blobref(") {
						iv = decodeBlobReference(t, key[j+1:])
					} else {
						iv = base.MakeInPlaceValue([]byte(key[j+1:]))
					}
					kvs = append(kvs, base.InternalKV{K: ik, V: iv})
				}
				rangeDelFragmenter.Finish()
				return ""

			case "iter":
				snapshots = snapshots[:0]
				elideTombstones = false
				allowZeroSeqnum = false
				preserveBlobRefs = false
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
					case "preserve-blob-refs":
						preserveBlobRefs = true
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
					var kv *base.InternalKV
					switch parts[0] {
					case "first":
						kv = iter.First()
					case "next":
						kv = iter.Next()
					default:
						d.Fatalf(t, "unknown iter command: %s", parts[0])
					}
					var value []byte
					if kv != nil {
						if kv.V.IsBlobValueHandle() {
							lv := kv.V.LazyValue()
							value = []byte(fmt.Sprintf("<blobref(%s, encodedHandle=%x, valLen=%d)>",
								lv.Fetcher.BlobFileNum, lv.ValueOrHandle, lv.Fetcher.Attribute.ValueLen))
						} else {
							var err error
							value, _, err = kv.Value(nil)
							require.NoError(t, err)
						}
					}

					if kv != nil {
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
						if kv.K.Kind() == base.InternalKeyKindDeleteSized && len(value) > 0 {
							vn, n := binary.Uvarint(value)
							if n != len(value) {
								v = fmt.Sprintf("err: %0x value not a uvarint", value)
							} else {
								v = fmt.Sprintf("varint(%d)", vn)
							}
						}
						fmt.Fprintf(&b, "%s:%s%s%s", kv.K, v, snapshotPinned, forceObsolete)
						if kv.K.Kind() == base.InternalKeyKindRangeDelete || rangekey.IsRangeKey(kv.K.Kind()) {
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

type mockBlobValueFetcher struct{}

var _ base.ValueFetcher = mockBlobValueFetcher{}

func (mockBlobValueFetcher) Fetch(
	ctx context.Context, handle []byte, fileNum base.DiskFileNum, valLen uint32, buf []byte,
) (val []byte, callerOwned bool, err error) {
	s := fmt.Sprintf("<a fetched value from blobref(%s, encodedHandle=%x, valLen=%d)>",
		fileNum, handle, valLen)
	return []byte(s), false, nil
}

// decodeBlobReference decodes a blob reference from a debug string. It expects
// a value of the form: blobref(<filenum>, blk<blocknum>, <offset>, <valLen>).
// For example: blobref(000124, blk255, 10, 9235)
func decodeBlobReference(t testing.TB, ref string) base.InternalValue {
	fields := strings.FieldsFunc(strings.TrimSuffix(strings.TrimPrefix(ref, "blobref("), ")"),
		func(r rune) bool { return r == ',' || unicode.IsSpace(r) })
	require.Equal(t, 4, len(fields))
	fileNum, err := strconv.ParseUint(fields[0], 10, 64)
	require.NoError(t, err)
	blockNum, err := strconv.ParseUint(strings.TrimPrefix(fields[1], "blk"), 10, 32)
	require.NoError(t, err)
	off, err := strconv.ParseUint(fields[2], 10, 32)
	require.NoError(t, err)
	valLen, err := strconv.ParseUint(fields[3], 10, 32)
	require.NoError(t, err)

	// TODO(jackson): Support short (and long, when introduced) attributes.
	return base.MakeLazyValue(base.LazyValue{
		ValueOrHandle: encodeRemainingHandle(uint32(blockNum), uint32(off)),
		Fetcher: &base.LazyFetcher{
			Fetcher:     mockBlobValueFetcher{},
			BlobFileNum: base.DiskFileNum(fileNum),
			Attribute: base.AttributeAndLen{
				ValueLen: uint32(valLen),
			},
		},
	})
}

func encodeRemainingHandle(blockNum uint32, offsetInBlock uint32) []byte {
	// TODO(jackson): Pull this into a common helper.
	dst := make([]byte, valblk.HandleMaxLen)
	n := valblk.EncodeHandle(dst, valblk.Handle{
		ValueLen:      0,
		BlockNum:      blockNum,
		OffsetInBlock: offsetInBlock,
	})
	return dst[1:n]
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
