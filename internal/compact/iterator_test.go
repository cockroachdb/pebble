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

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/rangekey"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/valblk"
	"github.com/stretchr/testify/require"
)

var errDebugFinish = errors.New("debugMerger: finish error")

type debugCloser struct {
	closed   bool
	closeErr error
}

func (c *debugCloser) Close() error {
	if c.closed {
		panic("debugCloser closed twice")
	}
	c.closed = true
	return c.closeErr
}

type debugMerger struct {
	buf        []byte
	failFinish bool
	closer     *debugCloser
	closers    *[]*debugCloser
}

// checkOperand inspects value for '!' failure prefixes used in compaction
// iterator tests. If the operand triggers an immediate error at the given stage
// (init=true for Merge, init=false for MergeOlder), it returns that error.
func (m *debugMerger) checkOperand(value []byte, init bool) error {
	switch {
	case !bytes.HasPrefix(value, []byte("!")):
		return nil
	case bytes.HasPrefix(value, []byte("!finish-close-err")):
		m.failFinish = true
		m.closer = &debugCloser{closeErr: errors.New("debugMerger: close error")}
		return nil
	case bytes.HasPrefix(value, []byte("!finish-close-ok")):
		m.failFinish = true
		m.closer = &debugCloser{}
		return nil
	case bytes.HasPrefix(value, []byte("!finish")):
		m.failFinish = true
		return nil
	case bytes.HasPrefix(value, []byte("!corrupt")):
		return base.MarkCorruptionError(errors.New("debugMerger: corrupt merge error"))
	case bytes.HasPrefix(value, []byte("!init")):
		if init {
			return errors.New("debugMerger: merge init error")
		}
		return nil
	case bytes.HasPrefix(value, []byte("!older")):
		if !init {
			return errors.New("debugMerger: merge older error")
		}
		return nil
	default:
		if init {
			return errors.New("debugMerger: merge init error")
		}
		return errors.New("debugMerger: merge older error")
	}
}

func (m *debugMerger) MergeNewer(value []byte) error {
	panic("unimplemented: compact.Iter never calls MergeNewer")
}

func (m *debugMerger) MergeOlder(value []byte) error {
	if err := m.checkOperand(value, false /* init */); err != nil {
		return err
	}
	buf := make([]byte, 0, len(m.buf)+len(value))
	buf = append(buf, value...)
	buf = append(buf, m.buf...)
	m.buf = buf
	return nil
}

func (m *debugMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	var closer io.Closer
	if m.closer != nil {
		if m.closers != nil {
			*m.closers = append(*m.closers, m.closer)
		}
		closer = m.closer
	}
	if m.failFinish {
		return nil, closer, errDebugFinish
	}
	if includesBase {
		m.buf = append(m.buf, []byte("[base]")...)
	}
	return m.buf, closer, nil
}

func newDebugMerge(closers *[]*debugCloser) base.Merge {
	return func(key, value []byte) (base.ValueMerger, error) {
		m := &debugMerger{closers: closers}
		if err := m.checkOperand(value, true /* init */); err != nil {
			return nil, err
		}
		m.buf = append(m.buf, value...)
		return m, nil
	}
}

func debugMerge(key, value []byte) (base.ValueMerger, error) {
	return newDebugMerge(nil)(key, value)
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
	var missizedDeleteInfo []string
	var mergeFailureInfo []string
	var closers []*debugCloser
	resetSingleDelStats := func() {
		ineffectualSingleDeleteKeys = ineffectualSingleDeleteKeys[:0]
		invariantViolationSingleDeleteKeys = invariantViolationSingleDeleteKeys[:0]
		missizedDeleteInfo = missizedDeleteInfo[:0]
		mergeFailureInfo = mergeFailureInfo[:0]
		closers = closers[:0]
	}
	newIter := func() *Iter {
		resetSingleDelStats()
		if merge == nil {
			merge = newDebugMerge(&closers)
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
			NondeterministicSingleDeleteCallback: func(userKey []byte) {
				invariantViolationSingleDeleteKeys = append(invariantViolationSingleDeleteKeys, string(userKey))
			},
			MissizedDeleteCallback: func(userKey []byte, elidedSize, expectedSize uint64) {
				info := fmt.Sprintf("%s (elided=%d, expected=%d)", string(userKey), elidedSize, expectedSize)
				missizedDeleteInfo = append(missizedDeleteInfo, info)
			},
			MergeFailureCallback: func(userKey []byte, err error) {
				mergeFailureInfo = append(mergeFailureInfo, fmt.Sprintf("%s (%v)", string(userKey), err))
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

				parsed, err := sstable.ParseTestKVsAndSpans(d.Input, nil /* bv */)
				require.NoError(t, err)

				rangeDelFragmenter := keyspan.Fragmenter{
					Cmp:    base.DefaultComparer.Compare,
					Format: base.DefaultComparer.FormatKey,
					Emit: func(s keyspan.Span) {
						rangeDels = append(rangeDels, s)
					},
				}

				for _, kv := range parsed {
					if kv.IsKeySpan() {
						// Pre-fragmented range key span
						if rangekey.IsRangeKey(kv.Span.Keys[0].Kind()) {
							rangeKeys = append(rangeKeys, *kv.Span)
						} else {
							// Range delete - add to fragmenter.
							rangeDelFragmenter.Add(*kv.Span)
						}
						continue
					}

					// Point key - check for range keys and range deletes
					if rangekey.IsRangeKey(kv.Key.Kind()) {
						panic("range keys must be pre-fragmented and formatted as spans")
					}

					if kv.Key.Kind() == base.InternalKeyKindRangeDelete {
						// Range delete in old format: key#seq,RANGEDEL = endKey
						rangeDelFragmenter.Add(keyspan.Span{
							Start: kv.Key.UserKey,
							End:   kv.Value,
							Keys:  []keyspan.Key{{Trailer: kv.Key.Trailer}},
						})
						continue
					}

					var iv base.InternalValue
					valueStr := string(kv.Value)
					if strings.HasPrefix(valueStr, "varint(") {
						v, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(valueStr, "varint("), ")"), 10, 64)
						require.NoError(t, err)
						iv = base.MakeInPlaceValue(binary.AppendUvarint([]byte(nil), v))
					} else if strings.HasPrefix(valueStr, "blobref(") {
						iv = decodeBlobReference(t, valueStr)
					} else {
						iv = base.MakeInPlaceValue(kv.Value)
					}
					kvs = append(kvs, base.InternalKV{K: kv.Key, V: iv})
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
				printMissizedDelInfo := false
				printMergeFailures := false
				printMergeFailureInfo := false
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
					case "print-missized-del-info":
						printMissizedDelInfo = true
					case "print-force-obsolete":
						printForceObsolete = true
					case "print-merge-failures":
						printMergeFailures = true
					case "print-merge-failure-info":
						printMergeFailureInfo = true
					default:
						return fmt.Sprintf("%s: unknown arg: %s", d.Cmd, arg.Key)
					}
				}
				slices.Sort(snapshots)

				iter := newIter()
				var b bytes.Buffer
				for line := range crstrings.LinesSeq(d.Input) {
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
								lv.Fetcher.BlobFileID, lv.ValueOrHandle, lv.Fetcher.Attribute.ValueLen))
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
				for _, c := range closers {
					require.True(t, c.closed, "debugCloser was not closed")
					if c.closeErr != nil {
						require.ErrorIs(t, iter.Error(), errDebugFinish)
						require.Contains(t, fmt.Sprintf("%+v", iter.Error()), "closing merge value closer: "+c.closeErr.Error())
					}
				}
				if printMissizedDels {
					fmt.Fprintf(&b, "missized-dels=%d\n", iter.stats.CountMissizedDels)
				}
				if printMergeFailures {
					fmt.Fprintf(&b, "merge-failures=%d\n", iter.stats.CountMergeFailures)
				}
				if len(ineffectualSingleDeleteKeys) > 0 {
					fmt.Fprintf(&b, "ineffectual-single-deletes: %s\n",
						strings.Join(ineffectualSingleDeleteKeys, ","))
				}
				if len(invariantViolationSingleDeleteKeys) > 0 {
					fmt.Fprintf(&b, "invariant-violation-single-deletes: %s\n",
						strings.Join(invariantViolationSingleDeleteKeys, ","))
				}
				if printMissizedDelInfo && len(missizedDeleteInfo) > 0 {
					fmt.Fprintf(&b, "missized-delete-info: %s\n",
						strings.Join(missizedDeleteInfo, "; "))
				}
				if printMergeFailureInfo && len(mergeFailureInfo) > 0 {
					fmt.Fprintf(&b, "merge-failure-info: %s\n",
						strings.Join(mergeFailureInfo, "; "))
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

// mockBlobValueFetcher is a dummy ValueFetcher implementation which produces
// string values referencing the blobref.
type mockBlobValueFetcher struct{}

var _ base.ValueFetcher = mockBlobValueFetcher{}

func (mockBlobValueFetcher) FetchHandle(
	ctx context.Context, handle []byte, blobFileID base.BlobFileID, valLen uint32, buf []byte,
) (val []byte, callerOwned bool, err error) {
	s := fmt.Sprintf("<fetched value from blobref(%s, encodedHandle=%x, valLen=%d)>",
		blobFileID, handle, valLen)
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
			Fetcher:    mockBlobValueFetcher{},
			BlobFileID: base.BlobFileID(fileNum),
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

type fakeMetaIter struct {
	base.InternalIterator
	metas []base.KVMeta
	idx   int
}

var _ base.InternalIteratorWithKVMeta = (*fakeMetaIter)(nil)

func (f *fakeMetaIter) FirstWithMeta() (*base.InternalKV, base.KVMeta) {
	f.idx = 0
	kv := f.InternalIterator.First()
	if kv == nil {
		return nil, base.KVMeta{}
	}
	return kv, f.metas[f.idx]
}

func (f *fakeMetaIter) NextWithMeta() (*base.InternalKV, base.KVMeta) {
	f.idx++
	kv := f.InternalIterator.Next()
	if kv == nil {
		return nil, base.KVMeta{}
	}
	return kv, f.metas[f.idx]
}

func (f *fakeMetaIter) SeekGEWithMeta(
	key []byte, flags base.SeekGEFlags,
) (*base.InternalKV, base.KVMeta) {
	panic("unimplemented")
}

// TestCompactionIterMergeKVMetaAndBufferCap verifies that:
//  1. KVMeta from an InternalIteratorWithKVMeta is preserved for each
//     uncombined MERGE operand emitted on merge failure as well as for any
//     trailing SET and for healthy merges.
//  2. A healthy merge whose operands exceed maxMergeBufBytes still succeeds,
//     while a failing merge whose buffered operands (values plus per-operand
//     savedMergeOpSize overhead) exceed maxMergeBufBytes falls back to failing
//     the compaction with a corruption error.
func TestCompactionIterMergeKVMetaAndBufferCap(t *testing.T) {
	largeVal := bytes.Repeat([]byte("x"), maxMergeBufBytes+1024)
	inputs := []struct {
		kv   base.InternalKV
		meta base.KVMeta
	}{
		{
			kv:   base.MakeInternalKV(base.ParseInternalKey("a#5,MERGE"), []byte("a5")),
			meta: base.KVMeta{TieringSpanID: 10, TieringAttribute: 1},
		},
		{
			kv:   base.MakeInternalKV(base.ParseInternalKey("a#4,MERGE"), []byte("!older")),
			meta: base.KVMeta{TieringSpanID: 20, TieringAttribute: 2},
		},
		{
			kv:   base.MakeInternalKV(base.ParseInternalKey("a#3,SET"), []byte("a3")),
			meta: base.KVMeta{TieringSpanID: 30, TieringAttribute: 3},
		},
		{
			kv:   base.MakeInternalKV(base.ParseInternalKey("b#2,MERGE"), largeVal),
			meta: base.KVMeta{TieringSpanID: 40, TieringAttribute: 4},
		},
		{
			kv:   base.MakeInternalKV(base.ParseInternalKey("b#1,SET"), []byte("b1")),
			meta: base.KVMeta{TieringSpanID: 50, TieringAttribute: 5},
		},
		{
			kv:   base.MakeInternalKV(base.ParseInternalKey("c#2,MERGE"), []byte("c2")),
			meta: base.KVMeta{TieringSpanID: 60, TieringAttribute: 6},
		},
		{
			kv:   base.MakeInternalKV(base.ParseInternalKey("c#1,SET"), []byte("c1")),
			meta: base.KVMeta{TieringSpanID: 70, TieringAttribute: 7},
		},
	}
	var kvs []base.InternalKV
	var metas []base.KVMeta
	for _, in := range inputs {
		kvs = append(kvs, in.kv)
		metas = append(metas, in.meta)
	}
	pointIter := &fakeMetaIter{
		InternalIterator: base.NewFakeIter(kvs),
		metas:            metas,
	}
	iter := NewIter(IterConfig{
		Comparer:         base.DefaultComparer,
		Merge:            debugMerge,
		TombstoneElision: NoTombstoneElision(),
		RangeKeyElision:  NoTombstoneElision(),
	}, pointIter, nil, nil)
	defer func() { require.NoError(t, iter.Close()) }()

	var gotKeys []string
	var gotMetas []base.KVMeta
	for kv := iter.First(); kv != nil; kv = iter.Next() {
		gotKeys = append(gotKeys, kv.K.String())
		gotMetas = append(gotMetas, iter.GetCurrentMeta())
	}
	require.NoError(t, iter.Error())
	require.Equal(t, []string{"a#5,MERGE", "a#4,MERGE", "a#3,SET", "b#2,SET", "c#2,SET"}, gotKeys)
	require.Equal(t, []base.KVMeta{metas[0], metas[1], metas[2], metas[3], metas[5]}, gotMetas)
	require.Equal(t, uint64(1), iter.Stats().CountMergeFailures)

	// When a stripe's MERGE operands exceed maxMergeBufBytes and the Merger
	// then fails, the iterator cannot emit the uncombined operands and falls
	// back to failing the compaction with a corruption error.
	overflowFailKVs := []base.InternalKV{
		base.MakeInternalKV(base.ParseInternalKey("d#3,MERGE"), largeVal),
		base.MakeInternalKV(base.ParseInternalKey("d#2,MERGE"), []byte("!older")),
		base.MakeInternalKV(base.ParseInternalKey("d#1,SET"), []byte("d1")),
	}
	overflowIter := NewIter(IterConfig{
		Comparer:         base.DefaultComparer,
		Merge:            debugMerge,
		TombstoneElision: NoTombstoneElision(),
		RangeKeyElision:  NoTombstoneElision(),
	}, base.NewFakeIter(overflowFailKVs), nil, nil)
	require.Nil(t, overflowIter.First())
	require.Error(t, overflowIter.Error())
	require.True(t, base.IsCorruptionError(overflowIter.Error()))
	require.Contains(t, overflowIter.Error().Error(), "exceeded 1048576 byte buffer limit")
	require.Zero(t, overflowIter.Stats().CountMergeFailures)
	require.Error(t, overflowIter.Close())

	// The per-operand savedMergeOpSize overhead counts against
	// maxMergeBufBytes, so a failing merge over many empty MERGE operands is
	// contained only while the operands fit in the buffer.
	failingOlder := []byte("!older")
	runManyOps := func(n int) *Iter {
		kvs := make([]base.InternalKV, 0, n+1)
		for j := 0; j < n; j++ {
			value := []byte{}
			if j == n-1 {
				value = failingOlder
			}
			kvs = append(kvs, base.MakeInternalKV(
				base.MakeInternalKey([]byte("e"), base.SeqNum(n+1-j), base.InternalKeyKindMerge), value))
		}
		kvs = append(kvs, base.MakeInternalKV(
			base.MakeInternalKey([]byte("e"), base.SeqNum(1), base.InternalKeyKindSet), []byte("e1")))
		return NewIter(IterConfig{
			Comparer:         base.DefaultComparer,
			Merge:            debugMerge,
			TombstoneElision: NoTombstoneElision(),
			RangeKeyElision:  NoTombstoneElision(),
		}, base.NewFakeIter(kvs), nil, nil)
	}

	// The largest operand count that fits is contained: every MERGE operand
	// and the trailing SET are emitted uncombined.
	maxFitOps := (maxMergeBufBytes - len(failingOlder)) / savedMergeOpSize
	fitIter := runManyOps(maxFitOps)
	var fitCount int
	for kv := fitIter.First(); kv != nil; kv = fitIter.Next() {
		fitCount++
	}
	require.NoError(t, fitIter.Error())
	require.Equal(t, maxFitOps+1, fitCount)
	require.Equal(t, uint64(1), fitIter.Stats().CountMergeFailures)
	require.NoError(t, fitIter.Close())

	// One more operand overflows the buffer even though the operand values
	// total only len(failingOlder) bytes, so the merge failure fails the
	// compaction.
	manyOpsIter := runManyOps(maxMergeBufBytes/savedMergeOpSize + 1)
	require.Nil(t, manyOpsIter.First())
	require.True(t, base.IsCorruptionError(manyOpsIter.Error()))
	require.Contains(t, manyOpsIter.Error().Error(), "exceeded 1048576 byte buffer limit")
	require.Zero(t, manyOpsIter.Stats().CountMergeFailures)
	require.Error(t, manyOpsIter.Close())
}
