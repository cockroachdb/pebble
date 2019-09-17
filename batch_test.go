// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestBatch(t *testing.T) {
	type testCase struct {
		kind       InternalKeyKind
		key, value string
	}

	verifyTestCases := func(b *Batch, testCases []testCase) {
		r := b.Reader()

		for _, tc := range testCases {
			kind, k, v, ok := r.Next()
			if !ok {
				t.Fatalf("next returned !ok: test case = %v", tc)
			}
			key, value := string(k), string(v)
			if kind != tc.kind || key != tc.key || value != tc.value {
				t.Errorf("got (%d, %q, %q), want (%d, %q, %q)",
					kind, key, value, tc.kind, tc.key, tc.value)
			}
		}
		if len(r) != 0 {
			t.Errorf("reader was not exhausted: remaining bytes = %q", r)
		}
	}

	testCases := []testCase{
		{InternalKeyKindSet, "roses", "red"},
		{InternalKeyKindSet, "violets", "blue"},
		{InternalKeyKindDelete, "roses", ""},
		{InternalKeyKindSingleDelete, "roses", ""},
		{InternalKeyKindSet, "", ""},
		{InternalKeyKindSet, "", "non-empty"},
		{InternalKeyKindDelete, "", ""},
		{InternalKeyKindSingleDelete, "", ""},
		{InternalKeyKindSet, "grass", "green"},
		{InternalKeyKindSet, "grass", "greener"},
		{InternalKeyKindSet, "eleventy", strings.Repeat("!!11!", 100)},
		{InternalKeyKindDelete, "nosuchkey", ""},
		{InternalKeyKindSingleDelete, "nosuchkey", ""},
		{InternalKeyKindSet, "binarydata", "\x00"},
		{InternalKeyKindSet, "binarydata", "\xff"},
		{InternalKeyKindMerge, "merge", "mergedata"},
		{InternalKeyKindMerge, "merge", ""},
		{InternalKeyKindMerge, "", ""},
		{InternalKeyKindRangeDelete, "a", "b"},
		{InternalKeyKindRangeDelete, "", ""},
		{InternalKeyKindLogData, "logdata", ""},
		{InternalKeyKindLogData, "", ""},
	}
	var b Batch
	for _, tc := range testCases {
		switch tc.kind {
		case InternalKeyKindSet:
			_ = b.Set([]byte(tc.key), []byte(tc.value), nil)
		case InternalKeyKindMerge:
			_ = b.Merge([]byte(tc.key), []byte(tc.value), nil)
		case InternalKeyKindDelete:
			_ = b.Delete([]byte(tc.key), nil)
		case InternalKeyKindSingleDelete:
			_ = b.SingleDelete([]byte(tc.key), nil)
		case InternalKeyKindRangeDelete:
			_ = b.DeleteRange([]byte(tc.key), []byte(tc.value), nil)
		case InternalKeyKindLogData:
			_ = b.LogData([]byte(tc.key), nil)
		}
	}
	verifyTestCases(&b, testCases)

	b.Reset()
	// Run the same operations, this time using the Deferred variants of each
	// operation (eg. SetDeferred).
	for _, tc := range testCases {
		key := []byte(tc.key)
		value := []byte(tc.value)
		switch tc.kind {
		case InternalKeyKindSet:
			d, _ := b.SetDeferred(len(key), len(value), nil)
			copy(d.Key, key)
			copy(d.Value, value)
			d.Finish()
		case InternalKeyKindMerge:
			d, _ := b.MergeDeferred(len(key), len(value), nil)
			copy(d.Key, key)
			copy(d.Value, value)
			d.Finish()
		case InternalKeyKindDelete:
			d, _ := b.DeleteDeferred(len(key), nil)
			copy(d.Key, key)
			copy(d.Value, value)
			d.Finish()
		case InternalKeyKindSingleDelete:
			d, _ := b.SingleDeleteDeferred(len(key), nil)
			copy(d.Key, key)
			copy(d.Value, value)
			d.Finish()
		case InternalKeyKindRangeDelete:
			d, _ := b.DeleteRangeDeferred(len(key), len(value), nil)
			copy(d.Key, key)
			copy(d.Value, value)
			d.Finish()
		case InternalKeyKindLogData:
			_ = b.LogData([]byte(tc.key), nil)
		}
	}
	verifyTestCases(&b, testCases)
}

func TestBatchEmpty(t *testing.T) {
	var b Batch
	require.True(t, b.Empty())

	b.Set(nil, nil, nil)
	require.False(t, b.Empty())
	b.Reset()
	require.True(t, b.Empty())

	b.Merge(nil, nil, nil)
	require.False(t, b.Empty())
	b.Reset()
	require.True(t, b.Empty())

	b.Delete(nil, nil)
	require.False(t, b.Empty())
	b.Reset()
	require.True(t, b.Empty())

	b.DeleteRange(nil, nil, nil)
	require.False(t, b.Empty())
	b.Reset()
	require.True(t, b.Empty())

	b.LogData(nil, nil)
	require.False(t, b.Empty())
	b.Reset()
	require.True(t, b.Empty())
}

func TestBatchIncrement(t *testing.T) {
	testCases := []uint32{
		0x00000000,
		0x00000001,
		0x00000002,
		0x0000007f,
		0x00000080,
		0x000000fe,
		0x000000ff,
		0x00000100,
		0x00000101,
		0x000001ff,
		0x00000200,
		0x00000fff,
		0x00001234,
		0x0000fffe,
		0x0000ffff,
		0x00010000,
		0x00010001,
		0x000100fe,
		0x000100ff,
		0x00020100,
		0x03fffffe,
		0x03ffffff,
		0x04000000,
		0x04000001,
		0x7fffffff,
		0xfffffffe,
		0xffffffff,
	}
	for _, tc := range testCases {
		var buf [batchHeaderLen]byte
		binary.LittleEndian.PutUint32(buf[8:12], tc)
		var b Batch
		b.SetRepr(buf[:])
		b.increment()
		got := binary.LittleEndian.Uint32(b.Repr()[8:12])
		want := tc + 1
		if tc == 0xffffffff {
			want = tc
		}
		if got != want {
			t.Errorf("input=%d: got %d, want %d", tc, got, want)
		}
	}
}

func TestBatchOpDoesIncrement(t *testing.T) {
	var b Batch
	key := []byte("foo")
	value := []byte("bar")

	if b.Count() != 0 {
		t.Fatalf("new batch has a nonzero count: %d", b.Count())
	}

	// Should increment count by 1
	_ = b.Set(key, value, nil)
	if b.Count() != 1 {
		t.Fatalf("expected count: %d, got %d", 1, b.Count())
	}

	var b2 Batch
	// Should increment count by 1 each
	_ = b2.Set(key, value, nil)
	_ = b2.Delete(key, nil)
	if b2.Count() != 2 {
		t.Fatalf("expected count: %d, got %d", 2, b2.Count())
	}

	// Should increment count by b2.count()
	_ = b.Apply(&b2, nil)
	if b.Count() != 3 {
		t.Fatalf("expected count: %d, got %d", 3, b.Count())
	}

	// Should increment count by 1
	_ = b.Merge(key, value, nil)
	if b.Count() != 4 {
		t.Fatalf("expected count: %d, got %d", 4, b.Count())
	}

	// Should NOT increment count.
	_ = b.LogData([]byte("foobarbaz"), nil)
	if b.Count() != 4 {
		t.Fatalf("expected count: %d, got %d", 4, b.Count())
	}
}

func TestBatchGet(t *testing.T) {
	for _, method := range []string{"build", "apply"} {
		t.Run(method, func(t *testing.T) {
			d, err := Open("", &Options{
				FS: vfs.NewMem(),
			})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer d.Close()
			var b *Batch

			datadriven.RunTest(t, "testdata/batch_get", func(td *datadriven.TestData) string {
				switch td.Cmd {
				case "define":
					switch method {
					case "build":
						b = d.NewIndexedBatch()
					case "apply":
						b = d.NewBatch()
					}

					if err := runBatchDefineCmd(td, b); err != nil {
						return err.Error()
					}

					switch method {
					case "apply":
						tmp := d.NewIndexedBatch()
						tmp.Apply(b, nil)
						b = tmp
					}
					return ""

				case "commit":
					if err := b.Commit(nil); err != nil {
						return err.Error()
					}
					return ""

				case "get":
					if len(td.CmdArgs) != 1 {
						return fmt.Sprintf("%s expects 1 argument", td.Cmd)
					}
					v, err := b.Get([]byte(td.CmdArgs[0].String()))
					if err != nil {
						return err.Error()
					}
					return string(v)

				default:
					return fmt.Sprintf("unknown command: %s", td.Cmd)
				}
			})
		})
	}
}

func TestBatchIter(t *testing.T) {
	var b *Batch

	for _, method := range []string{"build", "apply"} {
		for _, testdata := range []string{
			"testdata/internal_iter_next", "testdata/internal_iter_bounds"} {
			t.Run(method, func(t *testing.T) {
				datadriven.RunTest(t, testdata, func(d *datadriven.TestData) string {
					switch d.Cmd {
					case "define":
						switch method {
						case "build":
							b = newIndexedBatch(nil, DefaultComparer)
						case "apply":
							b = newBatch(nil)
						}

						for _, key := range strings.Split(d.Input, "\n") {
							j := strings.Index(key, ":")
							ikey := base.ParseInternalKey(key[:j])
							value := []byte(key[j+1:])
							b.Set(ikey.UserKey, value, nil)
						}

						switch method {
						case "apply":
							tmp := newIndexedBatch(nil, DefaultComparer)
							tmp.Apply(b, nil)
							b = tmp
						}
						return ""

					case "iter":
						var options IterOptions
						for _, arg := range d.CmdArgs {
							switch arg.Key {
							case "lower":
								if len(arg.Vals) != 1 {
									return fmt.Sprintf(
										"%s expects at most 1 value for lower", d.Cmd)
								}
								options.LowerBound = []byte(arg.Vals[0])
							case "upper":
								if len(arg.Vals) != 1 {
									return fmt.Sprintf(
										"%s expects at most 1 value for upper", d.Cmd)
								}
								options.UpperBound = []byte(arg.Vals[0])
							default:
								return fmt.Sprintf("unknown arg: %s", arg.Key)
							}
						}
						iter := b.newInternalIter(&options)
						defer iter.Close()
						return runInternalIterCmd(d, iter)

					default:
						return fmt.Sprintf("unknown command: %s", d.Cmd)
					}
				})
			})
		}
	}
}

func TestBatchDeleteRange(t *testing.T) {
	var b *Batch

	datadriven.RunTest(t, "testdata/batch_delete_range", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "clear":
			b = nil
			return ""

		case "define":
			if b == nil {
				b = newIndexedBatch(nil, DefaultComparer)
			}
			if err := runBatchDefineCmd(td, b); err != nil {
				return err.Error()
			}
			return ""

		case "scan":
			var iter internalIterAdapter
			if len(td.CmdArgs) > 1 {
				return fmt.Sprintf("%s expects at most 1 argument", td.Cmd)
			}
			if len(td.CmdArgs) == 1 {
				if td.CmdArgs[0].String() != "range-del" {
					return fmt.Sprintf("%s unknown argument %s", td.Cmd, td.CmdArgs[0])
				}
				iter.internalIterator = b.newRangeDelIter(nil)
			} else {
				iter.internalIterator = b.newInternalIter(nil)
			}
			defer iter.Close()

			var buf bytes.Buffer
			for valid := iter.First(); valid; valid = iter.Next() {
				key := iter.Key()
				key.SetSeqNum(key.SeqNum() &^ InternalKeySeqNumBatch)
				fmt.Fprintf(&buf, "%s:%s\n", key, iter.Value())
			}
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestFlushableBatchIter(t *testing.T) {
	var b *flushableBatch
	datadriven.RunTest(t, "testdata/internal_iter_next", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			batch := newBatch(nil)
			for _, key := range strings.Split(d.Input, "\n") {
				j := strings.Index(key, ":")
				ikey := base.ParseInternalKey(key[:j])
				value := []byte(fmt.Sprint(ikey.SeqNum()))
				batch.Set(ikey.UserKey, value, nil)
			}
			b = newFlushableBatch(batch, DefaultComparer)
			return ""

		case "iter":
			iter := b.newIter(nil)
			defer iter.Close()
			return runInternalIterCmd(d, iter)

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

func TestFlushableBatch(t *testing.T) {
	var b *flushableBatch
	datadriven.RunTest(t, "testdata/flushable_batch", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			batch := newBatch(nil)
			for _, key := range strings.Split(d.Input, "\n") {
				j := strings.Index(key, ":")
				ikey := base.ParseInternalKey(key[:j])
				value := []byte(fmt.Sprint(ikey.SeqNum()))
				switch ikey.Kind() {
				case InternalKeyKindDelete:
					batch.Delete(ikey.UserKey, nil)
				case InternalKeyKindSet:
					batch.Set(ikey.UserKey, value, nil)
				case InternalKeyKindMerge:
					batch.Merge(ikey.UserKey, value, nil)
				}
			}
			b = newFlushableBatch(batch, DefaultComparer)
			return ""

		case "iter":
			var opts IterOptions
			for _, arg := range d.CmdArgs {
				if len(arg.Vals) != 1 {
					return fmt.Sprintf("%s: %s=<value>", d.Cmd, arg.Key)
				}
				switch arg.Key {
				case "lower":
					opts.LowerBound = []byte(arg.Vals[0])
				case "upper":
					opts.UpperBound = []byte(arg.Vals[0])
				default:
					return fmt.Sprintf("%s: unknown arg: %s", d.Cmd, arg.Key)
				}
			}

			iter := b.newIter(&opts)
			defer iter.Close()
			return runInternalIterCmd(d, iter)

		case "dump":
			if len(d.CmdArgs) != 1 || len(d.CmdArgs[0].Vals) != 1 || d.CmdArgs[0].Key != "seq" {
				return fmt.Sprintf("dump seq=<value>\n")
			}
			seqNum, err := strconv.Atoi(d.CmdArgs[0].Vals[0])
			if err != nil {
				return err.Error()
			}
			b.seqNum = uint64(seqNum)

			iter := newInternalIterAdapter(b.newIter(nil))
			var buf bytes.Buffer
			for valid := iter.First(); valid; valid = iter.Next() {
				fmt.Fprintf(&buf, "%s:%s\n", iter.Key(), iter.Value())
			}
			iter.Close()
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

func TestFlushableBatchDeleteRange(t *testing.T) {
	var fb *flushableBatch
	var input string

	datadriven.RunTest(t, "testdata/delete_range", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "clear":
			input = ""
			return ""

		case "define":
			b := newBatch(nil)
			// NB: We can't actually add to the flushable batch as we can to a
			// memtable (which shares the "testdata/delete_range" data), so we fake
			// it be concatenating the input and rebuilding the flushable batch from
			// scratch.
			input += "\n" + td.Input
			td.Input = input
			if err := runBatchDefineCmd(td, b); err != nil {
				return err.Error()
			}
			fb = newFlushableBatch(b, DefaultComparer)
			return ""

		case "scan":
			var iter internalIterAdapter
			if len(td.CmdArgs) > 1 {
				return fmt.Sprintf("%s expects at most 1 argument", td.Cmd)
			}
			if len(td.CmdArgs) == 1 {
				if td.CmdArgs[0].String() != "range-del" {
					return fmt.Sprintf("%s unknown argument %s", td.Cmd, td.CmdArgs[0])
				}
				iter.internalIterator = fb.newRangeDelIter(nil)
			} else {
				iter.internalIterator = fb.newIter(nil)
			}
			defer iter.Close()

			var buf bytes.Buffer
			for valid := iter.First(); valid; valid = iter.Next() {
				fmt.Fprintf(&buf, "%s:%s\n", iter.Key(), iter.Value())
			}
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestFlushableBatchBytesIterated(t *testing.T) {
	batch := newBatch(nil)
	for j := 0; j < 1000; j++ {
		key := make([]byte, 8+j%3)
		value := make([]byte, 7+j%5)
		batch.Set(key, value, nil)

		fb := newFlushableBatch(batch, DefaultComparer)

		var bytesIterated uint64
		it := fb.newFlushIter(nil, &bytesIterated)

		var prevIterated uint64
		for key, _ := it.First(); key != nil; key, _ = it.Next() {
			if bytesIterated < prevIterated {
				t.Fatalf("bytesIterated moved backward: %d < %d", bytesIterated, prevIterated)
			}
			prevIterated = bytesIterated
		}

		expected := fb.totalBytes()
		if bytesIterated != expected {
			t.Fatalf("bytesIterated: got %d, want %d", bytesIterated, expected)
		}
	}
}

func TestEmptyFlushableBatch(t *testing.T) {
	// Verify that we can create a flushable batch on an empty batch.
	fb := newFlushableBatch(newBatch(nil), DefaultComparer)
	it := newInternalIterAdapter(fb.newIter(nil))
	require.False(t, it.First())
}

func BenchmarkBatchSet(b *testing.B) {
	value := make([]byte, 10)
	for i := range value {
		value[i] = byte(i)
	}
	key := make([]byte, 8)
	batch := newBatch(nil)

	b.ResetTimer()

	const batchSize = 1000
	for i := 0; i < b.N; i += batchSize {
		end := i + batchSize
		if end > b.N {
			end = b.N
		}

		for j := i; j < end; j++ {
			binary.BigEndian.PutUint64(key, uint64(j))
			batch.Set(key, value, nil)
		}
		batch.Reset()
	}

	b.StopTimer()
}

func BenchmarkIndexedBatchSet(b *testing.B) {
	value := make([]byte, 10)
	for i := range value {
		value[i] = byte(i)
	}
	key := make([]byte, 8)
	batch := newIndexedBatch(nil, DefaultComparer)

	b.ResetTimer()

	const batchSize = 1000
	for i := 0; i < b.N; i += batchSize {
		end := i + batchSize
		if end > b.N {
			end = b.N
		}

		for j := i; j < end; j++ {
			binary.BigEndian.PutUint64(key, uint64(j))
			batch.Set(key, value, nil)
		}
		batch.Reset()
	}

	b.StopTimer()
}

func BenchmarkBatchSetDeferred(b *testing.B) {
	value := make([]byte, 10)
	for i := range value {
		value[i] = byte(i)
	}
	key := make([]byte, 8)
	batch := newBatch(nil)

	b.ResetTimer()

	const batchSize = 1000
	for i := 0; i < b.N; i += batchSize {
		end := i + batchSize
		if end > b.N {
			end = b.N
		}

		for j := i; j < end; j++ {
			binary.BigEndian.PutUint64(key, uint64(j))
			deferredOp, err := batch.SetDeferred(len(key), len(value), nil)
			if err != nil {
				b.Fatal(err)
			}

			copy(deferredOp.Key, key)
			copy(deferredOp.Value, value)

			deferredOp.Finish()
		}
		batch.Reset()
	}

	b.StopTimer()
}

func BenchmarkIndexedBatchSetDeferred(b *testing.B) {
	value := make([]byte, 10)
	for i := range value {
		value[i] = byte(i)
	}
	key := make([]byte, 8)
	batch := newIndexedBatch(nil, DefaultComparer)

	b.ResetTimer()

	const batchSize = 1000
	for i := 0; i < b.N; i += batchSize {
		end := i + batchSize
		if end > b.N {
			end = b.N
		}

		for j := i; j < end; j++ {
			binary.BigEndian.PutUint64(key, uint64(j))
			deferredOp, err := batch.SetDeferred(len(key), len(value), nil)
			if err != nil {
				b.Fatal(err)
			}

			copy(deferredOp.Key, key)
			copy(deferredOp.Value, value)

			deferredOp.Finish()
		}
		batch.Reset()
	}

	b.StopTimer()
}
