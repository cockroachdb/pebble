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

	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/internal/datadriven"
	"github.com/petermattis/pebble/vfs"
)

func TestBatch(t *testing.T) {
	testCases := []struct {
		kind       db.InternalKeyKind
		key, value string
	}{
		{db.InternalKeyKindSet, "roses", "red"},
		{db.InternalKeyKindSet, "violets", "blue"},
		{db.InternalKeyKindDelete, "roses", ""},
		{db.InternalKeyKindSet, "", ""},
		{db.InternalKeyKindSet, "", "non-empty"},
		{db.InternalKeyKindDelete, "", ""},
		{db.InternalKeyKindSet, "grass", "green"},
		{db.InternalKeyKindSet, "grass", "greener"},
		{db.InternalKeyKindSet, "eleventy", strings.Repeat("!!11!", 100)},
		{db.InternalKeyKindDelete, "nosuchkey", ""},
		{db.InternalKeyKindSet, "binarydata", "\x00"},
		{db.InternalKeyKindSet, "binarydata", "\xff"},
		{db.InternalKeyKindMerge, "merge", "mergedata"},
	}
	var b Batch
	for _, tc := range testCases {
		switch tc.kind {
		case db.InternalKeyKindSet:
			b.Set([]byte(tc.key), []byte(tc.value), nil)
		case db.InternalKeyKindMerge:
			b.Merge([]byte(tc.key), []byte(tc.value), nil)
		case db.InternalKeyKindDelete:
			b.Delete([]byte(tc.key), nil)
		}
	}
	iter := b.iter()
	for _, tc := range testCases {
		kind, k, v, ok := iter.next()
		if !ok {
			t.Fatalf("next returned !ok: test case = %v", tc)
		}
		key, value := string(k), string(v)
		if kind != tc.kind || key != tc.key || value != tc.value {
			t.Errorf("got (%d, %q, %q), want (%d, %q, %q)",
				kind, key, value, tc.kind, tc.key, tc.value)
		}
	}
	if len(iter) != 0 {
		t.Errorf("iterator was not exhausted: remaining bytes = %q", iter)
	}
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
		var buf [12]byte
		binary.LittleEndian.PutUint32(buf[8:12], tc)
		var b Batch
		b.storage.data = buf[:]
		b.increment()
		got := binary.LittleEndian.Uint32(buf[8:12])
		want := tc + 1
		if tc == 0xffffffff {
			want = tc
		}
		if got != want {
			t.Errorf("input=%d: got %d, want %d", tc, got, want)
		}
	}
}

func TestBatchGet(t *testing.T) {
	for _, method := range []string{"build", "apply"} {
		t.Run(method, func(t *testing.T) {
			d, err := Open("", &db.Options{
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
		t.Run(method, func(t *testing.T) {
			datadriven.RunTest(t, "testdata/internal_iter_next", func(d *datadriven.TestData) string {
				switch d.Cmd {
				case "define":
					switch method {
					case "build":
						b = newIndexedBatch(nil, db.DefaultComparer)
					case "apply":
						b = newBatch(nil)
					}

					for _, key := range strings.Split(d.Input, "\n") {
						j := strings.Index(key, ":")
						ikey := db.ParseInternalKey(key[:j])
						value := []byte(fmt.Sprint(ikey.SeqNum()))
						b.Set(ikey.UserKey, value, nil)
					}

					switch method {
					case "apply":
						tmp := newIndexedBatch(nil, db.DefaultComparer)
						tmp.Apply(b, nil)
						b = tmp
					}
					return ""

				case "iter":
					iter := b.newInternalIter(nil)
					defer iter.Close()
					return runInternalIterCmd(d, iter)

				default:
					return fmt.Sprintf("unknown command: %s", d.Cmd)
				}
			})
		})
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
				b = newIndexedBatch(nil, db.DefaultComparer)
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
				key.SetSeqNum(key.SeqNum() &^ db.InternalKeySeqNumBatch)
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
				ikey := db.ParseInternalKey(key[:j])
				value := []byte(fmt.Sprint(ikey.SeqNum()))
				batch.Set(ikey.UserKey, value, nil)
			}
			b = newFlushableBatch(batch, db.DefaultComparer)
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

func TestFlushableBatchSeqNum(t *testing.T) {
	var b *flushableBatch
	datadriven.RunTest(t, "testdata/flushable_batch", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			batch := newBatch(nil)
			for _, key := range strings.Split(d.Input, "\n") {
				j := strings.Index(key, ":")
				ikey := db.ParseInternalKey(key[:j])
				value := []byte(fmt.Sprint(ikey.SeqNum()))
				switch ikey.Kind() {
				case db.InternalKeyKindDelete:
					batch.Delete(ikey.UserKey, nil)
				case db.InternalKeyKindSet:
					batch.Set(ikey.UserKey, value, nil)
				case db.InternalKeyKindMerge:
					batch.Merge(ikey.UserKey, value, nil)
				}
			}
			b = newFlushableBatch(batch, db.DefaultComparer)
			return ""

		case "dump":
			if len(d.CmdArgs) != 1 || len(d.CmdArgs[0].Vals) != 1 || d.CmdArgs[0].Key != "seq" {
				return fmt.Sprintf("dump seq=<value>\n")
			}
			seqNum, err := strconv.Atoi(d.CmdArgs[0].Vals[0])
			if err != nil {
				return err.Error()
			}
			b.seqNum = uint64(seqNum)

			iter := internalIterAdapter{b.newIter(nil)}
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
			fb = newFlushableBatch(b, db.DefaultComparer)
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

func BenchmarkBatchSet(b *testing.B) {
	value := make([]byte, 10)
	for i := range value {
		value[i] = byte(i)
	}
	key := make([]byte, 8)

	b.ResetTimer()

	const batchSize = 1000
	for i := 0; i < b.N; i += batchSize {
		end := i + batchSize
		if end > b.N {
			end = b.N
		}

		batch := newBatch(nil)
		for j := i; j < end; j++ {
			binary.BigEndian.PutUint64(key, uint64(j))
			batch.Set(key, value, nil)
		}
		batch.release()
	}

	b.StopTimer()
}

func BenchmarkIndexedBatchSet(b *testing.B) {
	value := make([]byte, 10)
	for i := range value {
		value[i] = byte(i)
	}
	key := make([]byte, 8)

	b.ResetTimer()

	const batchSize = 1000
	for i := 0; i < b.N; i += batchSize {
		end := i + batchSize
		if end > b.N {
			end = b.N
		}

		batch := newIndexedBatch(nil, db.DefaultComparer)
		for j := i; j < end; j++ {
			binary.BigEndian.PutUint64(key, uint64(j))
			batch.Set(key, value, nil)
		}
		batch.release()
	}

	b.StopTimer()
}
