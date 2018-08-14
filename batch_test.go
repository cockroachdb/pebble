// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/petermattis/pebble/db"
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
	}
	var b Batch
	for _, tc := range testCases {
		if tc.kind == db.InternalKeyKindDelete {
			b.Delete([]byte(tc.key), nil)
		} else {
			b.Set([]byte(tc.key), []byte(tc.value), nil)
		}
	}
	iter := b.iter()
	for _, tc := range testCases {
		kind, k, v, ok := iter.next()
		if !ok {
			t.Fatalf("next returned !ok: test case = %q", tc)
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
		b.data = buf[:]
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
	testCases := []struct {
		key      []byte
		value    []byte
		expected []byte
	}{
		{[]byte("a"), []byte("b"), []byte("b")},
		{[]byte("a"), []byte("c"), []byte("c")},
		{[]byte("a"), nil, nil},
		{[]byte("a"), []byte("d"), []byte("d")},
	}

	b := newIndexedBatch(nil, db.DefaultComparer)
	for i, c := range testCases {
		if c.value == nil {
			b.Delete(c.key, nil)
		} else {
			b.Set(c.key, c.value, nil)
		}
		v, err := b.Get(c.key)
		if err != nil {
			t.Fatalf("%d: %s: %v", i, c.key, err)
		}
		if c.expected == nil && v != nil {
			t.Fatalf("unexpected value: %q", v)
		} else if string(c.expected) != string(v) {
			t.Fatalf("expected %q, but found %q", c.expected, v)
		}
	}
}

func TestBatchIterNextPrev(t *testing.T) {
	b := newIndexedBatch(nil, db.DefaultComparer)
	for _, key := range []string{"a.SET.1", "a.SET.2", "b.SET.1", "b.SET.2", "c.SET.1", "c.SET.2"} {
		ikey := db.ParseInternalKey(key)
		value := []byte(fmt.Sprint(ikey.SeqNum()))
		b.Set(ikey.UserKey, value, nil)
	}
	iter := b.newInternalIter(nil)
	iter.First()

	// These test cases are shared with TestMergingIterNextPrev.
	testCases := []struct {
		dir      string
		expected string
	}{
		{"+", "<a:1>"}, // 0
		{"+", "<b:2>"}, // 1
		{"-", "<b:1>"}, // 2
		{"-", "<a:2>"}, // 3
		{"-", "<a:1>"}, // 4
		{"-", "."},     // 5
		{"+", "<a:2>"}, // 6
		{"+", "<a:1>"}, // 7
		{"+", "<b:2>"}, // 8
		{"+", "<b:1>"}, // 9
		{"+", "<c:2>"}, // 10
		{"+", "<c:1>"}, // 11
		{"-", "<b:2>"}, // 12
		{"-", "<b:1>"}, // 13
		{"+", "<c:2>"}, // 14
		{"-", "<c:1>"}, // 15
		{"-", "<b:2>"}, // 16
		{"+", "<b:1>"}, // 17
		{"+", "<c:2>"}, // 18
		{"+", "<c:1>"}, // 19
		{"+", "."},     // 20
		{"-", "<c:2>"}, // 21
	}
	for i, c := range testCases {
		switch c.dir {
		case "+":
			iter.Next()
		case "-":
			iter.Prev()
		default:
			t.Fatalf("unexpected direction: %q", c.dir)
		}
		var got string
		if !iter.Valid() {
			got = "."
		} else if (iter.Key().SeqNum() & db.InternalKeySeqNumBatch) == 0 {
			got = fmt.Sprintf("<%s:bad-seq-num>", iter.Key().UserKey)
		} else {
			got = fmt.Sprintf("<%s:%s>", iter.Key().UserKey, iter.Value())
		}
		if got != c.expected {
			t.Fatalf("%d: got  %q\nwant %q", i, got, c.expected)
		}
	}
}

func TestBatchIterNextPrevUserKey(t *testing.T) {
	b := newIndexedBatch(nil, db.DefaultComparer)
	for _, key := range []string{"a.SET.1", "a.SET.2", "b.SET.1", "b.SET.2", "c.SET.1", "c.SET.2"} {
		ikey := db.ParseInternalKey(key)
		value := []byte(fmt.Sprint(ikey.SeqNum()))
		b.Set(ikey.UserKey, value, nil)
	}
	iter := b.newInternalIter(nil)
	iter.First()

	// These test cases are shared with TestMergingIterNextPrevUserKey.
	testCases := []struct {
		dir      string
		expected string
	}{
		{"+", "<b:2>"}, // 0
		{"-", "<a:2>"}, // 1
		{"-", "."},     // 2
		{"+", "<a:2>"}, // 3
		{"+", "<b:2>"}, // 4
		{"+", "<c:2>"}, // 5
		{"+", "."},     // 6
		{"-", "<c:2>"}, // 7
		{"-", "<b:2>"}, // 8
		{"-", "<a:2>"}, // 9
		{"+", "<b:2>"}, // 10
		{"+", "<c:2>"}, // 11
		{"-", "<b:2>"}, // 12
		{"+", "<c:2>"}, // 13
		{"+", "."},     // 14
		{"-", "<c:2>"}, // 14
	}
	for i, c := range testCases {
		switch c.dir {
		case "+":
			iter.NextUserKey()
		case "-":
			iter.PrevUserKey()
		default:
			t.Fatalf("unexpected direction: %q", c.dir)
		}
		var got string
		if !iter.Valid() {
			got = "."
		} else if (iter.Key().SeqNum() & db.InternalKeySeqNumBatch) == 0 {
			got = fmt.Sprintf("<%s:bad-seq-num>", iter.Key().UserKey)
		} else {
			got = fmt.Sprintf("<%s:%s>", iter.Key().UserKey, iter.Value())
		}
		if got != c.expected {
			t.Fatalf("%d: got  %q\nwant %q", i, got, c.expected)
		}
	}
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

		var batch Batch
		for j := i; j < end; j++ {
			binary.BigEndian.PutUint64(key, uint64(j))
			batch.Set(key, value, nil)
		}
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
	}

	b.StopTimer()
}
