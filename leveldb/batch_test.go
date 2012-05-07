// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package leveldb

import (
	"encoding/binary"
	"strings"
	"testing"
)

func TestBatch(t *testing.T) {
	testCases := []struct {
		kind       internalKeyKind
		key, value string
	}{
		{internalKeyKindSet, "roses", "red"},
		{internalKeyKindSet, "violets", "blue"},
		{internalKeyKindDelete, "roses", ""},
		{internalKeyKindSet, "", ""},
		{internalKeyKindSet, "", "non-empty"},
		{internalKeyKindDelete, "", ""},
		{internalKeyKindSet, "grass", "green"},
		{internalKeyKindSet, "grass", "greener"},
		{internalKeyKindSet, "eleventy", strings.Repeat("!!11!", 100)},
		{internalKeyKindDelete, "nosuchkey", ""},
		{internalKeyKindSet, "binarydata", "\x00"},
		{internalKeyKindSet, "binarydata", "\xff"},
	}
	var b Batch
	for _, tc := range testCases {
		if tc.kind == internalKeyKindDelete {
			b.Delete([]byte(tc.key))
		} else {
			b.Set([]byte(tc.key), []byte(tc.value))
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
		b := Batch{buf[:]}
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
