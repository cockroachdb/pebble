// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package leveldb

import (
	"testing"

	"code.google.com/p/leveldb-go/leveldb/db"
)

func TestInternalKey(t *testing.T) {
	k := makeInternalKey(nil, []byte("foo"), 1, 0x08070605040302)
	if got, want := string(k), "foo\x01\x02\x03\x04\x05\x06\x07\x08"; got != want {
		t.Fatalf("k = %q want %q", got, want)
	}
	if !k.valid() {
		t.Fatalf("invalid key")
	}
	if got, want := string(k.ukey()), "foo"; got != want {
		t.Errorf("ukey = %q want %q", got, want)
	}
	if got, want := k.kind(), internalKeyKind(1); got != want {
		t.Errorf("kind = %d want %d", got, want)
	}
	if got, want := k.seqNum(), uint64(0x08070605040302); got != want {
		t.Errorf("seqNum = %d want %d", got, want)
	}
}

func TestInvalidInternalKey(t *testing.T) {
	testCases := []string{
		"",
		"\x01\x02\x03\x04\x05\x06\x07",
		"foo",
		"foo\x08\x07\x06\x05\x04\x03\x02",
		"foo\x08\x07\x06\x05\x04\x03\x02\x01",
	}
	for _, tc := range testCases {
		if internalKey(tc).valid() {
			t.Errorf("%q is a valid key, want invalid", tc)
		}
	}
}

func TestInternalKeyComparer(t *testing.T) {
	// keys are some internal keys, in sorted order.
	keys := []string{
		// The empty key is not a valid internal key, but it still must
		// sort lower than any other key. It is used as a zero value when
		// checking that a sequence of internal keys are in sorted order.
		"",
		// The next two keys are also invalid internal keys. They are 'less
		// than' any valid internal key, and 'greater than' the empty key.
		"A",
		"B",
		// The remaining test keys are all valid.
		"" + "\x01\xff\xff\xff\xff\xff\xff\xff",
		"" + "\x00\xff\xff\xff\xff\xff\xff\xff",
		"" + "\x01\x01\x00\x00\x00\x00\x00\x00",
		"" + "\x00\x01\x00\x00\x00\x00\x00\x00",
		"" + "\x01\x00\x00\x00\x00\x00\x00\x00",
		"" + "\x00\x00\x00\x00\x00\x00\x00\x00",
		"\x00" + "\x00\x00\x00\x00\x00\x00\x00\x00",
		"\x00blue" + "\x01\x11\x00\x00\x00\x00\x00\x00",
		"bl\x00ue" + "\x01\x11\x00\x00\x00\x00\x00\x00",
		"blue" + "\x01\x11\x00\x00\x00\x00\x00\x00",
		"blue\x00" + "\x01\x11\x00\x00\x00\x00\x00\x00",
		"green" + "\x01\x11\x00\x00\x00\x00\x00\x00",
		"red" + "\x01\xff\xff\xff\xff\xff\xff\xff",
		"red" + "\x01\x72\x73\x74\x75\x76\x77\x78",
		"red" + "\x01\x00\x00\x00\x00\x00\x00\x11",
		"red" + "\x01\x00\x00\x00\x00\x00\x11\x00",
		"red" + "\x01\x00\x00\x00\x00\x11\x00\x00",
		"red" + "\x01\x00\x00\x00\x11\x00\x00\x00",
		"red" + "\x01\x00\x00\x11\x00\x00\x00\x00",
		"red" + "\x01\x00\x11\x00\x00\x00\x00\x00",
		"red" + "\x01\x11\x00\x00\x00\x00\x00\x00",
		"red" + "\x00\x11\x00\x00\x00\x00\x00\x00",
		"red" + "\x00\x00\x00\x00\x00\x00\x00\x00",
		"\xfe" + "\x01\xff\xff\xff\xff\xff\xff\xff",
		"\xfe" + "\x00\x00\x00\x00\x00\x00\x00\x00",
		"\xff" + "\x01\xff\xff\xff\xff\xff\xff\xff",
		"\xff" + "\x00\x00\x00\x00\x00\x00\x00\x00",
		"\xff\x40" + "\x01\xff\xff\xff\xff\xff\xff\xff",
		"\xff\x40" + "\x00\x00\x00\x00\x00\x00\x00\x00",
		"\xff\xff" + "\x01\xff\xff\xff\xff\xff\xff\xff",
		"\xff\xff" + "\x00\x00\x00\x00\x00\x00\x00\x00",
	}
	c := internalKeyComparer{db.DefaultComparer}
	for i := range keys {
		for j := range keys {
			got := c.Compare([]byte(keys[i]), []byte(keys[j]))
			want := 0
			if i < j {
				want = -1
			} else if i > j {
				want = +1
			}
			if got != want {
				t.Errorf("i=%d, j=%d, keys[i]=%q, keys[j]=%q: got %d, want %d",
					i, j, keys[i], keys[j], got, want)
			}
		}
	}
}
