// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package testkeys

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestGenerateAlphabetKey(t *testing.T) {
	testCases := []struct {
		alphabet string
		i        int
		depth    int
		want     string
	}{
		{"abc", 0, 1, "a"},
		{"abc", 0, 2, "a"},
		{"abc", 0, 3, "a"},

		{"abc", 1, 1, "b"},
		{"abc", 2, 1, "c"},

		{"abc", 0, 2, "a"},
		{"abc", 1, 2, "aa"},
		{"abc", 2, 2, "ab"},
		{"abc", 3, 2, "ac"},
		{"abc", 4, 2, "b"},
		{"abc", 5, 2, "ba"},
		{"abc", 6, 2, "bb"},
		{"abc", 7, 2, "bc"},
		{"abc", 8, 2, "c"},
		{"abc", 9, 2, "ca"},
		{"abc", 10, 2, "cb"},
		{"abc", 11, 2, "cc"},
	}

	buf := make([]byte, 10)
	for _, tc := range testCases {
		kc := keyCount(len(tc.alphabet), tc.depth)
		n := generateAlphabetKey(buf, []byte(tc.alphabet), tc.i, kc)
		got := string(buf[:n])
		if got != tc.want {
			t.Errorf("generateAlphabetKey(%q, %d, %d) = %q, want %q", tc.alphabet, tc.i, kc, got, tc.want)
		}
	}
}

func TestKeyCount(t *testing.T) {
	type params struct {
		n, l int
	}
	testCases := map[params]int{
		{26, 1}: 26,
		{52, 1}: 52,
		{2, 2}:  6,
		{2, 3}:  14,
		{2, 4}:  30,
		{3, 2}:  12,
	}
	for p, want := range testCases {
		got := keyCount(p.n, p.l)
		if got != want {
			t.Errorf("keyCount(%d, %d) = %d, want %d", p.n, p.l, got, want)
		}
	}
}

func TestFullKeyspaces(t *testing.T) {
	testCases := []struct {
		ks   Keyspace
		want string
	}{
		{
			Alpha(1),
			"a b c d e f g h i j k l m n o p q r s t u v w x y z",
		},
		{
			alphabet{[]byte("abc"), 2, 0, 0, 1},
			"a aa ab ac b ba bb bc c ca cb cc",
		},
		{
			alphabet{[]byte("abc"), 2, 0, 0, 2},
			"a ab b bb c cb",
		},
		{
			alphabet{[]byte("abc"), 3, 0, 0, 1},
			"a aa aaa aab aac ab aba abb abc ac aca acb acc b ba baa bab bac bb bba bbb bbc bc bca bcb bcc c ca caa cab cac cb cba cbb cbc cc cca ccb ccc",
		},
		{
			alphabet{[]byte("abc"), 3, 7, 10, 1},
			"abb abc ac aca acb acc b ba baa bab bac bb bba bbb bbc bc bca bcb bcc c ca caa",
		},
	}
	for _, tc := range testCases {
		require.Equal(t, tc.want, keyspaceToString(tc.ks))
	}
}

func TestSlice(t *testing.T) {
	testCases := []struct {
		orig Keyspace
		i, j int
		want string
	}{
		{Alpha(1), 1, 25, "b c d e f g h i j k l m n o p q r s t u v w x y"},
		{Alpha(1).Slice(1, 25), 1, 23, "c d e f g h i j k l m n o p q r s t u v w x"},
		{Alpha(1).Slice(1, 25).Slice(1, 23), 10, 22, "m n o p q r s t u v w x"},
	}
	for _, tc := range testCases {
		got := keyspaceToString(tc.orig.Slice(tc.i, tc.j))
		if got != tc.want {
			t.Errorf("(%q).Slice(%d, %d) = %q, want %q",
				keyspaceToString(tc.orig), tc.i, tc.j, got, tc.want)
		}
	}
}

func TestSuffix(t *testing.T) {
	ks := Alpha(3)
	require.Equal(t, "a@1", string(KeyAt(ks, 0, 1)))
	require.Equal(t, "a@10", string(KeyAt(ks, 0, 10)))
	require.Equal(t, "aab@5", string(KeyAt(ks, 3, 5)))

	assertCmp := func(want int, a, b []byte) {
		got := Comparer.Compare(a, b)
		if got != want {
			t.Helper()
			t.Errorf("Compare(%q, %q) = %d, want %d", a, b, got, want)
		}
	}

	for i := 1; i < ks.Count(); i++ {
		assertCmp(-1, KeyAt(ks, i-1, 1), KeyAt(ks, i, 1))
		assertCmp(-1, Key(ks, i-1), Key(ks, i))
		assertCmp(0, Key(ks, i), Key(ks, i))
		for ts := 2; ts < 11; ts++ {
			assertCmp(+1, KeyAt(ks, i, ts-1), KeyAt(ks, i, ts))
			assertCmp(-1, KeyAt(ks, i-1, ts-1), KeyAt(ks, i, ts))
		}
	}

	// Suffixes should be comparable on their own too.
	a, b := make([]byte, MaxSuffixLen), make([]byte, MaxSuffixLen)
	for ts := 2; ts < 150; ts++ {
		an := WriteSuffix(a, ts-1)
		bn := WriteSuffix(b, ts)
		assertCmp(+1, a[:an], b[:bn])
	}
}

func TestSuffixLen(t *testing.T) {
	testCases := map[int]int{
		0:    2,
		1:    2,
		5:    2,
		9:    2,
		10:   3,
		17:   3,
		20:   3,
		99:   3,
		100:  4,
		101:  4,
		999:  4,
		1000: 5,
	}
	for ts, want := range testCases {
		if got := SuffixLen(ts); got != want {
			t.Errorf("SuffixLen(%d) = %d, want %d", ts, got, want)
		}
	}
}

func TestDivvy(t *testing.T) {
	var buf bytes.Buffer
	datadriven.RunTest(t, "testdata/divvy", func(t *testing.T, d *datadriven.TestData) string {
		buf.Reset()
		switch d.Cmd {
		case "divvy":
			var alphaLen, portions int
			d.ScanArgs(t, "alpha", &alphaLen)
			d.ScanArgs(t, "portions", &portions)

			input := Alpha(alphaLen)
			for _, ks := range Divvy(input, portions) {
				fmt.Fprintln(&buf, keyspaceToString(ks))
			}
			return buf.String()
		default:
			return fmt.Sprintf("unrecognized command %q", d.Cmd)
		}
	})
}

func keyspaceToString(ks Keyspace) string {
	var buf bytes.Buffer
	b := make([]byte, ks.MaxLen())
	for i := 0; i < ks.Count(); i++ {
		n := ks.key(b, i)
		if i > 0 {
			buf.WriteRune(' ')
		}
		buf.Write(b[:n])
	}
	return buf.String()
}
