// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package testkeys

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/stretchr/testify/require"
)

func TestGenerateAlphabetKey(t *testing.T) {
	testCases := []struct {
		i     uint64
		depth int
		want  string
	}{
		{0, 1, "a"},
		{0, 2, "a"},
		{0, 3, "a"},

		{1, 1, "b"},
		{2, 1, "c"},

		{0, 2, "a"},
		{1, 2, "aa"},
		{2, 2, "ab"},
		{3, 2, "ac"},
		{4, 2, "b"},
		{5, 2, "ba"},
		{6, 2, "bb"},
		{7, 2, "bc"},
		{8, 2, "c"},
		{9, 2, "ca"},
		{10, 2, "cb"},
		{11, 2, "cc"},
	}
	testAlphabet := []byte{byte('a'), byte('b'), byte('c')}
	testInverseAlphabet := map[byte]uint64{byte('a'): 0, byte('b'): 1, byte('c'): 2}

	buf := make([]byte, 10)
	for _, tc := range testCases {
		kc := keyCount(len(testAlphabet), tc.depth)
		n := generateAlphabetKey(buf, testAlphabet, tc.i, kc)
		got := string(buf[:n])
		if got != tc.want {
			t.Errorf("generateAlphabetKey(%q, %d, %d) = %q, want %q", testAlphabet, tc.i, kc, got, tc.want)
		}
		i := computeAlphabetKeyIndex([]byte(got), testInverseAlphabet, tc.depth)
		if i != tc.i {
			t.Errorf("computeAlphabetKeyIndex(%q, %d) = %d, want %d", got, tc.depth, i, tc.i)
		}
	}
}

func TestKeyCount(t *testing.T) {
	type params struct {
		n, l int
	}
	testCases := map[params]uint64{
		{26, 1}: 26,
		{26, 2}: 702,
		{26, 3}: 18278,
		{26, 4}: 475254,
		{26, 5}: 12356630,
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
		i, j uint64
		want string
	}{
		{Alpha(1), 1, 25, "b c d e f g h i j k l m n o p q r s t u v w x y"},
		{Slice(Alpha(1), 1, 25), 1, 23, "c d e f g h i j k l m n o p q r s t u v w x"},
		{Slice(Slice(Alpha(1), 1, 25), 1, 23), 10, 22, "m n o p q r s t u v w x"},
	}
	for _, tc := range testCases {
		got := keyspaceToString(Slice(tc.orig, tc.i, tc.j))
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

	for i := uint64(1); i < ks.Count(); i++ {
		assertCmp(-1, KeyAt(ks, i-1, 1), KeyAt(ks, i, 1))
		assertCmp(-1, Key(ks, i-1), Key(ks, i))
		assertCmp(0, Key(ks, i), Key(ks, i))
		for ts := int64(2); ts < 11; ts++ {
			assertCmp(+1, KeyAt(ks, i, ts-1), KeyAt(ks, i, ts))
			assertCmp(-1, KeyAt(ks, i-1, ts-1), KeyAt(ks, i, ts))
		}
	}

	// Test CompareSuffixes.
	a, b := make([]byte, MaxSuffixLen), make([]byte, MaxSuffixLen)
	for ts := int64(2); ts < 150; ts++ {
		an := WriteSuffix(a, ts-1)
		bn := WriteSuffix(b, ts)
		got := Comparer.CompareRangeSuffixes(a[:an], b[:bn])
		if want := +1; got != want {
			t.Errorf("CompareSuffixes(%q, %q) = %d, want %d", a, b, got, want)
		}
	}
}

func TestSuffixLen(t *testing.T) {
	testCases := map[int64]int{
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
			var alphaLen int
			var portions uint64
			d.ScanArgs(t, "alpha", &alphaLen)
			d.ScanArgs(t, "portions", &portions)

			input := Alpha(alphaLen)
			for _, ks := range Divvy(input, portions) {
				fmt.Fprintf(&buf, "%s (%d keys)\n", keyspaceToString(ks), ks.Count())
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
	for i := uint64(0); i < ks.Count(); i++ {
		n := ks.key(b, i)
		if i > 0 {
			buf.WriteRune(' ')
		}
		buf.Write(b[:n])
	}
	return buf.String()
}

func TestRandomPrefixInRange(t *testing.T) {
	testCases := []struct {
		a, b            string
		maxPossibleKeys int
	}{
		{a: "abc", b: "def"},
		{a: "a", b: "aa", maxPossibleKeys: 1},
		{a: "a", b: "aaa", maxPossibleKeys: 2},
		{a: "a", b: "ab"},
		{a: "longcommonprefixabcdef", b: "longcommonprefixb"},
		{a: "longcommonprefix", b: "longcommonprefixb"},
		{a: "longcommonprefix", b: "longcommonprefixa", maxPossibleKeys: 1},
		{a: "longcommonprefix", b: "longcommonprefixaaaaa", maxPossibleKeys: 5},
		{a: "a", b: "abthiskeywillneedtobetrimmed"},
		{a: "abthiskeywillneedtobetrimmed", b: "ac"},
		{a: "abthiskeywillneedtobetrimmed", b: "acthiskeywillneedtobetrimmed"},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(0, 0))
			keys := make(map[string]struct{})
			for i := 1; i < 100; i++ {
				key := RandomPrefixInRange([]byte(tc.a), []byte(tc.b), rng)
				require.True(t, compare([]byte(tc.a), key) <= 0)
				require.True(t, compare(key, []byte(tc.b)) < 0)
				keys[string(key)] = struct{}{}
			}
			if tc.maxPossibleKeys != 0 {
				require.Equal(t, len(keys), tc.maxPossibleKeys)
			} else {
				// Make sure we are generating many different keys.
				require.GreaterOrEqual(t, len(keys), 50)
			}
		})
	}

	t.Run("randomized", func(t *testing.T) {
		rng := rand.New(rand.NewPCG(0, 0))
		keys := [][]byte{[]byte("a"), []byte("zzz")}
		for n := 0; n < 1000; n++ {
			i := rng.IntN(len(keys) - 1)
			j := i + 1 + rng.IntN(len(keys)-1-i)
			a, b := keys[i], keys[j]
			key := RandomPrefixInRange(a, b, rng)
			if Comparer.Compare(key, a) == 0 {
				// It is legal to return a.
				continue
			}
			for k := 0; k < len(keys); k++ {
				v := Comparer.Compare(key, keys[k])
				if k <= i && v <= 0 || k >= j && v >= 0 {
					t.Fatalf("RandomPrefixInRange(%q, %q) = %q; but Compare(%q,%q) = %d\n", a, b, key, key, keys[k], v)
				}
			}
			keys = append(keys, key)
			slices.SortFunc(keys, Comparer.Compare)
		}
	})
}

func TestOverflowPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("did not panic")
		} else {
			t.Logf("panic: %v", r)
		}
	}()
	keyCount(6, len(alpha))
}

func TestComparer(t *testing.T) {
	if err := base.CheckComparer(Comparer,
		[][]byte{[]byte("abc"), []byte("d"), []byte("ef")},
		[][]byte{{}, []byte("@3"), []byte("@2"), []byte("@1")}); err != nil {
		t.Error(err)
	}
}

func TestIgnorableSuffix(t *testing.T) {
	require.Equal(t, 0, Comparer.Compare([]byte("foo@1"), []byte("foo@1_synthetic")))
	require.Equal(t, 1, Comparer.Compare([]byte("foo@1"), []byte("foo@2_synthetic")))
	require.Equal(t, 1, Comparer.Compare([]byte("foo@1_synthetic"), []byte("foo@2")))
	require.Equal(t, -1, Comparer.CompareRangeSuffixes([]byte("@1"), []byte("@1_synthetic")))
	require.Equal(t, 1, Comparer.CompareRangeSuffixes([]byte("@1_synthetic"), []byte("@1")))
	require.Equal(t, 0, Comparer.CompareRangeSuffixes([]byte("@1_synthetic"), []byte("@1_synthetic")))
	require.Equal(t, 0, Comparer.CompareRangeSuffixes([]byte("@1"), []byte("@1")))
}

func TestExtractKVMeta(t *testing.T) {
	require.Equal(t, base.KVMeta{}, ExtractKVMeta([]byte("foo")))
	require.Equal(t, base.KVMeta{TieringSpanID: 1, TieringAttribute: 123}, ExtractKVMeta([]byte("foo;tiering:span=1,attr=123")))
}
