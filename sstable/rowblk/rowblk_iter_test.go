// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rowblk

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"unsafe"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/itertest"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/stretchr/testify/require"
)

func TestInvalidInternalKeyDecoding(t *testing.T) {
	// Invalid keys since they don't have an 8 byte trailer.
	testCases := []string{
		"",
		"\x01\x02\x03\x04\x05\x06\x07",
		"foo",
	}
	for _, tc := range testCases {
		i := Iter{}
		i.decodeInternalKey([]byte(tc))
		require.Nil(t, i.ikv.K.UserKey)
		require.Equal(t, base.InternalKeyTrailer(base.InternalKeyKindInvalid), i.ikv.K.Trailer)
	}
}

func TestBlockIter(t *testing.T) {
	// k is a block that maps three keys "apple", "apricot", "banana" to empty strings.
	k := []byte(
		"\x00\x05\x00apple" +
			"\x02\x05\x00ricot" +
			"\x00\x06\x00banana" +
			"\x00\x00\x00\x00\x01\x00\x00\x00")
	var testcases = []struct {
		index int
		key   string
	}{
		{0, ""},
		{0, "a"},
		{0, "aaaaaaaaaaaaaaa"},
		{0, "app"},
		{0, "apple"},
		{1, "appliance"},
		{1, "apricos"},
		{1, "apricot"},
		{2, "azzzzzzzzzzzzzz"},
		{2, "b"},
		{2, "banan"},
		{2, "banana"},
		{3, "banana\x00"},
		{3, "c"},
	}
	for _, tc := range testcases {
		i, err := NewRawIter(bytes.Compare, k)
		require.NoError(t, err)
		i.SeekGE([]byte(tc.key))
		for j, keyWant := range []string{"apple", "apricot", "banana"}[tc.index:] {
			if !i.Valid() {
				t.Fatalf("key=%q, index=%d, j=%d: Valid got false, keyWant true", tc.key, tc.index, j)
			}
			if keyGot := string(i.Key().UserKey); keyGot != keyWant {
				t.Fatalf("key=%q, index=%d, j=%d: got %q, keyWant %q", tc.key, tc.index, j, keyGot, keyWant)
			}
			i.Next()
		}
		if i.Valid() {
			t.Fatalf("key=%q, index=%d: Valid got true, keyWant false", tc.key, tc.index)
		}
		if err := i.Close(); err != nil {
			t.Fatalf("key=%q, index=%d: got err=%v", tc.key, tc.index, err)
		}
	}

	{
		i, err := NewRawIter(bytes.Compare, k)
		require.NoError(t, err)
		i.Last()
		for j, keyWant := range []string{"banana", "apricot", "apple"} {
			if !i.Valid() {
				t.Fatalf("j=%d: Valid got false, want true", j)
			}
			if keyGot := string(i.Key().UserKey); keyGot != keyWant {
				t.Fatalf("j=%d: got %q, want %q", j, keyGot, keyWant)
			}
			i.Prev()
		}
		if i.Valid() {
			t.Fatalf("Valid got true, want false")
		}
		if err := i.Close(); err != nil {
			t.Fatalf("got err=%v", err)
		}
	}
}

func TestBlockIter2(t *testing.T) {
	makeIkey := func(s string) base.InternalKey {
		j := strings.Index(s, ":")
		seqNum := base.ParseSeqNum(s[j+1:])
		return base.MakeInternalKey([]byte(s[:j]), seqNum, base.InternalKeyKindSet)
	}

	var blk []byte

	for _, r := range []int{1, 2, 3, 4} {
		t.Run(fmt.Sprintf("restart=%d", r), func(t *testing.T) {
			datadriven.RunTest(t, "testdata/rowblk_iter", func(t *testing.T, d *datadriven.TestData) string {
				switch d.Cmd {
				case "build":
					w := &Writer{RestartInterval: r}
					for _, e := range strings.Split(strings.TrimSpace(d.Input), ",") {
						w.Add(makeIkey(e), nil)
					}
					blk = w.Finish()
					return ""

				case "iter":
					var globalSeqNum uint64
					d.MaybeScanArgs(t, "globalSeqNum", &globalSeqNum)
					transforms := block.IterTransforms{SyntheticSeqNum: block.SyntheticSeqNum(globalSeqNum)}
					iter, err := NewIter(bytes.Compare, nil, blk, transforms)
					if err != nil {
						return err.Error()
					}

					return itertest.RunInternalIterCmd(t, d, iter, itertest.Condensed)

				default:
					return fmt.Sprintf("unknown command: %s", d.Cmd)
				}
			})
		})
	}
}

func TestBlockIterKeyStability(t *testing.T) {
	w := &Writer{RestartInterval: 1}
	expected := [][]byte{
		[]byte("apple"),
		[]byte("apricot"),
		[]byte("banana"),
	}
	for i := range expected {
		w.Add(base.InternalKey{UserKey: expected[i]}, nil)
	}
	blk := w.Finish()

	i, err := NewIter(bytes.Compare, nil, blk, block.NoTransforms)
	require.NoError(t, err)

	// Check that the supplied slice resides within the bounds of the block.
	check := func(v []byte) {
		t.Helper()
		begin := unsafe.Pointer(&v[0])
		end := unsafe.Pointer(uintptr(begin) + uintptr(len(v)))
		blockBegin := unsafe.Pointer(&blk[0])
		blockEnd := unsafe.Pointer(uintptr(blockBegin) + uintptr(len(blk)))
		if uintptr(begin) < uintptr(blockBegin) || uintptr(end) > uintptr(blockEnd) {
			t.Fatalf("key %p-%p resides outside of block %p-%p", begin, end, blockBegin, blockEnd)
		}
	}

	// Check that various means of iterating over the data match our expected
	// values. Note that this is only guaranteed because of the usage of a
	// restart-interval of 1 so that prefix compression was not performed.
	for j := range expected {
		keys := [][]byte{}
		for kv := i.SeekGE(expected[j], base.SeekGEFlagsNone); kv != nil; kv = i.Next() {
			check(kv.K.UserKey)
			keys = append(keys, kv.K.UserKey)
		}
		require.EqualValues(t, expected[j:], keys)
	}

	for j := range expected {
		keys := [][]byte{}
		for kv := i.SeekLT(expected[j], base.SeekLTFlagsNone); kv != nil; kv = i.Prev() {
			check(kv.K.UserKey)
			keys = append(keys, kv.K.UserKey)
		}
		for i, j := 0, len(keys)-1; i < j; i, j = i+1, j-1 {
			keys[i], keys[j] = keys[j], keys[i]
		}
		require.EqualValues(t, expected[:j], keys)
	}
}

// Regression test for a bug in blockIter.Next where it was failing to handle
// the case where it is switching from reverse to forward iteration. When that
// switch occurs we need to populate blockIter.fullKey so that prefix
// decompression works properly.
func TestBlockIterReverseDirections(t *testing.T) {
	w := &Writer{RestartInterval: 4}
	keys := [][]byte{
		[]byte("apple0"),
		[]byte("apple1"),
		[]byte("apple2"),
		[]byte("banana"),
		[]byte("carrot"),
	}
	for i := range keys {
		w.Add(base.InternalKey{UserKey: keys[i]}, nil)
	}
	blk := w.Finish()

	for targetPos := 0; targetPos < w.RestartInterval; targetPos++ {
		t.Run("", func(t *testing.T) {
			i, err := NewIter(bytes.Compare, nil, blk, block.NoTransforms)
			require.NoError(t, err)

			pos := 3
			if kv := i.SeekLT([]byte("carrot"), base.SeekLTFlagsNone); !bytes.Equal(keys[pos], kv.K.UserKey) {
				t.Fatalf("expected %s, but found %s", keys[pos], kv.K.UserKey)
			}
			for pos > targetPos {
				pos--
				if kv := i.Prev(); !bytes.Equal(keys[pos], kv.K.UserKey) {
					t.Fatalf("expected %s, but found %s", keys[pos], kv.K.UserKey)
				}
			}
			pos++
			if kv := i.Next(); !bytes.Equal(keys[pos], kv.K.UserKey) {
				t.Fatalf("expected %s, but found %s", keys[pos], kv.K.UserKey)
			}
		})
	}
}

// checker is a test helper that verifies that two different iterators running
// the same sequence of operations return the same result. To use correctly, pass
// the iter call directly as an arg to check(), i.e.:
//
// c.check(expect.SeekGE([]byte("apricot@2"), base.SeekGEFlagsNone))(got.SeekGE([]byte("apricot@2"), base.SeekGEFlagsNone))
// c.check(expect.Next())(got.Next())
//
// NB: the signature to check is not simply `check(eKey,eVal,gKey,gVal)` because
// `check(expect.Next(),got.Next())` does not compile.
type checker struct {
	t         *testing.T
	notValid  bool
	alsoCheck func()
}

func (c *checker) check(eKV *base.InternalKV) func(*base.InternalKV) {
	return func(gKV *base.InternalKV) {
		c.t.Helper()
		if eKV != nil {
			require.NotNil(c.t, gKV, "expected %q", eKV.K.UserKey)
			c.t.Logf("expected %q, got %q", eKV.K.UserKey, gKV.K.UserKey)
			require.Equal(c.t, eKV, gKV)
			c.notValid = false
		} else {
			c.t.Logf("expected nil, got %v", gKV)
			require.Nil(c.t, gKV)
			c.notValid = true
		}
		c.alsoCheck()
	}
}

func TestBlockSyntheticPrefix(t *testing.T) {
	for _, prefix := range []string{"_", "", "~", "fruits/"} {
		for _, restarts := range []int{1, 2, 3, 4, 10} {
			t.Run(fmt.Sprintf("prefix=%s/restarts=%d", prefix, restarts), func(t *testing.T) {

				elidedPrefixWriter, includedPrefixWriter := &Writer{RestartInterval: restarts}, &Writer{RestartInterval: restarts}
				keys := []string{
					"apple", "apricot", "banana",
					"grape", "orange", "peach",
					"pear", "persimmon",
				}
				for _, k := range keys {
					elidedPrefixWriter.Add(ikey(k), nil)
					includedPrefixWriter.Add(ikey(prefix+k), nil)
				}

				elidedPrefixBlock, includedPrefixBlock := elidedPrefixWriter.Finish(), includedPrefixWriter.Finish()

				expect, err := NewIter(bytes.Compare, nil, includedPrefixBlock, block.IterTransforms{})
				require.NoError(t, err)

				got, err := NewIter(bytes.Compare, nil, elidedPrefixBlock, block.IterTransforms{
					SyntheticPrefixAndSuffix: block.MakeSyntheticPrefixAndSuffix([]byte(prefix), nil),
				})
				require.NoError(t, err)

				c := checker{
					t: t,
					alsoCheck: func() {
						require.Equal(t, expect.Valid(), got.Valid())
						require.Equal(t, expect.Error(), got.Error())
					},
				}

				c.check(expect.First())(got.First())
				c.check(expect.Next())(got.Next())
				c.check(expect.Prev())(got.Prev())

				c.check(expect.SeekGE([]byte(prefix+"or"), base.SeekGEFlagsNone))(got.SeekGE([]byte(prefix+"or"), base.SeekGEFlagsNone))
				c.check(expect.SeekGE([]byte(prefix+"peach"), base.SeekGEFlagsNone))(got.SeekGE([]byte(prefix+"peach"), base.SeekGEFlagsNone))
				c.check(expect.Next())(got.Next())
				c.check(expect.Next())(got.Next())
				c.check(expect.Next())(got.Next())

				c.check(expect.SeekLT([]byte(prefix+"banana"), base.SeekLTFlagsNone))(got.SeekLT([]byte(prefix+"banana"), base.SeekLTFlagsNone))
				c.check(expect.SeekLT([]byte(prefix+"pomegranate"), base.SeekLTFlagsNone))(got.SeekLT([]byte(prefix+"pomegranate"), base.SeekLTFlagsNone))
				c.check(expect.SeekLT([]byte(prefix+"apple"), base.SeekLTFlagsNone))(got.SeekLT([]byte(prefix+"apple"), base.SeekLTFlagsNone))

				// Check Prefix-less seeks behave identically
				c.check(expect.First())(got.First())
				c.check(expect.SeekGE([]byte("peach"), base.SeekGEFlagsNone))(got.SeekGE([]byte("peach"), base.SeekGEFlagsNone))
				c.check(expect.Prev())(got.Prev())

				c.check(expect.Last())(got.Last())
				c.check(expect.SeekLT([]byte("peach"), base.SeekLTFlagsNone))(got.SeekLT([]byte("peach"), base.SeekLTFlagsNone))
				c.check(expect.Next())(got.Next())

				// Iterate past last key and call prev
				c.check(expect.SeekGE([]byte(prefix+"pomegranate"), base.SeekGEFlagsNone))(got.SeekGE([]byte(prefix+"pomegranate"), base.SeekGEFlagsNone))
				c.check(expect.Prev())(got.Prev())

				// Iterate before first key and call next
				c.check(expect.SeekLT([]byte(prefix+"ant"), base.SeekLTFlagsNone))(got.SeekLT([]byte(prefix+"ant"), base.SeekLTFlagsNone))
				c.check(expect.Next())(got.Next())
			})
		}
	}
}

func TestBlockSyntheticSuffix(t *testing.T) {
	expectedSuffix := "15"
	synthSuffix := []byte("@" + expectedSuffix)

	// Use testkeys.Comparer.Compare which approximates EngineCompare by ordering
	// multiple keys with same prefix in descending suffix order.
	cmp := testkeys.Comparer.Compare
	split := testkeys.Comparer.Split

	for _, restarts := range []int{1, 2, 3, 4, 10} {
		for _, replacePrefix := range []bool{false, true} {

			t.Run(fmt.Sprintf("restarts=%d;replacePrefix=%t", restarts, replacePrefix), func(t *testing.T) {

				suffixWriter, expectedSuffixWriter := &Writer{RestartInterval: restarts}, &Writer{RestartInterval: restarts}
				keys := []string{
					"apple@2", "apricot@2", "banana@13", "cantaloupe",
					"grape@2", "orange@14", "peach@4",
					"pear@1", "persimmon@4",
				}

				var synthPrefix []byte
				var prefixStr string
				if replacePrefix {
					prefixStr = "fruit/"
					synthPrefix = []byte(prefixStr)
				}
				for _, key := range keys {
					suffixWriter.Add(ikey(key), nil)
					replacedKey := strings.Split(key, "@")[0] + "@" + expectedSuffix
					expectedSuffixWriter.Add(ikey(prefixStr+replacedKey), nil)
				}

				suffixReplacedBlock := suffixWriter.Finish()
				expectedBlock := expectedSuffixWriter.Finish()

				expect, err := NewIter(cmp, split, expectedBlock, block.NoTransforms)
				require.NoError(t, err)

				got, err := NewIter(cmp, split, suffixReplacedBlock, block.IterTransforms{
					SyntheticPrefixAndSuffix: block.MakeSyntheticPrefixAndSuffix(synthPrefix, synthSuffix),
				})
				require.NoError(t, err)

				c := checker{t: t,
					alsoCheck: func() {
						require.Equal(t, expect.Valid(), got.Valid())
						require.Equal(t, expect.Error(), got.Error())
					}}

				c.check(expect.First())(got.First())
				c.check(expect.Next())(got.Next())
				c.check(expect.Prev())(got.Prev())

				// Try searching with a key that matches the target key before replacement
				c.check(expect.SeekGE([]byte(prefixStr+"apricot@2"), base.SeekGEFlagsNone))(got.SeekGE([]byte(prefixStr+"apricot@2"), base.SeekGEFlagsNone))
				c.check(expect.Next())(got.Next())

				// Try searching with a key that matches the target key after replacement
				c.check(expect.SeekGE([]byte(prefixStr+"orange@15"), base.SeekGEFlagsNone))(got.SeekGE([]byte(prefixStr+"orange@15"), base.SeekGEFlagsNone))
				c.check(expect.Next())(got.Next())
				c.check(expect.Next())(got.Next())

				// Try searching with a key that results in off by one handling
				c.check(expect.First())(got.First())
				c.check(expect.SeekGE([]byte(prefixStr+"grape@10"), base.SeekGEFlagsNone))(got.SeekGE([]byte(prefixStr+"grape@10"), base.SeekGEFlagsNone))
				c.check(expect.Next())(got.Next())
				c.check(expect.Next())(got.Next())

				// Try searching with a key with suffix greater than the replacement
				c.check(expect.SeekGE([]byte(prefixStr+"orange@16"), base.SeekGEFlagsNone))(got.SeekGE([]byte(prefixStr+"orange@16"), base.SeekGEFlagsNone))
				c.check(expect.Next())(got.Next())

				// Exhaust the iterator via searching
				c.check(expect.SeekGE([]byte(prefixStr+"persimmon@17"), base.SeekGEFlagsNone))(got.SeekGE([]byte(prefixStr+"persimmon@17"), base.SeekGEFlagsNone))

				// Ensure off by one handling works at end of iterator
				c.check(expect.SeekGE([]byte(prefixStr+"persimmon@10"), base.SeekGEFlagsNone))(got.SeekGE([]byte(prefixStr+"persimmon@10"), base.SeekGEFlagsNone))

				// Try reverse search with a key that matches the target key before replacement
				c.check(expect.SeekLT([]byte(prefixStr+"banana@13"), base.SeekLTFlagsNone))(got.SeekLT([]byte(prefixStr+"banana@13"), base.SeekLTFlagsNone))
				c.check(expect.Prev())(got.Prev())
				c.check(expect.Next())(got.Next())

				// Try reverse searching with a key that matches the target key after replacement
				c.check(expect.Last())(got.Last())
				c.check(expect.SeekLT([]byte(prefixStr+"apricot@15"), base.SeekLTFlagsNone))(got.SeekLT([]byte(prefixStr+"apricot@15"), base.SeekLTFlagsNone))

				// Try reverse searching with a key with suffix in between original and target replacement
				c.check(expect.Last())(got.Last())
				c.check(expect.SeekLT([]byte(prefixStr+"peach@10"), base.SeekLTFlagsNone))(got.SeekLT([]byte(prefixStr+"peach@10"), base.SeekLTFlagsNone))
				c.check(expect.Prev())(got.Prev())
				c.check(expect.Next())(got.Next())

				// Exhaust the iterator via reverse searching
				c.check(expect.SeekLT([]byte(prefixStr+"apple@17"), base.SeekLTFlagsNone))(got.SeekLT([]byte(prefixStr+"apple@17"), base.SeekLTFlagsNone))

				// Ensure off by one handling works at end of iterator
				c.check(expect.Last())(got.Last())
				c.check(expect.SeekLT([]byte(prefixStr+"apple@10"), base.SeekLTFlagsNone))(got.SeekLT([]byte(prefixStr+"apple@10"), base.SeekLTFlagsNone))

				// Try searching on the suffixless key
				c.check(expect.First())(got.First())
				c.check(expect.SeekGE([]byte(prefixStr+"cantaloupe"), base.SeekGEFlagsNone))(got.SeekGE([]byte(prefixStr+"cantaloupe"), base.SeekGEFlagsNone))

				c.check(expect.First())(got.First())
				c.check(expect.SeekGE([]byte(prefixStr+"cantaloupe@16"), base.SeekGEFlagsNone))(got.SeekGE([]byte(prefixStr+"cantaloupe@16"), base.SeekGEFlagsNone))

				c.check(expect.First())(got.First())
				c.check(expect.SeekGE([]byte(prefixStr+"cantaloupe@14"), base.SeekGEFlagsNone))(got.SeekGE([]byte(prefixStr+"cantaloupe@14"), base.SeekGEFlagsNone))

				c.check(expect.Last())(got.Last())
				c.check(expect.SeekLT([]byte(prefixStr+"cantaloupe"), base.SeekLTFlagsNone))(got.SeekLT([]byte(prefixStr+"cantaloupe"), base.SeekLTFlagsNone))

				c.check(expect.Last())(got.Last())
				c.check(expect.SeekLT([]byte(prefixStr+"cantaloupe10"), base.SeekLTFlagsNone))(got.SeekLT([]byte(prefixStr+"cantaloupe10"), base.SeekLTFlagsNone))

				c.check(expect.Last())(got.Last())
				c.check(expect.SeekLT([]byte(prefixStr+"cantaloupe16"), base.SeekLTFlagsNone))(got.SeekLT([]byte(prefixStr+"cantaloupe16"), base.SeekLTFlagsNone))
			})
		}
	}
}

func ikey(s string) base.InternalKey {
	return base.InternalKey{UserKey: []byte(s)}
}
