// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/itertest"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func ikey(s string) InternalKey {
	return InternalKey{UserKey: []byte(s)}
}

func TestBlockWriter(t *testing.T) {
	w := &rawBlockWriter{
		blockWriter: blockWriter{restartInterval: 16},
	}
	w.add(ikey("apple"), nil)
	w.add(ikey("apricot"), nil)
	w.add(ikey("banana"), nil)
	block := w.finish()

	expected := []byte(
		"\x00\x05\x00apple" +
			"\x02\x05\x00ricot" +
			"\x00\x06\x00banana" +
			"\x00\x00\x00\x00\x01\x00\x00\x00")
	if !bytes.Equal(expected, block) {
		t.Fatalf("expected\n%q\nfound\n%q", expected, block)
	}
}

func TestBlockWriterWithPrefix(t *testing.T) {
	w := &rawBlockWriter{
		blockWriter: blockWriter{restartInterval: 2},
	}
	curKey := func() string {
		return string(base.DecodeInternalKey(w.curKey).UserKey)
	}
	addAdapter := func(
		key InternalKey,
		value []byte,
		addValuePrefix bool,
		valuePrefix valuePrefix,
		setHasSameKeyPrefix bool) {
		w.addWithOptionalValuePrefix(
			key, false, value, len(key.UserKey), addValuePrefix, valuePrefix, setHasSameKeyPrefix)
	}
	addAdapter(
		ikey("apple"), []byte("red"), false, 0, true)
	require.Equal(t, "apple", curKey())
	require.Equal(t, "red", string(w.curValue))
	addAdapter(
		ikey("apricot"), []byte("orange"), true, '\xff', false)
	require.Equal(t, "apricot", curKey())
	require.Equal(t, "orange", string(w.curValue))
	// Even though this call has setHasSameKeyPrefix=true, the previous call,
	// which was after the last restart set it to false. So the restart encoded
	// with banana has this cumulative bit set to false.
	addAdapter(
		ikey("banana"), []byte("yellow"), true, '\x00', true)
	require.Equal(t, "banana", curKey())
	require.Equal(t, "yellow", string(w.curValue))
	addAdapter(
		ikey("cherry"), []byte("red"), false, 0, true)
	require.Equal(t, "cherry", curKey())
	require.Equal(t, "red", string(w.curValue))
	// All intervening calls has setHasSameKeyPrefix=true, so the cumulative bit
	// will be set to true in this restart.
	addAdapter(
		ikey("mango"), []byte("juicy"), false, 0, true)
	require.Equal(t, "mango", curKey())
	require.Equal(t, "juicy", string(w.curValue))

	block := w.finish()

	expected := []byte(
		"\x00\x0d\x03apple\x00\x00\x00\x00\x00\x00\x00\x00red" +
			"\x02\x0d\x07ricot\x00\x00\x00\x00\x00\x00\x00\x00\xfforange" +
			"\x00\x0e\x07banana\x00\x00\x00\x00\x00\x00\x00\x00\x00yellow" +
			"\x00\x0e\x03cherry\x00\x00\x00\x00\x00\x00\x00\x00red" +
			"\x00\x0d\x05mango\x00\x00\x00\x00\x00\x00\x00\x00juicy" +
			// Restarts are:
			// 00000000 (restart at apple), 2a000000 (restart at banana), 56000080 (restart at mango)
			// 03000000 (number of restart, i.e., 3). The restart at mango has 1 in the most significant
			// bit of the uint32, so the last byte in the little endian encoding is \x80.
			"\x00\x00\x00\x00\x2a\x00\x00\x00\x56\x00\x00\x80\x03\x00\x00\x00")
	if !bytes.Equal(expected, block) {
		t.Fatalf("expected\n%x\nfound\n%x", expected, block)
	}
}

func testBlockCleared(t *testing.T, w, b *blockWriter) {
	require.Equal(t, w.restartInterval, b.restartInterval)
	require.Equal(t, w.nEntries, b.nEntries)
	require.Equal(t, w.nextRestart, b.nextRestart)
	require.Equal(t, len(w.buf), len(b.buf))
	require.Equal(t, len(w.restarts), len(b.restarts))
	require.Equal(t, len(w.curKey), len(b.curKey))
	require.Equal(t, len(w.prevKey), len(b.prevKey))
	require.Equal(t, len(w.curValue), len(b.curValue))
	require.Equal(t, w.tmp, b.tmp)

	// Make sure that we didn't lose the allocated byte slices.
	require.True(t, cap(w.buf) > 0 && cap(b.buf) == 0)
	require.True(t, cap(w.restarts) > 0 && cap(b.restarts) == 0)
	require.True(t, cap(w.curKey) > 0 && cap(b.curKey) == 0)
	require.True(t, cap(w.prevKey) > 0 && cap(b.prevKey) == 0)
	require.True(t, cap(w.curValue) > 0 && cap(b.curValue) == 0)
}

func TestBlockClear(t *testing.T) {
	w := blockWriter{restartInterval: 16}
	w.add(ikey("apple"), nil)
	w.add(ikey("apricot"), nil)
	w.add(ikey("banana"), nil)

	w.clear()

	// Once a block is cleared, we expect its fields to be cleared, but we expect
	// it to keep its allocated byte slices.
	b := blockWriter{}
	testBlockCleared(t, &w, &b)
}

func TestInvalidInternalKeyDecoding(t *testing.T) {
	// Invalid keys since they don't have an 8 byte trailer.
	testCases := []string{
		"",
		"\x01\x02\x03\x04\x05\x06\x07",
		"foo",
	}
	for _, tc := range testCases {
		i := blockIter{}
		i.decodeInternalKey([]byte(tc))
		require.Nil(t, i.ikv.K.UserKey)
		require.Equal(t, base.Trailer(InternalKeyKindInvalid), i.ikv.K.Trailer)
	}
}

func TestBlockIter(t *testing.T) {
	// k is a block that maps three keys "apple", "apricot", "banana" to empty strings.
	k := block([]byte(
		"\x00\x05\x00apple" +
			"\x02\x05\x00ricot" +
			"\x00\x06\x00banana" +
			"\x00\x00\x00\x00\x01\x00\x00\x00"))
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
		i, err := newRawBlockIter(bytes.Compare, k)
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
		i, err := newRawBlockIter(bytes.Compare, k)
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
	makeIkey := func(s string) InternalKey {
		j := strings.Index(s, ":")
		seqNum := base.ParseSeqNum(s[j+1:])
		return base.MakeInternalKey([]byte(s[:j]), seqNum, InternalKeyKindSet)
	}

	var block []byte

	for _, r := range []int{1, 2, 3, 4} {
		t.Run(fmt.Sprintf("restart=%d", r), func(t *testing.T) {
			datadriven.RunTest(t, "testdata/block", func(t *testing.T, d *datadriven.TestData) string {
				switch d.Cmd {
				case "build":
					w := &blockWriter{restartInterval: r}
					for _, e := range strings.Split(strings.TrimSpace(d.Input), ",") {
						w.add(makeIkey(e), nil)
					}
					block = w.finish()
					return ""

				case "iter":
					globalSeqNum, err := scanGlobalSeqNum(d)
					transforms := IterTransforms{SyntheticSeqNum: SyntheticSeqNum(globalSeqNum)}
					if err != nil {
						return err.Error()
					}
					iter, err := newBlockIter(bytes.Compare, nil, block, transforms)
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
	w := &blockWriter{restartInterval: 1}
	expected := [][]byte{
		[]byte("apple"),
		[]byte("apricot"),
		[]byte("banana"),
	}
	for i := range expected {
		w.add(InternalKey{UserKey: expected[i]}, nil)
	}
	block := w.finish()

	i, err := newBlockIter(bytes.Compare, nil, block, NoTransforms)
	require.NoError(t, err)

	// Check that the supplied slice resides within the bounds of the block.
	check := func(v []byte) {
		t.Helper()
		begin := unsafe.Pointer(&v[0])
		end := unsafe.Pointer(uintptr(begin) + uintptr(len(v)))
		blockBegin := unsafe.Pointer(&block[0])
		blockEnd := unsafe.Pointer(uintptr(blockBegin) + uintptr(len(block)))
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
	w := &blockWriter{restartInterval: 4}
	keys := [][]byte{
		[]byte("apple0"),
		[]byte("apple1"),
		[]byte("apple2"),
		[]byte("banana"),
		[]byte("carrot"),
	}
	for i := range keys {
		w.add(InternalKey{UserKey: keys[i]}, nil)
	}
	block := w.finish()

	for targetPos := 0; targetPos < w.restartInterval; targetPos++ {
		t.Run("", func(t *testing.T) {
			i, err := newBlockIter(bytes.Compare, nil, block, NoTransforms)
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

				elidedPrefixWriter, includedPrefixWriter := &blockWriter{restartInterval: restarts}, &blockWriter{restartInterval: restarts}
				keys := []string{
					"apple", "apricot", "banana",
					"grape", "orange", "peach",
					"pear", "persimmon",
				}
				for _, k := range keys {
					elidedPrefixWriter.add(ikey(k), nil)
					includedPrefixWriter.add(ikey(prefix+k), nil)
				}

				elidedPrefixBlock, includedPrefixBlock := elidedPrefixWriter.finish(), includedPrefixWriter.finish()

				expect, err := newBlockIter(bytes.Compare, nil, includedPrefixBlock, IterTransforms{})
				require.NoError(t, err)

				got, err := newBlockIter(bytes.Compare, nil, elidedPrefixBlock, IterTransforms{SyntheticPrefix: []byte(prefix)})
				require.NoError(t, err)

				c := checker{
					t: t,
					alsoCheck: func() {
						require.Equal(t, expect.valid(), got.valid())
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

				suffixWriter, expectedSuffixWriter := &blockWriter{restartInterval: restarts}, &blockWriter{restartInterval: restarts}
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
					suffixWriter.add(ikey(key), nil)
					replacedKey := strings.Split(key, "@")[0] + "@" + expectedSuffix
					expectedSuffixWriter.add(ikey(prefixStr+replacedKey), nil)
				}

				suffixReplacedBlock := suffixWriter.finish()
				expectedBlock := expectedSuffixWriter.finish()

				expect, err := newBlockIter(cmp, split, expectedBlock, NoTransforms)
				require.NoError(t, err)

				got, err := newBlockIter(cmp, split, suffixReplacedBlock, IterTransforms{SyntheticSuffix: synthSuffix, SyntheticPrefix: synthPrefix})
				require.NoError(t, err)

				c := checker{t: t,
					alsoCheck: func() {
						require.Equal(t, expect.valid(), got.valid())
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

var (
	benchSynthSuffix = []byte("@15")
	benchPrefix      = []byte("2_")

	// Use testkeys.Comparer.Compare which approximates EngineCompare by ordering
	// multiple keys with same prefix in descending suffix order.
	benchCmp   = testkeys.Comparer.Compare
	benchSplit = testkeys.Comparer.Split
)

// choosOrigSuffix randomly chooses a suffix that is either 1 or 2 bytes large.
// This ensures we benchmark when suffix replacement adds a larger suffix.
func chooseOrigSuffix(rng *rand.Rand) []byte {
	origSuffix := []byte("@10")
	if rng.Intn(10)%2 == 0 {
		origSuffix = []byte("@9")
	}
	return origSuffix
}

// createBenchBlock writes a block of keys and outputs a list of keys that will
// be surfaced from the block, and the expected synthetic suffix and prefix the
// block should be read with.
func createBenchBlock(
	blockSize int, w *blockWriter, rng *rand.Rand, withSyntheticPrefix, withSyntheticSuffix bool,
) ([][]byte, []byte, []byte) {

	origSuffix := chooseOrigSuffix(rng)
	var ikey InternalKey
	var readKeys [][]byte

	var writtenPrefix []byte
	if !withSyntheticPrefix {
		// If the keys will not be read with a synthetic prefix, write the prefix to
		// the block for a more comparable benchmark comparison between a block iter
		// with and without prefix synthesis.
		writtenPrefix = benchPrefix
	}
	for i := 0; w.estimatedSize() < blockSize; i++ {
		key := []byte(fmt.Sprintf("%s%05d%s", string(writtenPrefix), i, origSuffix))
		ikey.UserKey = key
		w.add(ikey, nil)
		var readKey []byte
		if withSyntheticPrefix {
			readKey = append(readKey, benchPrefix...)
		}
		readKey = append(readKey, key...)
		readKeys = append(readKeys, readKey)
	}

	var syntheticSuffix []byte
	var syntheticPrefix []byte
	if withSyntheticSuffix {
		syntheticSuffix = benchSynthSuffix
	}
	if withSyntheticPrefix {
		syntheticPrefix = []byte(benchPrefix)
	}
	return readKeys, syntheticPrefix, syntheticSuffix
}

func BenchmarkBlockIterSeekGE(b *testing.B) {
	const blockSize = 32 << 10
	for _, withSyntheticPrefix := range []bool{false, true} {
		for _, withSyntheticSuffix := range []bool{false, true} {
			for _, restartInterval := range []int{16} {
				b.Run(fmt.Sprintf("syntheticPrefix=%t;syntheticSuffix=%t;restart=%d", withSyntheticPrefix, withSyntheticSuffix, restartInterval),
					func(b *testing.B) {
						w := &blockWriter{
							restartInterval: restartInterval,
						}
						rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

						keys, syntheticPrefix, syntheticSuffix := createBenchBlock(blockSize, w, rng, withSyntheticPrefix, withSyntheticSuffix)

						it, err := newBlockIter(benchCmp, benchSplit, w.finish(), IterTransforms{SyntheticSuffix: syntheticSuffix, SyntheticPrefix: syntheticPrefix})
						if err != nil {
							b.Fatal(err)
						}
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							k := keys[rng.Intn(len(keys))]
							it.SeekGE(k, base.SeekGEFlagsNone)
							if testing.Verbose() {
								if !it.valid() && !withSyntheticSuffix {
									b.Fatal("expected to find key")
								}
								if !bytes.Equal(k, it.Key().UserKey) && !withSyntheticSuffix {
									b.Fatalf("expected %s, but found %s", k, it.Key().UserKey)
								}
							}
						}
					})
			}
		}
	}
}

func BenchmarkBlockIterSeekLT(b *testing.B) {
	const blockSize = 32 << 10
	for _, withSyntheticPrefix := range []bool{false, true} {
		for _, withSyntheticSuffix := range []bool{false, true} {
			for _, restartInterval := range []int{16} {
				b.Run(fmt.Sprintf("syntheticPrefix=%t;syntheticSuffix=%t;restart=%d", withSyntheticPrefix, withSyntheticSuffix, restartInterval),
					func(b *testing.B) {
						w := &blockWriter{
							restartInterval: restartInterval,
						}
						rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

						keys, syntheticPrefix, syntheticSuffix := createBenchBlock(blockSize, w, rng, withSyntheticPrefix, withSyntheticSuffix)

						it, err := newBlockIter(benchCmp, benchSplit, w.finish(), IterTransforms{SyntheticSuffix: syntheticSuffix, SyntheticPrefix: syntheticPrefix})
						if err != nil {
							b.Fatal(err)
						}
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							j := rng.Intn(len(keys))
							it.SeekLT(keys[j], base.SeekLTFlagsNone)
							if testing.Verbose() {
								if j == 0 {
									if it.valid() && !withSyntheticSuffix {
										b.Fatal("unexpected key")
									}
								} else {
									if !it.valid() && !withSyntheticSuffix {
										b.Fatal("expected to find key")
									}
									k := keys[j-1]
									if !bytes.Equal(k, it.Key().UserKey) && !withSyntheticSuffix {
										b.Fatalf("expected %s, but found %s", k, it.Key().UserKey)
									}
								}
							}
						}
					})
			}
		}
	}
}

func BenchmarkBlockIterNext(b *testing.B) {
	const blockSize = 32 << 10
	for _, withSyntheticPrefix := range []bool{false, true} {
		for _, withSyntheticSuffix := range []bool{false, true} {
			for _, restartInterval := range []int{16} {
				b.Run(fmt.Sprintf("syntheticPrefix=%t;syntheticSuffix=%t;restart=%d", withSyntheticPrefix, withSyntheticSuffix, restartInterval),
					func(b *testing.B) {
						w := &blockWriter{
							restartInterval: restartInterval,
						}
						rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

						_, syntheticPrefix, syntheticSuffix := createBenchBlock(blockSize, w, rng, withSyntheticPrefix, withSyntheticSuffix)

						it, err := newBlockIter(benchCmp, benchSplit, w.finish(), IterTransforms{SyntheticSuffix: syntheticSuffix, SyntheticPrefix: syntheticPrefix})
						if err != nil {
							b.Fatal(err)
						}

						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							if !it.valid() {
								it.First()
							}
							it.Next()
						}
					})
			}
		}
	}
}

func BenchmarkBlockIterPrev(b *testing.B) {
	const blockSize = 32 << 10
	for _, withSyntheticPrefix := range []bool{false, true} {
		for _, withSyntheticSuffix := range []bool{false, true} {
			for _, restartInterval := range []int{16} {
				b.Run(fmt.Sprintf("syntheticPrefix=%t;syntheticSuffix=%t;restart=%d", withSyntheticPrefix, withSyntheticSuffix, restartInterval),
					func(b *testing.B) {
						w := &blockWriter{
							restartInterval: restartInterval,
						}
						rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

						_, syntheticPrefix, syntheticSuffix := createBenchBlock(blockSize, w, rng, withSyntheticPrefix, withSyntheticSuffix)

						it, err := newBlockIter(benchCmp, benchSplit, w.finish(), IterTransforms{SyntheticSuffix: syntheticSuffix, SyntheticPrefix: syntheticPrefix})
						if err != nil {
							b.Fatal(err)
						}

						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							if !it.valid() {
								it.Last()
							}
							it.Prev()
						}
					})
			}
		}
	}
}
