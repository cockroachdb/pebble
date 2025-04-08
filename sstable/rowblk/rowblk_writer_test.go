// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rowblk

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/stretchr/testify/require"
)

func TestBlockWriterClear(t *testing.T) {
	w := Writer{RestartInterval: 16}
	require.NoError(t, w.Add(ikey("apple"), nil))
	require.NoError(t, w.Add(ikey("apricot"), nil))
	require.NoError(t, w.Add(ikey("banana"), nil))

	w.Reset()

	// Once a block is cleared, we expect its fields to be cleared, but we expect
	// it to keep its allocated byte slices.
	require.Equal(t, w.RestartInterval, 0)
	require.Equal(t, w.nEntries, 0)
	require.Equal(t, w.nextRestart, 0)
	require.Equal(t, len(w.buf), 0)
	require.Equal(t, len(w.restarts), 0)
	require.Equal(t, len(w.curKey), 0)
	require.Equal(t, len(w.prevKey), 0)
	require.Equal(t, len(w.curValue), 0)
	require.Equal(t, w.tmp, [4]byte{})

	// Make sure that we didn't lose the allocated byte slices.
	require.True(t, cap(w.buf) > 0)
	require.True(t, cap(w.restarts) > 0)
	require.True(t, cap(w.curKey) > 0)
	require.True(t, cap(w.prevKey) > 0)
	require.True(t, cap(w.curValue) > 0)
}

func TestBlockWriter(t *testing.T) {
	w := &Writer{RestartInterval: 16}
	require.NoError(t, w.AddRawString("apple", nil))
	require.NoError(t, w.AddRawString("apricot", nil))
	require.NoError(t, w.AddRawString("banana", nil))
	block := w.Finish()

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
	w := &Writer{RestartInterval: 2}
	curKey := func() string {
		return string(base.DecodeInternalKey(w.curKey).UserKey)
	}
	addAdapter := func(
		key base.InternalKey,
		value []byte,
		addValuePrefix bool,
		valuePrefix block.ValuePrefix,
		setHasSameKeyPrefix bool) {
		err := w.AddWithOptionalValuePrefix(
			key, false, value, len(key.UserKey), addValuePrefix, valuePrefix, setHasSameKeyPrefix)
		require.NoError(t, err)
	}
	addAdapter(
		ikey("apple"), []byte("red"), false, 0, true)
	require.Equal(t, "apple", curKey())
	require.Equal(t, "red", string(w.CurValue()))
	addAdapter(
		ikey("apricot"), []byte("orange"), true, '\xff', false)
	require.Equal(t, "apricot", curKey())
	require.Equal(t, "orange", string(w.CurValue()))
	// Even though this call has setHasSameKeyPrefix=true, the previous call,
	// which was after the last restart set it to false. So the restart encoded
	// with banana has this cumulative bit set to false.
	addAdapter(
		ikey("banana"), []byte("yellow"), true, '\x00', true)
	require.Equal(t, "banana", curKey())
	require.Equal(t, "yellow", string(w.CurValue()))
	addAdapter(
		ikey("cherry"), []byte("red"), false, 0, true)
	require.Equal(t, "cherry", curKey())
	require.Equal(t, "red", string(w.CurValue()))
	// All intervening calls has setHasSameKeyPrefix=true, so the cumulative bit
	// will be set to true in this restart.
	addAdapter(
		ikey("mango"), []byte("juicy"), false, 0, true)
	require.Equal(t, "mango", curKey())
	require.Equal(t, "juicy", string(w.CurValue()))

	blk := w.Finish()

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
	if !bytes.Equal(expected, blk) {
		t.Fatalf("expected\n%x\nfound\n%x", expected, blk)
	}
}
