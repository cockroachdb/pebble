// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build amd64 || arm64 || arm64be || ppc64 || ppc64le || mips64 || mips64le || s390x || sparc64 || riscv64 || loong64

package rowblk

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/internal/buildtags"
	"github.com/cockroachdb/pebble/v2/sstable/block"
	"github.com/stretchr/testify/require"
)

// TestSingularKVBlockRestartsOverflow tests a scenario where a large key-value
// pair is written to a block, such that the total block size exceeds 4GiB. This
// works becasue the restart table never needs to encode a restart offset beyond
// the 1st key-value pair. The offset of the restarts table itself may exceed
// 2^32-1 but the iterator takes care to support this.
func TestSingularKVBlockRestartsOverflow(t *testing.T) {
	_, isCI := os.LookupEnv("CI")
	if isCI {
		t.Skip("Skipping test: requires too much memory for CI now.")
	}
	if buildtags.SlowBuild {
		t.Skip("Skipping test: requires too much memory for instrumented builds")
	}

	// Test that SeekGE() and SeekLT() function correctly
	// with a singular large KV > 2GB.

	const largeKeySize = 2 << 30   // 2GB key size
	const largeValueSize = 2 << 30 // 2GB value size

	largeKey := bytes.Repeat([]byte("k"), largeKeySize)
	largeValue := bytes.Repeat([]byte("v"), largeValueSize)

	writer := &Writer{RestartInterval: 1}
	require.NoError(t, writer.Add(base.InternalKey{UserKey: largeKey}, largeValue))
	blockData := writer.Finish()
	iter, err := NewIter(bytes.Compare, nil, nil, blockData, block.NoTransforms)
	require.NoError(t, err, "failed to create iterator for block")

	// Ensure that SeekGE() does not raise panic due to integer overflow
	// indexing problems.
	kv := iter.SeekGE(largeKey, base.SeekGEFlagsNone)

	// Ensure that SeekGE() finds the correct KV
	require.NotNil(t, kv, "failed to find the key")
	require.Equal(t, largeKey, kv.K.UserKey, "unexpected key")
	require.Equal(t, largeValue, kv.InPlaceValue(), "unexpected value")

	// Ensure that SeekGE() does not raise panic due to integer overflow
	// indexing problems.
	kv = iter.SeekLT([]byte("z"), base.SeekLTFlagsNone)

	// Ensure that SeekLT() finds the correct KV
	require.NotNil(t, kv, "failed to find the key")
	require.Equal(t, largeKey, kv.K.UserKey, "unexpected key")
	require.Equal(t, largeValue, kv.InPlaceValue(), "unexpected value")
}

// TestExceedingMaximumRestartOffset tests that writing a block that exceeds the
// maximum restart offset errors.
func TestExceedingMaximumRestartOffset(t *testing.T) {
	_, isCI := os.LookupEnv("CI")
	if isCI {
		t.Skip("Skipping test: requires too much memory for CI now.")
	}
	if buildtags.SlowBuild {
		t.Skip("Skipping test: requires too much memory for instrumented builds")
	}

	// Test that writing to a block that is already >= 2GiB
	// returns an error.

	// Adding 512 KVs each with size 4MiB will create a block
	// size of >= 2GiB.
	const numKVs = 512
	const valueSize = (4 << 20)

	type KVTestPair struct {
		key   []byte
		value []byte
	}

	kvTestPairs := make([]KVTestPair, numKVs)
	value4MB := bytes.Repeat([]byte("a"), valueSize)
	for i := 0; i < numKVs; i++ {
		key := fmt.Sprintf("key-%04d", i)
		kvTestPairs[i] = KVTestPair{key: []byte(key), value: value4MB}
	}
	writer := &Writer{RestartInterval: 1}
	for _, KVPair := range kvTestPairs {
		require.NoError(t, writer.Add(base.InternalKey{UserKey: KVPair.key}, KVPair.value))
	}

	// Check that buffer is larger than 2GiB.
	require.Greater(t, len(writer.buf), MaximumRestartOffset)

	// Check that an error is returned after the final write after the 2GiB
	// threshold has been crossed
	err := writer.Add(base.InternalKey{UserKey: []byte("arbitrary-last-key")}, []byte("arbitrary-last-value"))
	require.NotNil(t, err)
	require.True(t, errors.Is(err, ErrBlockTooBig))
}

// TestMultipleKVBlockRestartsOverflow tests that SeekGE() works when
// iter.restarts is greater than math.MaxUint32 for multiple KVs. Test writes
// <2GiB to the block and then 4GiB causing iter.restarts to be an int >
// math.MaxUint32. Reaching just shy of 2GiB before adding 4GiB allows the
// final write to succeed without surpassing 2GiB limit. Then verify that
// SeekGE() returns valid output without integer overflow.
//
// Although the block exceeds math.MaxUint32 bytes, no individual KV pair has an
// offset that exceeds MaximumRestartOffset.
func TestMultipleKVBlockRestartsOverflow(t *testing.T) {
	if _, isCI := os.LookupEnv("CI"); isCI {
		t.Skip("Skipping test: requires too much memory for CI.")
	}
	if buildtags.SlowBuild {
		t.Skip("Skipping test: requires too much memory for instrumented builds")
	}

	// Write just shy of 2GiB to the block 511 * 4MiB < 2GiB.
	const numKVs = 511
	const valueSize = 4 * (1 << 20)
	const fourGB = 4 * (1 << 30)

	type KVTestPair struct {
		key   []byte
		value []byte
	}

	kvTestPairs := make([]KVTestPair, numKVs)
	value4MB := bytes.Repeat([]byte("a"), valueSize)
	for i := 0; i < numKVs; i++ {
		key := fmt.Sprintf("key-%04d", i)
		kvTestPairs[i] = KVTestPair{key: []byte(key), value: value4MB}
	}

	writer := &Writer{RestartInterval: 1}
	for _, KVPair := range kvTestPairs {
		writer.Add(base.InternalKey{UserKey: KVPair.key}, KVPair.value)
	}

	// Add the 4GiB KV, causing iter.restarts >= math.MaxUint32.
	// Ensure that SeekGE() works thereafter without integer
	// overflows.
	writer.Add(base.InternalKey{UserKey: []byte("large-kv")}, bytes.Repeat([]byte("v"), fourGB))

	blockData := writer.Finish()
	iter, err := NewIter(bytes.Compare, nil, nil, blockData, block.NoTransforms)
	require.NoError(t, err, "failed to create iterator for block")
	require.Greater(t, int64(iter.restarts), int64(MaximumRestartOffset), "check iter.restarts > 2GiB")
	require.Greater(t, int64(iter.restarts), int64(math.MaxUint32), "check iter.restarts > 2^32-1")

	for i := 0; i < numKVs; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		value := bytes.Repeat([]byte("a"), valueSize)
		kv := iter.SeekGE(key, base.SeekGEFlagsNone)
		require.NotNil(t, kv, "failed to find the large key")
		require.Equal(t, key, kv.K.UserKey, "unexpected key")
		require.Equal(t, value, kv.InPlaceValue(), "unexpected value")
	}
}
