// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cockroachkvs

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/crbytes"
	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/crlib/testutils/require"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/cockroachdb/pebble/internal/treeprinter"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/blockiter"
	"github.com/cockroachdb/pebble/sstable/colblk"
)

// TestKeySchema tests the cockroachKeyWriter and cockroachKeySeeker.
func TestKeySchema(t *testing.T) {
	for _, file := range []string{"suffix_types", "block_encoding", "seek"} {
		t.Run(file, func(t *testing.T) {
			runDataDrivenTest(t, fmt.Sprintf("testdata/%s", file))
		})
	}
}

func runDataDrivenTest(t *testing.T, path string) {
	var blockData []byte
	var e colblk.DataBlockEncoder
	e.Init(colblk.ColumnFormatv1, &KeySchema)
	var iter colblk.DataBlockIter
	iter.InitOnce(colblk.ColumnFormatv1, &KeySchema, &Comparer, nil)

	datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "init":
			e.Reset()
			var buf []byte
			for _, l := range crstrings.Lines(td.Input) {
				key, value := parseInternalKV(l)
				kcmp := e.KeyWriter.ComparePrev(key.UserKey)
				e.Add(key, value, 0, kcmp, false /* isObsolete */, base.KVMeta{})
				buf = e.MaterializeLastUserKey(buf[:0])
				if !Comparer.Equal(key.UserKey, buf) {
					td.Fatalf(t, "incorrect MaterializeLastKey: %s instead of %s", formatUserKey(buf), formatUserKey(key.UserKey))
				}
			}
			numRows := e.Rows()
			size := e.Size()
			blockData, _ = e.Finish(numRows, size)
			require.Equal(t, size, len(blockData))
			return fmt.Sprintf("%d rows, total size %dB", numRows, size)

		case "describe":
			var d colblk.DataBlockDecoder
			d.Init(colblk.ColumnFormatv1, &KeySchema, blockData)
			f := binfmt.New(blockData)
			tp := treeprinter.New()
			d.Describe(f, tp)
			return tp.String()

		case "suffix-types":
			var d colblk.DataBlockDecoder
			d.Init(colblk.ColumnFormatv1, &KeySchema, blockData)
			var ks cockroachKeySeeker
			ks.init(&d)
			return fmt.Sprintf("suffix-types: %s", ks.suffixTypes)

		case "keys":
			var d colblk.DataBlockDecoder
			d.Init(colblk.ColumnFormatv1, &KeySchema, blockData)
			require.NoError(t, iter.Init(&d, blockiter.Transforms{}))
			defer iter.Close()
			var buf bytes.Buffer
			var prevKey base.InternalKey
			for kv := iter.First(); kv != nil; kv = iter.Next() {
				fmt.Fprintf(&buf, "%s", formatInternalKV(*kv))
				if prevKey.UserKey != nil && base.InternalCompare(Comparer.Compare, prevKey, kv.K) != -1 {
					buf.WriteString(" !!! OUT OF ORDER KEY !!!")
				}
				buf.WriteString("\n")
				prevKey = kv.K.Clone()
			}
			return buf.String()

		case "seek":
			var d colblk.DataBlockDecoder
			d.Init(colblk.ColumnFormatv1, &KeySchema, blockData)
			require.NoError(t, iter.Init(&d, blockiter.Transforms{}))
			defer iter.Close()
			var buf strings.Builder
			for _, l := range crstrings.Lines(td.Input) {
				key := parseUserKey(l)
				fmt.Fprintf(&buf, "%s: ", formatUserKey(key))
				kv := iter.SeekGE(key, base.SeekGEFlagsNone)
				require.NoError(t, iter.Error())
				if kv == nil {
					buf.WriteString(".\n")
				} else {
					fmt.Fprintf(&buf, "%s\n", formatInternalKV(*kv))
				}
			}
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestKeySchema_RandomKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rng := rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))
	maxUserKeyLen := testutils.RandIntInRange(rng, 2, 10)
	keys := make([][]byte, testutils.RandIntInRange(rng, 1, 1000))
	for i := range keys {
		keys[i] = randomSerializedEngineKey(rng, maxUserKeyLen)
	}
	slices.SortFunc(keys, Compare)

	var enc colblk.DataBlockEncoder
	enc.Init(colblk.ColumnFormatv1, &KeySchema)
	for i := range keys {
		ikey := pebble.InternalKey{
			UserKey: keys[i],
			Trailer: pebble.MakeInternalKeyTrailer(0, pebble.InternalKeyKindSet),
		}
		enc.Add(ikey, keys[i], block.InPlaceValuePrefix(false), enc.KeyWriter.ComparePrev(keys[i]),
			false /* isObsolete */, base.KVMeta{})
	}
	blk, _ := enc.Finish(len(keys), enc.Size())
	blk = crbytes.CopyAligned(blk)

	var dec colblk.DataBlockDecoder
	dec.Init(colblk.ColumnFormatv1, &KeySchema, blk)
	var it colblk.DataBlockIter
	it.InitOnce(colblk.ColumnFormatv1, &KeySchema, &Comparer, nil)
	require.NoError(t, it.Init(&dec, blockiter.NoTransforms))
	// Ensure that a scan across the block finds all the relevant keys.
	var valBuf []byte
	for k, kv := 0, it.First(); kv != nil; k, kv = k+1, it.Next() {
		require.True(t, Equal(keys[k], kv.K.UserKey))
		require.Equal(t, 0, Compare(keys[k], kv.K.UserKey))
		// Note we allow the key read from the block to be physically different,
		// because the above randomization generates point keys with the
		// synthetic bit encoding. However the materialized key should not be
		// longer than the original key, because we depend on the max key length
		// during writing bounding the key length during reading.
		if n := len(kv.K.UserKey); n > len(keys[k]) {
			t.Fatalf("key %q is longer than original key %q", kv.K.UserKey, keys[k])
		}
		validateEngineKey.MustValidate(kv.K.UserKey)

		// We write keys[k] as the value too, so check that it's verbatim equal.
		value, callerOwned, err := kv.Value(valBuf)
		require.NoError(t, err)
		require.Equal(t, keys[k], value)
		if callerOwned {
			valBuf = value
		}
	}
	// Ensure that seeking to each key finds the key.
	for i := range keys {
		kv := it.SeekGE(keys[i], 0)
		require.True(t, Equal(keys[i], kv.K.UserKey))
		require.Equal(t, 0, Compare(keys[i], kv.K.UserKey))
	}
	// Ensure seeking to just the prefix of each key finds a key with the same
	// prefix.
	for i := range keys {
		si := Split(keys[i])
		kv := it.SeekGE(keys[i][:si], 0)
		require.True(t, Equal(keys[i][:si], pebble.Split(Split).Prefix(kv.K.UserKey)))
	}
	// Ensure seeking to the key but in random order finds the key.
	for _, i := range rng.Perm(len(keys)) {
		kv := it.SeekGE(keys[i], 0)
		require.True(t, Equal(keys[i], kv.K.UserKey))
		require.Equal(t, 0, Compare(keys[i], kv.K.UserKey))

		// We write keys[k] as the value too, so check that it's verbatim equal.
		value, callerOwned, err := kv.Value(valBuf)
		require.NoError(t, err)
		require.Equal(t, keys[i], value)
		if callerOwned {
			valBuf = value
		}
	}

	require.NoError(t, it.Close())
}

var possibleVersionLens = []int{
	engineKeyNoVersion,
	engineKeyVersionWallTimeLen,
	engineKeyVersionWallAndLogicalTimeLen,
	engineKeyVersionWallLogicalAndSyntheticTimeLen,
	engineKeyVersionLockTableLen,
}

func randomSerializedEngineKey(r *rand.Rand, maxUserKeyLen int) []byte {
	userKeyLen := testutils.RandIntInRange(r, 1, maxUserKeyLen)
	versionLen := possibleVersionLens[r.IntN(len(possibleVersionLens))]
	serializedLen := userKeyLen + versionLen + 1
	if versionLen > 0 {
		serializedLen++ // sentinel
	}
	k := testutils.RandBytes(r, serializedLen)
	k[userKeyLen] = 0x00
	if versionLen > 0 {
		k[len(k)-1] = byte(versionLen + 1)
	}
	return k
}
