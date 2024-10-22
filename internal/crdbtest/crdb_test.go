// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package crdbtest

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/colblk"
	"github.com/stretchr/testify/require"
)

func TestComparer(t *testing.T) {
	prefixes := [][]byte{
		EncodeMVCCKey(nil, []byte("abc"), 0, 0),
		EncodeMVCCKey(nil, []byte("d"), 0, 0),
		EncodeMVCCKey(nil, []byte("ef"), 0, 0),
	}

	suffixes := [][]byte{{}}
	for walltime := 3; walltime > 0; walltime-- {
		for logical := 2; logical >= 0; logical-- {
			key := EncodeMVCCKey(nil, []byte("foo"), uint64(walltime), uint32(logical))
			suffix := key[Comparer.Split(key):]
			suffixes = append(suffixes, suffix)

			if len(suffix) == suffixLenWithWall {
				// Append a suffix that encodes a zero logical value that should be
				// ignored in key comparisons, but not suffix comparisons.
				newSuffix := slices.Concat(suffix[:suffixLenWithWall-1], zeroLogical[:], []byte{suffixLenWithLogical})
				if Comparer.CompareRangeSuffixes(suffix, newSuffix) != 1 {
					t.Fatalf("expected suffixes %x < %x", suffix, newSuffix)
				}
				if Comparer.Compare(slices.Concat(prefixes[0], suffix), slices.Concat(prefixes[0], newSuffix)) != 0 {
					t.Fatalf("expected keys with suffixes %x and %x to be equal", suffix, newSuffix)
				}
				suffixes = append(suffixes, newSuffix)
				suffix = newSuffix
			}
			if len(suffix) != suffixLenWithLogical {
				t.Fatalf("unexpected suffix %x", suffix)
			}
			// Append a synthetic bit that should be ignored in key comparisons, but
			// not suffix comparisons.
			newSuffix := slices.Concat(suffix[:suffixLenWithLogical-1], []byte{1}, []byte{suffixLenWithSynthetic})
			if Comparer.CompareRangeSuffixes(suffix, newSuffix) != 1 {
				t.Fatalf("expected suffixes %x < %x", suffix, newSuffix)
			}
			if Comparer.Compare(slices.Concat(prefixes[0], suffix), slices.Concat(prefixes[0], newSuffix)) != 0 {
				t.Fatalf("expected keys with suffixes %x and %x to be equal", suffix, newSuffix)
			}
			suffixes = append(suffixes, newSuffix)
		}
	}
	// Add some lock table suffixes.
	suffixes = append(suffixes, append(bytes.Repeat([]byte{1}, engineKeyVersionLockTableLen), suffixLenWithLockTable))
	suffixes = append(suffixes, append(bytes.Repeat([]byte{2}, engineKeyVersionLockTableLen), suffixLenWithLockTable))
	if err := base.CheckComparer(&Comparer, prefixes, suffixes); err != nil {
		t.Error(err)
	}
}

func TestRandKeys(t *testing.T) {
	var keys [][]byte
	datadriven.RunTest(t, "testdata/rand_keys", func(t *testing.T, d *datadriven.TestData) string {
		seed := uint64(1234)
		count := 10
		valueLen := 4
		cfg := keyGenConfig{
			PrefixAlphabetLen: 8,
			PrefixLenShared:   4,
			PrefixLen:         8,
			AvgKeysPerPrefix:  2,
			PercentLogical:    10,
		}
		const layout = "2006-01-02T15:04:05"
		baseWallTime := "2020-01-01T00:00:00"
		d.MaybeScanArgs(t, "seed", &seed)
		d.MaybeScanArgs(t, "count", &count)
		d.MaybeScanArgs(t, "alpha-len", &cfg.PrefixAlphabetLen)
		d.MaybeScanArgs(t, "prefix-len-shared", &cfg.PrefixLenShared)
		d.MaybeScanArgs(t, "prefix-len", &cfg.PrefixLen)
		d.MaybeScanArgs(t, "avg-keys-pre-prefix", &cfg.AvgKeysPerPrefix)
		d.MaybeScanArgs(t, "percent-logical", &cfg.PercentLogical)
		d.MaybeScanArgs(t, "base-wall-time", &baseWallTime)
		d.MaybeScanArgs(t, "value-len", &valueLen)
		cfg.BaseWallTime = uint64(testutils.CheckErr(time.Parse(layout, baseWallTime)).UnixNano())

		rng := rand.New(rand.NewPCG(0, seed))
		var buf strings.Builder
		switch d.Cmd {
		case "rand-kvs":
			var vals [][]byte
			keys, vals = randomKVs(rng, count, cfg, valueLen)
			for i, key := range keys {
				n := Split(key)
				prefix := key[:n-1]
				suffix := key[n : len(key)-1]
				fmt.Fprintf(&buf, "%s @ %X = %X\n", prefix, suffix, vals[i])
			}

		case "rand-query-keys":
			queryKeys := randomQueryKeys(rng, count, keys, cfg.BaseWallTime)
			for _, key := range queryKeys {
				n := Split(key)
				prefix := key[:n-1]
				suffix := key[n : len(key)-1]
				fmt.Fprintf(&buf, "%s @ %X\n", prefix, suffix)
			}

		default:
			d.Fatalf(t, "unknown command %q", d.Cmd)
		}
		return buf.String()
	})
}

func TestCockroachDataColBlock(t *testing.T) {
	const targetBlockSize = 32 << 10
	const valueLen = 100
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))
	serializedBlock, keys, values := generateDataBlock(rng, targetBlockSize, keyGenConfig{
		PrefixAlphabetLen: 26,
		PrefixLen:         12,
		PercentLogical:    rng.IntN(25),
		AvgKeysPerPrefix:  2,
		BaseWallTime:      seed,
	}, valueLen)

	var decoder colblk.DataBlockDecoder
	var it colblk.DataBlockIter
	it.InitOnce(&KeySchema, Compare, Split, getLazyValuer(func([]byte) base.LazyValue {
		return base.LazyValue{ValueOrHandle: []byte("mock external value")}
	}))
	decoder.Init(&KeySchema, serializedBlock)
	if err := it.Init(&decoder, block.IterTransforms{}); err != nil {
		t.Fatal(err)
	}

	t.Run("Next", func(t *testing.T) {
		// Scan the block using Next and ensure that all the keys values match.
		i := 0
		for kv := it.First(); kv != nil; i, kv = i+1, it.Next() {
			if !bytes.Equal(kv.K.UserKey, keys[i]) {
				t.Fatalf("expected %q, but found %q", keys[i], kv.K.UserKey)
			}
			if !bytes.Equal(kv.V.InPlaceValue(), values[i]) {
				t.Fatalf("expected %x, but found %x", values[i], kv.V.InPlaceValue())
			}
		}
		require.Equal(t, len(keys), i)
	})
	t.Run("SeekGE", func(t *testing.T) {
		rng := rand.New(rand.NewPCG(0, seed))
		for _, i := range rng.Perm(len(keys)) {
			kv := it.SeekGE(keys[i], base.SeekGEFlagsNone)
			if kv == nil {
				t.Fatalf("%q not found", keys[i])
			}
			if !bytes.Equal(kv.V.InPlaceValue(), values[i]) {
				t.Fatalf(
					"expected:\n    %x\nfound:\n    %x\nquery key:\n    %x\nreturned key:\n    %x",
					values[i], kv.V.InPlaceValue(), keys[i], kv.K.UserKey)
			}
		}
	})
}

// generateDataBlock writes out a random cockroach data block using the given
// parameters. Returns the serialized block data and the keys and values
// written.
func generateDataBlock(
	rng *rand.Rand, targetBlockSize int, cfg keyGenConfig, valueLen int,
) (data []byte, keys [][]byte, values [][]byte) {
	keys, values = randomKVs(rng, targetBlockSize/valueLen, cfg, valueLen)

	var w colblk.DataBlockEncoder
	w.Init(&KeySchema)
	count := 0
	for w.Size() < targetBlockSize {
		ik := base.MakeInternalKey(keys[count], base.SeqNum(rng.Uint64N(uint64(base.SeqNumMax))), base.InternalKeyKindSet)
		kcmp := w.KeyWriter.ComparePrev(ik.UserKey)
		w.Add(ik, values[count], block.InPlaceValuePrefix(kcmp.PrefixEqual()), kcmp, false /* isObsolete */)
		count++
	}
	data, _ = w.Finish(w.Rows(), w.Size())
	return data, keys[:count], values[:count]
}

type getLazyValuer func([]byte) base.LazyValue

func (g getLazyValuer) GetLazyValueForPrefixAndValueHandle(handle []byte) base.LazyValue {
	return g(handle)
}

// keyGenConfig configures the shape of the random keys generated.
type keyGenConfig struct {
	PrefixAlphabetLen int    // Number of bytes in the alphabet used for the prefix.
	PrefixLenShared   int    // Number of bytes shared by all key prefixes.
	PrefixLen         int    // Number of bytes in the prefix.
	AvgKeysPerPrefix  int    // Average number of keys (with varying suffixes) per prefix.
	BaseWallTime      uint64 // Smallest MVCC WallTime.
	PercentLogical    int    // Percent of keys with non-zero MVCC logical time.
}

func (cfg keyGenConfig) String() string {
	return fmt.Sprintf(
		"AlphaLen=%d,Prefix=%d,Shared=%d,KeysPerPrefix=%d%s",
		cfg.PrefixAlphabetLen, cfg.PrefixLen, cfg.PrefixLenShared,
		cfg.AvgKeysPerPrefix,
		crstrings.If(cfg.PercentLogical != 0, fmt.Sprintf(",Logical=%d", cfg.PercentLogical)),
	)
}

// randomKVs constructs count random KVs with the provided parameters.
func randomKVs(rng *rand.Rand, count int, cfg keyGenConfig, valueLen int) (keys, vals [][]byte) {
	g := makeCockroachKeyGen(rng, cfg)
	sharedPrefix := make([]byte, cfg.PrefixLenShared)
	for i := 0; i < len(sharedPrefix); i++ {
		sharedPrefix[i] = byte(rng.IntN(cfg.PrefixAlphabetLen) + 'a')
	}

	keys = make([][]byte, 0, count)
	vals = make([][]byte, 0, count)
	for len(keys) < count {
		prefix := g.randPrefix(sharedPrefix)
		// We use the exponential distribution so that we occasionally have many
		// suffixes
		n := int(rng.ExpFloat64() * float64(cfg.AvgKeysPerPrefix))
		n = max(n, 1)
		for i := 0; i < n && len(keys) < count; i++ {
			wallTime, logicalTime := g.randTimestamp()
			k := makeKey(prefix, wallTime, logicalTime)
			v := make([]byte, valueLen)
			for j := range v {
				v[j] = byte(rng.Uint32())
			}
			keys = append(keys, k)
			vals = append(vals, v)
		}
	}
	slices.SortFunc(keys, Compare)
	return keys, vals
}

func makeKey(prefix []byte, wallTime uint64, logicalTime uint32) []byte {
	k := make([]byte, 0, len(prefix)+MaxSuffixLen)
	k = append(k, prefix...)
	return EncodeTimestamp(k, wallTime, logicalTime)
}

// randomQueryKeys returns a slice of count random query keys. Each key has a
// random prefix uniformly chosen from  the distinct prefixes in writtenKeys and
// a random timestamp.
//
// Note that by setting baseWallTime to be large enough, we can simulate a query
// pattern that always retrieves the latest version of any prefix.
func randomQueryKeys(
	rng *rand.Rand, count int, writtenKeys [][]byte, baseWallTime uint64,
) [][]byte {
	// Gather prefixes.
	prefixes := make([][]byte, len(writtenKeys))
	for i, k := range writtenKeys {
		prefixes[i] = k[:Split(k)-1]
	}
	slices.SortFunc(prefixes, bytes.Compare)
	prefixes = slices.CompactFunc(prefixes, bytes.Equal)
	result := make([][]byte, count)
	for i := range result {
		prefix := prefixes[rng.IntN(len(prefixes))]
		wallTime := baseWallTime + rng.Uint64N(uint64(time.Hour))
		var logicalTime uint32
		if rng.IntN(10) == 0 {
			logicalTime = rng.Uint32()
		}
		result[i] = makeKey(prefix, wallTime, logicalTime)
	}
	return result
}

type cockroachKeyGen struct {
	rng *rand.Rand
	cfg keyGenConfig
}

func makeCockroachKeyGen(rng *rand.Rand, cfg keyGenConfig) cockroachKeyGen {
	return cockroachKeyGen{
		rng: rng,
		cfg: cfg,
	}
}

func (g *cockroachKeyGen) randPrefix(blockPrefix []byte) []byte {
	prefix := make([]byte, 0, g.cfg.PrefixLen+MaxSuffixLen)
	prefix = append(prefix, blockPrefix...)
	for len(prefix) < g.cfg.PrefixLen {
		prefix = append(prefix, byte(g.rng.IntN(g.cfg.PrefixAlphabetLen)+'a'))
	}
	return prefix
}

func (g *cockroachKeyGen) randTimestamp() (wallTime uint64, logicalTime uint32) {
	wallTime = g.cfg.BaseWallTime + g.rng.Uint64N(uint64(time.Hour))
	if g.cfg.PercentLogical > 0 && g.rng.IntN(100) < g.cfg.PercentLogical {
		logicalTime = g.rng.Uint32()
	}
	return wallTime, logicalTime
}
