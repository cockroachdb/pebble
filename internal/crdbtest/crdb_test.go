// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package crdbtest

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
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
			RoachKeyLen:       8,
			AvgKeysPerPrefix:  2,
			PercentLogical:    10,
		}
		const layout = "2006-01-02T15:04:05"
		baseWallTime := "2020-01-01T00:00:00"
		d.MaybeScanArgs(t, "seed", &seed)
		d.MaybeScanArgs(t, "count", &count)
		d.MaybeScanArgs(t, "alpha-len", &cfg.PrefixAlphabetLen)
		d.MaybeScanArgs(t, "prefix-len-shared", &cfg.PrefixLenShared)
		d.MaybeScanArgs(t, "roach-key-len", &cfg.RoachKeyLen)
		d.MaybeScanArgs(t, "avg-keys-pre-prefix", &cfg.AvgKeysPerPrefix)
		d.MaybeScanArgs(t, "percent-logical", &cfg.PercentLogical)
		d.MaybeScanArgs(t, "base-wall-time", &baseWallTime)
		d.MaybeScanArgs(t, "value-len", &valueLen)
		d.MaybeScanArgs(t, "percent-logical", &cfg.PercentLogical)
		d.MaybeScanArgs(t, "percent-empty-suffix", &cfg.PercentEmptySuffix)
		d.MaybeScanArgs(t, "percent-lock-suffix", &cfg.PercentLockSuffix)
		cfg.BaseWallTime = uint64(testutils.CheckErr(time.Parse(layout, baseWallTime)).UnixNano())

		rng := rand.New(rand.NewPCG(0, seed))
		var buf strings.Builder
		switch d.Cmd {
		case "rand-kvs":
			var vals [][]byte
			keys, vals = randomKVs(rng, count, cfg, valueLen)
			for i, key := range keys {
				fmt.Fprintf(&buf, "%s = %X\n", formatUserKey(key), vals[i])
			}

		case "rand-query-keys":
			queryKeys := randomQueryKeys(rng, count, keys, cfg.BaseWallTime)
			for _, key := range queryKeys {
				fmt.Fprintf(&buf, "%s\n", formatUserKey(key))
			}

		default:
			d.Fatalf(t, "unknown command %q", d.Cmd)
		}
		return buf.String()
	})
}

func TestCockroachDataColBlock(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)

	rng := rand.New(rand.NewPCG(0, seed))

	for i := 0; i < 100; i++ {
		keyCfg := keyGenConfig{
			PrefixAlphabetLen: 2 + rng.IntN(25),
			RoachKeyLen:       10 + int(rng.ExpFloat64()*10),
			AvgKeysPerPrefix:  1 + int(rng.ExpFloat64()*[]float64{1, 10, 100}[rng.IntN(3)]),
			BaseWallTime:      seed,
		}
		keyCfg.PrefixLenShared = rand.IntN(keyCfg.RoachKeyLen / 2)
		switch rng.IntN(4) {
		case 0:
			keyCfg.PercentLogical = 100
		case 1:
			keyCfg.PercentLogical = rng.IntN(25)
		}

		switch rng.IntN(10) {
		case 0:
			keyCfg.PercentEmptySuffix = 100
		case 1:
			keyCfg.PercentLockSuffix = 100
		case 2:
			p := 1 + rng.IntN(100)
			keyCfg.PercentEmptySuffix = rng.IntN(p)
			keyCfg.PercentLockSuffix = rng.IntN(p - keyCfg.PercentEmptySuffix)
		}
		testCockroachDataColBlock(t, rng.Uint64(), keyCfg)
		if t.Failed() {
			break
		}
	}
}

func testCockroachDataColBlock(t *testing.T, seed uint64, keyCfg keyGenConfig) {
	const targetBlockSize = 32 << 10
	rng := rand.New(rand.NewPCG(0, seed))
	valueLen := 1 + rng.IntN(300)
	serializedBlock, keys, values := generateDataBlock(rng, targetBlockSize, keyCfg, valueLen)

	var decoder colblk.DataBlockDecoder
	var it colblk.DataBlockIter
	it.InitOnce(&KeySchema, &Comparer, getLazyValuer(func([]byte) base.LazyValue {
		return base.LazyValue{ValueOrHandle: []byte("mock external value")}
	}))
	decoder.Init(&KeySchema, serializedBlock)
	if err := it.Init(&decoder, block.IterTransforms{}); err != nil {
		t.Fatal(err)
	}
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

	// Check that we can SeekGE to any key.
	for _, i := range rng.Perm(len(keys)) {
		kv := it.SeekGE(keys[i], base.SeekGEFlagsNone)
		if kv == nil {
			t.Fatalf("%q not found", keys[i])
		}
		if Compare(kv.K.UserKey, keys[i]) != 0 {
			t.Fatalf(
				"query key:\n    %s\nreturned key:\n    %s",
				formatUserKey(keys[i]), formatUserKey(kv.K.UserKey),
			)
		}
		// Search for the correct value among all keys that are equal.
		for !bytes.Equal(kv.V.InPlaceValue(), values[i]) {
			kv = it.Next()
			if kv == nil || Compare(kv.K.UserKey, keys[i]) != 0 {
				t.Fatalf("could not find correct value for %s", formatUserKey(keys[i]))
			}
		}
	}
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
	PrefixAlphabetLen  int    // Number of bytes in the alphabet used for the prefix.
	PrefixLenShared    int    // Number of bytes shared by all key prefixes.
	RoachKeyLen        int    // Number of bytes in the prefix (without the 0 sentinel byte).
	AvgKeysPerPrefix   int    // Average number of keys (with varying suffixes) per prefix.
	BaseWallTime       uint64 // Smallest MVCC WallTime.
	PercentLogical     int    // Percent of MVCC keys with non-zero MVCC logical time.
	PercentEmptySuffix int    // Percent of keys with empty suffix.
	PercentLockSuffix  int    // Percent of keys with lock suffix.
}

func (cfg keyGenConfig) String() string {
	return fmt.Sprintf(
		"AlphaLen=%d,Prefix=%d,Shared=%d,KeysPerPrefix=%d%s",
		cfg.PrefixAlphabetLen, cfg.RoachKeyLen, cfg.PrefixLenShared,
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
	for len(keys) < count {
		roachKey := g.randRoachKey(sharedPrefix)
		// We use the exponential distribution so that we occasionally have many
		// suffixes
		n := int(rng.ExpFloat64() * float64(cfg.AvgKeysPerPrefix))
		n = max(n, 1)
		for i := 0; i < n && len(keys) < count; i++ {
			if cfg.PercentEmptySuffix+cfg.PercentLockSuffix > 0 {
				if r := rng.IntN(100); r < cfg.PercentEmptySuffix+cfg.PercentLockSuffix {
					k := append(roachKey, 0)
					if r < cfg.PercentLockSuffix {
						// Generate a lock key suffix.
						for j := 0; j < engineKeyVersionLockTableLen; j++ {
							k = append(k, byte(g.rng.IntN(g.cfg.PrefixAlphabetLen)+'a'))
						}
						k = append(k, engineKeyVersionLockTableLen+1)
					}
					keys = append(keys, k)
					continue
				}
			}

			wallTime, logicalTime := g.randTimestamp()
			k := makeMVCCKey(roachKey, wallTime, logicalTime)
			keys = append(keys, k)
		}
	}
	slices.SortFunc(keys, Compare)
	vals = make([][]byte, count)
	for i := range vals {
		v := make([]byte, valueLen)
		for j := range v {
			v[j] = byte(rng.Uint32())
		}
		vals[i] = v
	}
	return keys, vals
}

func makeMVCCKey(roachKey []byte, wallTime uint64, logicalTime uint32) []byte {
	k := slices.Grow(slices.Clip(roachKey), MaxSuffixLen)
	return EncodeTimestamp(k, wallTime, logicalTime)
}

// randomQueryKeys returns a slice of count random query keys. Each key has a
// random prefix uniformly chosen from the distinct prefixes in writtenKeys and
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
		result[i] = makeMVCCKey(prefix, wallTime, logicalTime)
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

func (g *cockroachKeyGen) randRoachKey(blockPrefix []byte) []byte {
	roachKey := make([]byte, 0, g.cfg.RoachKeyLen+MaxSuffixLen)
	roachKey = append(roachKey, blockPrefix...)
	for len(roachKey) < g.cfg.RoachKeyLen {
		roachKey = append(roachKey, byte(g.rng.IntN(g.cfg.PrefixAlphabetLen)+'a'))
	}
	return roachKey
}

func (g *cockroachKeyGen) randTimestamp() (wallTime uint64, logicalTime uint32) {
	wallTime = g.cfg.BaseWallTime + g.rng.Uint64N(uint64(time.Hour))
	if g.cfg.PercentLogical > 0 && g.rng.IntN(100) < g.cfg.PercentLogical {
		logicalTime = g.rng.Uint32()
	}
	return wallTime, logicalTime
}

// formatUserKey formats a user key in the format:
//
//	<roach-key> [ @ <version-hex> ]
func formatUserKey(key []byte) string {
	n := Split(key)
	if key[n-1] != 0 {
		panic("expected sentinel byte")
	}
	prefix := key[:n-1]
	if n == len(key) {
		return string(prefix)
	}
	suffix := key[n : len(key)-1]
	if key[len(key)-1] != byte(len(suffix)+1) {
		panic("invalid suffix length byte")
	}
	if bytes.ContainsFunc(prefix, func(r rune) bool { return r < ' ' || r > '~' }) {
		return fmt.Sprintf("%q @ %X", prefix, suffix)
	}
	return fmt.Sprintf("%s @ %X", prefix, suffix)
}

// formatKey formats an internal key in the format:
//
//	<roach-key> [ @ <version-hex> ] #<seq-num>,<kind>
func formatKey(key base.InternalKey) string {
	return fmt.Sprintf("%s #%d,%s", formatUserKey(key.UserKey), key.SeqNum(), key.Kind())
}

// formatKV formats an internal key in the format:
//
//	<roach-key> [ @ <version-hex> ] #<seq-num>,<kind> = value
//
// For example:
//
//	foo @ 0001020304050607 #1,SET
func formatKV(kv base.InternalKV) string {
	val, _, err := kv.V.Value(nil)
	if err != nil {
		panic(err)
	}
	if len(val) == 0 {
		return formatKey(kv.K)
	}
	return fmt.Sprintf("%s = %s", formatKey(kv.K), val)
}

// parseKey parses a cockroach user key in the following format:
//
//	<roach-key> [@ <version-hex>]
//
// For example:
//
//	foo @ 0001020304050607
func parseUserKey(userKeyStr string) []byte {
	roachKey, versionStr := splitStringAt(userKeyStr, " @ ")

	if len(roachKey) >= 2 && roachKey[0] == '"' && roachKey[len(roachKey)-1] == '"' {
		var err error
		roachKey, err = strconv.Unquote(roachKey)
		if err != nil {
			panic(fmt.Sprintf("invalid user key string %s: %v", userKeyStr, err))
		}
	}

	// Append sentinel byte.
	userKey := append([]byte(roachKey), 0)
	if versionStr != "" {
		var version []byte
		if _, err := fmt.Sscanf(versionStr, "%X", &version); err != nil {
			panic(fmt.Sprintf("invalid user key string %q: cannot parse version %X", userKeyStr, version))
		}
		userKey = append(userKey, version...)
		userKey = append(userKey, byte(len(version)+1))
	}
	return userKey
}

// parseKey parses a cockroach key in the following format:
//
//	<roach-key> [@ <version-hex>] #<seq-num>,<kind>
//
// For example:
//
//	foo @ 0001020304050607 #1,SET
func parseKey(keyStr string) base.InternalKey {
	userKeyStr, trailerStr := splitStringAt(keyStr, " #")
	return base.InternalKey{
		UserKey: parseUserKey(userKeyStr),
		Trailer: base.ParseInternalKey(fmt.Sprintf("foo#%s", trailerStr)).Trailer,
	}
}

// parseKey parses a cockroach KV in the following format:
//
//	<roach-key> [@ <version-hex>] #<seq-num>,<kind> = value
//
// For example:
//
//	foo @ 0001020304050607 #1,SET = bar
func parseKV(input string) (key base.InternalKey, value []byte) {
	keyStr, valStr := splitStringAt(input, " = ")
	return parseKey(keyStr), []byte(valStr)
}

func splitStringAt(str string, sep string) (before, after string) {
	if s := strings.SplitN(str, sep, 2); len(s) == 2 {
		return s[0], s[1]
	}
	return str, ""
}
