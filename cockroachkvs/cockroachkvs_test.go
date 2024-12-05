// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cockroachkvs

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/cockroachdb/crlib/crbytes"
	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/crlib/testutils/require"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/colblk"
	"github.com/olekukonko/tablewriter"
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

func TestKeySchema_KeyWriter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var kw colblk.KeyWriter
	var row int
	var buf bytes.Buffer
	var keyBuf []byte
	datadriven.RunTest(t, "testdata/key_schema_key_writer", func(t *testing.T, td *datadriven.TestData) string {
		buf.Reset()
		switch td.Cmd {
		case "init":
			// Exercise both resetting and retrieving a new writer.
			if kw != nil && rand.IntN(2) == 1 {
				kw.Reset()
			} else {
				kw = KeySchema.NewKeyWriter()
			}
			row = 0
			keyBuf = keyBuf[:0]
			return ""
		case "write":
			for i, line := range crstrings.Lines(td.Input) {
				k := parseUserKey(line)
				fmt.Fprintf(&buf, "Parse(%s) = hex:%x\n", line, k)
				kcmp := kw.ComparePrev(k)
				if v := Compare(k, keyBuf); v < 0 {
					t.Fatalf("line %d: Compare(%s, hex:%x) = %d", i, line, keyBuf, v)
				} else if v != int(kcmp.UserKeyComparison) {
					t.Fatalf("line %d: Compare(%s, hex:%x) = %d; kcmp.UserKeyComparison = %d",
						i, line, keyBuf, v, kcmp.UserKeyComparison)
				}

				fmt.Fprintf(&buf, "%02d: ComparePrev(%s): PrefixLen=%d; CommonPrefixLen=%d; UserKeyComparison=%d\n",
					i, line, kcmp.PrefixLen, kcmp.CommonPrefixLen, kcmp.UserKeyComparison)
				kw.WriteKey(row, k, kcmp.PrefixLen, kcmp.CommonPrefixLen)
				fmt.Fprintf(&buf, "%02d: WriteKey(%d, %s, PrefixLen=%d, CommonPrefixLen=%d)\n",
					i, row, line, kcmp.PrefixLen, kcmp.CommonPrefixLen)

				keyBuf = kw.MaterializeKey(keyBuf[:0], row)
				if !Equal(k, keyBuf) {
					t.Fatalf("line %d: Equal(hex:%x, hex:%x) == false", i, k, keyBuf)
				}
				if v := Compare(k, keyBuf); v != 0 {
					t.Fatalf("line %d: Compare(hex:%x, hex:%x) = %d", i, k, keyBuf, v)
				}

				fmt.Fprintf(&buf, "%02d: MaterializeKey(_, %d) = hex:%x\n", i, row, keyBuf)
				row++
			}
			return buf.String()
		case "finish":
			b := crbytes.AllocAligned(int(kw.Size(row, 0) + 1))
			offs := make([]uint32, kw.NumColumns()+1)
			for i := 0; i < kw.NumColumns(); i++ {
				offs[i+1] = kw.Finish(i, row, offs[i], b)
			}
			roachKeys, _ := colblk.DecodePrefixBytes(b, offs[cockroachColRoachKey], row)
			mvccWallTimes, _ := colblk.DecodeUnsafeUints(b, offs[cockroachColMVCCWallTime], row)
			mvccLogicalTimes, _ := colblk.DecodeUnsafeUints(b, offs[cockroachColMVCCLogical], row)
			untypedVersions, _ := colblk.DecodeRawBytes(b, offs[cockroachColUntypedVersion], row)
			tbl := tablewriter.NewWriter(&buf)
			tbl.SetHeader([]string{"Key", "Wall", "Logical", "Untyped"})
			for i := 0; i < row; i++ {
				tbl.Append([]string{
					asciiOrHex(roachKeys.At(i)),
					fmt.Sprintf("%d", mvccWallTimes.At(i)),
					fmt.Sprintf("%d", mvccLogicalTimes.At(i)),
					fmt.Sprintf("%x", untypedVersions.At(i)),
				})
			}
			tbl.Render()
			return buf.String()
		default:
			panic(fmt.Sprintf("unrecognized command %q", td.Cmd))
		}
	})
}

func TestKeySchema_KeySeeker(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var buf bytes.Buffer
	var enc colblk.DataBlockEncoder
	var dec colblk.DataBlockDecoder
	var ks colblk.KeySeeker
	var maxKeyLen int
	enc.Init(&KeySchema)

	initKeySeeker := func() {
		ksPointer := &cockroachKeySeeker{}
		KeySchema.InitKeySeekerMetadata((*colblk.KeySeekerMetadata)(unsafe.Pointer(ksPointer)), &dec)
		ks = KeySchema.KeySeeker((*colblk.KeySeekerMetadata)(unsafe.Pointer(ksPointer)))
	}

	datadriven.RunTest(t, "testdata/key_schema_key_seeker", func(t *testing.T, td *datadriven.TestData) string {
		buf.Reset()
		switch td.Cmd {
		case "define-block":
			enc.Reset()
			maxKeyLen = 0
			var rows int
			for _, line := range crstrings.Lines(td.Input) {
				k := parseUserKey(line)
				fmt.Fprintf(&buf, "Parse(%s) = hex:%x\n", line, k)
				maxKeyLen = max(maxKeyLen, len(k))
				kcmp := enc.KeyWriter.ComparePrev(k)
				ikey := base.InternalKey{
					UserKey: k,
					Trailer: pebble.MakeInternalKeyTrailer(0, base.InternalKeyKindSet),
				}
				enc.Add(ikey, k, block.InPlaceValuePrefix(false), kcmp, false /* isObsolete */)
				rows++
			}
			blk, _ := enc.Finish(rows, enc.Size())
			dec.Init(&KeySchema, blk)
			return buf.String()
		case "is-lower-bound":
			initKeySeeker()
			syntheticSuffix, syntheticSuffixStr, _ := getSyntheticSuffix(t, td)

			for _, line := range crstrings.Lines(td.Input) {
				k := parseUserKey(line)
				got := ks.IsLowerBound(k, syntheticSuffix)
				fmt.Fprintf(&buf, "IsLowerBound(%s, %q) = %t\n", line, syntheticSuffixStr, got)
			}
			return buf.String()
		case "seek-ge":
			initKeySeeker()
			for _, line := range crstrings.Lines(td.Input) {
				k := parseUserKey(line)
				boundRow := -1
				searchDir := 0
				row, equalPrefix := ks.SeekGE(k, boundRow, int8(searchDir))

				fmt.Fprintf(&buf, "SeekGE(%s, boundRow=%d, searchDir=%d) = (row=%d, equalPrefix=%t)",
					line, boundRow, searchDir, row, equalPrefix)
				if row >= 0 && row < dec.BlockDecoder().Rows() {
					var kiter colblk.PrefixBytesIter
					kiter.Buf = make([]byte, maxKeyLen+1)
					key := ks.MaterializeUserKey(&kiter, -1, row)
					fmt.Fprintf(&buf, " [hex:%x]", key)
				}
				fmt.Fprintln(&buf)
			}
			return buf.String()
		case "materialize-user-key":
			initKeySeeker()
			syntheticSuffix, syntheticSuffixStr, syntheticSuffixOk := getSyntheticSuffix(t, td)

			var kiter colblk.PrefixBytesIter
			kiter.Buf = make([]byte, maxKeyLen+len(syntheticSuffix)+1)
			prevRow := -1
			for _, line := range crstrings.Lines(td.Input) {
				row, err := strconv.Atoi(line)
				if err != nil {
					t.Fatalf("bad row number %q: %s", line, err)
				}
				if syntheticSuffixOk {
					key := ks.MaterializeUserKeyWithSyntheticSuffix(&kiter, syntheticSuffix, prevRow, row)
					fmt.Fprintf(&buf, "MaterializeUserKeyWithSyntheticSuffix(%d, %d, %s) = hex:%x\n", prevRow, row, syntheticSuffixStr, key)
				} else {
					key := ks.MaterializeUserKey(&kiter, prevRow, row)
					fmt.Fprintf(&buf, "MaterializeUserKey(%d, %d) = hex:%x\n", prevRow, row, key)
				}
				prevRow = row
			}
			return buf.String()
		default:
			panic(fmt.Sprintf("unrecognized command %q", td.Cmd))
		}
	})
}

func getSyntheticSuffix(t *testing.T, td *datadriven.TestData) ([]byte, string, bool) {
	var syntheticSuffix []byte
	var syntheticSuffixStr string
	cmdArg, ok := td.Arg("synthetic-suffix")
	if ok {
		syntheticSuffixStr = strings.TrimPrefix(cmdArg.SingleVal(t), "@")
		syntheticSuffix = parseVersion(syntheticSuffixStr)
	}
	return syntheticSuffix, syntheticSuffixStr, ok
}

func asciiOrHex(b []byte) string {
	if bytes.ContainsFunc(b, func(r rune) bool { return r < ' ' || r > '~' }) {
		return fmt.Sprintf("hex:%x", b)
	}
	return string(b)
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

// formatInternalKey formats an internal key in the format:
//
//	<roach-key> [ @ <version-hex> ] #<seq-num>,<kind>
func formatInternalKey(key base.InternalKey) string {
	return fmt.Sprintf("%s #%d,%s", formatUserKey(key.UserKey), key.SeqNum(), key.Kind())
}

// formatInternalKV formats an internal key in the format:
//
//	<roach-key> [ @ <version-hex> ] #<seq-num>,<kind> = value
//
// For example:
//
//	foo @ 0001020304050607 #1,SET
func formatInternalKV(kv base.InternalKV) string {
	val, _, err := kv.V.Value(nil)
	if err != nil {
		panic(err)
	}
	if len(val) == 0 {
		return formatInternalKey(kv.K)
	}
	return fmt.Sprintf("%s = %s", formatInternalKey(kv.K), val)
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
	k := append(append([]byte(roachKey), 0), parseVersion(versionStr)...)
	checkEngineKey(k)
	return k
}

func parseVersion(versionStr string) []byte {
	if versionStr == "" {
		// No version.
		return nil
	}

	if ci := strings.IndexByte(versionStr, ','); ci >= 0 {
		// Parse as a MVCC timestamp version.
		wallTime, err := strconv.ParseUint(versionStr[:ci], 10, 64)
		if err != nil {
			panic(err)
		}
		logicalTime, err := strconv.ParseUint(versionStr[ci+1:], 10, 32)
		if err != nil {
			panic(err)
		}
		ret := AppendTimestamp([]byte{0x00}, wallTime, uint32(logicalTime))
		if len(ret) != 14 && len(ret) != 10 {
			panic(fmt.Sprintf("expected 10 or 14-length ret got %d", len(ret)))
		}
		checkEngineKey(ret)
		// TODO(jackson): Refactor to allow us to generate a suffix without a
		// sentinel byte rather than stripping it like this.
		return ret[1:]
	}

	// Parse as a hex-encoded version.
	var version []byte
	if _, err := fmt.Sscanf(versionStr, "%X", &version); err != nil {
		panic(fmt.Sprintf("invalid version string %q", versionStr))
	}
	return append(version, byte(len(version)+1))
}

// parseInternalKey parses a cockroach key in the following format:
//
//	<roach-key> [@ <version-hex>] #<seq-num>,<kind>
//
// For example:
//
//	foo @ 0001020304050607 #1,SET
func parseInternalKey(keyStr string) base.InternalKey {
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
func parseInternalKV(input string) (key base.InternalKey, value []byte) {
	keyStr, valStr := splitStringAt(input, " = ")
	return parseInternalKey(keyStr), []byte(valStr)
}

func splitStringAt(str string, sep string) (before, after string) {
	if s := strings.SplitN(str, sep, 2); len(s) == 2 {
		return s[0], s[1]
	}
	return str, ""
}
