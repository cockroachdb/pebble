// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package crdbtest provides facilities for representing keys, workloads, etc
// representative of CockroachDB's use of Pebble.
package crdbtest

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand/v2"
	"slices"
	"sync"
	"time"
	"unsafe"

	"github.com/cockroachdb/crlib/crbytes"
	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/sstable/colblk"
)

const withWall = 9
const withLogical = withWall + 4
const withSynthetic = withLogical + 1
const withLockTableLen = 18

// MaxSuffixLen is the maximum length of the CockroachDB key suffix.
const MaxSuffixLen = max(withLockTableLen, withSynthetic, withLogical, withWall)

// Comparer is a base.Comparer for CockroachDB keys.
var Comparer = base.Comparer{
	Split:           Split,
	CompareSuffixes: CompareSuffixes,
	Compare:         Compare,
	Equal:           Equal,
	AbbreviatedKey: func(k []byte) uint64 {
		key, ok := getKeyPartFromEngineKey(k)
		if !ok {
			return 0
		}
		return base.DefaultComparer.AbbreviatedKey(key)
	},
	FormatKey: base.DefaultFormatter,
	Separator: func(dst, a, b []byte) []byte {
		aKey, ok := getKeyPartFromEngineKey(a)
		if !ok {
			return append(dst, a...)
		}
		bKey, ok := getKeyPartFromEngineKey(b)
		if !ok {
			return append(dst, a...)
		}
		// If the keys are the same just return a.
		if bytes.Equal(aKey, bKey) {
			return append(dst, a...)
		}
		n := len(dst)
		dst = base.DefaultComparer.Separator(dst, aKey, bKey)
		// Did it pick a separator different than aKey -- if it did not we can't do better than a.
		buf := dst[n:]
		if bytes.Equal(aKey, buf) {
			return append(dst[:n], a...)
		}
		// The separator is > aKey, so we only need to add the sentinel.
		return append(dst, 0)
	},
	Successor: func(dst, a []byte) []byte {
		aKey, ok := getKeyPartFromEngineKey(a)
		if !ok {
			return append(dst, a...)
		}
		n := len(dst)
		// Engine key comparison uses bytes.Compare on the roachpb.Key, which is the same semantics as
		// pebble.DefaultComparer, so reuse the latter's SeekSetBitGE implementation.
		dst = base.DefaultComparer.Successor(dst, aKey)
		// Did it pick a successor different than aKey -- if it did not we can't do better than a.
		buf := dst[n:]
		if bytes.Equal(aKey, buf) {
			return append(dst[:n], a...)
		}
		// The successor is > aKey, so we only need to add the sentinel.
		return append(dst, 0)
	},
	ImmediateSuccessor: func(dst, a []byte) []byte {
		// The key `a` is guaranteed to be a bare prefix: It's a
		// `engineKeyNoVersion` key without a version—just a trailing 0-byte to
		// signify the length of the version. For example the user key "foo" is
		// encoded as: "foo\0". We need to encode the immediate successor to
		// "foo", which in the natural byte ordering is "foo\0".  Append a
		// single additional zero, to encode the user key "foo\0" with a
		// zero-length version.
		return append(append(dst, a...), 0)
	},
	Name: "cockroach_comparator",
}

// EncodeMVCCKey encodes a MVCC key into dst, growing dst as necessary.
func EncodeMVCCKey(dst []byte, key []byte, walltime uint64, logical uint32) []byte {
	if cap(dst) < len(key)+withSynthetic {
		dst = make([]byte, 0, len(key)+withSynthetic)
	}
	dst = append(dst[:0], key...)
	return EncodeTimestamp(dst, walltime, logical)
}

// AppendTimestamp appends an encoded MVCC timestamp onto key, returning the new
// key. The provided key should already have the 0x00 sentinel byte (i.e., key
// should be a proper prefix from the perspective of Pebble).
func AppendTimestamp(key []byte, walltime uint64, logical uint32) []byte {
	if key[len(key)-1] != 0 {
		panic(errors.AssertionFailedf("key does not end with 0x00 sentinel byte: %x", key))
	}
	if logical == 0 {
		if walltime == 0 {
			return key
		}
		key = append(key, make([]byte, 9)...)
		binary.BigEndian.PutUint64(key[len(key)-9:], walltime)
		key[len(key)-1] = 9 // Version length byte
		return key
	}
	key = append(key, make([]byte, 13)...)
	binary.BigEndian.PutUint64(key[len(key)-13:], walltime)
	binary.BigEndian.PutUint32(key[len(key)-5:], logical)
	key[len(key)-1] = 13 // Version length byte
	return key

}

// EncodeTimestamp encodes a MVCC timestamp into a key, returning the new key.
// The key's capacity must be sufficiently large to hold the encoded timestamp.
func EncodeTimestamp(key []byte, walltime uint64, logical uint32) []byte {
	pos := len(key)
	if logical == 0 {
		if walltime == 0 {
			key = key[:pos+1]
			key[pos] = 0 // sentinel byte
			return key
		}

		key = key[:pos+1+8+1]
		key[pos] = 0 // sentinel byte
		key[pos+1+8] = 9
		binary.BigEndian.PutUint64(key[pos+1:], walltime)
		return key
	}

	key = key[:pos+1+12+1]
	key[pos] = 0 // sentinel byte
	key[pos+1+8+4] = 13
	binary.BigEndian.PutUint64(key[pos+1:], walltime)
	binary.BigEndian.PutUint32(key[pos+1+8:], logical)
	return key
}

// DecodeEngineKey decodes an Engine key (the key that gets stored in Pebble).
// Returns:
//
//   - roachKey: key prefix without the separator byte; corresponds to
//     MVCCKey.Key / EngineKey.Key (roachpb.Key type).
//
//   - untypedVersion: when the suffix does not correspond to a timestamp,
//     untypedVersion is set to the suffix without the length
//     byte; corresponds to EngineKey.Version.
//
//   - wallTime, logicalTime: timestamp for the key, or 0 if the key does not
//     correspond to a timestamp.
func DecodeEngineKey(
	engineKey []byte,
) (roachKey []byte, untypedVersion []byte, wallTime uint64, logicalTime uint32) {
	tsLen := int(engineKey[len(engineKey)-1])
	tsStart := len(engineKey) - tsLen
	if tsStart < 0 {
		if invariants.Enabled {
			panic("invalid length byte")
		}
		return nil, nil, 0, 0
	}
	roachKeyEnd := tsStart - 1
	if roachKeyEnd < 0 {
		roachKeyEnd = 0
	} else if invariants.Enabled && engineKey[roachKeyEnd] != 0 {
		panic("invalid separator byte")
	}

	roachKey = engineKey[:roachKeyEnd]
	untypedVersion, wallTime, logicalTime = DecodeSuffix(engineKey[tsStart:])
	return roachKey, untypedVersion, wallTime, logicalTime
}

// DecodeSuffix decodes the suffix of a key. If the suffix is a timestamp,
// wallTime and logicalTime are returned; otherwise untypedVersion contains the
// version (which is the suffix without the terminator length byte).
//
//gcassert:inline
func DecodeSuffix(suffix []byte) (untypedVersion []byte, wallTime uint64, logicalTime uint32) {
	if invariants.Enabled && len(suffix) > 0 && len(suffix) != int(suffix[len(suffix)-1]) {
		panic("invalid length byte")
	}
	switch len(suffix) {
	case 0:
		return nil, 0, 0
	case 9:
		return nil, binary.BigEndian.Uint64(suffix[:8]), 0
	case 13, 14:
		return nil, binary.BigEndian.Uint64(suffix[:8]), binary.BigEndian.Uint32(suffix[8:12])
	default:
		return suffix[:len(suffix)-1], 0, 0
	}
}

// Split implements base.Split for CockroachDB keys.
func Split(key []byte) int {
	if len(key) == 0 {
		return 0
	}

	// Last byte is the version length + 1 when there is a version, else it is
	// 0.
	versionLen := int(key[len(key)-1])
	if versionLen > len(key) {
		panic(errors.AssertionFailedf("invalid version length"))
	}
	return len(key) - versionLen
}

// Compare compares cockroach keys, including the version (which could be MVCC
// timestamps).
func Compare(a, b []byte) int {
	if len(a) == 0 || len(b) == 0 {
		return cmp.Compare(len(a), len(b))
	}

	// NB: For performance, this routine manually splits the key into the
	// user-key and version components rather than using DecodeEngineKey. In
	// most situations, use DecodeEngineKey or GetKeyPartFromEngineKey or
	// SplitMVCCKey instead of doing this.
	aEnd := len(a) - 1
	bEnd := len(b) - 1

	// Compute the index of the separator between the key and the version. If the
	// separator is found to be at -1 for both keys, then we are comparing bare
	// suffixes without a user key part. Pebble requires bare suffixes to be
	// comparable with the same ordering as if they had a common user key.
	aSep := aEnd - int(a[aEnd])
	bSep := bEnd - int(b[bEnd])
	if aSep < 0 || bSep < 0 || a[aSep] != 0 || b[bSep] != 0 {
		panic(errors.AssertionFailedf("malformed key: %x, %x", a, b))
	}
	// Compare the "user key" part of the key.
	if c := bytes.Compare(a[:aSep], b[:bSep]); c != 0 {
		return c
	}

	// Compare the version part of the key. Note that when the version is a
	// timestamp, the timestamp encoding causes byte comparison to be equivalent
	// to timestamp comparison.
	a, b = a[aSep+1:], b[bSep+1:]
	if len(a) == 0 || len(b) == 0 {
		// Empty suffixes come before non-empty suffixes.
		return cmp.Compare(len(a), len(b))
	}
	return bytes.Compare(
		normalizeEngineKeyVersionForCompare(b),
		normalizeEngineKeyVersionForCompare(a),
	)
}

// CompareSuffixes compares suffixes (normally timestamps).
func CompareSuffixes(a, b []byte) int {
	if len(a) == 0 || len(b) == 0 {
		// Empty suffixes come before non-empty suffixes.
		return cmp.Compare(len(a), len(b))
	}
	// Here we are not using normalizeEngineKeyVersionForCompare for historical
	// reasons, summarized in
	// https://github.com/cockroachdb/cockroach/issues/130533.
	return bytes.Compare(b[:len(b)-1], a[:len(a)-1])
}

// Equal implements base.Equal for Cockroach keys.
func Equal(a, b []byte) bool {
	// TODO(radu): Pebble sometimes passes empty "keys" and we have to tolerate
	// them until we fix that.
	if len(a) == 0 || len(b) == 0 {
		return len(a) == len(b)
	}
	aEnd := len(a) - 1
	bEnd := len(b) - 1

	// Last byte is the version length + 1 when there is a version,
	// else it is 0.
	aVerLen := int(a[aEnd])
	bVerLen := int(b[bEnd])

	// Fast-path. If the key version is empty or contains only a walltime
	// component then normalizeEngineKeyVersionForCompare is a no-op, so we don't
	// need to split the "user key" from the version suffix before comparing to
	// compute equality. Instead, we can check for byte equality immediately.
	if (aVerLen <= withWall && bVerLen <= withWall) || (aVerLen == withLockTableLen && bVerLen == withLockTableLen) {
		return bytes.Equal(a, b)
	}

	// Compute the index of the separator between the key and the version. If the
	// separator is found to be at -1 for both keys, then we are comparing bare
	// suffixes without a user key part. Pebble requires bare suffixes to be
	// comparable with the same ordering as if they had a common user key.
	aSep := aEnd - aVerLen
	bSep := bEnd - bVerLen
	// Compare the "user key" part of the key.
	if !bytes.Equal(a[:aSep], b[:bSep]) {
		return false
	}
	if aVerLen == 0 || bVerLen == 0 {
		return aVerLen == bVerLen
	}

	// Compare the version part of the key.
	aVer := a[aSep+1:]
	bVer := b[bSep+1:]
	aVer = normalizeEngineKeyVersionForCompare(aVer)
	bVer = normalizeEngineKeyVersionForCompare(bVer)
	return bytes.Equal(aVer, bVer)
}

var zeroLogical [4]byte

func normalizeEngineKeyVersionForCompare(a []byte) []byte {
	// Check sentinel byte.
	if len(a) != int(a[len(a)-1]) {
		panic(errors.AssertionFailedf("malformed suffix: %x", a))
	}
	// Strip off sentinel byte.
	a = a[:len(a)-1]
	// In general, the version could also be a non-timestamp version, but we know
	// that engineKeyVersionLockTableLen+mvccEncodedTimeSentinelLen is a different
	// constant than the above, so there is no danger here of stripping parts from
	// a non-timestamp version.
	if len(a) == withSynthetic-1 {
		// Strip the synthetic bit component from the timestamp version. The
		// presence of the synthetic bit does not affect key ordering or equality.
		a = a[:withLogical-1]
	}
	if len(a) == withLogical-1 {
		// If the timestamp version contains a logical timestamp component that is
		// zero, strip the component. encodeMVCCTimestampToBuf will typically omit
		// the entire logical component in these cases as an optimization, but it
		// does not guarantee to never include a zero logical component.
		// Additionally, we can fall into this case after stripping off other
		// components of the key version earlier on in this function.
		if bytes.Equal(a[withWall-1:], zeroLogical[:]) {
			a = a[:withWall-1]
		}
	}
	return a
}

func getKeyPartFromEngineKey(engineKey []byte) (key []byte, ok bool) {
	if len(engineKey) == 0 {
		return nil, false
	}
	// Last byte is the version length + 1 when there is a version,
	// else it is 0.
	versionLen := int(engineKey[len(engineKey)-1])
	// keyPartEnd points to the sentinel byte.
	keyPartEnd := len(engineKey) - 1 - versionLen
	if keyPartEnd < 0 || engineKey[keyPartEnd] != 0x00 {
		return nil, false
	}
	// Key excludes the sentinel byte.
	return engineKey[:keyPartEnd], true
}

// KeyConfig configures the shape of the random keys generated.
type KeyConfig struct {
	PrefixAlphabetLen int    // Number of bytes in the alphabet used for the prefix.
	PrefixLenShared   int    // Number of bytes shared by all key prefixes.
	PrefixLen         int    // Number of bytes in the prefix.
	AvgKeysPerPrefix  int    // Average number of keys (with varying suffixes) per prefix.
	BaseWallTime      uint64 // Smallest MVCC WallTime.
	PercentLogical    int    // Percent of keys with non-zero MVCC logical time.
}

func (cfg KeyConfig) String() string {
	return fmt.Sprintf(
		"AlphaLen=%d,Prefix=%d,Shared=%d,KeysPerPrefix=%d%s",
		cfg.PrefixAlphabetLen, cfg.PrefixLen, cfg.PrefixLenShared,
		cfg.AvgKeysPerPrefix,
		crstrings.If(cfg.PercentLogical != 0, fmt.Sprintf(",Logical=%d", cfg.PercentLogical)),
	)
}

// RandomKVs constructs count random KVs with the provided parameters.
func RandomKVs(rng *rand.Rand, count int, cfg KeyConfig, valueLen int) (keys, vals [][]byte) {
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

// RandomQueryKeys returns a slice of count random query keys. Each key has a
// random prefix uniformly chosen from  the distinct prefixes in writtenKeys and
// a random timestamp.
//
// Note that by setting baseWallTime to be large enough, we can simulate a query
// pattern that always retrieves the latest version of any prefix.
func RandomQueryKeys(
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
	cfg KeyConfig
}

func makeCockroachKeyGen(rng *rand.Rand, cfg KeyConfig) cockroachKeyGen {
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

const (
	cockroachColRoachKey int = iota
	cockroachColMVCCWallTime
	cockroachColMVCCLogical
	cockroachColUntypedVersion
	cockroachColCount
)

var KeySchema = colblk.KeySchema{
	Name: "crdb1",
	ColumnTypes: []colblk.DataType{
		cockroachColRoachKey:       colblk.DataTypePrefixBytes,
		cockroachColMVCCWallTime:   colblk.DataTypeUint,
		cockroachColMVCCLogical:    colblk.DataTypeUint,
		cockroachColUntypedVersion: colblk.DataTypeBytes,
	},
	NewKeyWriter: func() colblk.KeyWriter {
		kw := &cockroachKeyWriter{}
		kw.roachKeys.Init(16)
		kw.wallTimes.Init()
		kw.logicalTimes.InitWithDefault()
		kw.untypedVersions.Init()
		return kw
	},
	NewKeySeeker: func() colblk.KeySeeker {
		return cockroachKeySeekerPool.Get().(*cockroachKeySeeker)
	},
}

type cockroachKeyWriter struct {
	roachKeys       colblk.PrefixBytesBuilder
	wallTimes       colblk.UintBuilder
	logicalTimes    colblk.UintBuilder
	untypedVersions colblk.RawBytesBuilder
	prevSuffix      []byte
}

func (kw *cockroachKeyWriter) ComparePrev(key []byte) colblk.KeyComparison {
	var cmpv colblk.KeyComparison
	cmpv.PrefixLen = int32(Split(key)) // TODO(jackson): Inline
	if kw.roachKeys.Rows() == 0 {
		cmpv.UserKeyComparison = 1
		return cmpv
	}
	lp := kw.roachKeys.UnsafeGet(kw.roachKeys.Rows() - 1)
	cmpv.CommonPrefixLen = int32(crbytes.CommonPrefix(lp, key[:cmpv.PrefixLen-1]))
	if cmpv.CommonPrefixLen == cmpv.PrefixLen-1 {
		// Adjust CommonPrefixLen to include the sentinel byte,
		cmpv.CommonPrefixLen = cmpv.PrefixLen
		cmpv.UserKeyComparison = int32(CompareSuffixes(key[cmpv.PrefixLen:], kw.prevSuffix))
		return cmpv
	}
	// The keys have different MVCC prefixes. We haven't determined which is
	// greater, but we know the index at which they diverge. The base.Comparer
	// contract dictates that prefixes must be lexicographically ordered.
	if len(lp) == int(cmpv.CommonPrefixLen) {
		// cmpv.PrefixLen > cmpv.PrefixLenShared; key is greater.
		cmpv.UserKeyComparison = +1
	} else {
		// Both keys have at least 1 additional byte at which they diverge.
		// Compare the diverging byte.
		cmpv.UserKeyComparison = int32(cmp.Compare(key[cmpv.CommonPrefixLen], lp[cmpv.CommonPrefixLen]))
	}
	return cmpv
}

func (kw *cockroachKeyWriter) WriteKey(
	row int, key []byte, keyPrefixLen, keyPrefixLenSharedWithPrev int32,
) {
	// TODO(jackson): Avoid copying the previous suffix.
	// TODO(jackson): Use keyPrefixLen to speed up decoding.
	roachKey, untypedVersion, wallTime, logicalTime := DecodeEngineKey(key)
	kw.prevSuffix = append(kw.prevSuffix[:0], key[keyPrefixLen:]...)
	// When the roach key is the same, keyPrefixLenSharedWithPrev includes the
	// separator byte.
	kw.roachKeys.Put(roachKey, min(int(keyPrefixLenSharedWithPrev), len(roachKey)))
	kw.wallTimes.Set(row, wallTime)
	// The w.logicalTimes builder was initialized with InitWithDefault, so if we
	// don't set a value, the column value is implicitly zero. We only need to
	// Set anything for non-zero values.
	if logicalTime > 0 {
		kw.logicalTimes.Set(row, uint64(logicalTime))
	}
	kw.untypedVersions.Put(untypedVersion)
}

func (kw *cockroachKeyWriter) MaterializeKey(dst []byte, i int) []byte {
	dst = append(dst, kw.roachKeys.UnsafeGet(i)...)
	// Append separator byte.
	dst = append(dst, 0)
	if untypedVersion := kw.untypedVersions.UnsafeGet(i); len(untypedVersion) > 0 {
		dst = append(dst, untypedVersion...)
		dst = append(dst, byte(len(untypedVersion)))
		return dst
	}
	return AppendTimestamp(dst, kw.wallTimes.Get(i), uint32(kw.logicalTimes.Get(i)))
}

func (kw *cockroachKeyWriter) Reset() {
	kw.roachKeys.Reset()
	kw.wallTimes.Reset()
	kw.logicalTimes.Reset()
	kw.untypedVersions.Reset()
}

func (kw *cockroachKeyWriter) WriteDebug(dst io.Writer, rows int) {
	fmt.Fprint(dst, "prefixes: ")
	kw.roachKeys.WriteDebug(dst, rows)
	fmt.Fprintln(dst)
	fmt.Fprint(dst, "wall times: ")
	kw.wallTimes.WriteDebug(dst, rows)
	fmt.Fprintln(dst)
	fmt.Fprint(dst, "logical times: ")
	kw.logicalTimes.WriteDebug(dst, rows)
	fmt.Fprintln(dst)
	fmt.Fprint(dst, "untyped suffixes: ")
	kw.untypedVersions.WriteDebug(dst, rows)
	fmt.Fprintln(dst)
}

func (kw *cockroachKeyWriter) NumColumns() int {
	return cockroachColCount
}

func (kw *cockroachKeyWriter) DataType(col int) colblk.DataType {
	return KeySchema.ColumnTypes[col]
}

func (kw *cockroachKeyWriter) Size(rows int, offset uint32) uint32 {
	offset = kw.roachKeys.Size(rows, offset)
	offset = kw.wallTimes.Size(rows, offset)
	offset = kw.logicalTimes.Size(rows, offset)
	offset = kw.untypedVersions.Size(rows, offset)
	return offset
}

func (kw *cockroachKeyWriter) Finish(
	col int, rows int, offset uint32, buf []byte,
) (endOffset uint32) {
	switch col {
	case cockroachColRoachKey:
		return kw.roachKeys.Finish(0, rows, offset, buf)
	case cockroachColMVCCWallTime:
		return kw.wallTimes.Finish(0, rows, offset, buf)
	case cockroachColMVCCLogical:
		return kw.logicalTimes.Finish(0, rows, offset, buf)
	case cockroachColUntypedVersion:
		return kw.untypedVersions.Finish(0, rows, offset, buf)
	default:
		panic(fmt.Sprintf("unknown default key column: %d", col))
	}
}

var cockroachKeySeekerPool = sync.Pool{
	New: func() interface{} { return &cockroachKeySeeker{} },
}

type cockroachKeySeeker struct {
	roachKeys       colblk.PrefixBytes
	roachKeyChanged colblk.Bitmap
	mvccWallTimes   colblk.UnsafeUints
	mvccLogical     colblk.UnsafeUints
	untypedVersions colblk.RawBytes
}

var _ colblk.KeySeeker = (*cockroachKeySeeker)(nil)

// Init is part of the KeySeeker interface.
func (ks *cockroachKeySeeker) Init(d *colblk.DataBlockDecoder) error {
	bd := d.BlockDecoder()
	ks.roachKeys = bd.PrefixBytes(cockroachColRoachKey)
	ks.roachKeyChanged = d.PrefixChanged()
	ks.mvccWallTimes = bd.Uints(cockroachColMVCCWallTime)
	ks.mvccLogical = bd.Uints(cockroachColMVCCLogical)
	ks.untypedVersions = bd.RawBytes(cockroachColUntypedVersion)
	return nil
}

// CompareFirstUserKey compares the provided key to the first user key
// contained within the data block. It's equivalent to performing
//
//	Compare(firstUserKey, k)
func (ks *cockroachKeySeeker) IsLowerBound(k []byte, syntheticSuffix []byte) bool {
	roachKey, untypedVersion, wallTime, logicalTime := DecodeEngineKey(k)
	if v := Compare(ks.roachKeys.UnsafeFirstSlice(), roachKey); v != 0 {
		return v > 0
	}
	if len(syntheticSuffix) > 0 {
		return Compare(syntheticSuffix, k[len(roachKey)+1:]) >= 0
	}
	if len(untypedVersion) > 0 {
		if invariants.Enabled && ks.mvccWallTimes.At(0) != 0 {
			panic("comparing timestamp with untyped suffix")
		}
		return Compare(ks.untypedVersions.At(0), untypedVersion) >= 0
	}
	if v := cmp.Compare(ks.mvccWallTimes.At(0), wallTime); v != 0 {
		return v > 0
	}
	return cmp.Compare(uint32(ks.mvccLogical.At(0)), logicalTime) >= 0
}

// SeekGE is part of the KeySeeker interface.
func (ks *cockroachKeySeeker) SeekGE(
	key []byte, boundRow int, searchDir int8,
) (row int, equalPrefix bool) {
	// TODO(jackson): Inline Split.
	si := Split(key)
	row, eq := ks.roachKeys.Search(key[:si-1])
	if eq {
		return ks.seekGEOnSuffix(row, key[si:]), true
	}
	return row, false
}

// seekGEOnSuffix is a helper function for SeekGE when a seek key's prefix
// exactly matches a row. seekGEOnSuffix finds the first row at index or later
// with the same prefix as index and a suffix greater than or equal to [suffix],
// or if no such row exists, the next row with a different prefix.
func (ks *cockroachKeySeeker) seekGEOnSuffix(index int, seekSuffix []byte) (row int) {
	// The search key's prefix exactly matches the prefix of the row at index.
	const withWall = 9
	const withLogical = withWall + 4
	const withSynthetic = withLogical + 1
	var seekWallTime uint64
	var seekLogicalTime uint32
	switch len(seekSuffix) {
	case 0:
		// The search key has no suffix, so it's the smallest possible key with
		// its prefix. Return the row. This is a common case where the user is
		// seeking to the most-recent row and just wants the smallest key with
		// the prefix.
		return index
	case withLogical, withSynthetic:
		seekWallTime = binary.BigEndian.Uint64(seekSuffix)
		seekLogicalTime = binary.BigEndian.Uint32(seekSuffix[8:])
	case withWall:
		seekWallTime = binary.BigEndian.Uint64(seekSuffix)
	default:
		// The suffix is untyped. Compare the untyped suffixes.
		// Binary search between [index, prefixChanged.SeekSetBitGE(index+1)].
		//
		// Define f(i) = true iff key at i is >= seek key.
		// Invariant: f(l-1) == false, f(u) == true.
		l := index
		u := ks.roachKeyChanged.SeekSetBitGE(index + 1)
		for l < u {
			h := int(uint(l+u) >> 1) // avoid overflow when computing h
			// l ≤ h < u
			if bytes.Compare(ks.untypedVersions.At(h), seekSuffix) >= 0 {
				u = h // preserves f(u) == true
			} else {
				l = h + 1 // preserves f(l-1) == false
			}
		}
		return l
	}
	// Seeking among MVCC versions using a MVCC timestamp.

	// TODO(jackson): What if the row has an untyped suffix?

	// First check the suffix at index, because querying for the latest value is
	// the most common case.
	if latestWallTime := ks.mvccWallTimes.At(index); latestWallTime < seekWallTime ||
		(latestWallTime == seekWallTime && uint32(ks.mvccLogical.At(index)) <= seekLogicalTime) {
		return index
	}

	// Binary search between [index+1, prefixChanged.SeekSetBitGE(index+1)].
	//
	// Define f(i) = true iff key at i is >= seek key.
	// Invariant: f(l-1) == false, f(u) == true.
	l := index + 1
	u := ks.roachKeyChanged.SeekSetBitGE(index + 1)
	for l < u {
		h := int(uint(l+u) >> 1) // avoid overflow when computing h
		// l ≤ h < u
		hWallTime := ks.mvccWallTimes.At(h)
		if hWallTime < seekWallTime ||
			(hWallTime == seekWallTime && uint32(ks.mvccLogical.At(h)) <= seekLogicalTime) {
			u = h // preserves f(u) = true
		} else {
			l = h + 1 // preserves f(l-1) = false
		}
	}
	return l
}

// MaterializeUserKey is part of the KeySeeker interface.
func (ks *cockroachKeySeeker) MaterializeUserKey(
	ki *colblk.PrefixBytesIter, prevRow, row int,
) []byte {
	if prevRow+1 == row && prevRow >= 0 {
		ks.roachKeys.SetNext(ki)
	} else {
		ks.roachKeys.SetAt(ki, row)
	}

	roachKeyLen := len(ki.Buf)
	ptr := unsafe.Pointer(uintptr(unsafe.Pointer(unsafe.SliceData(ki.Buf))) + uintptr(roachKeyLen))
	mvccWall := ks.mvccWallTimes.At(row)
	mvccLogical := uint32(ks.mvccLogical.At(row))
	if mvccWall == 0 && mvccLogical == 0 {
		// This is not an MVCC key. Use the untyped suffix.
		untypedVersion := ks.untypedVersions.At(row)
		if len(untypedVersion) == 0 {
			res := ki.Buf[:roachKeyLen+1]
			res[roachKeyLen] = 0
			return res
		}
		// Slice first, to check that the capacity is sufficient.
		res := ki.Buf[:roachKeyLen+1+len(untypedVersion)]
		*(*byte)(ptr) = 0
		memmove(
			unsafe.Pointer(uintptr(ptr)+1),
			unsafe.Pointer(unsafe.SliceData(untypedVersion)),
			uintptr(len(untypedVersion)),
		)
		return res
	}

	// Inline binary.BigEndian.PutUint64. Note that this code is converted into
	// word-size instructions by the compiler.
	*(*byte)(ptr) = 0
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 1)) = byte(mvccWall >> 56)
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 2)) = byte(mvccWall >> 48)
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 3)) = byte(mvccWall >> 40)
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 4)) = byte(mvccWall >> 32)
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 5)) = byte(mvccWall >> 24)
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 6)) = byte(mvccWall >> 16)
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 7)) = byte(mvccWall >> 8)
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 8)) = byte(mvccWall)

	ptr = unsafe.Pointer(uintptr(ptr) + 9)
	// This is an MVCC key.
	if mvccLogical == 0 {
		*(*byte)(ptr) = 9
		return ki.Buf[:len(ki.Buf)+10]
	}

	// Inline binary.BigEndian.PutUint32.
	*(*byte)(ptr) = byte(mvccLogical >> 24)
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 1)) = byte(mvccLogical >> 16)
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 2)) = byte(mvccLogical >> 8)
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 3)) = byte(mvccLogical)
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 4)) = 13
	return ki.Buf[:len(ki.Buf)+14]
}

// MaterializeUserKeyWithSyntheticSuffix is part of the KeySeeker interface.
func (ks *cockroachKeySeeker) MaterializeUserKeyWithSyntheticSuffix(
	ki *colblk.PrefixBytesIter, suffix []byte, prevRow, row int,
) []byte {
	if prevRow+1 == row && prevRow >= 0 {
		ks.roachKeys.SetNext(ki)
	} else {
		ks.roachKeys.SetAt(ki, row)
	}

	// Slice first, to check that the capacity is sufficient.
	res := ki.Buf[:len(ki.Buf)+1+len(suffix)]
	ptr := unsafe.Pointer(uintptr(unsafe.Pointer(unsafe.SliceData(ki.Buf))) + uintptr(len(ki.Buf)))
	*(*byte)(ptr) = 0
	memmove(unsafe.Pointer(uintptr(ptr)+1), unsafe.Pointer(unsafe.SliceData(suffix)), uintptr(len(suffix)))
	return res
}

// Release is part of the KeySeeker interface.
func (ks *cockroachKeySeeker) Release() {
	*ks = cockroachKeySeeker{}
	cockroachKeySeekerPool.Put(ks)
}

//go:linkname memmove runtime.memmove
func memmove(to, from unsafe.Pointer, n uintptr)
