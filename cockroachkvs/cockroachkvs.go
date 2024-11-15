// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package cockroachkvs provides CockroachDB's key-value schema and comparer
// implementation.
package cockroachkvs

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"unsafe"

	"github.com/cockroachdb/crlib/crbytes"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/sstable/colblk"
)

const (
	engineKeyNoVersion                             = 0
	engineKeyVersionWallTimeLen                    = 8
	engineKeyVersionWallAndLogicalTimeLen          = 12
	engineKeyVersionWallLogicalAndSyntheticTimeLen = 13
	engineKeyVersionLockTableLen                   = 17

	mvccEncodedTimeSentinelLen  = 1
	mvccEncodedTimeWallLen      = 8
	mvccEncodedTimeLogicalLen   = 4
	mvccEncodedTimeSyntheticLen = 1
	mvccEncodedTimeLengthLen    = 1

	suffixLenWithWall      = engineKeyVersionWallTimeLen + 1
	suffixLenWithLogical   = engineKeyVersionWallAndLogicalTimeLen + 1
	suffixLenWithSynthetic = engineKeyVersionWallLogicalAndSyntheticTimeLen + 1
	suffixLenWithLockTable = engineKeyVersionLockTableLen + 1
)

// MaxSuffixLen is the maximum length of the CockroachDB key suffix.
const MaxSuffixLen = 1 + max(
	engineKeyVersionLockTableLen,
	engineKeyVersionWallLogicalAndSyntheticTimeLen,
	engineKeyVersionWallAndLogicalTimeLen,
	engineKeyVersionWallTimeLen,
)

// Comparer is a base.Comparer for CockroachDB keys.
var Comparer = base.Comparer{
	Split:                Split,
	ComparePointSuffixes: ComparePointSuffixes,
	CompareRangeSuffixes: CompareRangeSuffixes,
	Compare:              Compare,
	Equal:                Equal,
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
	if cap(dst) < len(key)+suffixLenWithSynthetic {
		dst = make([]byte, 0, len(key)+suffixLenWithSynthetic)
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

// DecodeEngineKey decodes the given bytes as an EngineKey.
func DecodeEngineKey(b []byte) (roachKey, version []byte, ok bool) {
	if len(b) == 0 {
		return nil, nil, false
	}
	// Last byte is the version length + 1 when there is a version,
	// else it is 0.
	versionLen := int(b[len(b)-1])
	if versionLen == 1 {
		// The key encodes an empty version, which is not valid.
		return nil, nil, false
	}
	// keyPartEnd points to the sentinel byte.
	keyPartEnd := len(b) - 1 - versionLen
	if keyPartEnd < 0 || b[keyPartEnd] != 0x00 {
		return nil, nil, false
	}
	// roachKey excludes the sentinel byte.
	roachKey = b[:keyPartEnd]
	if versionLen > 0 {
		// version consists of the bytes after the sentinel and before the
		// length.
		version = b[keyPartEnd+1 : len(b)-1]
	}
	return roachKey, version, true
}

// Split implements base.Split for CockroachDB keys.
//
//go:inline
func Split(key []byte) int {
	if len(key) == 0 {
		return 0
	}
	if invariants.Enabled {
		checkEngineKey(key)
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
	if invariants.Enabled {
		checkEngineKey(a)
		checkEngineKey(b)
	}
	aSuffixLen := int(a[len(a)-1])
	aSuffixStart := len(a) - aSuffixLen
	bSuffixLen := int(b[len(b)-1])
	bSuffixStart := len(b) - bSuffixLen

	// Compare the "user key" part of the key.
	if c := bytes.Compare(a[:aSuffixStart], b[:bSuffixStart]); c != 0 {
		return c
	}
	if aSuffixLen == 0 || bSuffixLen == 0 {
		// Empty suffixes come before non-empty suffixes.
		return cmp.Compare(aSuffixLen, bSuffixLen)
	}

	return bytes.Compare(
		normalizeEngineSuffixForCompare(b[bSuffixStart:]),
		normalizeEngineSuffixForCompare(a[aSuffixStart:]),
	)
}

// CompareRangeSuffixes compares range key suffixes (normally timestamps).
func CompareRangeSuffixes(a, b []byte) int {
	if len(a) == 0 || len(b) == 0 {
		// Empty suffixes come before non-empty suffixes.
		return cmp.Compare(len(a), len(b))
	}
	// Here we are not using normalizeEngineKeyVersionForCompare for historical
	// reasons, summarized in
	// https://github.com/cockroachdb/cockroach/issues/130533.
	return bytes.Compare(b[:len(b)-1], a[:len(a)-1])
}

// ComparePointSuffixes compares point key suffixes (normally timestamps).
func ComparePointSuffixes(a, b []byte) int {
	if len(a) == 0 || len(b) == 0 {
		// Empty suffixes sort before non-empty suffixes.
		return cmp.Compare(len(a), len(b))
	}
	return bytes.Compare(normalizeEngineSuffixForCompare(b), normalizeEngineSuffixForCompare(a))
}

// Equal implements base.Equal for Cockroach keys.
func Equal(a, b []byte) bool {
	if len(a) == 0 || len(b) == 0 {
		return len(a) == len(b)
	}
	if invariants.Enabled {
		checkEngineKey(a)
		checkEngineKey(b)
	}
	aSuffixLen := int(a[len(a)-1])
	aSuffixStart := len(a) - aSuffixLen
	bSuffixLen := int(b[len(b)-1])
	bSuffixStart := len(b) - bSuffixLen

	// Fast-path: normalizeEngineSuffixForCompare doesn't strip off bytes when the
	// length is withWall or withLockTableLen. In this case, as well as cases with
	// no prefix, we can check for byte equality immediately.
	const withWall = mvccEncodedTimeSentinelLen + mvccEncodedTimeWallLen
	const withLockTableLen = mvccEncodedTimeSentinelLen + engineKeyVersionLockTableLen
	if (aSuffixLen <= withWall && bSuffixLen <= withWall) ||
		(aSuffixLen == withLockTableLen && bSuffixLen == withLockTableLen) ||
		aSuffixLen == 0 || bSuffixLen == 0 {
		return bytes.Equal(a, b)
	}

	// Compare the "user key" part of the key.
	if !bytes.Equal(a[:aSuffixStart], b[:bSuffixStart]) {
		return false
	}

	return bytes.Equal(
		normalizeEngineSuffixForCompare(a[aSuffixStart:]),
		normalizeEngineSuffixForCompare(b[bSuffixStart:]),
	)
}

var zeroLogical [4]byte

// normalizeEngineSuffixForCompare takes a non-empty key suffix (including the
// trailing sentinel byte) and returns a prefix of the buffer that should be
// used for byte-wise comparison. It trims the trailing suffix length byte and
// any other trailing bytes that need to be ignored (like a synthetic bit or
// zero logical component).
//
//gcassert:inline
func normalizeEngineSuffixForCompare(a []byte) []byte {
	// Check sentinel byte.
	if invariants.Enabled && len(a) != int(a[len(a)-1]) {
		panic(errors.AssertionFailedf("malformed suffix: %x", a))
	}
	// Strip off sentinel byte.
	a = a[:len(a)-1]
	switch len(a) {
	case engineKeyVersionWallLogicalAndSyntheticTimeLen:
		// Strip the synthetic bit component from the timestamp version. The
		// presence of the synthetic bit does not affect key ordering or equality.
		a = a[:engineKeyVersionWallAndLogicalTimeLen]
		fallthrough
	case engineKeyVersionWallAndLogicalTimeLen:
		// If the timestamp version contains a logical timestamp component that is
		// zero, strip the component. encodeMVCCTimestampToBuf will typically omit
		// the entire logical component in these cases as an optimization, but it
		// does not guarantee to never include a zero logical component.
		// Additionally, we can fall into this case after stripping off other
		// components of the key version earlier on in this function.
		if bytes.Equal(a[engineKeyVersionWallTimeLen:], zeroLogical[:]) {
			a = a[:engineKeyVersionWallTimeLen]
		}
		fallthrough
	case engineKeyVersionWallTimeLen:
		// Nothing to do.

	case engineKeyVersionLockTableLen:
		// We rely on engineKeyVersionLockTableLen being different from the other
		// lengths above to ensure that we don't strip parts from a non-timestamp
		// version.

	default:
		if invariants.Enabled {
			panic(errors.AssertionFailedf("version with unexpected length: %x", a))
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

func checkEngineKey(k []byte) {
	if len(k) == 0 {
		panic(errors.AssertionFailedf("empty key"))
	}
	if int(k[len(k)-1]) >= len(k) {
		panic(errors.AssertionFailedf("malformed key terminator byte: %x", k))
	}
	if k[len(k)-1] == 1 {
		panic(errors.AssertionFailedf("invalid key terminator byte 1"))
	}
}

const (
	cockroachColRoachKey int = iota
	cockroachColMVCCWallTime
	cockroachColMVCCLogical
	cockroachColUntypedVersion
	cockroachColCount
)

var KeySchema = colblk.KeySchema{
	Name:       "crdb1",
	HeaderSize: 1,
	ColumnTypes: []colblk.DataType{
		cockroachColRoachKey:       colblk.DataTypePrefixBytes,
		cockroachColMVCCWallTime:   colblk.DataTypeUint,
		cockroachColMVCCLogical:    colblk.DataTypeUint,
		cockroachColUntypedVersion: colblk.DataTypeBytes,
	},
	NewKeyWriter: func() colblk.KeyWriter {
		return makeCockroachKeyWriter()
	},
	InitKeySeekerMetadata: func(meta *colblk.KeySeekerMetadata, d *colblk.DataBlockDecoder) {
		ks := (*cockroachKeySeeker)(unsafe.Pointer(meta))
		ks.init(d)
	},
	KeySeeker: func(meta *colblk.KeySeekerMetadata) colblk.KeySeeker {
		return (*cockroachKeySeeker)(unsafe.Pointer(meta))
	},
}

// suffixTypes is a bitfield indicating what kind of suffixes are present in a
// block.
type suffixTypes uint8

const (
	// hasMVCCSuffixes is set if there is at least one key with an MVCC suffix in
	// the block.
	hasMVCCSuffixes suffixTypes = (1 << iota)
	// hasEmptySuffixes is set if there is at least one key with no suffix in the block.
	hasEmptySuffixes
	// hasNonMVCCSuffixes is set if there is at least one key with a non-empty,
	// non-MVCC suffix.
	hasNonMVCCSuffixes
)

func (s suffixTypes) String() string {
	var suffixes []string
	if s&hasMVCCSuffixes != 0 {
		suffixes = append(suffixes, "mvcc")
	}
	if s&hasEmptySuffixes != 0 {
		suffixes = append(suffixes, "empty")
	}
	if s&hasNonMVCCSuffixes != 0 {
		suffixes = append(suffixes, "non-mvcc")
	}
	if len(suffixes) == 0 {
		return "none"
	}
	return strings.Join(suffixes, ",")
}

type cockroachKeyWriter struct {
	roachKeys       colblk.PrefixBytesBuilder
	wallTimes       colblk.UintBuilder
	logicalTimes    colblk.UintBuilder
	untypedVersions colblk.RawBytesBuilder
	suffixTypes     suffixTypes
	prevRoachKeyLen int32
	prevSuffix      []byte
}

func makeCockroachKeyWriter() *cockroachKeyWriter {
	kw := &cockroachKeyWriter{}
	kw.roachKeys.Init(16)
	kw.wallTimes.Init()
	kw.logicalTimes.InitWithDefault()
	kw.untypedVersions.Init()
	return kw
}

func (kw *cockroachKeyWriter) Reset() {
	kw.roachKeys.Reset()
	kw.wallTimes.Reset()
	kw.logicalTimes.Reset()
	kw.untypedVersions.Reset()
	kw.suffixTypes = 0
	kw.prevRoachKeyLen = 0
}

func (kw *cockroachKeyWriter) ComparePrev(key []byte) colblk.KeyComparison {
	prefixLen := Split(key)
	if kw.roachKeys.Rows() == 0 {
		return colblk.KeyComparison{
			PrefixLen:         int32(prefixLen),
			CommonPrefixLen:   0,
			UserKeyComparison: +1,
		}
	}
	lastRoachKey := kw.roachKeys.UnsafeGet(kw.roachKeys.Rows() - 1)
	commonPrefixLen := crbytes.CommonPrefix(lastRoachKey, key[:prefixLen-1])
	if len(lastRoachKey) == commonPrefixLen {
		if invariants.Enabled && len(lastRoachKey) > prefixLen-1 {
			panic(errors.AssertionFailedf("out-of-order keys: previous roach key %q > roach key of key %q",
				lastRoachKey, key))
		}
		// All the bytes of the previous roach key form a byte-wise prefix of
		// [key]'s prefix. The last byte of the previous prefix is the 0x00
		// sentinel byte, which is not stored within roachKeys. It's possible
		// that [key] also has a 0x00 byte in the same position (either also
		// serving as a sentinel byte, in which case the prefixes are equal, or
		// not in which case [key] is greater). In both cases, we need to
		// increment CommonPrefixLen.
		if key[commonPrefixLen] == 0x00 {
			commonPrefixLen++
			if commonPrefixLen == prefixLen {
				// The prefixes are equal; compare the suffixes.
				return colblk.KeyComparison{
					PrefixLen:         int32(prefixLen),
					CommonPrefixLen:   int32(commonPrefixLen),
					UserKeyComparison: int32(ComparePointSuffixes(key[prefixLen:], kw.prevSuffix)),
				}
			}
		}
		// prefixLen > commonPrefixLen; key is greater.
		return colblk.KeyComparison{
			PrefixLen:         int32(prefixLen),
			CommonPrefixLen:   int32(commonPrefixLen),
			UserKeyComparison: +1,
		}
	}
	// Both keys have at least 1 additional byte at which they diverge.
	// Compare the diverging byte.
	return colblk.KeyComparison{
		PrefixLen:         int32(prefixLen),
		CommonPrefixLen:   int32(commonPrefixLen),
		UserKeyComparison: int32(cmp.Compare(key[commonPrefixLen], lastRoachKey[commonPrefixLen])),
	}
}

func (kw *cockroachKeyWriter) WriteKey(
	row int, key []byte, keyPrefixLen, keyPrefixLenSharedWithPrev int32,
) {
	if len(key) == 0 {
		panic(errors.AssertionFailedf("empty key"))
	}
	// Last byte is the version length + 1 when there is a version,
	// else it is 0.
	versionLen := int(key[len(key)-1])
	if (len(key)-versionLen) != int(keyPrefixLen) || key[keyPrefixLen-1] != 0x00 {
		panic(errors.AssertionFailedf("invalid %d-byte key with %d-byte prefix (%q)",
			len(key), keyPrefixLen, key))
	}
	// TODO(jackson): Avoid copying the previous suffix.
	kw.prevSuffix = append(kw.prevSuffix[:0], key[keyPrefixLen:]...)

	// When the roach key is the same or contain the previous key as a prefix,
	// keyPrefixLenSharedWithPrev includes the previous key's separator byte.
	kw.roachKeys.Put(key[:keyPrefixLen-1], min(int(keyPrefixLenSharedWithPrev), int(kw.prevRoachKeyLen)))
	kw.prevRoachKeyLen = keyPrefixLen - 1

	// NB: The w.logicalTimes builder was initialized with InitWithDefault, so
	// if we don't set a value, the column value is implicitly zero. We only
	// need to Set anything for non-zero values.
	var wallTime uint64
	var untypedVersion []byte
	switch versionLen {
	case 0:
		// No-op.
		kw.suffixTypes |= hasEmptySuffixes
	case 9:
		kw.suffixTypes |= hasMVCCSuffixes
		wallTime = binary.BigEndian.Uint64(key[keyPrefixLen : keyPrefixLen+8])
	case 13, 14:
		kw.suffixTypes |= hasMVCCSuffixes
		wallTime = binary.BigEndian.Uint64(key[keyPrefixLen : keyPrefixLen+8])
		kw.logicalTimes.Set(row, uint64(binary.BigEndian.Uint32(key[keyPrefixLen+8:keyPrefixLen+12])))
		// NOTE: byte 13 used to store the timestamp's synthetic bit, but this is no
		// longer consulted and can be ignored during decoding.
	default:
		// Not a MVCC timestamp.
		kw.suffixTypes |= hasNonMVCCSuffixes
		untypedVersion = key[keyPrefixLen : len(key)-1]
	}
	kw.wallTimes.Set(row, wallTime)
	kw.untypedVersions.Put(untypedVersion)
}

func (kw *cockroachKeyWriter) MaterializeKey(dst []byte, i int) []byte {
	dst = append(dst, kw.roachKeys.UnsafeGet(i)...)
	// Append separator byte.
	dst = append(dst, 0)
	if untypedVersion := kw.untypedVersions.UnsafeGet(i); len(untypedVersion) > 0 {
		dst = append(dst, untypedVersion...)
		dst = append(dst, byte(len(untypedVersion)+1))
		return dst
	}
	return AppendTimestamp(dst, kw.wallTimes.Get(i), uint32(kw.logicalTimes.Get(i)))
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
	fmt.Fprint(dst, "suffix types: ")
	fmt.Fprintln(dst, kw.suffixTypes.String())
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

func (kw *cockroachKeyWriter) FinishHeader(buf []byte) {
	buf[0] = byte(kw.suffixTypes)
}

type cockroachKeySeeker struct {
	roachKeys       colblk.PrefixBytes
	roachKeyChanged colblk.Bitmap
	mvccWallTimes   colblk.UnsafeUints
	mvccLogical     colblk.UnsafeUints
	untypedVersions colblk.RawBytes
	suffixTypes     suffixTypes
}

// Assert that the cockroachKeySeeker fits inside KeySeekerMetadata.
var _ uint = colblk.KeySeekerMetadataSize - uint(unsafe.Sizeof(cockroachKeySeeker{}))

var _ colblk.KeySeeker = (*cockroachKeySeeker)(nil)

func (ks *cockroachKeySeeker) init(d *colblk.DataBlockDecoder) {
	bd := d.BlockDecoder()
	ks.roachKeys = bd.PrefixBytes(cockroachColRoachKey)
	ks.roachKeyChanged = d.PrefixChanged()
	ks.mvccWallTimes = bd.Uints(cockroachColMVCCWallTime)
	ks.mvccLogical = bd.Uints(cockroachColMVCCLogical)
	ks.untypedVersions = bd.RawBytes(cockroachColUntypedVersion)
	header := d.KeySchemaHeader()
	if len(header) != 1 {
		panic(errors.AssertionFailedf("invalid key schema-specific header %x", header))
	}
	ks.suffixTypes = suffixTypes(header[0])
}

// IsLowerBound is part of the KeySeeker interface.
func (ks *cockroachKeySeeker) IsLowerBound(k []byte, syntheticSuffix []byte) bool {
	roachKey, version, ok := DecodeEngineKey(k)
	if !ok {
		panic(errors.AssertionFailedf("invalid key %q", k))
	}
	if v := bytes.Compare(ks.roachKeys.UnsafeFirstSlice(), roachKey); v != 0 {
		return v > 0
	}
	// If there's a synthetic suffix, we ignore the block's suffix columns and
	// compare the key's suffix to the synthetic suffix.
	if len(syntheticSuffix) > 0 {
		return ComparePointSuffixes(syntheticSuffix, k[len(roachKey)+1:]) >= 0
	}
	var wallTime uint64
	var logicalTime uint32
	switch len(version) {
	case engineKeyNoVersion:
	case engineKeyVersionWallTimeLen:
		wallTime = binary.BigEndian.Uint64(version[:8])
	case engineKeyVersionWallAndLogicalTimeLen, engineKeyVersionWallLogicalAndSyntheticTimeLen:
		wallTime = binary.BigEndian.Uint64(version[:8])
		logicalTime = binary.BigEndian.Uint32(version[8:12])
	default:
		// The provided key `k` is not a MVCC key. Assert that the first key in
		// the block is also not an MVCC key. If it were, that would mean there
		// exists both a MVCC key and a non-MVCC key with the same prefix.
		//
		// TODO(jackson): Double check that we'll never produce index separators
		// that are invalid version lengths.
		if invariants.Enabled && ks.mvccWallTimes.At(0) != 0 {
			panic("comparing timestamp with untyped suffix")
		}
		return ComparePointSuffixes(ks.untypedVersions.At(0), version) >= 0
	}

	// NB: The sign comparison is inverted because suffixes are sorted such that
	// the largest timestamps are "smaller" in the lexicographical ordering.
	if v := cmp.Compare(ks.mvccWallTimes.At(0), wallTime); v != 0 {
		return v < 0
	}
	return cmp.Compare(uint32(ks.mvccLogical.At(0)), logicalTime) <= 0
}

// SeekGE is part of the KeySeeker interface.
func (ks *cockroachKeySeeker) SeekGE(
	key []byte, boundRow int, searchDir int8,
) (row int, equalPrefix bool) {
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
	// We have three common cases:
	// 1. The seek key has no suffix.
	// 2. We are seeking to an MVCC timestamp in a block where all keys have
	//    MVCC timestamps (e.g. SQL table data).
	// 3. We are seeking to a non-MVCC timestamp in a block where no keys have
	//    MVCC timestamps (e.g. lock keys).

	if len(seekSuffix) == 0 {
		// The search key has no suffix, so it's the smallest possible key with its
		// prefix. Return the row. This is a common case where the user is seeking
		// to the most-recent row and just wants the smallest key with the prefix.
		return index
	}

	const withWall = 9
	const withLogical = withWall + 4
	const withSynthetic = withLogical + 1

	// If suffixTypes == hasMVCCSuffixes, all keys in the block have MVCC
	// suffixes. Note that blocks that contain both MVCC and non-MVCC should be
	// very rare, so it's ok to use the more general path below in that case.
	if ks.suffixTypes == hasMVCCSuffixes && (len(seekSuffix) == withWall || len(seekSuffix) == withLogical || len(seekSuffix) == withSynthetic) {
		// Fast path: seeking among MVCC versions using a MVCC timestamp.
		seekWallTime := binary.BigEndian.Uint64(seekSuffix)
		var seekLogicalTime uint32
		if len(seekSuffix) >= withLogical {
			seekLogicalTime = binary.BigEndian.Uint32(seekSuffix[8:])
		}

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
			m := int(uint(l+u) >> 1) // avoid overflow when computing m
			// l ≤ m < u
			mWallTime := ks.mvccWallTimes.At(m)
			if mWallTime < seekWallTime ||
				(mWallTime == seekWallTime && uint32(ks.mvccLogical.At(m)) <= seekLogicalTime) {
				u = m // preserves f(u) = true
			} else {
				l = m + 1 // preserves f(l-1) = false
			}
		}
		return l
	}

	// Remove the terminator byte, which we know is equal to len(seekSuffix)
	// because we obtained the suffix by splitting the seek key.
	version := seekSuffix[:len(seekSuffix)-1]
	if invariants.Enabled && seekSuffix[len(version)] != byte(len(seekSuffix)) {
		panic(errors.AssertionFailedf("invalid seek suffix %x", seekSuffix))
	}

	// Binary search for version between [index, prefixChanged.SeekSetBitGE(index+1)].
	//
	// Define f(i) = true iff key at i is >= seek key (i.e. suffix at i is <= seek suffix).
	// Invariant: f(l-1) == false, f(u) == true.
	l := index
	u := ks.roachKeyChanged.SeekSetBitGE(index + 1)
	for l < u {
		m := int(uint(l+u) >> 1) // avoid overflow when computing m
		// l ≤ m < u
		mVer := ks.untypedVersions.At(m)
		if len(mVer) == 0 {
			wallTime := ks.mvccWallTimes.At(m)
			logicalTime := uint32(ks.mvccLogical.At(m))
			if wallTime == 0 && logicalTime == 0 {
				// This row has an empty suffix. Since the seek suffix is not empty, it comes after.
				l = m + 1
				continue
			}

			// Note: this path is not very performance sensitive: blocks that mix MVCC
			// suffixes with non-MVCC suffixes should be rare.

			//gcassert:noescape
			var buf [12]byte
			//gcassert:inline
			binary.BigEndian.PutUint64(buf[:], wallTime)
			if logicalTime == 0 {
				mVer = buf[:8]
			} else {
				//gcassert:inline
				binary.BigEndian.PutUint32(buf[8:], logicalTime)
				mVer = buf[:12]
			}
		}
		if bytes.Compare(mVer, version) <= 0 {
			u = m // preserves f(u) == true
		} else {
			l = m + 1 // preserves f(l-1) == false
		}
	}
	return l
}

// MaterializeUserKey is part of the KeySeeker interface.
func (ks *cockroachKeySeeker) MaterializeUserKey(
	ki *colblk.PrefixBytesIter, prevRow, row int,
) []byte {
	if invariants.Enabled && (row < 0 || row >= ks.roachKeys.Rows()) {
		panic(errors.AssertionFailedf("invalid row number %d", row))
	}
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
		res := ki.Buf[:roachKeyLen+2+len(untypedVersion)]
		*(*byte)(ptr) = 0
		memmove(
			unsafe.Pointer(uintptr(ptr)+1),
			unsafe.Pointer(unsafe.SliceData(untypedVersion)),
			uintptr(len(untypedVersion)),
		)
		*(*byte)(unsafe.Pointer(uintptr(ptr) + uintptr(len(untypedVersion)+1))) = byte(len(untypedVersion) + 1)
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

//go:linkname memmove runtime.memmove
func memmove(to, from unsafe.Pointer, n uintptr)

func checkEngineKey(k []byte) {
	if len(k) == 0 {
		panic(errors.AssertionFailedf("empty key"))
	}
	if int(k[len(k)-1]) >= len(k) {
		panic(errors.AssertionFailedf("malformed key terminator byte: %x", k))
	}
	if k[len(k)-1] == 1 {
		panic(errors.AssertionFailedf("invalid key terminator byte 1"))
	}
}
