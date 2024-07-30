// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package crdbtest provides facilities for representing keys, workloads, etc
// representative of CockroachDB's use of Pebble.
package crdbtest

import (
	"bytes"
	"encoding/binary"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
)

const withWall = 9
const withLogical = withWall + 4
const withSynthetic = withLogical + 1
const withLockTableLen = 18

// MaxSuffixLen is the maximum length of the CockroachDB key suffix.
const MaxSuffixLen = max(withLockTableLen, withSynthetic, withLogical, withWall)

// Comparer is a base.Comparer for CockroachDB keys.
var Comparer = base.Comparer{
	Compare: Compare,
	Equal:   Equal,
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
		// pebble.DefaultComparer, so reuse the latter's Successor implementation.
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
	Split: Split,
	Name:  "cockroach_comparator",
}

// EncodeMVCCKey encodes a MVCC key into dst, growing dst as necessary.
func EncodeMVCCKey(dst []byte, key []byte, walltime uint64, logical uint32) []byte {
	if cap(dst) < len(key)+withSynthetic {
		dst = make([]byte, 0, len(key)+withSynthetic)
	}
	dst = append(dst[:0], key...)
	return EncodeTimestamp(dst, walltime, logical)
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

// DecodeTimestamp decodes a MVCC timestamp from a serialized MVCC key.
func DecodeTimestamp(mvccKey []byte) ([]byte, []byte, uint64, uint32) {
	tsLen := int(mvccKey[len(mvccKey)-1])
	keyPartEnd := len(mvccKey) - 1 - tsLen
	if keyPartEnd < 0 {
		return nil, nil, 0, 0
	}

	key := mvccKey[:keyPartEnd]
	if tsLen > 0 {
		ts := mvccKey[keyPartEnd+1 : len(mvccKey)-1]
		switch len(ts) {
		case 8:
			return key, nil, binary.BigEndian.Uint64(ts[:8]), 0
		case 12, 13:
			return key, nil, binary.BigEndian.Uint64(ts[:8]), binary.BigEndian.Uint32(ts[8:12])
		default:
			return key, ts, 0, 0
		}
	}
	return key, nil, 0, 0
}

// Split implements base.Split for CockroachDB keys.
func Split(key []byte) int {
	keyLen := len(key)
	if keyLen == 0 {
		return 0
	}
	// Last byte is the version length + 1 when there is a version, else it is
	// 0.
	versionLen := int(key[keyLen-1])
	keyPartEnd := keyLen - 1 - versionLen
	if keyPartEnd < 0 {
		return keyLen
	}
	return keyPartEnd + 1
}

// Compare compares cockroach keys, including the version (which could be MVCC
// timestamps).
func Compare(a, b []byte) int {
	if len(a) == 0 || len(b) == 0 {
		return bytes.Compare(a, b)
	}

	// NB: For performance, this routine manually splits the key into the
	// user-key and version components rather than using DecodeEngineKey. In
	// most situations, use DecodeEngineKey or GetKeyPartFromEngineKey or
	// SplitMVCCKey instead of doing this.
	aEnd := len(a) - 1
	bEnd := len(b) - 1
	if aEnd < 0 || bEnd < 0 {
		// This should never happen unless there is some sort of corruption of
		// the keys.
		panic(errors.AssertionFailedf("malformed key: %x, %x", a, b))
	}

	// Compute the index of the separator between the key and the version. If the
	// separator is found to be at -1 for both keys, then we are comparing bare
	// suffixes without a user key part. Pebble requires bare suffixes to be
	// comparable with the same ordering as if they had a common user key.
	aSep := aEnd - int(a[aEnd])
	bSep := bEnd - int(b[bEnd])
	if aSep == -1 && bSep == -1 {
		aSep, bSep = 0, 0 // comparing bare suffixes
	}
	if aSep < 0 || bSep < 0 {
		// This should never happen unless there is some sort of corruption of
		// the keys.
		return bytes.Compare(a, b)
	}
	// Compare the "user key" part of the key.
	if c := bytes.Compare(a[:aSep], b[:bSep]); c != 0 {
		return c
	}

	// Compare the version part of the key. Note that when the version is a
	// timestamp, the timestamp encoding causes byte comparison to be equivalent
	// to timestamp comparison.
	aVer := a[aSep:aEnd]
	bVer := b[bSep:bEnd]
	if len(aVer) == 0 {
		if len(bVer) == 0 {
			return 0
		}
		return -1
	} else if len(bVer) == 0 {
		return 1
	}
	aVer = normalizeEngineKeyVersionForCompare(aVer)
	bVer = normalizeEngineKeyVersionForCompare(bVer)
	return bytes.Compare(bVer, aVer)
}

// Equal implements base.Equal for Cockroach keys.
func Equal(a, b []byte) bool {
	aEnd := len(a) - 1
	bEnd := len(b) - 1
	if aEnd < 0 || bEnd < 0 {
		panic("empty keys")
	}

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
	if aSep == -1 && bSep == -1 {
		aSep, bSep = 0, 0 // comparing bare suffixes
	}
	if aSep < 0 || bSep < 0 {
		// This should never happen unless there is some sort of corruption of
		// the keys.
		return bytes.Equal(a, b)
	}

	// Compare the "user key" part of the key.
	if !bytes.Equal(a[:aSep], b[:bSep]) {
		return false
	}

	// Compare the version part of the key.
	aVer := a[aSep:aEnd]
	bVer := b[bSep:bEnd]
	aVer = normalizeEngineKeyVersionForCompare(aVer)
	bVer = normalizeEngineKeyVersionForCompare(bVer)
	return bytes.Equal(aVer, bVer)
}

var zeroLogical [4]byte

func normalizeEngineKeyVersionForCompare(a []byte) []byte {
	// In general, the version could also be a non-timestamp version, but we know
	// that engineKeyVersionLockTableLen+mvccEncodedTimeSentinelLen is a different
	// constant than the above, so there is no danger here of stripping parts from
	// a non-timestamp version.
	if len(a) == withSynthetic {
		// Strip the synthetic bit component from the timestamp version. The
		// presence of the synthetic bit does not affect key ordering or equality.
		a = a[:withLogical]
	}
	if len(a) == withLogical {
		// If the timestamp version contains a logical timestamp component that is
		// zero, strip the component. encodeMVCCTimestampToBuf will typically omit
		// the entire logical component in these cases as an optimization, but it
		// does not guarantee to never include a zero logical component.
		// Additionally, we can fall into this case after stripping off other
		// components of the key version earlier on in this function.
		if bytes.Equal(a[withWall:], zeroLogical[:]) {
			a = a[:withWall]
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
