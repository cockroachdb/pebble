// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package rangekey provides facilities for encoding, decoding and merging range
// keys.
//
// Range keys map a span of keyspan `[start, end)`, at an optional suffix, to a
// value.
//
// # Encoding
//
// Unlike other Pebble keys, range keys encode several fields of information:
// start key, end key, suffix and value. Internally within Pebble and its
// sstables, all keys including range keys are represented as a key-value tuple.
// Range keys map to internal key-value tuples by mapping the start key to the
// key and encoding the remainder of the fields in the value.
//
// ## `RANGEKEYSET`
//
// A `RANGEKEYSET` represents one more range keys set over a single region of
// user key space. Each represented range key must have a unique suffix.  A
// `RANGEKEYSET` encapsulates a start key, an end key and a set of SuffixValue
// pairs.
//
// A `RANGEKEYSET` key's user key holds the start key. Its value is a varstring
// end key, followed by a set of SuffixValue pairs. A `RANGEKEYSET` may have
// multiple SuffixValue pairs if the keyspan was set at multiple unique suffix
// values.
//
// ## `RANGEKEYUNSET`
//
// A `RANGEKEYUNSET` represents the removal of range keys at specific suffixes
// over a single region of user key space. A `RANGEKEYUNSET` encapsulates a
// start key, an end key and a set of suffixes.
//
// A `RANGEKEYUNSET` key's user key holds the start key. Its value is a
// varstring end key, followed by a set of suffixes. A `RANGEKEYUNSET` may have
// multiple suffixes if the keyspan was unset at multiple unique suffixes.
//
// ## `RANGEKEYDEL`
//
// A `RANGEKEYDEL` represents the removal of all range keys over a single region
// of user key space, regardless of suffix. A `RANGEKEYDEL` encapsulates a
// start key and an end key. The end key is stored in the value, without any
// varstring length prefixing.
package rangekey

// TODO(jackson): Document the encoding of RANGEKEYSET and RANGEKEYUNSET values
// once we're confident they're stable.

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
)

// SuffixValue represents a tuple of a suffix and a corresponding value. A
// physical RANGEKEYSET key may contain many logical RangeKeySets, each
// represented with a separate SuffixValue tuple.
type SuffixValue struct {
	Suffix []byte
	Value  []byte
}

// EncodedSetSuffixValuesLen precomputes the length of the given slice of
// SuffixValues, when encoded for a RangeKeySet. It may be used to construct a
// buffer of the appropriate size before encoding.
func EncodedSetSuffixValuesLen(suffixValues []SuffixValue) int {
	var n int
	for i := 0; i < len(suffixValues); i++ {
		n += lenVarint(len(suffixValues[i].Suffix))
		n += len(suffixValues[i].Suffix)
		n += lenVarint(len(suffixValues[i].Value))
		n += len(suffixValues[i].Value)
	}
	return n
}

// EncodeSetSuffixValues encodes a slice of SuffixValues for a RangeKeySet into
// dst. The length of dst must be greater than or equal to
// EncodedSetSuffixValuesLen. EncodeSetSuffixValues returns the number of bytes
// written, which should always equal the EncodedSetValueLen with the same
// arguments.
func EncodeSetSuffixValues(dst []byte, suffixValues []SuffixValue) int {
	// Encode the list of (suffix, value-len) tuples.
	var n int
	for i := 0; i < len(suffixValues); i++ {
		// Encode the length of the suffix.
		n += binary.PutUvarint(dst[n:], uint64(len(suffixValues[i].Suffix)))

		// Encode the suffix itself.
		n += copy(dst[n:], suffixValues[i].Suffix)

		// Encode the value length.
		n += binary.PutUvarint(dst[n:], uint64(len(suffixValues[i].Value)))

		// Encode the value itself.
		n += copy(dst[n:], suffixValues[i].Value)
	}
	return n
}

// EncodedSetValueLen precomputes the length of a RangeKeySet's value when
// encoded. It may be used to construct a buffer of the appropriate size before
// encoding.
func EncodedSetValueLen(endKey []byte, suffixValues []SuffixValue) int {
	n := lenVarint(len(endKey))
	n += len(endKey)
	n += EncodedSetSuffixValuesLen(suffixValues)
	return n
}

// EncodeSetValue encodes a RangeKeySet's value into dst. The length of dst must
// be greater than or equal to EncodedSetValueLen. EncodeSetValue returns the
// number of bytes written, which should always equal the EncodedSetValueLen
// with the same arguments.
func EncodeSetValue(dst []byte, endKey []byte, suffixValues []SuffixValue) int {
	// First encode the end key as a varstring.
	n := binary.PutUvarint(dst, uint64(len(endKey)))
	n += copy(dst[n:], endKey)
	n += EncodeSetSuffixValues(dst[n:], suffixValues)
	return n
}

// DecodeEndKey reads the end key from the beginning of a range key (RANGEKEYSET,
// RANGEKEYUNSET or RANGEKEYDEL)'s physical encoded value. Both sets and unsets
// encode the range key, plus additional data in the value.
func DecodeEndKey(kind base.InternalKeyKind, data []byte) (endKey, value []byte, ok bool) {
	switch kind {
	case base.InternalKeyKindRangeKeyDelete:
		// No splitting is necessary for range key deletes. The value is the end
		// key, and there is no additional associated value.
		return data, nil, true
	case base.InternalKeyKindRangeKeySet, base.InternalKeyKindRangeKeyUnset:
		v, n := binary.Uvarint(data)
		if n <= 0 {
			return nil, nil, false
		}
		endKey, value = data[n:n+int(v)], data[n+int(v):]
		return endKey, value, true
	default:
		panic(errors.Newf("key kind %s is not a range key kind", kind))
	}
}

// DecodeSuffixValue decodes a single encoded SuffixValue from a RangeKeySet's
// split value. The end key must have already been stripped from the
// RangeKeySet's value (see DecodeEndKey).
func DecodeSuffixValue(data []byte) (sv SuffixValue, rest []byte, ok bool) {
	// Decode the suffix.
	sv.Suffix, data, ok = decodeVarstring(data)
	if !ok {
		return SuffixValue{}, nil, false
	}
	// Decode the value.
	sv.Value, data, ok = decodeVarstring(data)
	if !ok {
		return SuffixValue{}, nil, false
	}
	return sv, data, true
}

// EncodedUnsetSuffixesLen precomputes the length of the given slice of
// suffixes, when encoded for a RangeKeyUnset. It may be used to construct a
// buffer of the appropriate size before encoding.
func EncodedUnsetSuffixesLen(suffixes [][]byte) int {
	var n int
	for i := 0; i < len(suffixes); i++ {
		n += lenVarint(len(suffixes[i]))
		n += len(suffixes[i])
	}
	return n
}

// EncodeUnsetSuffixes encodes a slice of suffixes for a RangeKeyUnset into dst.
// The length of dst must be greater than or equal to EncodedUnsetSuffixesLen.
// EncodeUnsetSuffixes returns the number of bytes written, which should always
// equal the EncodedUnsetSuffixesLen with the same arguments.
func EncodeUnsetSuffixes(dst []byte, suffixes [][]byte) int {
	// Encode the list of (suffix, value-len) tuples.
	var n int
	for i := 0; i < len(suffixes); i++ {
		//  Encode the length of the suffix.
		n += binary.PutUvarint(dst[n:], uint64(len(suffixes[i])))

		// Encode the suffix itself.
		n += copy(dst[n:], suffixes[i])
	}
	return n
}

// EncodedUnsetValueLen precomputes the length of a RangeKeyUnset's value when
// encoded.  It may be used to construct a buffer of the appropriate size before
// encoding.
func EncodedUnsetValueLen(endKey []byte, suffixes [][]byte) int {
	n := lenVarint(len(endKey))
	n += len(endKey)
	n += EncodedUnsetSuffixesLen(suffixes)
	return n
}

// EncodeUnsetValue encodes a RangeKeyUnset's value into dst. The length of dst
// must be greater than or equal to EncodedUnsetValueLen. EncodeUnsetValue
// returns the number of bytes written, which should always equal the
// EncodedUnsetValueLen with the same arguments.
func EncodeUnsetValue(dst []byte, endKey []byte, suffixes [][]byte) int {
	// First encode the end key as a varstring.
	n := binary.PutUvarint(dst, uint64(len(endKey)))
	n += copy(dst[n:], endKey)
	n += EncodeUnsetSuffixes(dst[n:], suffixes)
	return n
}

// DecodeSuffix decodes a single suffix from the beginning of data. If decoding
// suffixes from a RangeKeyUnset's value, the end key must have already been
// stripped from the RangeKeyUnset's value (see DecodeEndKey).
func DecodeSuffix(data []byte) (suffix, rest []byte, ok bool) {
	return decodeVarstring(data)
}

func decodeVarstring(data []byte) (v, rest []byte, ok bool) {
	// Decode the length of the string.
	l, n := binary.Uvarint(data)
	if n <= 0 {
		return nil, nil, ok
	}

	// Extract the string itself.
	return data[n : n+int(l)], data[n+int(l):], true
}

// Format returns a formatter for the range key (either a RANGEKEYSET,
// RANGEKEYUNSET or RANGEKEYDEL) represented by s. The formatting returned is
// parseable with Parse.
func Format(formatKey base.FormatKey, s keyspan.Span) fmt.Formatter {
	return prettyRangeKeySpan{Span: s, formatKey: formatKey}
}

type prettyRangeKeySpan struct {
	keyspan.Span
	formatKey base.FormatKey
}

func (k prettyRangeKeySpan) Format(s fmt.State, c rune) {
	fmt.Fprintf(s, "%s.%s.%d: %s",
		k.formatKey(k.Start.UserKey),
		k.Start.Kind(),
		k.Start.SeqNum(),
		k.End)
	switch k.Start.Kind() {
	case base.InternalKeyKindRangeKeySet:
		fmt.Fprint(s, " [")
		value := k.Value
		for len(value) > 0 {
			if len(value) < len(k.Value) {
				fmt.Fprint(s, ",")
			}
			sv, rest, ok := DecodeSuffixValue(value)
			if !ok {
				panic(base.CorruptionErrorf("corrupt set value: unable to decode suffix-value tuple"))
			}
			value = rest
			fmt.Fprintf(s, "(%s=%s)", k.formatKey(sv.Suffix), sv.Value)
		}
		fmt.Fprint(s, "]")
	case base.InternalKeyKindRangeKeyUnset:
		fmt.Fprint(s, " [")
		value := k.Value
		for len(value) > 0 {
			if len(value) < len(k.Value) {
				fmt.Fprint(s, ",")
			}
			suffix, rest, ok := DecodeSuffix(value)
			if !ok {
				panic(base.CorruptionErrorf("corrupt unset value: unable to decode suffix"))
			}
			value = rest
			fmt.Fprint(s, k.formatKey(suffix))
		}
		fmt.Fprint(s, "]")
	case base.InternalKeyKindRangeKeyDelete:
		if len(k.Value) > 0 {
			panic("unexpected value on a RANGEKEYDEL")
		}
		// No additional value to format.
	default:
		panic(fmt.Sprintf("%s keys are not range keys", k.Start.Kind()))
	}
}

// Parse parses a string representation of a range key (eg, RANGEKEYSET,
// RANGEKEYUNSET or RANGEKEYDEL). Parse is used in tests and debugging
// facilities. It's exported for use in tests outside of the rangekey package.
//
// Parse expects the input string to be in one of the three formats:
// - start.RANGEKEYSET.seqnum: end [(s1=v1), (s2=v2), (s3=v3)]
// - start.RANGEKEYUNSET.seqnum: end [s1, s2, s3]
// - start.RANGEKEYDEL.seqnum: end
//
// For example:
// - a.RANGEKEYSET.5: c [(@t10=foo), (@t9=bar)]
// - a.RANGEKEYUNSET.5: c [@t10, @t9]
// - a.RANGEKEYDEL.5: c
func Parse(s string) (key base.InternalKey, value []byte) {
	sep := strings.IndexByte(s, ':')
	if sep == -1 {
		panic("range key string representation missing key-value separator :")
	}
	startKey := base.ParseInternalKey(strings.TrimSpace(s[:sep]))

	switch startKey.Kind() {
	case base.InternalKeyKindRangeKeySet:
		openBracket := strings.IndexByte(s[sep:], '[')
		closeBracket := strings.IndexByte(s[sep:], ']')
		endKey := strings.TrimSpace(s[sep+1 : sep+openBracket])
		itemStrs := strings.Split(s[sep+openBracket+1:sep+closeBracket], ",")

		var suffixValues []SuffixValue
		for _, itemStr := range itemStrs {
			itemStr = strings.Trim(itemStr, "() \n\t")
			i := strings.IndexByte(itemStr, '=')
			if i == -1 {
				panic(fmt.Sprintf("range key string %q missing '=' key,value tuple delim", s))
			}
			suffixValues = append(suffixValues, SuffixValue{
				Suffix: []byte(strings.TrimSpace(itemStr[:i])),
				Value:  []byte(strings.TrimSpace(itemStr[i+1:])),
			})
		}
		value = make([]byte, EncodedSetValueLen([]byte(endKey), suffixValues))
		EncodeSetValue(value, []byte(endKey), suffixValues)
		return startKey, value

	case base.InternalKeyKindRangeKeyUnset:
		openBracket := strings.IndexByte(s[sep:], '[')
		closeBracket := strings.IndexByte(s[sep:], ']')
		endKey := strings.TrimSpace(s[sep+1 : sep+openBracket])
		itemStrs := strings.Split(s[sep+openBracket+1:sep+closeBracket], ",")

		var suffixes [][]byte
		for _, itemStr := range itemStrs {
			suffixes = append(suffixes, []byte(strings.TrimSpace(itemStr)))
		}
		value = make([]byte, EncodedUnsetValueLen([]byte(endKey), suffixes))
		EncodeUnsetValue(value, []byte(endKey), suffixes)
		return startKey, value

	case base.InternalKeyKindRangeKeyDelete:
		return startKey, []byte(strings.TrimSpace(s[sep+1:]))

	default:
		panic(fmt.Sprintf("key kind %q not a range key", startKey.Kind()))
	}
}

// RecombinedValueLen returns the length of the byte slice that results from
// re-encoding the end key and the user-value as a physical range key value.
func RecombinedValueLen(kind base.InternalKeyKind, endKey, userValue []byte) int {
	n := len(endKey)
	if kind == base.InternalKeyKindRangeKeyDelete {
		// RANGEKEYDELs are not varint encoded.
		return n
	}
	return lenVarint(len(endKey)) + len(endKey) + len(userValue)
}

// RecombineValue re-encodes the end key and user-value as a physical range key
// value into the destination byte slice.
func RecombineValue(kind base.InternalKeyKind, dst, endKey, userValue []byte) int {
	if kind == base.InternalKeyKindRangeKeyDelete {
		// RANGEKEYDELs are not varint encoded.
		return copy(dst, endKey)
	}
	n := binary.PutUvarint(dst, uint64(len(endKey)))
	n += copy(dst[n:], endKey)
	n += copy(dst[n:], userValue)
	return n
}

// IsRangeKey returns true if the given key kind is one of the range key kinds.
func IsRangeKey(kind base.InternalKeyKind) bool {
	switch kind {
	case base.InternalKeyKindRangeKeyDelete,
		base.InternalKeyKindRangeKeyUnset,
		base.InternalKeyKindRangeKeySet:
		return true
	default:
		return false
	}
}

func lenVarint(v int) (n int) {
	x := uint32(v)
	n++
	for x >= 0x80 {
		x >>= 7
		n++
	}
	return n
}
