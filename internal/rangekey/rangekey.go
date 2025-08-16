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

	"github.com/cockroachdb/crlib/crencoding"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
)

// Encode takes a Span containing only range keys. It invokes the provided
// closure with the encoded internal keys that represent the Span's state. The
// keys and values passed to emit are only valid until the closure returns.
// If emit returns an error, Encode stops and returns the error.
func Encode(s keyspan.Span, emit func(k base.InternalKey, v []byte) error) error {
	enc := Encoder{Emit: emit}
	return enc.Encode(s)
}

// An Encoder encodes range keys into their on-disk InternalKey format. An
// Encoder holds internal buffers, reused between Emit calls.
type Encoder struct {
	Emit   func(base.InternalKey, []byte) error
	buf    []byte
	unsets [][]byte
	sets   []SuffixValue
}

// Encode takes a Span containing only range keys. It invokes the Encoder's Emit
// closure with the encoded internal keys that represent the Span's state. The
// keys and values passed to emit are only valid until the closure returns.  If
// Emit returns an error, Encode stops and returns the error.
//
// The encoded key-value pair passed to Emit is only valid until the closure
// completes.
func (e *Encoder) Encode(s keyspan.Span) error {
	if s.Empty() {
		return nil
	}

	// This for loop iterates through the span's keys, which are sorted by
	// sequence number descending, grouping them into sequence numbers. All keys
	// with identical sequence numbers are flushed together.
	var del bool
	var seqNum base.SeqNum
	for i := range s.Keys {
		if i == 0 || s.Keys[i].SeqNum() != seqNum {
			if i > 0 {
				// Flush all the existing internal keys that exist at seqNum.
				if err := e.flush(s, seqNum, del); err != nil {
					return err
				}
			}

			// Reset sets, unsets, del.
			seqNum = s.Keys[i].SeqNum()
			del = false
			e.sets = e.sets[:0]
			e.unsets = e.unsets[:0]
		}

		switch s.Keys[i].Kind() {
		case base.InternalKeyKindRangeKeySet:
			e.sets = append(e.sets, SuffixValue{
				Suffix: s.Keys[i].Suffix,
				Value:  s.Keys[i].Value,
			})
		case base.InternalKeyKindRangeKeyUnset:
			e.unsets = append(e.unsets, s.Keys[i].Suffix)
		case base.InternalKeyKindRangeKeyDelete:
			del = true
		default:
			return base.CorruptionErrorf("pebble: %s key kind is not a range key", s.Keys[i].Kind())
		}
	}
	return e.flush(s, seqNum, del)
}

// flush constructs internal keys for accumulated key state, and emits the
// internal keys.
func (e *Encoder) flush(s keyspan.Span, seqNum base.SeqNum, del bool) error {
	if len(e.sets) > 0 {
		ik := base.MakeInternalKey(s.Start, seqNum, base.InternalKeyKindRangeKeySet)
		l := EncodedSetValueLen(s.End, e.sets)
		if l > cap(e.buf) {
			e.buf = make([]byte, l)
		}
		EncodeSetValue(e.buf[:l], s.End, e.sets)
		if err := e.Emit(ik, e.buf[:l]); err != nil {
			return err
		}
	}
	if len(e.unsets) > 0 {
		ik := base.MakeInternalKey(s.Start, seqNum, base.InternalKeyKindRangeKeyUnset)
		l := EncodedUnsetValueLen(s.End, e.unsets)
		if l > cap(e.buf) {
			e.buf = make([]byte, l)
		}
		EncodeUnsetValue(e.buf[:l], s.End, e.unsets)
		if err := e.Emit(ik, e.buf[:l]); err != nil {
			return err
		}
	}
	if del {
		ik := base.MakeInternalKey(s.Start, seqNum, base.InternalKeyKindRangeKeyDelete)
		// s.End is stored directly in the value for RangeKeyDeletes.
		if err := e.Emit(ik, s.End); err != nil {
			return err
		}
	}
	return nil
}

// Decode takes an internal key pair encoding range key(s) and returns a decoded
// keyspan containing the keys. If keysBuf is provided, keys will be appended to
// it.
func Decode(ik base.InternalKey, v []byte, keysBuf []keyspan.Key) (keyspan.Span, error) {
	var s keyspan.Span
	s.Start = ik.UserKey
	var err error
	s.End, v, err = DecodeEndKey(ik.Kind(), v)
	if err != nil {
		return keyspan.Span{}, err
	}
	s.Keys, err = appendKeys(keysBuf, ik, v)
	if err != nil {
		return keyspan.Span{}, err
	}
	return s, nil
}

// DecodeIntoSpan decodes an internal key pair encoding range key(s) and appends
// them to the given span. The start and end keys must match those in the span.
func DecodeIntoSpan(cmp base.Compare, ik base.InternalKey, v []byte, s *keyspan.Span) error {
	// Hydrate the user key bounds.
	startKey := ik.UserKey
	endKey, v, err := DecodeEndKey(ik.Kind(), v)
	if err != nil {
		return err
	}
	// This function should only be called when ik.UserKey matches the Start of
	// the span we already have. If this is not the case, it is a bug in the
	// calling code.
	if invariants.Enabled && cmp(s.Start, startKey) != 0 {
		return base.AssertionFailedf("DecodeIntoSpan called with different start key")
	}
	// The value can come from disk or from the user, so we want to check the end
	// key in all builds.
	if cmp(s.End, endKey) != 0 {
		return base.CorruptionErrorf("pebble: corrupt range key fragmentation")
	}
	s.Keys, err = appendKeys(s.Keys, ik, v)
	return err
}

func appendKeys(buf []keyspan.Key, ik base.InternalKey, v []byte) ([]keyspan.Key, error) {
	// Hydrate the contents of the range key(s).
	switch ik.Kind() {
	case base.InternalKeyKindRangeKeySet:
		for len(v) > 0 {
			var sv SuffixValue
			var ok bool
			sv, v, ok = decodeSuffixValue(v)
			if !ok {
				return nil, base.CorruptionErrorf("pebble: unable to decode range key suffix-value tuple")
			}
			buf = append(buf, keyspan.Key{
				Trailer: ik.Trailer,
				Suffix:  sv.Suffix,
				Value:   sv.Value,
			})
		}
	case base.InternalKeyKindRangeKeyUnset:
		for len(v) > 0 {
			var suffix []byte
			var ok bool
			suffix, v, ok = decodeSuffix(v)
			if !ok {
				return nil, base.CorruptionErrorf("pebble: unable to decode range key unset suffix")
			}
			buf = append(buf, keyspan.Key{
				Trailer: ik.Trailer,
				Suffix:  suffix,
			})
		}
	case base.InternalKeyKindRangeKeyDelete:
		if len(v) > 0 {
			return nil, base.CorruptionErrorf("pebble: RANGEKEYDELs must not contain additional data")
		}
		buf = append(buf, keyspan.Key{Trailer: ik.Trailer})
	default:
		return nil, base.CorruptionErrorf("pebble: %s is not a range key", ik.Kind())
	}
	return buf, nil
}

// SuffixValue represents a tuple of a suffix and a corresponding value. A
// physical RANGEKEYSET key may contain many logical RangeKeySets, each
// represented with a separate SuffixValue tuple.
type SuffixValue struct {
	Suffix []byte
	Value  []byte
}

// encodedSetSuffixValuesLen precomputes the length of the given slice of
// SuffixValues, when encoded for a RangeKeySet. It may be used to construct a
// buffer of the appropriate size before encoding.
func encodedSetSuffixValuesLen(suffixValues []SuffixValue) int {
	var n int
	for i := 0; i < len(suffixValues); i++ {
		n += crencoding.UvarintLen32(uint32(len(suffixValues[i].Suffix)))
		n += len(suffixValues[i].Suffix)
		n += crencoding.UvarintLen32(uint32(len(suffixValues[i].Value)))
		n += len(suffixValues[i].Value)
	}
	return n
}

// encodeSetSuffixValues encodes a slice of SuffixValues for a RangeKeySet into
// dst. The length of dst must be greater than or equal to
// encodedSetSuffixValuesLen. encodeSetSuffixValues returns the number of bytes
// written, which should always equal the EncodedSetValueLen with the same
// arguments.
func encodeSetSuffixValues(dst []byte, suffixValues []SuffixValue) int {
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
	n := crencoding.UvarintLen32(uint32(len(endKey)))
	n += len(endKey)
	n += encodedSetSuffixValuesLen(suffixValues)
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
	n += encodeSetSuffixValues(dst[n:], suffixValues)
	return n
}

// DecodeEndKey reads the end key from the beginning of a range key (RANGEKEYSET,
// RANGEKEYUNSET or RANGEKEYDEL)'s physical encoded value. Both sets and unsets
// encode the range key, plus additional data in the value.
func DecodeEndKey(kind base.InternalKeyKind, data []byte) (endKey, value []byte, _ error) {
	switch kind {
	case base.InternalKeyKindRangeKeyDelete:
		// No splitting is necessary for range key deletes. The value is the end
		// key, and there is no additional associated value.
		return data, nil, nil

	case base.InternalKeyKindRangeKeySet, base.InternalKeyKindRangeKeyUnset:
		v, n := binary.Uvarint(data)
		if n <= 0 || uint64(n)+v >= uint64(len(data)) {
			return nil, nil, base.CorruptionErrorf("pebble: unable to decode range key end from %s", kind)
		}
		endKey, value = data[n:n+int(v)], data[n+int(v):]
		return endKey, value, nil

	default:
		return nil, nil, base.AssertionFailedf("key kind %s is not a range key kind", kind)
	}
}

// decodeSuffixValue decodes a single encoded SuffixValue from a RangeKeySet's
// split value. The end key must have already been stripped from the
// RangeKeySet's value (see DecodeEndKey).
func decodeSuffixValue(data []byte) (sv SuffixValue, rest []byte, ok bool) {
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

// encodedUnsetSuffixesLen precomputes the length of the given slice of
// suffixes, when encoded for a RangeKeyUnset. It may be used to construct a
// buffer of the appropriate size before encoding.
func encodedUnsetSuffixesLen(suffixes [][]byte) int {
	var n int
	for i := 0; i < len(suffixes); i++ {
		n += crencoding.UvarintLen32(uint32(len(suffixes[i])))
		n += len(suffixes[i])
	}
	return n
}

// encodeUnsetSuffixes encodes a slice of suffixes for a RangeKeyUnset into dst.
// The length of dst must be greater than or equal to EncodedUnsetSuffixesLen.
// EncodeUnsetSuffixes returns the number of bytes written, which should always
// equal the EncodedUnsetSuffixesLen with the same arguments.
func encodeUnsetSuffixes(dst []byte, suffixes [][]byte) int {
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
	n := crencoding.UvarintLen32(uint32(len(endKey)))
	n += len(endKey)
	n += encodedUnsetSuffixesLen(suffixes)
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
	n += encodeUnsetSuffixes(dst[n:], suffixes)
	return n
}

// decodeSuffix decodes a single suffix from the beginning of data. If decoding
// suffixes from a RangeKeyUnset's value, the end key must have already been
// stripped from the RangeKeyUnset's value (see DecodeEndKey).
func decodeSuffix(data []byte) (suffix, rest []byte, ok bool) {
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
