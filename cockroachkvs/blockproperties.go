// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cockroachkvs

import (
	"encoding/binary"
	"math"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/sstable"
)

const mvccWallTimeIntervalCollector = "MVCCTimeInterval"

// BlockPropertyCollectors is a list of constructors for block-property
// collectors used by CockroachDB.
var BlockPropertyCollectors = []func() pebble.BlockPropertyCollector{
	func() pebble.BlockPropertyCollector {
		return sstable.NewBlockIntervalCollector(
			mvccWallTimeIntervalCollector,
			pebbleIntervalMapper{},
			MVCCBlockIntervalSuffixReplacer{},
		)
	},
}

// NewMVCCTimeIntervalFilter constructs a new block-property filter that skips
// keys that encode timestamps with wall times that do not fall within the
// interval [minWallTime,maxWallTime].
func NewMVCCTimeIntervalFilter(minWallTime, maxWallTime uint64) sstable.BlockPropertyFilter {
	return sstable.NewBlockIntervalFilter(mvccWallTimeIntervalCollector,
		uint64(minWallTime),
		uint64(maxWallTime)+1,
		MVCCBlockIntervalSuffixReplacer{},
	)
}

// MVCCWallTimeIntervalRangeKeyMask implements pebble.BlockPropertyFilterMask
// for filtering blocks using the MVCCTimeInterval block property during range
// key masking.
type MVCCWallTimeIntervalRangeKeyMask struct {
	sstable.BlockIntervalFilter
}

// Init initializees the mask and its block interval filter.
func (m *MVCCWallTimeIntervalRangeKeyMask) Init() {
	m.BlockIntervalFilter.Init(
		mvccWallTimeIntervalCollector,
		0, math.MaxUint64,
		MVCCBlockIntervalSuffixReplacer{})
}

// SetSuffix implements the pebble.BlockPropertyFilterMask interface.
func (m *MVCCWallTimeIntervalRangeKeyMask) SetSuffix(suffix []byte) error {
	if len(suffix) == 0 {
		// This is currently impossible, because the only range key Cockroach
		// writes today is the MVCC Delete Range that's always suffixed.
		return nil
	}
	wall, _, err := DecodeMVCCTimestampSuffix(suffix)
	if err != nil {
		return err
	}
	m.BlockIntervalFilter.SetInterval(wall, math.MaxUint64)
	return nil
}

var _ sstable.BlockIntervalSuffixReplacer = MVCCBlockIntervalSuffixReplacer{}

// MVCCBlockIntervalSuffixReplacer implements the
// sstable.BlockIntervalSuffixReplacer interface for MVCC timestamp intervals.
type MVCCBlockIntervalSuffixReplacer struct{}

func (MVCCBlockIntervalSuffixReplacer) ApplySuffixReplacement(
	interval sstable.BlockInterval, newSuffix []byte,
) (sstable.BlockInterval, error) {
	synthDecodedWalltime, _, err := DecodeMVCCTimestampSuffix(newSuffix)
	if err != nil {
		return sstable.BlockInterval{}, errors.AssertionFailedf("could not decode synthetic suffix")
	}
	// The returned bound includes the synthetic suffix, regardless of its logical
	// component.
	return sstable.BlockInterval{Lower: synthDecodedWalltime, Upper: synthDecodedWalltime + 1}, nil
}

type pebbleIntervalMapper struct{}

var _ sstable.IntervalMapper = pebbleIntervalMapper{}

// MapPointKey is part of the sstable.IntervalMapper interface.
func (pebbleIntervalMapper) MapPointKey(
	key pebble.InternalKey, value []byte,
) (sstable.BlockInterval, error) {
	return mapSuffixToInterval(key.UserKey)
}

// MapRangeKey is part of the sstable.IntervalMapper interface.
func (pebbleIntervalMapper) MapRangeKeys(span sstable.Span) (sstable.BlockInterval, error) {
	var res sstable.BlockInterval
	for _, k := range span.Keys {
		i, err := mapSuffixToInterval(k.Suffix)
		if err != nil {
			return sstable.BlockInterval{}, err
		}
		res.UnionWith(i)
	}
	return res, nil
}

// mapSuffixToInterval maps the suffix of a key to a timestamp interval.
// The buffer can be an entire key or just the suffix.
func mapSuffixToInterval(b []byte) (sstable.BlockInterval, error) {
	if len(b) == 0 {
		return sstable.BlockInterval{}, nil
	}
	// Last byte is the version length + 1 when there is a version,
	// else it is 0.
	versionLen := int(b[len(b)-1])
	if versionLen == 0 {
		// This is not an MVCC key that we can collect.
		return sstable.BlockInterval{}, nil
	}
	// prefixPartEnd points to the sentinel byte, unless this is a bare suffix, in
	// which case the index is -1.
	prefixPartEnd := len(b) - 1 - versionLen
	// Sanity check: the index should be >= -1. Additionally, if the index is >=
	// 0, it should point to the sentinel byte, as this is a full EngineKey.
	if prefixPartEnd < -1 || (prefixPartEnd >= 0 && b[prefixPartEnd] != 0x00) {
		return sstable.BlockInterval{}, errors.Errorf("invalid key %x", b)
	}
	// We don't need the last byte (the version length).
	versionLen--
	// Only collect if this looks like an MVCC timestamp.
	if versionLen == engineKeyVersionWallTimeLen ||
		versionLen == engineKeyVersionWallAndLogicalTimeLen ||
		versionLen == engineKeyVersionWallLogicalAndSyntheticTimeLen {
		// INVARIANT: -1 <= prefixPartEnd < len(b) - 1.
		// Version consists of the bytes after the sentinel and before the length.
		ts := binary.BigEndian.Uint64(b[prefixPartEnd+1:])
		return sstable.BlockInterval{Lower: ts, Upper: ts + 1}, nil
	}
	return sstable.BlockInterval{}, nil
}

type MaxMVCCTimestampProperty struct{}

// Name is part of the cockroachkvs.MaxMVCCTimestampProperty interface.
func (MaxMVCCTimestampProperty) Name() string {
	return mvccWallTimeIntervalCollector
}

// Extract is part of the cockroachkvs.MaxMVCCTimestampProperty interface.
// It extracts the maximum MVCC timestamp from the encoded block property and
// returns it as a CockroachDB-formatted suffix.
func (MaxMVCCTimestampProperty) Extract(
	dst []byte, encodedProperty []byte,
) (suffix []byte, ok bool, err error) {
	if len(encodedProperty) <= 1 {
		return nil, false, nil
	}
	// First byte is shortID, skip it and decode interval from remainder.
	buf := encodedProperty[1:]
	if len(buf) == 0 {
		return nil, false, nil
	}
	// Decode the block interval using the same logic as sstable.decodeBlockInterval
	var interval sstable.BlockInterval
	var n int
	interval.Lower, n = binary.Uvarint(buf)
	if n <= 0 || n >= len(buf) {
		return nil, false, base.CorruptionErrorf("cannot decode interval from buf %x", buf)
	}
	pos := n
	interval.Upper, n = binary.Uvarint(buf[pos:])
	pos += n
	if pos != len(buf) || n <= 0 {
		return nil, false, base.CorruptionErrorf("cannot decode interval from buf %x", buf)
	}
	// Delta decode.
	interval.Upper += interval.Lower
	if interval.Upper < interval.Lower {
		return nil, false, base.CorruptionErrorf("unexpected overflow, upper %d < lower %d", interval.Upper, interval.Lower)
	}
	if interval.IsEmpty() {
		return nil, false, nil
	}
	dst = append(dst, make([]byte, 9)...)
	binary.BigEndian.PutUint64(dst[len(dst)-9:], interval.Upper)
	dst[len(dst)-1] = 9
	return dst, true, nil
}
