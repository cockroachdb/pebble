// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cockroachkvs

import (
	"encoding/binary"
	"math"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/sstable"
)

const mvccWallTimeIntervalCollector = "MVCCTimeInterval"

// BlockPropertyCollectors is a list of constructors for block-property
// collectors used by CockroachDB.
var BlockPropertyCollectors = []func() sstable.BlockPropertyCollector{
	func() sstable.BlockPropertyCollector {
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
	key sstable.InternalKey, value []byte,
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

// Name implements pebble.MaximumSuffixProperty interface.
func (MaxMVCCTimestampProperty) Name() string {
	return mvccWallTimeIntervalCollector
}

// Extract implements the sstable.MaximumSuffixProperty interface. It returns
// a wall-time-only suffix that, per that interface's contract, sorts
// at-or-before every real suffix in the block.
//
// The encoded BlockInterval is half-open [Lower, Upper), with each MVCC key
// contributing {Lower: ts, Upper: ts+1} (see mapSuffixToInterval). Hence
// Upper is one greater than the largest wall time present in the block.
//
// We encode wall = Upper rather than Upper-1 (the literal largest wall in
// the block). Upper-1 would not be a valid upper bound: a suffix at
// (Upper-1, logical=0) sorts *after* a real key at (Upper-1, logical>0),
// since larger logical sorts first within a wall. Encoding Upper sidesteps
// the logical component entirely.
func (MaxMVCCTimestampProperty) Extract(
	dst []byte, encodedProperty []byte,
) (suffix []byte, ok bool, err error) {
	if len(encodedProperty) <= 1 {
		return nil, false, nil
	}
	// First byte is shortID, skip it and decode interval from remainder.
	interval, err := sstable.DecodeBlockInterval(encodedProperty[1:])
	if err != nil {
		return nil, false, err
	} else if interval.IsEmpty() {
		return nil, false, nil
	}
	dst = append(dst, make([]byte, suffixLenWithWall)...)
	binary.BigEndian.PutUint64(dst[len(dst)-suffixLenWithWall:], interval.Upper)
	dst[len(dst)-1] = suffixLenWithWall
	return dst, true, nil
}
