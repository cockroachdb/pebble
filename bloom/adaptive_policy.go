// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package bloom

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
)

// AdaptivePolicy implements base.TableFilterPolicy for Bloom filters. It
// automatically reduces the number of bits per key so the filter size stays
// under a given limit.
func AdaptivePolicy(targetBitsPerKey uint32, maxFilterSize uint64) base.TableFilterPolicy {
	return adaptivePolicyImpl{TargetBitsPerKey: targetBitsPerKey, MaxSize: maxFilterSize}
}

type adaptivePolicyImpl struct {
	TargetBitsPerKey uint32
	MaxSize          uint64
}

var _ base.TableFilterPolicy = adaptivePolicyImpl{}

func (p adaptivePolicyImpl) Name() string {
	return fmt.Sprintf("adaptive_bloom(%d,%d)", p.TargetBitsPerKey, p.MaxSize)
}

func (p adaptivePolicyImpl) NewWriter() base.TableFilterWriter {
	return newAdaptiveFilterWriter(p.TargetBitsPerKey, p.MaxSize)
}

// adaptiveFilterWriter is a TableFilterWriter that uses up to w.bitsPerKey to
// create a filter of up to maxSize bytes.
type adaptiveFilterWriter struct {
	hc               hashCollector
	targetBitsPerKey uint32
	maxSize          uint64
}

func (aw *adaptiveFilterWriter) AddKey(key []byte) {
	aw.hc.Add(hash(key))
}

func (aw *adaptiveFilterWriter) Finish() (_ []byte, _ base.TableFilterFamily, ok bool) {
	numHashes := aw.hc.NumHashes()
	if numHashes == 0 {
		return nil, "", false
	}
	bitsPerKey := aw.targetBitsPerKey
	if filterSize := FilterSize(numHashes, bitsPerKey); filterSize > aw.maxSize {
		bitsPerKey = MaxBitsPerKey(numHashes, aw.maxSize)
		// A single-bit filter is not very useful; it is large (at least maxSize/2) so
		// it wastes memory and bandwidth.
		if bitsPerKey < 2 {
			return nil, "", false
		}
	}
	nLines := calculateNumLines(numHashes, bitsPerKey)
	numProbes := calculateProbes(bitsPerKey)
	filter := buildFilter(nLines, numProbes, &aw.hc)
	aw.hc.Reset()
	return filter, Family, true
}

var _ base.TableFilterWriter = (*adaptiveFilterWriter)(nil)

func newAdaptiveFilterWriter(targetBitsPerKey uint32, maxSize uint64) *adaptiveFilterWriter {
	aw := &adaptiveFilterWriter{
		targetBitsPerKey: targetBitsPerKey,
		maxSize:          maxSize,
	}
	aw.hc.Init()
	return aw
}

// FilterSize returns the size in bytes of a bloom filter for the given number
// of keys (hashes) and bits per key. The size includes the 5-byte trailer that
// stores the number of probes and cache lines.
func FilterSize(numKeys uint, bitsPerKey uint32) uint64 {
	if numKeys <= 0 || bitsPerKey <= 0 {
		return 0
	}
	// Filter format: nLines * cacheLineSize bytes of filter bits, plus 5 bytes
	// for the trailer (1 byte for number of probes, 4 bytes for number of lines).
	return uint64(calculateNumLines(numKeys, bitsPerKey))*cacheLineSize + 5
}

// MaxBitsPerKey returns the maximum bits per key that can be used to create a
// bloom filter with at most maxFilterSize bytes for the given number of keys.
// Returns 0 if the constraints cannot be satisfied (i.e., even 1 bit per key
// would exceed the maximum filter size).
func MaxBitsPerKey(numKeys uint, maxFilterSize uint64) uint32 {
	if numKeys <= 0 || maxFilterSize <= 5 {
		return 0
	}
	// Compute the maximum number of cache lines that fit in maxFilterSize.
	maxLines := (maxFilterSize - 5) / cacheLineSize
	if maxLines == 0 {
		return 0
	}
	// The filter always uses an odd number of cache lines (nLines |= 1 in
	// filterNumLines). If maxLines is even, we can only use maxLines-1 lines.
	if maxLines&1 == 0 {
		maxLines--
	}
	// Given maxLines cache lines, we have maxLines * cacheLineBits bits available.
	// The maximum bits per key is the total bits divided by number of keys.
	result := uint32(min(cacheLineBits, maxLines*cacheLineBits/uint64(numKeys)))
	if invariants.Enabled && result > 0 {
		// Cross-check: FilterSize with result should be <= maxFilterSize.
		if size := FilterSize(numKeys, result); size > maxFilterSize {
			panic(errors.AssertionFailedf(
				"MaxBitsPerKey invariant violated: FilterSize(%d, %d) = %d > %d",
				numKeys, result, size, maxFilterSize))
		}
		// Cross-check: FilterSize with result+1 should be > maxFilterSize.
		if result < cacheLineBits {
			if sizeNext := FilterSize(numKeys, result+1); sizeNext <= maxFilterSize {
				panic(errors.AssertionFailedf(
					"MaxBitsPerKey invariant violated: FilterSize(%d, %d) = %d <= %d but returned %d",
					numKeys, result+1, sizeNext, maxFilterSize, result))
			}
		}
	}
	return result
}
