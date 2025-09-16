// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"fmt"
	"math/rand/v2"
	"slices"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/cockroachkvs"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/colblk"
)

// CockroachKeyFormat provides a KeyFormat implementation that uses
// CockroachDB's key encoding (as defined in the cockroachkvs package).
var CockroachKeyFormat = KeyFormat{
	Name:                    "cockroachkvs",
	Comparer:                &cockroachkvs.Comparer,
	KeySchema:               func() *colblk.KeySchema { return &cockroachkvs.KeySchema }(),
	BlockPropertyCollectors: cockroachkvs.BlockPropertyCollectors,
	FormatKey: func(k UserKey) string {
		if len(k) == 0 {
			return ""
		}
		return fmt.Sprint(cockroachkvs.FormatKey(k))
	},
	FormatKeySuffix: func(s UserKeySuffix) string {
		if len(s) == 0 {
			return ""
		}
		return fmt.Sprint(cockroachkvs.FormatKeySuffix(s))
	},
	ParseFormattedKey: func(formattedKey string) UserKey {
		return UserKey(cockroachkvs.ParseFormattedKey(formattedKey))
	},
	ParseFormattedKeySuffix: func(formattedKeySuffix string) UserKeySuffix {
		return UserKeySuffix(cockroachkvs.ParseFormattedKeySuffix(formattedKeySuffix))
	},
	NewGenerator: func(km *keyManager, rng *rand.Rand, cfg OpConfig) KeyGenerator {
		return &cockroachKeyGenerator{
			keyManager: km,
			rng:        rng,
			cfg:        cfg,
			// TODO(jackson): Vary maxLogical.
			suffixSpace: cockroachSuffixKeyspace{maxLogical: 2},
		}
	},
	NewSuffixFilterMask: func() pebble.BlockPropertyFilterMask {
		return &cockroachkvs.MVCCWallTimeIntervalRangeKeyMask{}
	},
	NewSuffixBlockPropertyFilter: func(minSuffix, maxSuffix []byte) sstable.BlockPropertyFilter {
		minWallTime, _, err := cockroachkvs.DecodeMVCCTimestampSuffix(maxSuffix)
		if err != nil {
			panic(err)
		}
		maxWallTime, _, err := cockroachkvs.DecodeMVCCTimestampSuffix(minSuffix)
		if err != nil {
			panic(err)
		}
		return cockroachkvs.NewMVCCTimeIntervalFilter(minWallTime, maxWallTime)
	},
}

type cockroachKeyGenerator struct {
	keyManager  *keyManager
	rng         *rand.Rand
	cfg         OpConfig
	suffixSpace cockroachSuffixKeyspace
}

// RecordPrecedingKey may be invoked before generating keys to inform the key
// generator of a key that was previously generated and used within a related
// test context.
func (kg *cockroachKeyGenerator) RecordPrecedingKey(key []byte) {
	// If the key has a suffix that's larger than the current max suffix,
	// ratchet up the maximum of the distribution of suffixes.
	if i := cockroachkvs.Comparer.Split(key); i < len(key) {
		suffixIdx := kg.suffixSpace.ToSuffixIndex(key[i:])
		if suffixIdx > suffixIndex(kg.cfg.writeSuffixDist.Max()) {
			diff := uint64(suffixIdx) - kg.cfg.writeSuffixDist.Max()
			kg.cfg.writeSuffixDist.IncMax(diff)
		}
	}
}

// ExtendPrefix extends the given prefix key with additional bytes,
// returning a new prefix that sorts after the given prefix.
func (kg *cockroachKeyGenerator) ExtendPrefix(prefix []byte) []byte {
	// Copy prefix and strip the delimiter byte.
	p := append(make([]byte, 0, len(prefix)+3), prefix[:len(prefix)-1]...)
	p = append(p, randBytes(kg.rng, 1, 3)...)
	p = append(p, 0x00) // Delimiter byte
	return p
}

// RandKey returns a random key (either a previously known key, or a new key).
func (kg *cockroachKeyGenerator) RandKey(newKeyProbability float64) []byte {
	return kg.randKey(newKeyProbability, nil /* bounds */)
}

// RandKeyInRange returns a random key (either a previously known key, or a new
// key) in the given key range.
func (kg *cockroachKeyGenerator) RandKeyInRange(
	newKeyProbability float64, kr pebble.KeyRange,
) []byte {
	return kg.randKey(newKeyProbability, &kr)
}

// RandPrefix returns a random prefix key (a key with no suffix).
func (kg *cockroachKeyGenerator) RandPrefix(newPrefix float64) []byte {
	prefixes := kg.keyManager.prefixes()
	if len(prefixes) > 0 && kg.rng.Float64() > newPrefix {
		return pickOneUniform(kg.rng, prefixes)
	}

	// Use a new prefix.
	for {
		prefix := kg.generateKeyWithSuffix(4, 12, 0)
		if !kg.keyManager.prefixExists(prefix) {
			if !kg.keyManager.addNewKey(prefix) {
				panic("key must not exist if prefix doesn't exist")
			}
			return prefix
		}
	}
}

// SkewedSuffix generates a random suffix according to the configuration's
// suffix distribution. It takes a probability 0 ≤ p ≤ 1.0 indicating the
// probability with which the generator should increase the max suffix generated
// by the generator.
//
// May return a nil suffix, with the probability the configuration's suffix
// distribution assigns to the zero suffix.
func (kg *cockroachKeyGenerator) SkewedSuffix(incMaxProb float64) []byte {
	if suffixIdx := kg.skewedSuffixInt(incMaxProb); suffixIdx != 0 {
		return kg.suffixSpace.ToMaterializedSuffix(suffixIdx)
	}
	return nil
}

// skewedSuffixInt is a helper of SkewedSuffix which returns the unencoded
// suffix as an integer.
func (kg *cockroachKeyGenerator) skewedSuffixInt(incMaxProb float64) suffixIndex {
	if kg.rng.Float64() < incMaxProb {
		kg.cfg.writeSuffixDist.IncMax(1)
	}
	return suffixIndex(kg.cfg.writeSuffixDist.Uint64(kg.rng))
}

// IncMaxSuffix increases the max suffix range and returns the new maximum
// suffix (which is guaranteed to be larger than any previously generated
// suffix).
func (kg *cockroachKeyGenerator) IncMaxSuffix() []byte {
	kg.cfg.writeSuffixDist.IncMax(1)
	s := suffixIndex(kg.cfg.writeSuffixDist.Max())
	return kg.suffixSpace.ToMaterializedSuffix(s)
}

// MaximumSuffixProperty returns the maximum suffix property.
func (kg *cockroachKeyGenerator) MaximumSuffixProperty() pebble.MaximumSuffixProperty {
	return cockroachkvs.MaxMVCCTimestampProperty{}
}

// SuffixRange generates a new uniformly random range of suffixes (low, high]
// such that high is guaranteed to be strictly greater (as defined by
// ComparePointSuffixes) than low.
//
// The high suffix may be nil, in which case the suffix range represents all
// suffixes ≥ low.
func (kg *cockroachKeyGenerator) SuffixRange() (low, high []byte) {
	a := kg.uniformSuffixInt()
	b := kg.uniformSuffixInt()
	if a < b {
		a, b = b, a
	} else if a == b {
		a++
	}
	return kg.suffixSpace.ToMaterializedSuffix(a), kg.suffixSpace.ToMaterializedSuffix(b)
}

// UniformSuffix returns a suffix in the same range as SkewedSuffix but with a
// uniform distribution. This is used during reads to better exercise reading a
// mix of older and newer keys. The suffix can be empty.
//
// May return a nil suffix.
func (kg *cockroachKeyGenerator) UniformSuffix() []byte {
	if suffix := kg.uniformSuffixInt(); suffix != 0 {
		return kg.suffixSpace.ToMaterializedSuffix(suffix)
	}
	return nil
}

// uniformSuffixInt is a helper of UniformSuffix which returns the suffix
// index.
func (kg *cockroachKeyGenerator) uniformSuffixInt() suffixIndex {
	maxVal := kg.cfg.writeSuffixDist.Max()
	return suffixIndex(kg.rng.Int64N(int64(maxVal)))
}

// randKey returns a random key (either a previously known key or a new key).
//
// If bounds is not nil, the key will be inside the bounds.
func (kg *cockroachKeyGenerator) randKey(
	newKeyProbability float64, bounds *pebble.KeyRange,
) []byte {
	var knownKeys [][]byte
	if bounds == nil {
		knownKeys = kg.keyManager.knownKeys()
	} else {
		if cockroachkvs.Compare(bounds.Start, bounds.End) >= 0 {
			panic(fmt.Sprintf("invalid bounds [%q, %q)", bounds.Start, bounds.End))
		}
		knownKeys = kg.keyManager.knownKeysInRange(*bounds)
	}
	switch {
	case len(knownKeys) > 0 && kg.rng.Float64() > newKeyProbability:
		// Use an existing user key.
		return pickOneUniform(kg.rng, knownKeys)

	case len(knownKeys) > 0 && kg.rng.Float64() > kg.cfg.newPrefix:
		// Use an existing prefix but a new suffix, producing a new user key.
		prefixes := kg.keyManager.prefixes()

		// If we're constrained to a key range, find which existing prefixes
		// fall within that key range.
		if bounds != nil {
			s, _ := slices.BinarySearchFunc(prefixes, bounds.Start, cockroachkvs.Compare)
			e, _ := slices.BinarySearchFunc(prefixes, bounds.End, cockroachkvs.Compare)
			prefixes = prefixes[s:e]
		}

		if len(prefixes) > 0 {
			for {
				// Pick a prefix on each iteration in case most or all suffixes are
				// already in use for any individual prefix.
				p := kg.rng.IntN(len(prefixes))
				suffix := suffixIndex(kg.cfg.writeSuffixDist.Uint64(kg.rng))

				var key []byte
				if suffix > 0 {
					key = append(append(key, prefixes[p]...), kg.suffixSpace.ToMaterializedSuffix(suffix)...)
				} else {
					key = append(key, prefixes[p]...)
				}
				if bounds == nil || (cockroachkvs.Compare(key, bounds.Start) >= 0 &&
					cockroachkvs.Compare(key, bounds.End) < 0) {
					if kg.keyManager.addNewKey(key) {
						return key
					}
				}

				// If the generated key already existed, or the generated key
				// fell outside the provided bounds, increase the suffix
				// distribution and loop.
				kg.cfg.writeSuffixDist.IncMax(1)
			}
		}
		// Otherwise fall through to generating a new prefix.
	}

	if bounds == nil {
		suffixIdx := kg.skewedSuffixInt(0.01)
		for {
			key := kg.generateKeyWithSuffix(4, 12, suffixIdx)
			if !kg.keyManager.prefixExists(kg.keyManager.kf.Comparer.Split.Prefix(key)) {
				if !kg.keyManager.addNewKey(key) {
					panic("key must not exist if prefix doesn't exist")
				}
				return key
			}
		}
	}
	// We need to generate a key between the bounds.
	startPrefix, startSuffixIdx := kg.suffixSpace.Split(bounds.Start)
	endPrefix, endSuffixIdx := kg.suffixSpace.Split(bounds.End)

	var prefix []byte
	var suffixIdx suffixIndex
	if cockroachkvs.Equal(startPrefix, endPrefix) {
		prefix = startPrefix
		// Bounds have the same prefix, generate a suffix in-between.
		if startSuffixIdx <= endSuffixIdx {
			panic(fmt.Sprintf("invalid bounds [%q, %q)", bounds.Start, bounds.End))
		}
		suffixIdx = kg.skewedSuffixInt(0.01)
		for i := 0; !(startSuffixIdx >= suffixIdx && endSuffixIdx < suffixIdx); i++ {
			if i > 10 {
				// This value is always >= startSuffix and < endSuffix.
				suffixIdx = (startSuffixIdx + endSuffixIdx) / 2
				break
			}
			// The suffix we want must exist in the current suffix range, we don't
			// want to keep increasing it here.
			suffixIdx = kg.skewedSuffixInt(0)
		}
	} else {
		prefix = append(testkeys.RandomPrefixInRange(
			startPrefix[:len(startPrefix)-1], // Strip the delimiter byte.
			endPrefix[:len(endPrefix)-1],     // Strip the delimiter byte.
			kg.rng,
		), 0x00) // Add back the delimiter byte.
		suffixIdx = kg.skewedSuffixInt(0.01)
		if cockroachkvs.Equal(prefix, startPrefix) {
			// We can't use a suffix which sorts before startSuffix.
			for i := 0; suffixIdx > startSuffixIdx; i++ {
				if i > 10 {
					suffixIdx = startSuffixIdx
					break
				}
				suffixIdx = kg.skewedSuffixInt(0)
			}
		}
	}
	key := slices.Clip(prefix)
	if suffixIdx != 0 {
		key = append(key, kg.suffixSpace.ToMaterializedSuffix(suffixIdx)...)
	}
	if cockroachkvs.Compare(key, bounds.Start) < 0 || cockroachkvs.Compare(key, bounds.End) >= 0 {
		panic(fmt.Sprintf("invalid randKey %q; bounds: [%q, %q) %v %v",
			key, bounds.Start, bounds.End,
			cockroachkvs.Compare(key, bounds.Start),
			cockroachkvs.Compare(key, bounds.End)))
	}
	// We might (rarely) produce an existing key here, that's ok.
	kg.keyManager.addNewKey(key)
	return key
}

// generateKeyWithSuffix generates a key with a random prefix and the suffix
// corresponding to the provided suffix index. If the given suffix index is 0,
// the key will not have a suffix.
func (kg *cockroachKeyGenerator) generateKeyWithSuffix(
	minPrefixLen, maxPrefixLen int, suffixIdx suffixIndex,
) []byte {
	prefix := randCockroachPrefix(kg.rng, minPrefixLen, maxPrefixLen)
	if suffixIdx == 0 {
		return prefix
	}
	return append(prefix, kg.suffixSpace.ToMaterializedSuffix(suffixIdx)...)
}

func randCockroachPrefix(rng *rand.Rand, minLen, maxLen int) []byte {
	n := minLen + rng.IntN(maxLen-minLen+1)
	if n == 0 {
		return nil
	}
	// NB: The actual random values are not particularly important. We only use
	// lowercase letters because that makes visual determination of ordering
	// easier, rather than having to remember the lexicographic ordering of
	// uppercase vs lowercase, or letters vs numbers vs punctuation.
	const letters = "abcdefghijklmnopqrstuvwxyz"
	const lettersLen = uint64(len(letters))
	const lettersCharsPerRand = 12 // floor(log(math.MaxUint64)/log(lettersLen))

	var r uint64
	var q int
	buf := make([]byte, n+1)
	for i := 0; i < n; i++ {
		if q == 0 {
			r = rng.Uint64()
			q = lettersCharsPerRand
		}
		buf[i] = letters[r%lettersLen]
		r = r / lettersLen
		q--
	}
	buf[n] = 0x00 // Delimiter byte
	return buf
}

// A suffixIndex represents a unique suffix. The suffixIndex exists within a
// one-dimensional space of int64s, but is remapped into a two-dimensional space
// of MVCC timestamps of (WallTime, Logical) tuples.
type suffixIndex int64

// cockroackSuffixKeyspace defines the mapping between a one-dimensional
// suffixIndex and the suffix it represents within the two-dimensional
// (wallTime, logical) space.
//
// The mapping is configued by the value of maxLogical, with maxLogical+1
// possible logical timestamps at each wall time.
//
// TODO(jackson): Update to disallow non-zero logical timestamps when the wall
// time is zero if we begin to prohibit it.
//
//	+------------------------------------------------------+
//	| suffix  |              maxLogical                    |
//	| index   | 0      | 1      | 2      | 3      | 4      |
//	+------------------------------------------------------+
//	|  0      | (0,0)  | (0,0)  | (0,0)  | (0,0)  | (0,0)  |
//	|  1      | (1,0)  | (0,1)  | (0,1)  | (0,1)  | (0,1)  |
//	|  2      | (2,0)  | (1,0)  | (0,2)  | (0,2)  | (0,2)  |
//	|  3      | (3,0)  | (1,1)  | (1,0)  | (0,3)  | (0,3)  |
//	|  4      | (4,0)  | (2,0)  | (1,1)  | (1,0)  | (0,4)  |
//	|  5      | (5,0)  | (2,1)  | (1,2)  | (1,1)  | (1,0)  |
//	|  6      | (6,0)  | (3,0)  | (2,0)  | (1,2)  | (1,1)  |
//	|  7      | (7,0)  | (3,1)  | (2,1)  | (1,3)  | (1,2)  |
//	+------------------------------------------------------+
type cockroachSuffixKeyspace struct {
	maxLogical int64
}

func (ks cockroachSuffixKeyspace) ToMaterializedSuffix(s suffixIndex) []byte {
	// There are maxLogical+1 possible logical timestamps at each wall time.
	wallTime := int64(s) / (ks.maxLogical + 1)
	logical := int64(s) % (ks.maxLogical + 1)
	return cockroachkvs.NewTimestampSuffix(uint64(wallTime), uint32(logical))
}

func (ks cockroachSuffixKeyspace) ToSuffixIndex(suffix []byte) suffixIndex {
	wallTime, logical, err := cockroachkvs.DecodeMVCCTimestampSuffix(suffix)
	if err != nil {
		panic(err)
	}
	return suffixIndex(int64(wallTime)*(ks.maxLogical+1) + int64(logical))
}

func (ks cockroachSuffixKeyspace) Split(key []byte) (prefix []byte, suffixIdx suffixIndex) {
	i := cockroachkvs.Split(key)
	return key[:i], ks.ToSuffixIndex(key[i:])
}
