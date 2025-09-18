// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"cmp"
	"fmt"
	"math/rand/v2"
	"slices"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/colblk"
)

var TestkeysKeyFormat = KeyFormat{
	Name:     "testkeys",
	Comparer: testkeys.Comparer,
	KeySchema: func() *colblk.KeySchema {
		kf := colblk.DefaultKeySchema(testkeys.Comparer, 16 /* bundle size */)
		return &kf
	}(),
	BlockPropertyCollectors: []func() pebble.BlockPropertyCollector{
		sstable.NewTestKeysBlockPropertyCollector,
	},
	FormatKey:       func(k UserKey) string { return string(k) },
	FormatKeySuffix: func(s UserKeySuffix) string { return string(s) },
	NewGenerator: func(km *keyManager, rng *rand.Rand, cfg OpConfig) KeyGenerator {
		return &testkeyKeyGenerator{
			keyManager: km,
			rng:        rng,
			cfg:        cfg,
		}
	},
	NewSuffixFilterMask: func() pebble.BlockPropertyFilterMask {
		return sstable.NewTestKeysMaskingFilter()
	},
	NewSuffixBlockPropertyFilter: func(filterMin, filterMax []byte) sstable.BlockPropertyFilter {
		var low, high int64
		var err error
		if filterMin != nil {
			low, err = testkeys.ParseSuffix(filterMin)
			if err != nil {
				panic(err)
			}
		}
		if filterMax != nil {
			high, err = testkeys.ParseSuffix(filterMax)
			if err != nil {
				panic(err)
			}
		}
		// The suffixes were encoded in descending order, so low should be the
		// max timestamp and high should be the min timestamp.
		if low <= high {
			panic(errors.AssertionFailedf("low <= high: %d <= %d", low, high))
		}
		return sstable.NewTestKeysBlockPropertyFilter(uint64(high), uint64(low))
	},
	ParseMaximumSuffixProperty: func(s string) pebble.MaximumSuffixProperty {
		return sstable.MaxTestKeysSuffixProperty{}
	},
	FormatMaximumSuffixProperty: func(prop pebble.MaximumSuffixProperty) string {
		if prop == nil {
			return ""
		}
		return "maxsuffixprop"
	},
	MaximumSuffixProperty: sstable.MaxTestKeysSuffixProperty{},
}

type testkeyKeyGenerator struct {
	keyManager *keyManager
	rng        *rand.Rand
	cfg        OpConfig
}

// RecordPrecedingKey may be invoked before generating keys to inform the key
// generator of a key that was previously generated and used within a related
// test context.
func (kg *testkeyKeyGenerator) RecordPrecedingKey(key []byte) {
	// If the key has a suffix that's larger than the current max suffix,
	// ratchet up the maximum of the distribution of suffixes.
	if i := testkeys.Comparer.Split(key); i < len(key) {
		s, err := testkeys.ParseSuffix(key[i:])
		if err != nil {
			panic(err)
		}
		if uint64(s) > kg.cfg.writeSuffixDist.Max() {
			diff := uint64(s) - kg.cfg.writeSuffixDist.Max()
			kg.cfg.writeSuffixDist.IncMax(diff)
		}
	}
}

// ExtendPrefix extends the given prefix key with additional bytes,
// returning a new prefix that sorts after the given prefix.
func (kg *testkeyKeyGenerator) ExtendPrefix(prefix []byte) []byte {
	return append(slices.Clip(prefix), randBytes(kg.rng, 1, 3)...)
}

// RandKey returns a random key (either a previously known key, or a new key).
func (kg *testkeyKeyGenerator) RandKey(newKeyProbability float64) []byte {
	return kg.randKey(newKeyProbability, nil /* bounds */)
}

// RandKeyInRange returns a random key (either a previously known key, or a new
// key) in the given key range.
func (kg *testkeyKeyGenerator) RandKeyInRange(
	newKeyProbability float64, kr pebble.KeyRange,
) []byte {
	return kg.randKey(newKeyProbability, &kr)
}

// RandPrefix returns a random prefix key (a key with no suffix).
func (kg *testkeyKeyGenerator) RandPrefix(newPrefix float64) []byte {
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
func (kg *testkeyKeyGenerator) SkewedSuffix(incMaxProb float64) []byte {
	if suffix := kg.skewedSuffixInt(incMaxProb); suffix != 0 {
		return testkeys.Suffix(suffix)
	}
	return nil
}

// skewedSuffixInt is a helper of SkewedSuffix which returns the unencoded
// suffix as an integer.
func (kg *testkeyKeyGenerator) skewedSuffixInt(incMaxProb float64) int64 {
	if kg.rng.Float64() < incMaxProb {
		kg.cfg.writeSuffixDist.IncMax(1)
	}
	return int64(kg.cfg.writeSuffixDist.Uint64(kg.rng))
}

// IncMaxSuffix increases the max suffix range and returns the new maximum
// suffix (which is guaranteed to be larger than any previously generated
// suffix).
func (kg *testkeyKeyGenerator) IncMaxSuffix() []byte {
	kg.cfg.writeSuffixDist.IncMax(1)
	return testkeys.Suffix(int64(kg.cfg.writeSuffixDist.Max()))
}

// SuffixRange generates a new uniformly random range of suffixes (low, high]
// such that high is guaranteed to be strictly greater (as defined by
// ComparePointSuffixes) than low.
//
// The high suffix may be nil, in which case the suffix range represents all
// suffixes ≥ low.
func (kg *testkeyKeyGenerator) SuffixRange() (low, high []byte) {
	a := kg.uniformSuffixInt()
	b := kg.uniformSuffixInt()
	// NB: Suffixes are sorted in descending order, so we need to generate the
	// timestamps such that a > b. This ensures that the returned suffixes sort
	// such that low < high.
	if a < b {
		a, b = b, a
	} else if a == b {
		a++
	}
	low = testkeys.Suffix(a) // NB: a > 0
	if b > 0 {
		high = testkeys.Suffix(b)
	}
	return low, high
}

// UniformSuffix returns a suffix in the same range as SkewedSuffix but with a
// uniform distribution. This is used during reads to better exercise reading a
// mix of older and newer keys. The suffix can be empty.
//
// May return a nil suffix.
func (kg *testkeyKeyGenerator) UniformSuffix() []byte {
	if suffix := kg.uniformSuffixInt(); suffix != 0 {
		return testkeys.Suffix(suffix)
	}
	return nil
}

// uniformSuffixInt is a helper of UniformSuffix which returns the unencoded
// suffix as an integer.
func (kg *testkeyKeyGenerator) uniformSuffixInt() int64 {
	maxVal := kg.cfg.writeSuffixDist.Max()
	return kg.rng.Int64N(int64(maxVal))
}

// randKey returns a random key (either a previously known key or a new key).
//
// If bounds is not nil, the key will be inside the bounds.
func (kg *testkeyKeyGenerator) randKey(newKeyProbability float64, bounds *pebble.KeyRange) []byte {
	var knownKeys [][]byte
	if bounds == nil {
		knownKeys = kg.keyManager.knownKeys()
	} else {
		if kg.cmp(bounds.Start, bounds.End) >= 0 {
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
			s, _ := slices.BinarySearchFunc(prefixes, bounds.Start, kg.cmp)
			e, _ := slices.BinarySearchFunc(prefixes, bounds.End, kg.cmp)
			prefixes = prefixes[s:e]
		}

		if len(prefixes) > 0 {
			for {
				// Pick a prefix on each iteration in case most or all suffixes are
				// already in use for any individual prefix.
				p := kg.rng.IntN(len(prefixes))
				suffix := int64(kg.cfg.writeSuffixDist.Uint64(kg.rng))

				var key []byte
				if suffix > 0 {
					key = resizeBuffer(key, len(prefixes[p]), testkeys.SuffixLen(suffix))
					n := copy(key, prefixes[p])
					testkeys.WriteSuffix(key[n:], suffix)
				} else {
					key = resizeBuffer(key, len(prefixes[p]), 0)
					copy(key, prefixes[p])
				}

				if bounds == nil || (kg.cmp(key, bounds.Start) >= 0 && kg.cmp(key, bounds.End) < 0) {
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
		suffix := kg.skewedSuffixInt(0.01)
		for {
			key := kg.generateKeyWithSuffix(4, 12, suffix)
			if !kg.keyManager.prefixExists(kg.prefix(key)) {
				if !kg.keyManager.addNewKey(key) {
					panic("key must not exist if prefix doesn't exist")
				}
				return key
			}
		}
	}
	// We need to generate a key between the bounds.
	startPrefix, startSuffix := kg.parseKey(bounds.Start)
	endPrefix, endSuffix := kg.parseKey(bounds.End)
	var prefix []byte
	var suffix int64
	if kg.equal(startPrefix, endPrefix) {
		prefix = startPrefix
		// Bounds have the same prefix, generate a suffix in-between.
		if cmpSuffix(startSuffix, endSuffix) >= 0 {
			panic(fmt.Sprintf("invalid bounds [%q, %q)", bounds.Start, bounds.End))
		}
		suffix = kg.skewedSuffixInt(0.01)
		for i := 0; !(cmpSuffix(startSuffix, suffix) <= 0 && cmpSuffix(suffix, endSuffix) < 0); i++ {
			if i > 10 {
				// This value is always >= startSuffix and < endSuffix.
				suffix = (startSuffix + endSuffix) / 2
				break
			}
			// The suffix we want must exist in the current suffix range, we don't
			// want to keep increasing it here.
			suffix = kg.skewedSuffixInt(0)
		}
	} else {
		prefix = testkeys.RandomPrefixInRange(startPrefix, endPrefix, kg.rng)
		suffix = kg.skewedSuffixInt(0.01)
		if kg.equal(prefix, startPrefix) {
			// We can't use a suffix which sorts before startSuffix.
			for i := 0; cmpSuffix(suffix, startSuffix) < 0; i++ {
				if i > 10 {
					suffix = startSuffix
					break
				}
				suffix = kg.skewedSuffixInt(0)
			}
		}
	}
	key := slices.Clip(prefix)
	if suffix != 0 {
		key = append(key, testkeys.Suffix(suffix)...)
	}
	if kg.cmp(key, bounds.Start) < 0 || kg.cmp(key, bounds.End) >= 0 {
		panic(fmt.Sprintf("invalid randKey %q; bounds: [%q, %q) %v %v",
			key, bounds.Start, bounds.End, kg.cmp(key, bounds.Start), kg.cmp(key, bounds.End)))
	}
	// We might (rarely) produce an existing key here, that's ok.
	kg.keyManager.addNewKey(key)
	return key
}

// generateKeyWithSuffix generates a key with a random prefix and the given
// suffix. If the given suffix is 0, the key will not have a suffix.
func (kg *testkeyKeyGenerator) generateKeyWithSuffix(
	minPrefixLen, maxPrefixLen int, suffix int64,
) []byte {
	prefix := randBytes(kg.rng, minPrefixLen, maxPrefixLen)
	if suffix == 0 {
		return prefix
	}
	return append(prefix, testkeys.Suffix(suffix)...)
}

// cmpSuffix compares two suffixes, where suffix 0 means there is no suffix.
func cmpSuffix(s1, s2 int64) int {
	switch {
	case s1 == s2:
		return 0
	case s1 == 0:
		return -1
	case s2 == 0:
		return +1
	default:
		return cmp.Compare(s2, s1)
	}
}

func (kg *testkeyKeyGenerator) cmp(a, b []byte) int {
	return kg.keyManager.kf.Comparer.Compare(a, b)
}

func (kg *testkeyKeyGenerator) equal(a, b []byte) bool {
	return kg.keyManager.kf.Comparer.Equal(a, b)
}

func (kg *testkeyKeyGenerator) split(a []byte) int {
	return kg.keyManager.kf.Comparer.Split(a)
}

func (kg *testkeyKeyGenerator) prefix(a []byte) []byte {
	n := kg.split(a)
	return a[:n:n]
}

func (kg *testkeyKeyGenerator) parseKey(k []byte) (prefix []byte, suffix int64) {
	n := kg.split(k)
	if n == len(k) {
		return k, 0
	}
	suffix, err := testkeys.ParseSuffix(k[n:])
	if err != nil {
		panic(fmt.Sprintf("error parsing suffix for key %q", k))
	}
	return k[:n:n], suffix
}

func randBytes(rng *rand.Rand, minLen, maxLen int) []byte {
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
	buf := make([]byte, n)
	for i := range buf {
		if q == 0 {
			r = rng.Uint64()
			q = lettersCharsPerRand
		}
		buf[i] = letters[r%lettersLen]
		r = r / lettersLen
		q--
	}
	return buf
}

func pickOneUniform[S ~[]E, E any](rng *rand.Rand, x S) E {
	return x[rng.IntN(len(x))]
}
