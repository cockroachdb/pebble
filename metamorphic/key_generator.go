// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"cmp"
	"fmt"
	"math/rand/v2"
	"slices"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/testkeys"
)

type keyGenerator struct {
	keyManager *keyManager
	rng        *rand.Rand
	cfg        OpConfig
}

func newKeyGenerator(km *keyManager, rng *rand.Rand, cfg OpConfig) *keyGenerator {
	return &keyGenerator{
		keyManager: km,
		rng:        rng,
		cfg:        cfg,
	}
}

// RandKey returns a random key (either a previously known key, or a new key).
func (kg *keyGenerator) RandKey(newKeyProbability float64) []byte {
	return kg.randKey(newKeyProbability, nil /* bounds */)
}

// RandKeyInRange returns a random key (either a previously known key, or a new
// key) in the given key range.
func (kg *keyGenerator) RandKeyInRange(newKeyProbability float64, kr pebble.KeyRange) []byte {
	return kg.randKey(newKeyProbability, &kr)
}

// RandPrefix returns a random prefix key (a key with no suffix).
func (kg *keyGenerator) RandPrefix(newPrefix float64) []byte {
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

// UniqueKeys takes a key-generating function and uses it to generate n unique
// keys, returning them in sorted order.
func (kg *keyGenerator) UniqueKeys(n int, genFn func() []byte) [][]byte {
	keys := make([][]byte, n)
	used := make(map[string]struct{}, n)
	for i := range keys {
		for attempts := 0; ; attempts++ {
			keys[i] = genFn()
			if _, exists := used[string(keys[i])]; !exists {
				break
			}
			if attempts > 100000 {
				panic("could not generate unique key")
			}
		}
		used[string(keys[i])] = struct{}{}
	}
	slices.SortFunc(keys, kg.cmp)
	return keys
}

// SkewedSuffix generates a random suffix according to the configuration's
// suffix distribution. It takes a probability 0 ≤ p ≤ 1.0 indicating the
// probability with which the generator should increase the max suffix generated
// by the generator.
//
// May return a nil suffix, with the probability the configuration's suffix
// distribution assigns to the zero suffix.
func (kg *keyGenerator) SkewedSuffix(incMaxProb float64) []byte {
	if suffix := kg.SkewedSuffixInt(incMaxProb); suffix != 0 {
		return testkeys.Suffix(suffix)
	}
	return nil
}

// SkewedSuffixInt is a variant of SkewedSuffix which returns the unencoded
// suffix as an integer.
func (kg *keyGenerator) SkewedSuffixInt(incMaxProb float64) int64 {
	if kg.rng.Float64() < incMaxProb {
		kg.cfg.writeSuffixDist.IncMax(1)
	}
	return int64(kg.cfg.writeSuffixDist.Uint64(kg.rng))
}

// IncMaxSuffix increases the max suffix range and returns the new maximum
// suffix (which is guaranteed to be larger than any previously generated
// suffix).
func (kg *keyGenerator) IncMaxSuffix() []byte {
	kg.cfg.writeSuffixDist.IncMax(1)
	return testkeys.Suffix(int64(kg.cfg.writeSuffixDist.Max()))
}

// UniformSuffix returns a suffix in the same range as SkewedSuffix but with a
// uniform distribution. This is used during reads to better exercise reading a
// mix of older and newer keys. The suffix can be empty.
//
// May return a nil suffix.
func (kg *keyGenerator) UniformSuffix() []byte {
	if suffix := kg.UniformSuffixInt(); suffix != 0 {
		return testkeys.Suffix(suffix)
	}
	return nil
}

// UniformSuffixInt is a variant of UniformSuffix which returns the unencoded
// suffix as an integer.
func (kg *keyGenerator) UniformSuffixInt() int64 {
	maxVal := kg.cfg.writeSuffixDist.Max()
	return kg.rng.Int64N(int64(maxVal))
}

// randKey returns a random key (either a previously known key or a new key).
//
// If bounds is not nil, the key will be inside the bounds.
func (kg *keyGenerator) randKey(newKeyProbability float64, bounds *pebble.KeyRange) []byte {
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
		suffix := kg.SkewedSuffixInt(0.01)
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
		suffix = kg.SkewedSuffixInt(0.01)
		for i := 0; !(cmpSuffix(startSuffix, suffix) <= 0 && cmpSuffix(suffix, endSuffix) < 0); i++ {
			if i > 10 {
				// This value is always >= startSuffix and < endSuffix.
				suffix = (startSuffix + endSuffix) / 2
				break
			}
			// The suffix we want must exist in the current suffix range, we don't
			// want to keep increasing it here.
			suffix = kg.SkewedSuffixInt(0)
		}
	} else {
		prefix = testkeys.RandomPrefixInRange(startPrefix, endPrefix, kg.rng)
		suffix = kg.SkewedSuffixInt(0.01)
		if kg.equal(prefix, startPrefix) {
			// We can't use a suffix which sorts before startSuffix.
			for i := 0; cmpSuffix(suffix, startSuffix) < 0; i++ {
				if i > 10 {
					suffix = startSuffix
					break
				}
				suffix = kg.SkewedSuffixInt(0)
			}
		}
	}
	key := slices.Clip(prefix)
	if suffix != 0 {
		key = append(key, testkeys.Suffix(suffix)...)
	}
	if kg.cmp(key, bounds.Start) < 0 || kg.cmp(key, bounds.End) >= 0 {
		panic(fmt.Sprintf("invalid randKey %q; bounds: [%q, %q) %v %v", key, bounds.Start, bounds.End, kg.cmp(key, bounds.Start), kg.cmp(key, bounds.End)))
	}
	// We might (rarely) produce an existing key here, that's ok.
	kg.keyManager.addNewKey(key)
	return key
}

// generateKeyWithSuffix generates a key with a random prefix and the given
// suffix. If the given suffix is 0, the key will not have a suffix.
func (kg *keyGenerator) generateKeyWithSuffix(minPrefixLen, maxPrefixLen int, suffix int64) []byte {
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

func (kg *keyGenerator) cmp(a, b []byte) int {
	return kg.keyManager.comparer.Compare(a, b)
}

func (kg *keyGenerator) equal(a, b []byte) bool {
	return kg.keyManager.comparer.Equal(a, b)
}

func (kg *keyGenerator) split(a []byte) int {
	return kg.keyManager.comparer.Split(a)
}

func (kg *keyGenerator) prefix(a []byte) []byte {
	n := kg.split(a)
	return a[:n:n]
}

func (kg *keyGenerator) parseKey(k []byte) (prefix []byte, suffix int64) {
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
