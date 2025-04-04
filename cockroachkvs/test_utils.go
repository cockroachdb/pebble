// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cockroachkvs

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/cockroachdb/crlib/crstrings"
)

// KeyGenConfig configures the shape of the random keys generated.
type KeyGenConfig struct {
	PrefixAlphabetLen  int    // Number of bytes in the alphabet used for the prefix.
	PrefixLenShared    int    // Number of bytes shared by all key prefixes.
	RoachKeyLen        int    // Number of bytes in the prefix (without the 0 sentinel byte).
	AvgKeysPerPrefix   int    // Average number of keys (with varying suffixes) per prefix.
	BaseWallTime       uint64 // Smallest MVCC WallTime.
	PercentLogical     int    // Percent of MVCC keys with non-zero MVCC logical time.
	PercentEmptySuffix int    // Percent of keys with empty suffix.
	PercentLockSuffix  int    // Percent of keys with lock suffix.
}

func (cfg KeyGenConfig) String() string {
	return fmt.Sprintf(
		"AlphaLen=%d,Prefix=%d,Shared=%d,KeysPerPrefix=%d%s",
		cfg.PrefixAlphabetLen, cfg.RoachKeyLen, cfg.PrefixLenShared,
		cfg.AvgKeysPerPrefix,
		crstrings.If(cfg.PercentLogical != 0, fmt.Sprintf(",Logical=%d", cfg.PercentLogical)),
	)
}

// RandomKVs constructs count random KVs with the provided parameters.
func RandomKVs(rng *rand.Rand, count int, cfg KeyGenConfig, valueLen int) (keys, vals [][]byte) {
	g := makeCockroachKeyGen(rng, cfg)
	sharedPrefix := make([]byte, cfg.PrefixLenShared)
	for i := 0; i < len(sharedPrefix); i++ {
		sharedPrefix[i] = byte(rng.IntN(cfg.PrefixAlphabetLen) + 'a')
	}

	keys = make([][]byte, 0, count)
	for len(keys) < count {
		roachKey := g.randRoachKey(sharedPrefix)
		// We use the exponential distribution so that we occasionally have many
		// suffixes
		n := int(rng.ExpFloat64() * float64(cfg.AvgKeysPerPrefix))
		n = max(n, 1)
		for i := 0; i < n && len(keys) < count; i++ {
			if cfg.PercentEmptySuffix+cfg.PercentLockSuffix > 0 {
				if r := rng.IntN(100); r < cfg.PercentEmptySuffix+cfg.PercentLockSuffix {
					k := append(roachKey, 0)
					if r < cfg.PercentLockSuffix {
						// Generate a lock key suffix.
						for j := 0; j < engineKeyVersionLockTableLen; j++ {
							k = append(k, byte(g.rng.IntN(g.cfg.PrefixAlphabetLen)+'a'))
						}
						k = append(k, engineKeyVersionLockTableLen+1)
					}
					keys = append(keys, k)
					continue
				}
			}

			wallTime, logicalTime := g.randTimestamp()
			k := makeMVCCKey(roachKey, wallTime, logicalTime)
			keys = append(keys, k)
		}
	}
	slices.SortFunc(keys, Compare)
	vals = make([][]byte, count)
	for i := range vals {
		v := make([]byte, valueLen)
		for j := range v {
			v[j] = byte(rng.Uint32())
		}
		vals[i] = v
	}
	return keys, vals
}

func makeMVCCKey(roachKey []byte, wallTime uint64, logicalTime uint32) []byte {
	k := slices.Grow(slices.Clip(roachKey), MaxSuffixLen)
	return EncodeTimestamp(k, wallTime, logicalTime)
}

type cockroachKeyGen struct {
	rng *rand.Rand
	cfg KeyGenConfig
}

func makeCockroachKeyGen(rng *rand.Rand, cfg KeyGenConfig) cockroachKeyGen {
	return cockroachKeyGen{
		rng: rng,
		cfg: cfg,
	}
}

func (g *cockroachKeyGen) randRoachKey(blockPrefix []byte) []byte {
	roachKey := make([]byte, 0, g.cfg.RoachKeyLen+MaxSuffixLen)
	roachKey = append(roachKey, blockPrefix...)
	for len(roachKey) < g.cfg.RoachKeyLen {
		roachKey = append(roachKey, byte(g.rng.IntN(g.cfg.PrefixAlphabetLen)+'a'))
	}
	return roachKey
}

func (g *cockroachKeyGen) randTimestamp() (wallTime uint64, logicalTime uint32) {
	wallTime = g.cfg.BaseWallTime + g.rng.Uint64N(uint64(time.Hour))
	if g.cfg.PercentLogical > 0 && g.rng.IntN(100) < g.cfg.PercentLogical {
		logicalTime = g.rng.Uint32()
	}
	return wallTime, logicalTime
}
