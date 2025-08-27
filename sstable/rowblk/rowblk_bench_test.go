// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rowblk

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/internal/testkeys"
	"github.com/cockroachdb/pebble/v2/sstable/block"
)

var (
	benchSynthSuffix = []byte("@15")
	benchPrefix      = []byte("2_")

	// Use testkeys.Comparer.Compare which approximates EngineCompare by ordering
	// multiple keys with same prefix in descending suffix order.
	benchComparer = testkeys.Comparer
)

// choosOrigSuffix randomly chooses a suffix that is either 1 or 2 bytes large.
// This ensures we benchmark when suffix replacement adds a larger suffix.
func chooseOrigSuffix(rng *rand.Rand) []byte {
	origSuffix := []byte("@10")
	if rng.IntN(10)%2 == 0 {
		origSuffix = []byte("@9")
	}
	return origSuffix
}

// createBenchBlock writes a block of keys and outputs a list of keys that will
// be surfaced from the block, and the expected synthetic suffix and prefix the
// block should be read with.
func createBenchBlock(
	blockSize int, w *Writer, rng *rand.Rand, withSyntheticPrefix, withSyntheticSuffix bool,
) ([][]byte, []byte, []byte) {

	origSuffix := chooseOrigSuffix(rng)
	var ikey base.InternalKey
	var readKeys [][]byte

	var writtenPrefix []byte
	if !withSyntheticPrefix {
		// If the keys will not be read with a synthetic prefix, write the prefix to
		// the block for a more comparable benchmark comparison between a block iter
		// with and without prefix synthesis.
		writtenPrefix = benchPrefix
	}
	for i := 0; w.EstimatedSize() < blockSize; i++ {
		key := []byte(fmt.Sprintf("%s%05d%s", string(writtenPrefix), i, origSuffix))
		ikey.UserKey = key
		if err := w.Add(ikey, nil); err != nil {
			panic(err)
		}
		var readKey []byte
		if withSyntheticPrefix {
			readKey = append(readKey, benchPrefix...)
		}
		readKey = append(readKey, key...)
		readKeys = append(readKeys, readKey)
	}

	var syntheticSuffix []byte
	var syntheticPrefix []byte
	if withSyntheticSuffix {
		syntheticSuffix = benchSynthSuffix
	}
	if withSyntheticPrefix {
		syntheticPrefix = []byte(benchPrefix)
	}
	return readKeys, syntheticPrefix, syntheticSuffix
}

func BenchmarkBlockIterSeekGE(b *testing.B) {
	const blockSize = 32 << 10
	for _, withSyntheticPrefix := range []bool{false, true} {
		for _, withSyntheticSuffix := range []bool{false, true} {
			for _, restartInterval := range []int{16} {
				b.Run(fmt.Sprintf("syntheticPrefix=%t;syntheticSuffix=%t;restart=%d", withSyntheticPrefix, withSyntheticSuffix, restartInterval),
					func(b *testing.B) {
						w := &Writer{RestartInterval: restartInterval}
						rng := rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))

						keys, syntheticPrefix, syntheticSuffix := createBenchBlock(blockSize, w, rng, withSyntheticPrefix, withSyntheticSuffix)

						it, err := NewIter(
							benchComparer.Compare,
							benchComparer.ComparePointSuffixes,
							benchComparer.Split,
							w.Finish(),
							block.IterTransforms{
								SyntheticPrefixAndSuffix: block.MakeSyntheticPrefixAndSuffix(syntheticPrefix, syntheticSuffix),
							})
						if err != nil {
							b.Fatal(err)
						}
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							k := keys[rng.IntN(len(keys))]
							kv := it.SeekGE(k, base.SeekGEFlagsNone)
							if testing.Verbose() {
								if !it.Valid() && !withSyntheticSuffix {
									b.Fatal("expected to find key")
								}
								if !bytes.Equal(k, kv.K.UserKey) && !withSyntheticSuffix {
									b.Fatalf("expected %s, but found %s", k, kv.K.UserKey)
								}
							}
						}
					})
			}
		}
	}
}

func BenchmarkBlockIterSeekLT(b *testing.B) {
	const blockSize = 32 << 10
	for _, withSyntheticPrefix := range []bool{false, true} {
		for _, withSyntheticSuffix := range []bool{false, true} {
			for _, restartInterval := range []int{16} {
				b.Run(fmt.Sprintf("syntheticPrefix=%t;syntheticSuffix=%t;restart=%d", withSyntheticPrefix, withSyntheticSuffix, restartInterval),
					func(b *testing.B) {
						w := &Writer{RestartInterval: restartInterval}
						rng := rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))

						keys, syntheticPrefix, syntheticSuffix := createBenchBlock(blockSize, w, rng, withSyntheticPrefix, withSyntheticSuffix)

						it, err := NewIter(
							benchComparer.Compare,
							benchComparer.ComparePointSuffixes,
							benchComparer.Split,
							w.Finish(),
							block.IterTransforms{
								SyntheticPrefixAndSuffix: block.MakeSyntheticPrefixAndSuffix(syntheticPrefix, syntheticSuffix),
							})
						if err != nil {
							b.Fatal(err)
						}
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							j := rng.IntN(len(keys))
							kv := it.SeekLT(keys[j], base.SeekLTFlagsNone)
							if testing.Verbose() {
								if j == 0 {
									if it.Valid() && !withSyntheticSuffix {
										b.Fatal("unexpected key")
									}
								} else {
									if !it.Valid() && !withSyntheticSuffix {
										b.Fatal("expected to find key")
									}
									k := keys[j-1]
									if !bytes.Equal(k, kv.K.UserKey) && !withSyntheticSuffix {
										b.Fatalf("expected %s, but found %s", k, kv.K.UserKey)
									}
								}
							}
						}
					})
			}
		}
	}
}

func BenchmarkBlockIterNext(b *testing.B) {
	const blockSize = 32 << 10
	for _, withSyntheticPrefix := range []bool{false, true} {
		for _, withSyntheticSuffix := range []bool{false, true} {
			for _, restartInterval := range []int{16} {
				b.Run(fmt.Sprintf("syntheticPrefix=%t;syntheticSuffix=%t;restart=%d", withSyntheticPrefix, withSyntheticSuffix, restartInterval),
					func(b *testing.B) {
						w := &Writer{RestartInterval: restartInterval}
						rng := rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))

						_, syntheticPrefix, syntheticSuffix := createBenchBlock(blockSize, w, rng, withSyntheticPrefix, withSyntheticSuffix)

						it, err := NewIter(
							benchComparer.Compare,
							benchComparer.ComparePointSuffixes,
							benchComparer.Split,
							w.Finish(),
							block.IterTransforms{
								SyntheticPrefixAndSuffix: block.MakeSyntheticPrefixAndSuffix(syntheticPrefix, syntheticSuffix),
							})
						if err != nil {
							b.Fatal(err)
						}

						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							if !it.Valid() {
								it.First()
							}
							it.Next()
						}
					})
			}
		}
	}
}

func BenchmarkBlockIterPrev(b *testing.B) {
	const blockSize = 32 << 10
	for _, withSyntheticPrefix := range []bool{false, true} {
		for _, withSyntheticSuffix := range []bool{false, true} {
			for _, restartInterval := range []int{16} {
				b.Run(fmt.Sprintf("syntheticPrefix=%t;syntheticSuffix=%t;restart=%d", withSyntheticPrefix, withSyntheticSuffix, restartInterval),
					func(b *testing.B) {
						w := &Writer{RestartInterval: restartInterval}
						rng := rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))

						_, syntheticPrefix, syntheticSuffix := createBenchBlock(blockSize, w, rng, withSyntheticPrefix, withSyntheticSuffix)

						it, err := NewIter(
							benchComparer.Compare,
							benchComparer.ComparePointSuffixes,
							benchComparer.Split,
							w.Finish(),
							block.IterTransforms{
								SyntheticPrefixAndSuffix: block.MakeSyntheticPrefixAndSuffix(syntheticPrefix, syntheticSuffix),
							})
						if err != nil {
							b.Fatal(err)
						}

						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							if !it.Valid() {
								it.Last()
							}
							it.Prev()
						}
					})
			}
		}
	}
}
