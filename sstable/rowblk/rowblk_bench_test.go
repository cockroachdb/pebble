// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rowblk

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/crdbtest"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/sstable/block"
	"golang.org/x/exp/rand"
)

var (
	benchSynthSuffix = []byte("@15")
	benchPrefix      = []byte("2_")

	// Use testkeys.Comparer.Compare which approximates EngineCompare by ordering
	// multiple keys with same prefix in descending suffix order.
	benchCmp   = testkeys.Comparer.Compare
	benchSplit = testkeys.Comparer.Split
)

// choosOrigSuffix randomly chooses a suffix that is either 1 or 2 bytes large.
// This ensures we benchmark when suffix replacement adds a larger suffix.
func chooseOrigSuffix(rng *rand.Rand) []byte {
	origSuffix := []byte("@10")
	if rng.Intn(10)%2 == 0 {
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
		w.Add(ikey, nil)
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
						rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

						keys, syntheticPrefix, syntheticSuffix := createBenchBlock(blockSize, w, rng, withSyntheticPrefix, withSyntheticSuffix)

						it, err := NewIter(benchCmp, benchSplit, w.Finish(), block.IterTransforms{SyntheticSuffix: syntheticSuffix, SyntheticPrefix: syntheticPrefix})
						if err != nil {
							b.Fatal(err)
						}
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							k := keys[rng.Intn(len(keys))]
							it.SeekGE(k, base.SeekGEFlagsNone)
							if testing.Verbose() {
								if !it.Valid() && !withSyntheticSuffix {
									b.Fatal("expected to find key")
								}
								if !bytes.Equal(k, it.Key().UserKey) && !withSyntheticSuffix {
									b.Fatalf("expected %s, but found %s", k, it.Key().UserKey)
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
						rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

						keys, syntheticPrefix, syntheticSuffix := createBenchBlock(blockSize, w, rng, withSyntheticPrefix, withSyntheticSuffix)

						it, err := NewIter(benchCmp, benchSplit, w.Finish(), block.IterTransforms{SyntheticSuffix: syntheticSuffix, SyntheticPrefix: syntheticPrefix})
						if err != nil {
							b.Fatal(err)
						}
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							j := rng.Intn(len(keys))
							it.SeekLT(keys[j], base.SeekLTFlagsNone)
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
									if !bytes.Equal(k, it.Key().UserKey) && !withSyntheticSuffix {
										b.Fatalf("expected %s, but found %s", k, it.Key().UserKey)
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
						rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

						_, syntheticPrefix, syntheticSuffix := createBenchBlock(blockSize, w, rng, withSyntheticPrefix, withSyntheticSuffix)

						it, err := NewIter(benchCmp, benchSplit, w.Finish(), block.IterTransforms{SyntheticSuffix: syntheticSuffix, SyntheticPrefix: syntheticPrefix})
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
						rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

						_, syntheticPrefix, syntheticSuffix := createBenchBlock(blockSize, w, rng, withSyntheticPrefix, withSyntheticSuffix)

						it, err := NewIter(benchCmp, benchSplit, w.Finish(), block.IterTransforms{SyntheticSuffix: syntheticSuffix, SyntheticPrefix: syntheticPrefix})
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

func BenchmarkCockroachDataBlockWriter(b *testing.B) {
	for _, alphaLen := range []int{4, 8, 26} {
		for _, lenSharedPct := range []float64{0.25, 0.5} {
			for _, prefixLen := range []int{8, 32, 128} {
				lenShared := int(float64(prefixLen) * lenSharedPct)
				for _, valueLen := range []int{8, 128, 1024} {
					keyConfig := crdbtest.KeyConfig{
						PrefixAlphabetLen: alphaLen,
						PrefixLen:         prefixLen,
						PrefixLenShared:   lenShared,
						Logical:           0,
						BaseWallTime:      uint64(time.Now().UnixNano()),
					}
					b.Run(fmt.Sprintf("%s,valueLen=%d", keyConfig, valueLen), func(b *testing.B) {
						benchmarkCockroachDataBlockWriter(b, keyConfig, valueLen)
					})
				}
			}
		}
	}
}

func benchmarkCockroachDataBlockWriter(b *testing.B, keyConfig crdbtest.KeyConfig, valueLen int) {
	const targetBlockSize = 32 << 10
	seed := uint64(time.Now().UnixNano())
	rng := rand.New(rand.NewSource(seed))
	keys, values := crdbtest.RandomKVs(rng, targetBlockSize/valueLen, keyConfig, valueLen)

	var w Writer
	w.RestartInterval = 16
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.Reset()
		var j int
		var prevKeyLen int
		for w.EstimatedSize() < targetBlockSize {
			ik := base.MakeInternalKey(keys[j], base.SeqNum(rng.Uint64n(uint64(base.SeqNumMax))), base.InternalKeyKindSet)
			var samePrefix bool
			if j > 0 {
				samePrefix = bytes.Equal(keys[j], keys[j-1])
			}
			w.AddWithOptionalValuePrefix(
				ik, false, values[j], prevKeyLen, true, block.InPlaceValuePrefix(samePrefix), samePrefix)
			j++
			prevKeyLen = len(ik.UserKey)
		}
		w.Finish()
	}
}

func BenchmarkCockroachDataBlockIter(b *testing.B) {
	for _, alphaLen := range []int{4, 8, 26} {
		for _, lenSharedPct := range []float64{0.25, 0.5} {
			for _, prefixLen := range []int{8, 32, 128} {
				lenShared := int(float64(prefixLen) * lenSharedPct)
				for _, logical := range []uint32{0, 1} {
					for _, valueLen := range []int{8, 128, 1024} {
						keyConfig := crdbtest.KeyConfig{
							PrefixAlphabetLen: alphaLen,
							PrefixLen:         prefixLen,
							PrefixLenShared:   lenShared,
							Logical:           logical,
							BaseWallTime:      uint64(time.Now().UnixNano()),
						}
						b.Run(fmt.Sprintf("%s,value=%d", keyConfig, valueLen),
							func(b *testing.B) {
								benchmarkCockroachDataBlockIter(b, keyConfig, valueLen)
							})
					}
				}
			}
		}
	}
}

func benchmarkCockroachDataBlockIter(b *testing.B, keyConfig crdbtest.KeyConfig, valueLen int) {
	const targetBlockSize = 32 << 10
	seed := uint64(time.Now().UnixNano())
	rng := rand.New(rand.NewSource(seed))
	keys, values := crdbtest.RandomKVs(rng, targetBlockSize/valueLen, keyConfig, valueLen)

	var w Writer
	w.RestartInterval = 16
	var count int
	var prevKeyLen int
	for w.EstimatedSize() < targetBlockSize {
		ik := base.MakeInternalKey(keys[count], base.SeqNum(rng.Uint64n(uint64(base.SeqNumMax))), base.InternalKeyKindSet)
		var samePrefix bool
		if count > 0 {
			samePrefix = bytes.Equal(keys[count], keys[count-1])
		}
		w.AddWithOptionalValuePrefix(
			ik, false, values[count], prevKeyLen, true, block.InPlaceValuePrefix(samePrefix), samePrefix)
		count++
		prevKeyLen = len(ik.UserKey)
	}
	serializedBlock := w.Finish()
	var it Iter
	it.Init(crdbtest.Compare, crdbtest.Split, serializedBlock, block.NoTransforms)
	avgRowSize := float64(len(serializedBlock)) / float64(count)

	b.Run("Next", func(b *testing.B) {
		kv := it.First()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if kv == nil {
				kv = it.First()
			} else {
				kv = it.Next()
			}
		}
		b.StopTimer()
		b.ReportMetric(avgRowSize, "bytes/row")
	})
	b.Run("SeekGE", func(b *testing.B) {
		rng := rand.New(rand.NewSource(seed))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			k := keys[rng.Intn(count)]
			if kv := it.SeekGE(k, base.SeekGEFlagsNone); kv == nil {
				b.Fatalf("%q not found", k)
			}
		}
		b.StopTimer()
		b.ReportMetric(avgRowSize, "bytes/row")
	})
}
