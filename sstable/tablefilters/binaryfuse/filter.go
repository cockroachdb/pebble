// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package binaryfuse

import (
	"context"
	"encoding/binary"
	"math/bits"
	"runtime"
	"sync"

	"github.com/FastFilter/xorfilter"
	"github.com/cockroachdb/crlib/fifo"
	"github.com/cockroachdb/pebble/sstable/tablefilters/binaryfuse/bitpacking"
)

// Building a binary fuse filter requires non-trivial memory, about 24 bytes per
// hash. We split into three regimes:
//   - for small filters (<= maxSizeForPool), we reuse builder via a sync.Pool.
//   - for medium filters (<= maxSizeForReuse), we limit parallelism to GOMAXPROCS/4
//     and reuse builders.
//   - for larger filters (<= maxSize), we further limit parallelism (proportional to the ratio
//     of filter size to maxSizeForReuse) and don't reuse builders.
const maxSizeForPool = 100_000
const maxSizeForReuse = 1_000_000
const maxSize = 10_000_000

var globalState struct {
	once       sync.Once
	sema       *fifo.Semaphore
	buildersMu sync.Mutex
	builders   []*builder
}

type builder struct {
	hashes    []uint64
	bfBuilder xorfilter.BinaryFuseBuilder
}

func ensureInitialized() {
	globalState.once.Do(func() {
		n := (runtime.GOMAXPROCS(0) + 3) / 4
		globalState.sema = fifo.NewSemaphore(int64(n))
	})
}

var builderPool = sync.Pool{
	New: func() interface{} {
		return &builder{}
	},
}

func withBuilder(numHashes uint, fn func(bld *builder)) {
	var bld *builder

	switch {
	case numHashes <= maxSizeForPool:
		bld = builderPool.Get().(*builder)
		defer builderPool.Put(bld)

	case numHashes <= maxSizeForReuse:
		ensureInitialized()
		_ = globalState.sema.Acquire(context.Background(), 1)
		globalState.buildersMu.Lock()

		if len(globalState.builders) > 0 {
			bld = globalState.builders[len(globalState.builders)-1]
			globalState.builders = globalState.builders[:len(globalState.builders)-1]
		} else {
			bld = &builder{}
		}
		globalState.buildersMu.Unlock()

		defer func() {
			globalState.buildersMu.Lock()
			globalState.builders = append(globalState.builders, bld)
			globalState.buildersMu.Unlock()
			globalState.sema.Release(1)
		}()

	default:
		ensureInitialized()
		units := (numHashes + maxSizeForReuse - 1) / maxSizeForReuse
		_ = globalState.sema.Acquire(context.Background(), int64(units))
		defer globalState.sema.Release(int64(units))
		bld = &builder{}
	}

	fn(bld)
}

// trailer:
//   - seed (8 bytes)
//   - segment count (4 bytes)
//   - segment shift (1 byte)
//   - bits per fingerprint (1 byte)
const trailerLen = 14

func buildFilter(hc *hashCollector, fpBits int) (data []byte, ok bool) {
	n := hc.NumHashes()
	if n == 0 || n > maxSize {
		return nil, false
	}
	withBuilder(n, func(bld *builder) {
		bld.hashes = bld.hashes[:0]
		for b := range hc.Blocks() {
			bld.hashes = append(bld.hashes, b...)
		}
		if fpBits <= 8 {
			data, ok = build[uint8](bld, fpBits)
		} else {
			data, ok = build[uint16](bld, fpBits)
		}
	})
	return data, ok
}

func build[T uint8 | uint16](bld *builder, fpBits int) (data []byte, ok bool) {
	filter, err := xorfilter.BuildBinaryFuse[T](&bld.bfBuilder, bld.hashes)
	if err != nil {
		// This shouldn't happen in practice, but in principle the filter
		// construction could fail (especially for small filters).
		return nil, false
	}
	encSize := bitpacking.EncodedSize(len(filter.Fingerprints), fpBits)
	data = make([]byte, encSize, encSize+trailerLen)

	switch fingerprints := any(filter.Fingerprints).(type) {
	case []uint8:
		bitpacking.Encode8(fingerprints, fpBits, data)
	case []uint16:
		bitpacking.Encode16(fingerprints, fpBits, data)
	default:
		panic("unsupported fingerprints type")
	}

	data = binary.LittleEndian.AppendUint64(data, filter.Seed)
	data = binary.LittleEndian.AppendUint32(data, filter.SegmentCount)
	segLenShift := bits.TrailingZeros32(filter.SegmentLength)
	data = append(data, byte(segLenShift), byte(fpBits))
	return data, true
}

// mayContain reports whether the given key may be contained in the
// filter (created by buildFilter).
func mayContain(filter []byte, hash uint64) bool {
	if len(filter) <= trailerLen {
		return false
	}
	n := len(filter) - trailerLen
	data, trailer := filter[:n], filter[n:]

	seed := binary.LittleEndian.Uint64(trailer[:])
	segCount := binary.LittleEndian.Uint32(trailer[8:])
	segShift := trailer[12]
	fpBits := trailer[13]

	// We cannot use xorfilter.BinaryFuse.Contains() because it expects the
	// fingerprints in a slice. We reimplement it here.
	hash = murmur64(hash + seed)
	f := uint16(hash^(hash>>32)) & (1<<fpBits - 1)

	segLen := uint32(1) << segShift

	// Code adapted from xorfilter.BinaryFuse.Contains().
	hi, _ := bits.Mul64(hash, uint64(segCount)<<segShift)
	h0 := uint32(hi)
	h1 := h0 + segLen
	h2 := h1 + segLen
	h1 ^= uint32(hash>>18) & (segLen - 1)
	h2 ^= uint32(hash) & (segLen - 1)

	f0, f1, f2 := bitpacking.Decode3(data, uint(h0), uint(h1), uint(h2), int(fpBits))
	return f == f0^f1^f2
}

func murmur64(h uint64) uint64 {
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}
