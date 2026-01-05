// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package bloom

import (
	"iter"
	"sync"
)

// hashCollector accumulates key hashes.
type hashCollector struct {
	numHashes uint
	lastHash  uint32

	// We store the hashes in blocks.
	blocks []*hashBlock
	// Initial "in-line" storage for the blocks slice (to avoid some small
	// allocations).
	blocksBuf [16]*hashBlock
}

const hashBlockLen = 16384

type hashBlock [hashBlockLen]uint32

var hashBlockPool = sync.Pool{
	New: func() interface{} {
		return &hashBlock{}
	},
}

func (hc *hashCollector) Init() {
	hc.blocks = hc.blocksBuf[:0]
}

func (hc *hashCollector) Add(h uint32) {
	if hc.numHashes != 0 && h == hc.lastHash {
		return
	}
	ofs := hc.numHashes % hashBlockLen
	if ofs == 0 {
		// Time for a new block.
		hc.blocks = append(hc.blocks, hashBlockPool.Get().(*hashBlock))
	}
	hc.blocks[len(hc.blocks)-1][ofs] = h
	hc.numHashes++
	hc.lastHash = h
}

func (hc *hashCollector) NumHashes() uint {
	return hc.numHashes
}

// Blocks returns an iterator over the collected hash blocks (each block is a
// batch of hashes).
//
// Must not be called if NumHashes() is zero.
func (hc *hashCollector) Blocks() iter.Seq[[]uint32] {
	return func(yield func([]uint32) bool) {
		for i := 0; i < len(hc.blocks)-1; i++ {
			if !yield(hc.blocks[i][:]) {
				return
			}
		}
		// Last block: may be partially filled.
		lastBlock := hc.blocks[len(hc.blocks)-1]
		ofs := hc.numHashes % hashBlockLen
		if ofs == 0 {
			if hc.numHashes == 0 {
				return
			}
			ofs = hashBlockLen
		}
		yield(lastBlock[:ofs])
	}
}

// Reset releases the hash blocks back to the pool.
func (hc *hashCollector) Reset() {
	// Release the hash blocks.
	for _, b := range hc.blocks {
		hashBlockPool.Put(b)
	}
	*hc = hashCollector{}
	hc.Init()
}
