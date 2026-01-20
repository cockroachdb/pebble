// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package binaryfuse

import (
	"iter"
	"sync"
)

// hashCollector accumulates key hashes.
type hashCollector struct {
	numHashes uint
	lastHash  uint64

	curBlock *hashBlock
	// We store the hashes in blocks.
	blocks []*hashBlock
	// Initial "in-line" storage for the blocks slice (to avoid some small
	// allocations).
	blocksBuf [16]*hashBlock
}

const hashBlockLen = 8192

type hashBlock [hashBlockLen]uint64

var hashBlockPool = sync.Pool{
	New: func() interface{} {
		return &hashBlock{}
	},
}

func (hc *hashCollector) Init() {
	hc.blocks = hc.blocksBuf[:0]
}

// Add a hash to the collector. Consecutive duplicate hashes are ignored.
func (hc *hashCollector) Add(h uint64) {
	if hc.numHashes != 0 && h == hc.lastHash {
		return
	}
	ofs := hc.numHashes % hashBlockLen
	if ofs == 0 {
		// Time for a new block.
		hc.curBlock = hashBlockPool.Get().(*hashBlock)
		hc.blocks = append(hc.blocks, hc.curBlock)
	}
	hc.curBlock[ofs] = h //gcassert:bce
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
func (hc *hashCollector) Blocks() iter.Seq[[]uint64] {
	return func(yield func([]uint64) bool) {
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
