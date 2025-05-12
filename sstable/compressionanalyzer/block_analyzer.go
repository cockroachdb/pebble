// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compressionanalyzer

import (
	"runtime"
	"slices"

	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/pebble/internal/compression"
)

// BlockAnalyzer is used to evaluate the performance and compressibility of different
// compression algorithms on sstable and blob file blocks.
type BlockAnalyzer struct {
	b             Buckets
	compressors   [numSettings]compression.Compressor
	decompressors [numSettings]compression.Decompressor
	minLZFastest  compression.Compressor
	buf1          []byte
	buf2          []byte
}

func NewBlockAnalyzer() *BlockAnalyzer {
	a := &BlockAnalyzer{}
	for i, s := range Settings {
		a.compressors[i] = compression.GetCompressor(s)
		a.decompressors[i] = compression.GetDecompressor(s.Algorithm)
	}
	a.minLZFastest = compression.GetCompressor(compression.MinLZFastest)
	a.buf1 = make([]byte, 256*1024)
	a.buf2 = make([]byte, 256*1024)
	return a
}

func (a *BlockAnalyzer) Close() {
	for _, c := range a.compressors {
		c.Close()
	}
	for _, d := range a.decompressors {
		d.Close()
	}
	a.minLZFastest.Close()
	*a = BlockAnalyzer{}
}

// Block analyzes a block by measuring its compressibility and the performance
// of various compression algorithms on it.
func (a *BlockAnalyzer) Block(kind BlockKind, block []byte) {
	size := MakeBlockSize(len(block))
	compressibility := MakeCompressibility(len(block), len(a.minLZFastest.Compress(a.buf1[:0], block)))
	bucket := &a.b[kind][size][compressibility]
	bucket.UncompressedSize.Add(float64(len(block)))
	for i := range Settings {
		a.runExperiment(&bucket.Experiments[i], block, a.compressors[i], a.decompressors[i])
	}
}

func (a *BlockAnalyzer) Buckets() *Buckets {
	return &a.b
}

func (a *BlockAnalyzer) runExperiment(
	pa *PerSetting,
	block []byte,
	compressor compression.Compressor,
	decompressor compression.Decompressor,
) {
	a.buf1 = ensureLen(a.buf1, len(block)+32)
	a.buf2 = ensureLen(a.buf2, len(block))
	// Yield the processor, reducing the chance that we get preempted during
	// Compress.
	runtime.Gosched()
	t1 := crtime.NowMono()
	compressed := compressor.Compress(a.buf1[:0], block)
	compressionTime := t1.Elapsed()

	// Yield the processor, reducing the chance that we get preempted during
	// DecompressInto.
	runtime.Gosched()
	t2 := crtime.NowMono()
	if err := decompressor.DecompressInto(a.buf2, compressed); err != nil {
		panic(err)
	}
	decompressionTime := t2.Elapsed()

	pa.CompressionTime.Add(compressionTime.Seconds() * 1e6)
	pa.DecompressionTime.Add(decompressionTime.Seconds() * 1e6)
	pa.CompressionRatio.Add(float64(len(block)) / float64(len(compressed)))
}

func ensureLen(b []byte, n int) []byte {
	if cap(b) < n {
		b = slices.Grow(b[:cap(b)], n-cap(b))
	}
	return b[:n]
}
