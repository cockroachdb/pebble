// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"encoding/binary"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/golang/snappy"
)

func decompressedLen(algo block.CompressionIndicator, b []byte) (int, int, error) {
	switch algo {
	case block.NoCompressionIndicator:
		return 0, 0, nil
	case block.SnappyCompressionIndicator:
		l, err := snappy.DecodedLen(b)
		return l, 0, err
	case block.ZstdCompressionIndicator:
		// This will also be used by zlib, bzip2 and lz4 to retrieve the decodedLen
		// if we implement these algorithms in the future.
		decodedLenU64, varIntLen := binary.Uvarint(b)
		if varIntLen <= 0 {
			return 0, 0, base.CorruptionErrorf("pebble/table: compression block has invalid length")
		}
		return int(decodedLenU64), varIntLen, nil
	default:
		return 0, 0, base.CorruptionErrorf("pebble/table: unknown block compression: %d", errors.Safe(algo))
	}
}

// decompressInto decompresses compressed into buf. The buf slice must have the
// exact size as the decompressed value.
func decompressInto(algo block.CompressionIndicator, compressed []byte, buf []byte) error {
	var result []byte
	var err error
	switch algo {
	case block.SnappyCompressionIndicator:
		result, err = snappy.Decode(buf, compressed)
	case block.ZstdCompressionIndicator:
		result, err = decodeZstd(buf, compressed)
	default:
		return base.CorruptionErrorf("pebble/table: unknown block compression: %d", errors.Safe(algo))
	}
	if err != nil {
		return base.MarkCorruptionError(err)
	}
	if len(result) != len(buf) || (len(result) > 0 && &result[0] != &buf[0]) {
		return base.CorruptionErrorf("pebble/table: decompressed into unexpected buffer: %p != %p",
			errors.Safe(result), errors.Safe(buf))
	}
	return nil
}

// decompressBlock decompresses an SST block, with manually-allocated space.
// NB: If decompressBlock returns (nil, nil), no decompression was necessary and
// the caller may use `b` directly.
func decompressBlock(algo block.CompressionIndicator, b []byte) (*cache.Value, error) {
	if algo == block.NoCompressionIndicator {
		return nil, nil
	}
	// first obtain the decoded length.
	decodedLen, prefixLen, err := decompressedLen(algo, b)
	if err != nil {
		return nil, err
	}
	b = b[prefixLen:]
	// Allocate sufficient space from the cache.
	decoded := cache.Alloc(decodedLen)
	decodedBuf := decoded.Buf()
	if err := decompressInto(algo, b, decodedBuf); err != nil {
		cache.Free(decoded)
		return nil, err
	}
	return decoded, nil
}

// compressBlock compresses an SST block, using compressBuf as the desired destination.
func compressBlock(
	compression block.Compression, b []byte, compressedBuf []byte,
) (blockType block.CompressionIndicator, compressed []byte) {
	switch compression {
	case block.SnappyCompression:
		return block.SnappyCompressionIndicator, snappy.Encode(compressedBuf, b)
	case block.NoCompression:
		return block.NoCompressionIndicator, b
	}

	if len(compressedBuf) < binary.MaxVarintLen64 {
		compressedBuf = append(compressedBuf, make([]byte, binary.MaxVarintLen64-len(compressedBuf))...)
	}
	varIntLen := binary.PutUvarint(compressedBuf, uint64(len(b)))
	switch compression {
	case block.ZstdCompression:
		return block.ZstdCompressionIndicator, encodeZstd(compressedBuf, varIntLen, b)
	default:
		return block.NoCompressionIndicator, b
	}
}
