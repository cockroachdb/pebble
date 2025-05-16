// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build !cgo

package compression

import (
	"encoding/binary"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/klauspost/compress/zstd"
)

type zstdCompressor zstd.Encoder

var _ Compressor = (*zstdCompressor)(nil)

func getZstdCompressor(level int) *zstdCompressor {
	return (*zstdCompressor)(zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level))))
}

// UseStandardZstdLib indicates whether the zstd implementation is a port of the
// official one in the facebook/zstd repository.
//
// This constant is only used in tests. Some tests rely on reproducibility of
// SST files, but a custom implementation of zstd will produce different
// compression result. So those tests have to be disabled in such cases.
//
// We cannot always use the official facebook/zstd implementation since it
// relies on CGo.
const UseStandardZstdLib = false

func (z *zstdCompressor) Compress(compressedBuf, b []byte) []byte {
	if len(compressedBuf) < binary.MaxVarintLen64 {
		compressedBuf = append(compressedBuf, make([]byte, binary.MaxVarintLen64-len(compressedBuf))...)
	}
	varIntLen := binary.PutUvarint(compressedBuf, uint64(len(b)))
	result := (*zstd.Encoder)(z).EncodeAll(b, compressedBuf[:varIntLen])
	if err := encoder.Close(); err != nil {
		panic(err)
	}
	return result
}

func (z *zstdCompressor) Close() {
	if err := (*zstd.Encoder)(z).Close(); err != nil {
		panic(err)
	}
}

type zstdDecompressor struct{}

var _ Decompressor = zstdDecompressor{}

func (zstdDecompressor) DecompressInto(dst, src []byte) error {
	// The payload is prefixed with a varint encoding the length of
	// the decompressed block.
	_, prefixLen := binary.Uvarint(src)
	src = src[prefixLen:]
	decoder, _ := zstd.NewReader(nil)
	defer decoder.Close()
	result, err := decoder.DecodeAll(src, dst[:0])
	if err != nil {
		return err
	}
	if len(result) != len(dst) || (len(result) > 0 && &result[0] != &dst[0]) {
		return base.CorruptionErrorf("pebble/table: decompressed into unexpected buffer: %p != %p",
			errors.Safe(result), errors.Safe(dst))
	}
	return nil
}

func (zstdDecompressor) DecompressedLen(b []byte) (decompressedLen int, err error) {
	// This will also be used by zlib, bzip2 and lz4 to retrieve the decodedLen
	// if we implement these algorithms in the future.
	decodedLenU64, varIntLen := binary.Uvarint(b)
	if varIntLen <= 0 {
		return 0, base.CorruptionErrorf("pebble: compression block has invalid length")
	}
	return int(decodedLenU64), nil
}

func (zstdDecompressor) Close() {}

func getZstdDecompressor() zstdDecompressor {
	return zstdDecompressor{}
}
