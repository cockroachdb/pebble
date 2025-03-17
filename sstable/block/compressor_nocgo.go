// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build !cgo

package block

import (
	"encoding/binary"

	"github.com/klauspost/compress/zstd"
)

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

// Compress compresses b with the Zstandard algorithm at default compression
// level (level 3). It reuses the preallocated capacity of compressedBuf if it
// is sufficient. The subslice `compressedBuf[:varIntLen]` should already encode
// the length of `b` before calling Compress. It returns the encoded byte
// slice, including the `compressedBuf[:varIntLen]` prefix.
func (ZstdCompressor) Compress(compressedBuf, b []byte) (CompressionIndicator, []byte) {
	if len(compressedBuf) < binary.MaxVarintLen64 {
		compressedBuf = append(compressedBuf, make([]byte, binary.MaxVarintLen64-len(compressedBuf))...)
	}
	varIntLen := binary.PutUvarint(compressedBuf, uint64(len(b)))
	encoder, _ := zstd.NewWriter(nil)
	defer encoder.Close()
	return ZstdCompressionIndicator, encoder.EncodeAll(b, compressedBuf[:varIntLen])
}

// Decode decompresses src with the Zstandard algorithm. The destination
// buffer must already be sufficiently sized, otherwise Decode may error.
func (ZstdDecoder) Decode(dst, src []byte) ([]byte, error) {
	decoder, _ := zstd.NewReader(nil)
	defer decoder.Close()
	return decoder.DecodeAll(src, dst[:0])
}
