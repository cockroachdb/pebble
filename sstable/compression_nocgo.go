// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build !cgo
// +build !cgo

package sstable

import "github.com/klauspost/compress/zstd"

// decodeZstd decompresses src with the Zstandard algorithm. The destination
// buffer must already be sufficiently sized, otherwise decodeZstd may error.
func decodeZstd(dst, src []byte) error {
	decoder, _ := zstd.NewReader(nil)
	defer decoder.Close()
	_, err := decoder.DecodeAll(src, dst[:0])
	return err
}

// encodeZstd compresses b with the Zstandard algorithm at default compression
// level (level 3). It reuses the preallocated capacity of compressedBuf if it
// is sufficient. The subslice `compressedBuf[:varIntLen]` should already encode
// the length of `b` before calling encodeZstd. It returns the encoded byte
// slice, including the `compressedBuf[:varIntLen]` prefix.
func encodeZstd(compressedBuf []byte, varIntLen int, b []byte) []byte {
	encoder, _ := zstd.NewWriter(nil)
	defer encoder.Close()
	return encoder.EncodeAll(b, compressedBuf[:varIntLen])
}
