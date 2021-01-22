// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// +build !cgo

package sstable

import (
	"github.com/klauspost/compress/zstd"
)

const useStandardZstdLib = false

func decodeZstd(decodedBuf, b []byte) ([]byte, error) {
	decoder, _ := zstd.NewReader(nil)
	defer decoder.Close()
	return decoder.DecodeAll(b, decodedBuf[:0])
}

func encodeZstd(compressedBuf []byte, varIntLen int, b []byte) []byte {
	encoder, _ := zstd.NewWriter(nil)
	defer encoder.Close()
	return encoder.EncodeAll(b, compressedBuf[:varIntLen])
}
