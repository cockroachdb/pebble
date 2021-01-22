// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// +build cgo

package sstable

import (
	"bytes"

	"github.com/DataDog/zstd"
)

const useStandardZstdLib = true

func decodeZstd(decodedBuf, b []byte) ([]byte, error) {
	return zstd.Decompress(decodedBuf, b)
}

func encodeZstd(compressedBuf []byte, varIntLen int, b []byte) []byte {
	buf := bytes.NewBuffer(compressedBuf[:varIntLen])
	writer := zstd.NewWriterLevel(buf, 3)
	writer.Write(b)
	writer.Close()
	return buf.Bytes()
}
