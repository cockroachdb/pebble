// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compression

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/golang/snappy"
)

type snappyCompressor struct{}

var _ Compressor = snappyCompressor{}

func (snappyCompressor) Algorithm() Algorithm { return SnappyAlgorithm }

func (snappyCompressor) Compress(dst, src []byte) []byte {
	dst = dst[:cap(dst):cap(dst)]
	return snappy.Encode(dst, src)
}

func (snappyCompressor) Close() {}

type snappyDecompressor struct{}

var _ Decompressor = snappyDecompressor{}

func (snappyDecompressor) DecompressInto(buf, compressed []byte) error {
	result, err := snappy.Decode(buf, compressed)
	if err != nil {
		return err
	}
	if len(result) != len(buf) || (len(result) > 0 && &result[0] != &buf[0]) {
		return base.CorruptionErrorf("pebble: decompressed into unexpected buffer: %p != %p",
			errors.Safe(result), errors.Safe(buf))
	}
	return nil
}

func (snappyDecompressor) DecompressedLen(b []byte) (decompressedLen int, err error) {
	return snappy.DecodedLen(b)
}

func (snappyDecompressor) Close() {}
