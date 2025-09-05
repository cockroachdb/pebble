// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compression

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/minio/minlz"
)

type minlzCompressor struct {
	level int
}

var _ Compressor = (*minlzCompressor)(nil)

func (c *minlzCompressor) Algorithm() Algorithm { return MinLZ }

func (c *minlzCompressor) Compress(dst, src []byte) ([]byte, Setting) {
	// MinLZ cannot encode blocks greater than 8MB. Fall back to Snappy in those
	// cases.
	if len(src) > minlz.MaxBlockSize {
		return (snappyCompressor{}).Compress(dst, src)
	}

	compressed, err := minlz.Encode(dst, src, c.level)
	if err != nil {
		panic(errors.Wrap(err, "minlz compression"))
	}
	msanWrite(compressed)
	return compressed, Setting{Algorithm: MinLZ, Level: uint8(c.level)}
}

func (c *minlzCompressor) Close() {}

var minlzCompressorFastest = &minlzCompressor{level: minlz.LevelFastest}
var minlzCompressorBalanced = &minlzCompressor{level: minlz.LevelBalanced}

func getMinlzCompressor(level int) Compressor {
	switch level {
	case minlz.LevelFastest:
		return minlzCompressorFastest
	case minlz.LevelBalanced:
		return minlzCompressorBalanced
	default:
		panic(errors.AssertionFailedf("unexpected MinLZ level %d", level))
	}
}

type minlzDecompressor struct{}

var _ Decompressor = minlzDecompressor{}

func (minlzDecompressor) DecompressInto(buf, compressed []byte) error {
	result, err := minlz.Decode(buf, compressed)
	if err != nil {
		return err
	}
	if len(result) != len(buf) || (len(result) > 0 && &result[0] != &buf[0]) {
		return base.CorruptionErrorf("pebble/table: decompressed into unexpected buffer: %p != %p",
			errors.Safe(result), errors.Safe(buf))
	}
	msanWrite(result)
	return nil
}

func (minlzDecompressor) DecompressedLen(b []byte) (decompressedLen int, err error) {
	l, err := minlz.DecodedLen(b)
	return l, err
}

func (minlzDecompressor) Close() {}
