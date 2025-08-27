// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build cgo

package compression

import (
	"encoding/binary"
	"sync"

	"github.com/DataDog/zstd"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2/internal/base"
)

type zstdCompressor struct {
	level int
	ctx   zstd.Ctx
}

var _ Compressor = (*zstdCompressor)(nil)

var zstdCompressorPool = sync.Pool{
	New: func() any {
		return &zstdCompressor{ctx: zstd.NewCtx()}
	},
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
const UseStandardZstdLib = true

func (z *zstdCompressor) Compress(compressedBuf []byte, b []byte) ([]byte, Setting) {
	if len(compressedBuf) < binary.MaxVarintLen64 {
		compressedBuf = append(compressedBuf, make([]byte, binary.MaxVarintLen64-len(compressedBuf))...)
	}

	// Get the bound and allocate the proper amount of memory instead of relying on
	// Datadog/zstd to do it for us. This allows us to avoid memcopying data around
	// for the varIntLen prefix.
	bound := zstd.CompressBound(len(b))
	if cap(compressedBuf) < binary.MaxVarintLen64+bound {
		compressedBuf = make([]byte, binary.MaxVarintLen64, binary.MaxVarintLen64+bound)
	}

	varIntLen := binary.PutUvarint(compressedBuf, uint64(len(b)))
	result, err := z.ctx.CompressLevel(compressedBuf[varIntLen:varIntLen+bound], b, z.level)
	if err != nil {
		panic("Error while compressing using Zstd.")
	}
	if &result[0] != &compressedBuf[varIntLen] {
		panic("Allocated a new buffer despite checking CompressBound.")
	}

	return compressedBuf[:varIntLen+len(result)], Setting{Algorithm: Zstd, Level: uint8(z.level)}
}

func (z *zstdCompressor) Close() {
	zstdCompressorPool.Put(z)
}

func getZstdCompressor(level int) *zstdCompressor {
	z := zstdCompressorPool.Get().(*zstdCompressor)
	z.level = level
	return z
}

type zstdDecompressor struct {
	ctx zstd.Ctx
}

var _ Decompressor = (*zstdDecompressor)(nil)

// DecompressInto decompresses src with the Zstandard algorithm. The destination
// buffer must already be sufficiently sized, otherwise DecompressInto may error.
func (z *zstdDecompressor) DecompressInto(dst, src []byte) error {
	// The payload is prefixed with a varint encoding the length of
	// the decompressed block.
	_, prefixLen := binary.Uvarint(src)
	src = src[prefixLen:]
	if len(src) == 0 {
		return errors.Errorf("decodeZstd: empty src buffer")
	}
	if len(dst) == 0 {
		return errors.Errorf("decodeZstd: empty dst buffer")
	}
	_, err := z.ctx.DecompressInto(dst, src)
	if err != nil {
		return err
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

func (z *zstdDecompressor) Close() {
	zstdDecompressorPool.Put(z)
}

var zstdDecompressorPool = sync.Pool{
	New: func() any {
		return &zstdDecompressor{ctx: zstd.NewCtx()}
	},
}

func getZstdDecompressor() *zstdDecompressor {
	return zstdDecompressorPool.Get().(*zstdDecompressor)
}
