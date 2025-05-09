// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compression

import "fmt"

type Algorithm uint8

// The available compression types.
const (
	None Algorithm = iota
	Snappy
	Zstd
	MinLZ
	nAlgorithms
)

// String implements fmt.Stringer, returning a human-readable name for the
// compression algorithm.
func (a Algorithm) String() string {
	switch a {
	case None:
		return "NoCompression"
	case Snappy:
		return "Snappy"
	case Zstd:
		return "ZSTD"
	case MinLZ:
		return "MinLZ"
	default:
		return fmt.Sprintf("unknown(%d)", a)
	}
}

type Compressor interface {
	// Compress a block, appending the compressed data to dst[:0].
	Compress(dst, src []byte) []byte

	// Close must be called when the Compressor is no longer needed.
	// After Close is called, the Compressor must not be used again.
	Close()
}

func GetCompressor(a Algorithm) Compressor {
	switch a {
	case None:
		return noopCompressor{}
	case Snappy:
		return snappyCompressor{}
	case Zstd:
		return getZstdCompressor()
	case MinLZ:
		return minlzCompressor{}
	default:
		panic("Invalid compression type.")
	}
}

type Decompressor interface {
	// DecompressInto decompresses compressed into buf. The buf slice must have the
	// exact size as the decompressed value. Callers may use DecompressedLen to
	// determine the correct size.
	DecompressInto(buf, compressed []byte) error

	// DecompressedLen returns the length of the provided block once decompressed,
	// allowing the caller to allocate a buffer exactly sized to the decompressed
	// payload.
	DecompressedLen(b []byte) (decompressedLen int, err error)

	// Close must be called when the Decompressor is no longer needed.
	// After Close is called, the Decompressor must not be used again.
	Close()
}

func GetDecompressor(a Algorithm) Decompressor {
	switch a {
	case None:
		return noopDecompressor{}
	case Snappy:
		return snappyDecompressor{}
	case Zstd:
		return getZstdDecompressor()
	case MinLZ:
		return minlzDecompressor{}
	default:
		panic("Invalid compression type.")
	}
}
