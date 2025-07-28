// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compression

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/minio/minlz"
)

// Algorithm identifies a compression algorithm. Some compression algorithms
// support multiple compression levels.
//
// Decompressing data requires only an Algorithm.
type Algorithm uint8

const (
	NoCompression Algorithm = iota
	SnappyAlgorithm
	Zstd
	MinLZ

	NumAlgorithms

	Unknown Algorithm = 100
)

// String implements fmt.Stringer, returning a human-readable name for the
// compression algorithm.
func (a Algorithm) String() string {
	switch a {
	case NoCompression:
		return "NoCompression"
	case SnappyAlgorithm:
		return "Snappy"
	case Zstd:
		return "ZSTD"
	case MinLZ:
		return "MinLZ"
	case Unknown:
		return "unknown"
	default:
		return fmt.Sprintf("invalid(%d)", a)
	}
}

// Setting contains the information needed to compress data. It includes an
// Algorithm and possibly a compression level.
type Setting struct {
	Algorithm Algorithm
	// Level depends on the algorithm. Some algorithms don't support a level (in
	// which case Level is 0).
	Level uint8
}

func (s Setting) String() string {
	if s.Level == 0 {
		return s.Algorithm.String()
	}
	return fmt.Sprintf("%s%d", s.Algorithm, s.Level)
}

// ParseSetting parses the result of Setting.String() back into the setting.
func ParseSetting(s string) (_ Setting, ok bool) {
	for i := range NumAlgorithms {
		remainder, ok := strings.CutPrefix(s, i.String())
		if !ok {
			continue
		}
		var level int
		if remainder != "" {
			var err error
			level, err = strconv.Atoi(remainder)
			if err != nil {
				continue
			}
		}
		return Setting{Algorithm: i, Level: uint8(level)}, true
	}
	return Setting{}, false
}

// Setting presets.
var (
	None          = makePreset(NoCompression, 0)
	Snappy        = makePreset(SnappyAlgorithm, 0)
	MinLZFastest  = makePreset(MinLZ, minlz.LevelFastest)
	MinLZBalanced = makePreset(MinLZ, minlz.LevelBalanced)
	ZstdLevel1    = makePreset(Zstd, 1)
	ZstdLevel3    = makePreset(Zstd, 3)
	ZstdLevel5    = makePreset(Zstd, 5)
	ZstdLevel7    = makePreset(Zstd, 7)
)

// Compressor is an interface for compressing data. An instance is associated
// with a specific Setting.
type Compressor interface {
	// Compress a block, appending the compressed data to dst[:0].
	// Returns setting used.
	Compress(dst, src []byte) ([]byte, Setting)

	// Close must be called when the Compressor is no longer needed.
	// After Close is called, the Compressor must not be used again.
	Close()
}

func GetCompressor(s Setting) Compressor {
	switch s.Algorithm {
	case NoCompression:
		return noopCompressor{}
	case SnappyAlgorithm:
		return snappyCompressor{}
	case Zstd:
		return getZstdCompressor(int(s.Level))
	case MinLZ:
		return getMinlzCompressor(int(s.Level))
	default:
		panic("Invalid compression type.")
	}
}

// Decompressor is an interface for compressing data. An instance is associated
// with a specific Algorithm.
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
	case NoCompression:
		return noopDecompressor{}
	case SnappyAlgorithm:
		return snappyDecompressor{}
	case Zstd:
		return getZstdDecompressor()
	case MinLZ:
		return minlzDecompressor{}
	default:
		panic("Invalid compression type.")
	}
}

var presets []Setting

func makePreset(algorithm Algorithm, level uint8) Setting {
	s := Setting{Algorithm: algorithm, Level: level}
	presets = append(presets, s)
	return s
}
