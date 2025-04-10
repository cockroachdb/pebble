//go:build cgo

package block

import (
	"encoding/binary"
	"sync"

	"github.com/DataDog/zstd"
	"github.com/cockroachdb/errors"
)

type zstdCompressor struct {
	ctx zstd.Ctx
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

// Compress compresses b with the Zstandard algorithm at default compression
// level (level 3). It reuses the preallocated capacity of compressedBuf if it
// is sufficient. The subslice `compressedBuf[:varIntLen]` should already encode
// the length of `b` before calling Compress. It returns the encoded byte
// slice, including the `compressedBuf[:varIntLen]` prefix.
func (z *zstdCompressor) Compress(
	compressedBuf []byte, b []byte, level CompressionLevel,
) (CompressionIndicator, []byte) {
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
	var encoderLevel int
	if level == LevelDefault {
		encoderLevel = int(ZstdLevelDefault)
	} else if level < ZstdLevelMin || level > ZstdLevelMax {
		panic("zstd compression: illegal level")
	} else {
		encoderLevel = int(level)
	}
	result, err := z.ctx.CompressLevel(compressedBuf[varIntLen:varIntLen+bound], b, encoderLevel)
	if err != nil {
		panic(errors.Wrap(err, "Error while compressing using Zstd."))
	}
	if &result[0] != &compressedBuf[varIntLen] {
		panic("Allocated a new buffer despite checking CompressBound.")
	}

	return ZstdCompressionIndicator, compressedBuf[:varIntLen+len(result)]
}

func (z *zstdCompressor) Close() {
	zstdCompressorPool.Put(z)
}

func getZstdCompressor() *zstdCompressor {
	return zstdCompressorPool.Get().(*zstdCompressor)
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
		return errors.Wrap(err, "Error while decompressing Zstd.")
	}
	return nil
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
