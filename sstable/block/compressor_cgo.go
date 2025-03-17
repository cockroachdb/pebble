//go:build cgo

package block

import (
	"bytes"
	"encoding/binary"

	"github.com/DataDog/zstd"
	"github.com/cockroachdb/errors"
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
const UseStandardZstdLib = true

// Compress compresses b with the Zstandard algorithm at default compression
// level (level 3). It reuses the preallocated capacity of compressedBuf if it
// is sufficient. The subslice `compressedBuf[:varIntLen]` should already encode
// the length of `b` before calling Compress. It returns the encoded byte
// slice, including the `compressedBuf[:varIntLen]` prefix.
func (ZstdCompressor) Compress(compressedBuf []byte, b []byte) (CompressionIndicator, []byte) {
	if len(compressedBuf) < binary.MaxVarintLen64 {
		compressedBuf = append(compressedBuf, make([]byte, binary.MaxVarintLen64-len(compressedBuf))...)
	}
	varIntLen := binary.PutUvarint(compressedBuf, uint64(len(b)))
	buf := bytes.NewBuffer(compressedBuf[:varIntLen])
	writer := zstd.NewWriterLevel(buf, 3)
	writer.Write(b)
	writer.Close()
	return ZstdCompressionIndicator, buf.Bytes()
}

// DecompressInto decompresses src with the Zstandard algorithm. The destination
// buffer must already be sufficiently sized, otherwise Decompress may error.
func (ZstdDecompressor) DecompressInto(dst, src []byte) error {
	if len(src) == 0 {
		return errors.Errorf("decodeZstd: empty src buffer")
	}
	if len(dst) == 0 {
		return errors.Errorf("decodeZstd: empty dst buffer")
	}
	_, err := zstd.DecompressInto(dst, src)
	if err != nil {
		return err
	}
	return nil
}
