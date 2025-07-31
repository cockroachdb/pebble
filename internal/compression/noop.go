// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compression

type noopCompressor struct{}

var _ Compressor = noopCompressor{}

func (noopCompressor) Compress(dst, src []byte) ([]byte, Setting) {
	return append(dst[:0], src...), NoCompression
}
func (noopCompressor) Close() {}

type noopDecompressor struct{}

var _ Decompressor = noopDecompressor{}

func (noopDecompressor) DecompressInto(dst, src []byte) error {
	dst = dst[:len(src)]
	copy(dst, src)
	return nil
}

func (noopDecompressor) DecompressedLen(b []byte) (decompressedLen int, err error) {
	return len(b), nil
}

func (noopDecompressor) Close() {}
