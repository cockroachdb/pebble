// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/aligned"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func TestRawBytes(t *testing.T) {
	var out bytes.Buffer
	var builder RawBytesBuilder
	var rawBytes RawBytes
	datadriven.RunTest(t, "testdata/raw_bytes", func(t *testing.T, td *datadriven.TestData) string {
		out.Reset()
		switch td.Cmd {
		case "build":
			builder.Reset()

			var startOffset int
			td.ScanArgs(t, "offset", &startOffset)

			var count int
			for _, k := range crstrings.Lines(td.Input) {
				builder.Put([]byte(k))
				count++
			}
			td.MaybeScanArgs(t, "count", &count)

			size := builder.Size(count, uint32(startOffset))
			fmt.Fprintf(&out, "Size: %d\n", size-uint32(startOffset))

			buf := aligned.ByteSlice(startOffset + int(size))
			endOffset := builder.Finish(0, count, uint32(startOffset), buf)

			// Validate that builder.Size() was correct in its estimate.
			require.Equal(t, size, endOffset)
			f := binfmt.New(buf).LineWidth(20)
			if startOffset > 0 {
				f.HexBytesln(startOffset, "start offset")
			}
			rawBytesToBinFormatter(f, count, nil)
			var decodedEndOffset uint32
			rawBytes, decodedEndOffset = DecodeRawBytes(buf[startOffset:], 0, count)
			require.Equal(t, endOffset, decodedEndOffset+uint32(startOffset))
			return f.String()
		case "at":
			var indices []int
			td.ScanArgs(t, "indices", &indices)
			for i, index := range indices {
				if i > 0 {
					fmt.Fprintln(&out)
				}
				fmt.Fprintf(&out, "%s", rawBytes.At(index))
			}
			return out.String()
		default:
			panic(fmt.Sprintf("unrecognized command %q", td.Cmd))
		}
	})
}

func BenchmarkRawBytes(b *testing.B) {
	seed := uint64(205295296)
	generateRandomSlices := func(sliceSize, totalByteSize int) [][]byte {
		rng := rand.New(rand.NewSource(seed))
		randInt := func(lo, hi int) int {
			return lo + rng.Intn(hi-lo)
		}
		data := make([]byte, totalByteSize)
		for i := range data {
			data[i] = byte(randInt(int('a'), int('z')+1))
		}
		slices := make([][]byte, 0, totalByteSize/sliceSize)
		for len(data) > 0 {
			s := data[:min(sliceSize, len(data))]
			slices = append(slices, s)
			data = data[len(s):]
		}
		return slices
	}

	var builder RawBytesBuilder
	var buf []byte
	buildRawBytes := func(slices [][]byte) []byte {
		builder.Reset()
		for _, s := range slices {
			builder.Put(s)
		}
		sz := builder.Size(len(slices), 0)
		if cap(buf) >= int(sz) {
			buf = buf[:sz]
		} else {
			buf = make([]byte, sz)
		}
		_ = builder.Finish(0, len(slices), 0, buf)
		return buf
	}

	sizes := []int{8, 128, 1024}

	b.Run("Builder", func(b *testing.B) {
		for _, sz := range sizes {
			b.Run(fmt.Sprintf("sliceLen=%d", sz), func(b *testing.B) {
				slices := generateRandomSlices(sz, 32<<10 /* 32 KiB */)
				b.ResetTimer()
				b.SetBytes(32 << 10)
				for i := 0; i < b.N; i++ {
					data := buildRawBytes(slices)
					fmt.Fprint(io.Discard, data)
				}
			})
		}
	})
	b.Run("At", func(b *testing.B) {
		for _, sz := range sizes {
			b.Run(fmt.Sprintf("sliceLen=%d", sz), func(b *testing.B) {
				slices := generateRandomSlices(sz, 32<<10 /* 32 KiB */)
				data := buildRawBytes(slices)
				rb, _ := DecodeRawBytes(data, 0, len(slices))
				b.ResetTimer()
				b.SetBytes(32 << 10)
				for i := 0; i < b.N; i++ {
					for j := 0; j < len(slices); j++ {
						fmt.Fprint(io.Discard, rb.At(j))
					}
				}
			})
		}
	})
}
