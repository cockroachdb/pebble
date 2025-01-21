// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"testing"
	"time"
	"unicode"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/v2/internal/binfmt"
	"github.com/cockroachdb/pebble/v2/internal/treeprinter"
	"github.com/stretchr/testify/require"
)

func TestBitmapFixed(t *testing.T) {
	var bitmap Bitmap
	datadriven.RunTest(t, "testdata/bitmap", func(t *testing.T, td *datadriven.TestData) string {
		var buf bytes.Buffer
		switch td.Cmd {
		case "build":
			var builder BitmapBuilder
			var n int
			var off uint32
			for _, r := range td.Input {
				if unicode.IsSpace(r) {
					continue
				}
				if r == '1' {
					builder.Set(n)
				}
				n++
			}
			td.MaybeScanArgs(t, "rows", &n)
			td.MaybeScanArgs(t, "offset", &off)

			size := builder.Size(n, off)
			if td.HasArg("invert") {
				size = builder.InvertedSize(n, off)
				builder.Invert(n)
				if newSize := builder.Size(n, off); size != newSize {
					td.Fatalf(t, "InvertedSize=%d, after invert Size=%d", size, newSize)
				}
			}
			data := make([]byte, size)

			endOffset := builder.Finish(0, n, off, data)
			if endOffset != size {
				td.Fatalf(t, "endOffset=%d size=%d", endOffset, size)
			}
			bitmap, _ = DecodeBitmap(data, off, n)
			dumpBitmap(&buf, bitmap)
			fmt.Fprint(&buf, "\nBinary representation:\n")
			f := binfmt.New(data)
			if off > 0 {
				f.HexBytesln(int(off), "initial offset")
			}
			tp := treeprinter.New()
			bitmapToBinFormatter(f, tp.Child("bitmap"), n)
			fmt.Fprint(&buf, tp.String())

		case "seek-set-ge":
			var indexes []int
			td.ScanArgs(t, "indexes", &indexes)
			for _, idx := range indexes {
				fmt.Fprintf(&buf, "bitmap.SeekSetBitGE(%d) = %d\n", idx, bitmap.SeekSetBitGE(idx))
			}

		case "seek-set-le":
			var indexes []int
			td.ScanArgs(t, "indexes", &indexes)
			for _, idx := range indexes {
				fmt.Fprintf(&buf, "bitmap.SeekSetBitLE(%d) = %d\n", idx, bitmap.SeekSetBitLE(idx))
			}

		case "seek-unset-ge":
			var indexes []int
			td.ScanArgs(t, "indexes", &indexes)
			for _, idx := range indexes {
				fmt.Fprintf(&buf, "bitmap.SeekUnsetBitGE(%d) = %d\n", idx, bitmap.SeekUnsetBitGE(idx))
			}

		case "seek-unset-le":
			var indexes []int
			td.ScanArgs(t, "indexes", &indexes)
			for _, idx := range indexes {
				fmt.Fprintf(&buf, "bitmap.SeekUnsetBitLE(%d) = %d\n", idx, bitmap.SeekUnsetBitLE(idx))
			}

		default:
			panic(fmt.Sprintf("unknown command: %s", td.Cmd))
		}
		return buf.String()
	})
}

func TestNextPrevBitInWord(t *testing.T) {
	words := []uint64{0, math.MaxUint64}
	for i := 0; i < 1000; i++ {
		words = append(words, rand.Uint64())
	}
	for _, w := range words {
		// Check that we can reconstruct the word if we jump from set bit to set
		// bit.
		var val uint64
		for i := 0; i < 64; i++ {
			i = nextBitInWord(w, uint(i))
			if i == 64 {
				break
			}
			val |= 1 << i
		}
		require.Equal(t, w, val)
		val = 0
		for i := 63; i >= 0; i-- {
			i = prevBitInWord(w, uint(i))
			if i == -1 {
				break
			}
			val |= 1 << i
		}
		require.Equal(t, w, val)
	}
}

func dumpBitmap(w io.Writer, b Bitmap) {
	for i := 0; i < b.bitCount; i++ {
		if i > 0 && i%64 == 0 {
			w.Write([]byte{'\n'})
		}
		if b.At(i) {
			w.Write([]byte{'1'})
		} else {
			w.Write([]byte{'0'})
		}
	}
}

func TestBitmapRandom(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	var builder BitmapBuilder
	testWithProbability := func(t *testing.T, p float64, size int, invert bool) {
		defer builder.Reset()
		rng := rand.New(rand.NewPCG(0, seed))

		// Sometimes Set +1 bit to test the common pattern of excluding the last
		// row from a data block.
		buildSize := size + rng.IntN(5)
		v := make([]bool, buildSize)
		for i := range v {
			v[i] = rng.Float64() < p
			if v[i] {
				builder.Set(i)
			}
		}

		// Sometimes invert the bitmap.
		if invert {
			builder.Invert(size)
			for i := 0; i < size; i++ {
				v[i] = !v[i]
			}
		}

		data := make([]byte, builder.Size(size, 0))
		_ = builder.Finish(0, size, 0, data)
		bitmap, endOffset := DecodeBitmap(data, 0, size)
		require.Equal(t, uint32(len(data)), endOffset)
		for i := 0; i < size; i++ {
			if got := bitmap.At(i); got != v[i] {
				t.Fatalf("b.Get(%d) = %t; want %t", i, got, v[i])
			}
		}
		for i := 0; i < size; i++ {
			succ := bitmap.SeekSetBitGE(i)
			// Ensure that SeekSetBitGE always returns the index of a set bit.
			if succ != size && !bitmap.At(succ) {
				t.Fatalf("b.SeekSetBitGE(%d) = %d; bit at index %d is not set", i, succ, succ)
			}
			pred := bitmap.SeekSetBitLE(i)
			// Ensure that SeekSetBitLE always returns the index of a set bit.
			if pred >= 0 && !bitmap.At(pred) {
				t.Fatalf("b.SeekSetBitLE(%d) = %d; bit at index %d is not set", i, pred, pred)
			}

			// Ensure there are no set bits between i and succ.
			for j := i; j < succ; j++ {
				if bitmap.At(j) {
					t.Fatalf("b.SeekSetBitGE(%d) = %d; bit at index %d is set", i, succ, j)
				}
			}
			// Ensure there are no set bits between pred and i.
			for j := pred + 1; j < i; j++ {
				if bitmap.At(j) {
					t.Fatalf("b.SeekSetBitLE(%d) = %d; bit at index %d is set", i, pred, j)
				}
			}
		}
		for i := 0; i < size; i++ {
			succ := bitmap.SeekUnsetBitGE(i)
			// Ensure that SeekUnsetBitGE always returns the index of an unset bit.
			if succ != size && bitmap.At(succ) {
				t.Fatalf("b.SeekUnsetBitGE(%d) = %d; bit at index %d is set", i, succ, succ)
			}
			pred := bitmap.SeekUnsetBitLE(i)
			// Ensure that SeekUnsetBitLE always returns the index of an unset bit.
			if pred >= 0 && bitmap.At(pred) {
				t.Fatalf("b.SeekUnsetBitLE(%d) = %d; bit at index %d is set", i, pred, pred)
			}

			// Ensure there are only set bits between i and succ.
			for j := i; j < succ; j++ {
				if !bitmap.At(j) {
					t.Fatalf("b.SeekUnsetBitGE(%d) = %d; bit at index %d is unset", i, succ, j)
				}
			}
			// Ensure there are only set bits between pred and i.
			for j := pred + 1; j < i; j++ {
				if !bitmap.At(j) {
					t.Fatalf("b.SeekUnsetBitLE(%d) = %d; bit at index %d is unset", i, pred, j)
				}
			}
		}
	}

	fixedSizes := []int{1, 2, 3, 4, 16, 63, 64, 65, 128, 129, 256, 257, 1024, 1025, 4096, 4097, 8012, 8200}
	fixedProbabilities := []float64{0.00001, 0.0001, 0.001, 0.1, 0.5, 0.9999}
	for _, p := range fixedProbabilities {
		t.Run(fmt.Sprintf("p=%05f", p), func(t *testing.T) {
			for _, sz := range fixedSizes {
				t.Run(fmt.Sprintf("size=%d", sz), func(t *testing.T) {
					t.Run("invert", func(t *testing.T) {
						testWithProbability(t, p, sz, true /* invert */)
					})
					t.Run("no-invert", func(t *testing.T) {
						testWithProbability(t, p, sz, false /* invert */)
					})
				})
			}
		})
	}
	for i := 0; i < 10; i++ {
		p := rng.ExpFloat64() * 0.1
		t.Run(fmt.Sprintf("p=%05f", p), func(t *testing.T) {
			testWithProbability(t, p, rng.IntN(8200)+1, rng.IntN(2) == 1)
		})
	}
}

func BenchmarkBitmapBuilder(b *testing.B) {
	seed := uint64(10024282523)
	rng := rand.New(rand.NewPCG(0, seed))
	size := rng.IntN(4096) + 1
	v := make([]bool, size)
	for i := 0; i < size; i++ {
		v[i] = rng.IntN(2) == 0
	}
	data := make([]byte, bitmapRequiredSize(size))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var builder BitmapBuilder
		for i := 0; i < size; i++ {
			if v[i] {
				builder.Set(i)
			}
		}
		_ = builder.Finish(0, size, 0, data)
	}
}
