// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/testutils"
)

func TestRunLengthBitmap(t *testing.T) {
	var enc BitmapRunLengthEncoder
	var encodedBuf []byte
	var roundtripBuf []byte
	var buf bytes.Buffer
	datadriven.RunTest(t, "testdata/runlength_bitmap", func(t *testing.T, td *datadriven.TestData) string {
		buf.Reset()
		switch td.Cmd {
		case "encode":
			enc.Init()
			var i int
			for _, r := range td.Input {
				if r == '1' {
					enc.Set(i)
					i++
				} else if r == '0' {
					i++
				}
			}
			fmt.Fprintf(&buf, "Size: %d\n", enc.Size())
			encodedBuf = enc.FinishAndAppend(encodedBuf[:0])
			fmt.Fprintln(&buf, "Binary encoding:")
			binfmt.FHexDump(&buf, encodedBuf, 20, false)

			// Validate that decoding the bitmap and re-encoding it yields the
			// same encoding.
			enc.Init()
			for i := range IterSetBitsInRunLengthBitmap(encodedBuf) {
				enc.Set(i)
			}
			roundtripBuf = enc.FinishAndAppend(roundtripBuf[:0])
			if !bytes.Equal(encodedBuf, roundtripBuf) {
				t.Fatalf("encodedBuf != roundtripBuf")
			}
			return buf.String()
		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestRunLengthBitmap_Randomized(t *testing.T) {
	prng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0))

	for range 20 {
		n := testutils.RandIntInRange(prng, 1, 100000)
		enc := BitmapRunLengthEncoder{}
		enc.Init()
		// Generate a random bitmap with a random density.
		// A cutoff is chosen from an exponential distribution.
		// Then randomly it's decided whether values less than the cutoff should
		// be set, or values greater than the cutoff should be set.
		// Then random values are chosen and compared to the cutoff.
		cutoff := prng.ExpFloat64()
		ltCutoff := prng.IntN(2) == 1

		for i := 0; i < n; i++ {
			if prng.Float64() < cutoff {
				if ltCutoff {
					enc.Set(i)
				}
			} else if !ltCutoff {
				enc.Set(i)
			}
		}

		// Ensure that the encoded bitmap can be decoded and re-encoded,
		// yielding exactly the same encoding.
		encoded := enc.FinishAndAppend(nil)
		enc = BitmapRunLengthEncoder{}
		enc.Init()
		for i := range IterSetBitsInRunLengthBitmap(encoded) {
			enc.Set(i)
		}
		reencoded := enc.FinishAndAppend(nil)
		if !bytes.Equal(encoded, reencoded) {
			t.Fatalf("encoded %x != reencoded %x", encoded, reencoded)
		}
	}
}
