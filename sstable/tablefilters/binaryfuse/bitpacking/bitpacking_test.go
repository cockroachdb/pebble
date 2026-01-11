package bitpacking

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncode8_BPV4(t *testing.T) {
	input := []uint8{0x0A, 0x0B, 0x0C, 0x0D}
	output := make([]byte, 2)
	Encode8(input, 4, output)
	// Expected: [0xBA, 0xDC] (0x0A in low nibble, 0x0B in high nibble, etc.)
	require.Equal(t, []byte{0xBA, 0xDC}, output)

	Encode8(input[:3], 4, output)
	require.Equal(t, []byte{0xBA, 0x0C}, output)
	Encode8(input[:2], 4, output)
	require.Equal(t, []byte{0xBA}, output[:1])
	Encode8(input[:1], 4, output)
	require.Equal(t, []byte{0x0A}, output[:1])
}

func TestEncode8_BPV8(t *testing.T) {
	input := []uint8{0x12, 0x34, 0x56}
	output := make([]byte, 3)
	Encode8(input, 8, output)
	require.Equal(t, input, output)
}

func TestEncode16_BPV12(t *testing.T) {
	// Two 12-bit values: 0x123 and 0x456
	input := []uint16{0x123, 0x456}
	output := make([]byte, 4)
	Encode16(input, 12, output)
	require.Equal(t, []byte{0x23, 0x61, 0x45, 0x00}, output)

	input = []uint16{0x123, 0x456, 0x789}
	output = make([]byte, 7)
	Encode16(input, 12, output)
	require.Equal(t, []byte{0x23, 0x61, 0x45, 0x89, 0x07, 0x00, 0x00}, output)
}

func TestEncode16_BPV16(t *testing.T) {
	input := []uint16{0x1234, 0x5678}
	output := make([]byte, 4)
	Encode16(input, 16, output)

	// Little-endian: 0x1234 -> [0x34, 0x12], 0x5678 -> [0x78, 0x56]
	expected := []byte{0x34, 0x12, 0x78, 0x56}
	for i, v := range expected {
		if output[i] != v {
			t.Errorf("index %d: expected 0x%02X, got 0x%02X", i, v, output[i])
		}
	}
}

func TestDecode_BPV4(t *testing.T) {
	data := []byte{0xBA, 0xDC}

	tests := []struct {
		i        uint
		expected uint16
	}{
		{0, 0x0A},
		{1, 0x0B},
		{2, 0x0C},
		{3, 0x0D},
	}

	for _, tc := range tests {
		got := Decode(data, tc.i, 4)
		if got != tc.expected {
			t.Errorf("Decode(data, %d, 4): expected 0x%X, got 0x%X", tc.i, tc.expected, got)
		}
	}
}

func TestDecode_BPV8(t *testing.T) {
	data := []byte{0x12, 0x34, 0x56}

	for i, v := range data {
		got := Decode(data, uint(i), 8)
		if got != uint16(v) {
			t.Errorf("Decode(data, %d, 8): expected 0x%X, got 0x%X", i, v, got)
		}
	}
}

func TestDecode_BPV12(t *testing.T) {
	// Encoded as: [low8_a=0x23, high4_a|low4_b<<4=0x61, high8_b=0x45]
	data := []byte{0x23, 0x61, 0x45, 0x00}

	got0 := Decode(data, 0, 12)
	if got0 != 0x123 {
		t.Errorf("Decode(data, 0, 12): expected 0x123, got 0x%X", got0)
	}

	got1 := Decode(data, 1, 12)
	if got1 != 0x456 {
		t.Errorf("Decode(data, 1, 12): expected 0x456, got 0x%X", got1)
	}
}

func TestDecode_BPV16(t *testing.T) {
	data := []byte{0x34, 0x12, 0x78, 0x56}

	got0 := Decode(data, 0, 16)
	if got0 != 0x1234 {
		t.Errorf("Decode(data, 0, 16): expected 0x1234, got 0x%X", got0)
	}

	got1 := Decode(data, 1, 16)
	if got1 != 0x5678 {
		t.Errorf("Decode(data, 1, 16): expected 0x5678, got 0x%X", got1)
	}
}

func TestRoundTrip(t *testing.T) {
	for range 100 {
		n := 1 + rand.IntN(100)
		input := make([]uint16, n)
		for i := range input {
			input[i] = uint16(rand.Uint32())
		}
		input8 := make([]uint8, n)
		for i := range input {
			input8[i] = uint8(input[i])
		}

		for _, bpv := range []int{4, 8, 12, 16} {
			encoded := make([]byte, EncodedSize(n, bpv))
			for i := range encoded {
				encoded[i] = 0xCC
			}
			if bpv <= 8 {
				Encode8(input8, bpv, encoded)
			} else {
				Encode16(input, bpv, encoded)
			}
			for i := range input {
				require.Equalf(t, Decode(encoded, uint(i), bpv), input[i]&(1<<bpv-1), "bpv=%d", bpv)
			}
			for range 10 {
				a, b, c := rand.IntN(n), rand.IntN(n), rand.IntN(n)
				va, vb, vc := Decode3(encoded, uint(a), uint(b), uint(c), bpv)
				require.Equalf(t, va, input[a]&(1<<bpv-1), "bpv=%d", bpv)
				require.Equalf(t, vb, input[b]&(1<<bpv-1), "bpv=%d", bpv)
				require.Equalf(t, vc, input[c]&(1<<bpv-1), "bpv=%d", bpv)
			}
		}
	}
}

// Results on an Apple M1:
//
// name             Gvals/s
// Encode/4b-10     8.88 ± 2%
// Encode/8b-10     76.4 ± 1%
// Encode/12b-10    4.96 ± 0%
// Encode/16b-10    38.2 ± 2%
func BenchmarkEncode(b *testing.B) {
	const size = 10000
	vals := make([]uint16, size)
	vals8 := make([]uint8, size)
	for i := range vals {
		vals[i] = uint16(rand.Uint32())
		vals8[i] = uint8(vals[i])
	}
	for _, bpv := range []int{4, 8, 12, 16} {
		b.Run(fmt.Sprintf("%db", bpv), func(b *testing.B) {
			output := make([]byte, EncodedSize(len(vals), bpv))
			b.ResetTimer()
			for range b.N {
				if bpv <= 8 {
					Encode8(vals8, bpv, output)
				} else {
					Encode16(vals, bpv, output)
				}
			}
			b.ReportMetric(float64(b.N*size)/b.Elapsed().Seconds()/1e9, "Gvals/s")
		})
	}
}

// Results on Apple M1:
//
// Decode3/4b-10   3.10ns ± 5%
// Decode3/8b-10   2.48ns ± 0%
// Decode3/12b-10  3.84ns ± 3%
// Decode3/16b-10  2.51ns ± 0%
func BenchmarkDecode3(b *testing.B) {
	const size = 10000
	vals := make([]uint16, size)
	vals8 := make([]uint8, size)
	for i := range vals {
		vals[i] = uint16(rand.Uint32())
		vals8[i] = uint8(vals[i])
	}
	for _, bpv := range []int{4, 8, 12, 16} {
		b.Run(fmt.Sprintf("%db", bpv), func(b *testing.B) {
			output := make([]byte, EncodedSize(len(vals), bpv))
			if bpv <= 8 {
				Encode8(vals8, bpv, output)
			} else {
				Encode16(vals, bpv, output)
			}
			const batch = 128
			indexes := make([][3]uint, batch)
			for i := range indexes {
				indexes[i] = [3]uint{rand.UintN(size), rand.UintN(size), rand.UintN(size)}
			}
			for i := uint(0); b.Loop(); i = (i + 1) % batch {
				Decode3(output, indexes[i][0], indexes[i][1], indexes[i][2], bpv)
			}
		})
	}
}
