package bitflip

// CheckSliceForBitFlip flips bits in data to see if it matches the expected checksum.
// Returns the index and bit if successful.
func CheckSliceForBitFlip(
	data []byte, computeChecksum func([]byte) uint32, expectedChecksum uint32,
) (found bool, indexFound int, bitFound int) {
	// TODO(edward) This checking process likely can be made faster.
	iterationLimit := 40 * (1 << 10) // 40KB
	for i := 0; i < min(len(data), iterationLimit); i++ {
		foundFlip, bit := checkByteForFlip(data, i, computeChecksum, expectedChecksum)
		if foundFlip {
			return true, i, bit
		}
	}
	return false, 0, 0
}

func checkByteForFlip(
	data []byte, i int, computeChecksum func([]byte) uint32, expectedChecksum uint32,
) (found bool, bit int) {
	for bit := 0; bit < 8; bit++ {
		data[i] ^= (1 << bit)
		var computedChecksum = computeChecksum(data)
		data[i] ^= (1 << bit)
		if computedChecksum == expectedChecksum {
			return true, bit
		}
	}
	return false, 0
}
