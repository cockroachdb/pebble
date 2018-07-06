package ptable

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func randNullBitmap(rng *rand.Rand, size int) NullBitmap {
	var builder nullBitmapBuilder
	for i := 0; i < size; i++ {
		builder = builder.set(i, rng.Intn(2) == 0)
	}
	builder.finish()
	return makeNullBitmap(builder)
}

func BenchmarkNullBitmapGet(b *testing.B) {
	const size = 4096
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	bmap := randNullBitmap(rng, size)
	b.ResetTimer()

	var sum int
	for i, k := 0, 0; i < b.N; i += k {
		k = size
		if k > b.N-i {
			k = b.N - i
		}
		for j := 0; j < k; j++ {
			if bmap.Null(j) {
				sum++
			}
		}
	}
	if testing.Verbose() {
		fmt.Println(sum)
	}
}

func BenchmarkNullBitmapRank(b *testing.B) {
	const size = 4096
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	bmap := randNullBitmap(rng, size)
	b.ResetTimer()

	var sum int
	for i, k := 0, 0; i < b.N; i += k {
		k = size
		if k > b.N-i {
			k = b.N - i
		}
		for j := 0; j < k; j++ {
			if r := bmap.Rank(j); r >= 0 {
				sum++
			}
		}
	}
	if testing.Verbose() {
		fmt.Println(sum)
	}
}
