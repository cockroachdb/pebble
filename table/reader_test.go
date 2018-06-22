package table

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/storage"
)

func buildBenchmarkTable(b *testing.B, blockSize, restartInterval int) (*Reader, [][]byte) {
	mem := storage.NewMem()
	f0, err := mem.Create("bench")
	if err != nil {
		b.Fatal(err)
	}
	defer f0.Close()

	w := newWriter(f0, &db.Options{
		BlockRestartInterval: restartInterval,
		BlockSize:            blockSize,
		Compression:          db.NoCompression,
		FilterPolicy:         nil,
	}, raw{})

	var keys [][]byte
	var ikey db.InternalKey
	for i := 0; w.EstimatedSize() < 4<<20; i++ {
		key := []byte(fmt.Sprintf("%05d", i))
		keys = append(keys, key)
		ikey.UserKey = key
		w.Set(&ikey, nil, nil)
	}

	if err := w.Close(); err != nil {
		b.Fatal(err)
	}

	// Re-open that filename for reading.
	f1, err := mem.Open("bench")
	if err != nil {
		b.Fatal(err)
	}
	return newReader(f1, nil, raw{}), keys
}

func BenchmarkTableIterSeekGE(b *testing.B) {
	const blockSize = 32 << 10

	for _, restartInterval := range []int{16} {
		b.Run(fmt.Sprintf("restart=%d", restartInterval),
			func(b *testing.B) {
				r, keys := buildBenchmarkTable(b, blockSize, restartInterval)
				it := r.NewIter(nil)
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))

				b.ResetTimer()
				var ikey db.InternalKey
				for i := 0; i < b.N; i++ {
					ikey.UserKey = keys[rng.Intn(len(keys))]
					it.SeekGE(&ikey)
				}
			})
	}
}

func BenchmarkTableIterNext(b *testing.B) {
	const blockSize = 32 << 10

	for _, restartInterval := range []int{16} {
		b.Run(fmt.Sprintf("restart=%d", restartInterval),
			func(b *testing.B) {
				r, _ := buildBenchmarkTable(b, blockSize, restartInterval)
				it := r.NewIter(nil)

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if !it.Valid() {
						it.First()
					}
					it.Next()
				}
			})
	}
}
