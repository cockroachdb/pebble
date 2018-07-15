package sstable

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/petermattis/pebble/cache"
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

	w := NewWriter(f0, nil, db.LevelOptions{
		BlockRestartInterval: restartInterval,
		BlockSize:            blockSize,
		FilterPolicy:         nil,
	})

	var keys [][]byte
	var ikey db.InternalKey
	for i := uint64(0); i < 1e6; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		keys = append(keys, key)
		ikey.UserKey = key
		w.Add(ikey, nil)
	}

	if err := w.Close(); err != nil {
		b.Fatal(err)
	}

	// Re-open that filename for reading.
	f1, err := mem.Open("bench")
	if err != nil {
		b.Fatal(err)
	}
	return NewReader(f1, 0, &db.Options{
		Cache: cache.New(128 << 20),
	}), keys
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
				for i := 0; i < b.N; i++ {
					it.SeekGE(keys[rng.Intn(len(keys))])
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
				var sum int64
				for i := 0; i < b.N; i++ {
					if !it.Valid() {
						it.First()
					}
					sum += int64(binary.BigEndian.Uint64(it.Key().UserKey))
					it.Next()
				}
				if testing.Verbose() {
					fmt.Println(sum)
				}
			})
	}
}

func BenchmarkTableIterPrev(b *testing.B) {
	const blockSize = 32 << 10

	for _, restartInterval := range []int{16} {
		b.Run(fmt.Sprintf("restart=%d", restartInterval),
			func(b *testing.B) {
				r, _ := buildBenchmarkTable(b, blockSize, restartInterval)
				it := r.NewIter(nil)

				b.ResetTimer()
				var sum int64
				for i := 0; i < b.N; i++ {
					if !it.Valid() {
						it.Last()
					}
					sum += int64(binary.BigEndian.Uint64(it.Key().UserKey))
					it.Prev()
				}
				if testing.Verbose() {
					fmt.Println(sum)
				}
			})
	}
}
