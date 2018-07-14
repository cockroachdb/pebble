package pebble

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/petermattis/pebble/cache"
	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/sstable"
	"github.com/petermattis/pebble/storage"
)

func TestLevelIter(t *testing.T) {
	testCases := []struct {
		key          string
		files        string
		expectedNext string
		expectedPrev string
	}{
		{
			"a",
			"a:1,b:2 c:3,d:4",
			"<a:1><b:2><c:3><d:4>.",
			".",
		},
		{
			"a",
			"a:1,b:2 c:3,d:4",
			"<a:1><b:2><c:3><d:4>.",
			".",
		},
		{
			"b",
			"a:1,b:2 c:3,d:4",
			"<b:2><c:3><d:4>.",
			"<a:1>.",
		},
		{
			"c",
			"a:1,b:2 c:3,d:4",
			"<c:3><d:4>.",
			"<b:2><a:1>.",
		},
		{
			"d",
			"a:1,b:2 c:3,d:4",
			"<d:4>.",
			"<c:3><b:2><a:1>.",
		},
		{
			"e",
			"a:1,b:2 c:3,d:4",
			".",
			"<d:4><c:3><b:2><a:1>.",
		},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			var files []fileMetadata
			var fileData [][]string

			for _, data := range strings.Split(c.files, " ") {
				keys := strings.Split(data, ",")
				meta := fileMetadata{
					fileNum: uint64(len(files)),
				}
				meta.smallest = fakeIkey(keys[0])
				meta.largest = fakeIkey(keys[len(keys)-1])
				files = append(files, meta)
				fileData = append(fileData, keys)
			}

			newIter := func(fileNum uint64) (db.InternalIterator, error) {
				return newFakeIterator(nil, fileData[fileNum]...), nil
			}

			iter := &levelIter{}
			iter.init(db.DefaultComparer.Compare, newIter, files)
			key := []byte(c.key)

			var b bytes.Buffer
			for iter.SeekGE(key); iter.Valid(); iter.Next() {
				fmt.Fprintf(&b, "<%s:%d>", iter.Key().UserKey, iter.Key().SeqNum())
			}
			if err := iter.Error(); err != nil {
				fmt.Fprintf(&b, "err=%v", err)
			} else {
				b.WriteByte('.')
			}
			if got := b.String(); got != c.expectedNext {
				t.Errorf("got  %q\nwant %q", got, c.expectedNext)
			}

			b.Reset()
			for iter.SeekLT(key); iter.Valid(); iter.Prev() {
				fmt.Fprintf(&b, "<%s:%d>", iter.Key().UserKey, iter.Key().SeqNum())
			}
			if err := iter.Close(); err != nil {
				fmt.Fprintf(&b, "err=%v", err)
			} else {
				b.WriteByte('.')
			}
			if got := b.String(); got != c.expectedPrev {
				t.Errorf("got  %q\nwant %q", got, c.expectedPrev)
			}
		})
	}
}

func buildLevelIterTables(
	b *testing.B, blockSize, restartInterval, count int,
) ([]*sstable.Reader, []fileMetadata, [][]byte) {
	mem := storage.NewMem()
	files := make([]storage.File, count)
	for i := range files {
		f, err := mem.Create(fmt.Sprintf("bench%d", i))
		if err != nil {
			b.Fatal(err)
		}
		defer f.Close()
		files[i] = f
	}

	writers := make([]*sstable.Writer, len(files))
	for i := range files {
		writers[i] = sstable.NewWriter(files[i], nil, &db.LevelOptions{
			BlockRestartInterval: restartInterval,
			BlockSize:            blockSize,
			Compression:          db.NoCompression,
		})
	}

	var keys [][]byte
	var i int
	const targetSize = 2 << 20
	for _, w := range writers {
		for ; w.EstimatedSize() < targetSize; i++ {
			key := []byte(fmt.Sprintf("%08d", i))
			keys = append(keys, key)
			ikey := db.MakeInternalKey(key, 0, db.InternalKeyKindSet)
			w.Add(ikey, nil)
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
	}

	cache := cache.New(128 << 20)
	readers := make([]*sstable.Reader, len(files))
	for i := range files {
		f, err := mem.Open(fmt.Sprintf("bench%d", i))
		if err != nil {
			b.Fatal(err)
		}
		readers[i] = sstable.NewReader(f, uint64(i), &db.Options{
			Cache: cache,
		})
	}

	meta := make([]fileMetadata, len(readers))
	for i := range readers {
		iter := readers[i].NewIter(nil)
		iter.First()
		meta[i].fileNum = uint64(i)
		meta[i].smallest = iter.Key()
		iter.Last()
		meta[i].largest = iter.Key()
	}
	return readers, meta, keys
}

func BenchmarkLevelIterSeekGE(b *testing.B) {
	const blockSize = 32 << 10

	for _, restartInterval := range []int{16} {
		b.Run(fmt.Sprintf("restart=%d", restartInterval),
			func(b *testing.B) {
				for _, count := range []int{5} {
					b.Run(fmt.Sprintf("count=%d", count),
						func(b *testing.B) {
							readers, files, keys := buildLevelIterTables(b, blockSize, restartInterval, count)
							newIter := func(fileNum uint64) (db.InternalIterator, error) {
								return readers[fileNum].NewIter(nil), nil
							}
							l := &levelIter{}
							l.init(db.DefaultComparer.Compare, newIter, files)
							rng := rand.New(rand.NewSource(time.Now().UnixNano()))

							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								l.SeekGE(keys[rng.Intn(len(keys))])
							}
						})
				}
			})
	}
}

func BenchmarkLevelIterNext(b *testing.B) {
	const blockSize = 32 << 10

	for _, restartInterval := range []int{16} {
		b.Run(fmt.Sprintf("restart=%d", restartInterval),
			func(b *testing.B) {
				for _, count := range []int{5} {
					b.Run(fmt.Sprintf("count=%d", count),
						func(b *testing.B) {
							readers, files, _ := buildLevelIterTables(b, blockSize, restartInterval, count)
							newIter := func(fileNum uint64) (db.InternalIterator, error) {
								return readers[fileNum].NewIter(nil), nil
							}
							l := &levelIter{}
							l.init(db.DefaultComparer.Compare, newIter, files)

							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								if !l.Valid() {
									l.First()
								}
								l.Next()
							}
						})
				}
			})
	}
}

func BenchmarkLevelIterPrev(b *testing.B) {
	const blockSize = 32 << 10

	for _, restartInterval := range []int{16} {
		b.Run(fmt.Sprintf("restart=%d", restartInterval),
			func(b *testing.B) {
				for _, count := range []int{5} {
					b.Run(fmt.Sprintf("count=%d", count),
						func(b *testing.B) {
							readers, files, _ := buildLevelIterTables(b, blockSize, restartInterval, count)
							newIter := func(fileNum uint64) (db.InternalIterator, error) {
								return readers[fileNum].NewIter(nil), nil
							}
							l := &levelIter{}
							l.init(db.DefaultComparer.Compare, newIter, files)

							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								if !l.Valid() {
									l.Last()
								}
								l.Prev()
							}
						})
				}
			})
	}
}
