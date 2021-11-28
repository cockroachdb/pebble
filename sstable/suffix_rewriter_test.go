package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/internal/base"
)

// memFile is a file-like struct that buffers all data written to it in memory.
// Implements the writeCloseSyncer interface.
type memFile struct {
	bytes.Buffer
}

// Close implements the writeCloseSyncer interface.
func (*memFile) Close() error {
	return nil
}

// Sync implements the writeCloseSyncer interface.
func (*memFile) Sync() error {
	return nil
}

// Data returns the in-memory buffer behind this MemFile.
func (f *memFile) Data() []byte {
	return f.Bytes()
}

// Flush is implemented so it prevents buffering inside Writter.
func (f *memFile) Flush() error {
	return nil
}

func BenchmarkRewriteSST(b *testing.B) {
	writerOpts := WriterOptions{
		FilterPolicy: bloom.FilterPolicy(10),
		Comparer:     test4bSuffixComparer,
	}

	key := make([]byte, 28)
	copy(key[24:], "_123")
	sizes := []int{100, 10000, 1e6}
	compressions := []Compression{NoCompression, SnappyCompression}

	files := make([][]*Reader, len(compressions))

	for comp := range compressions {
		files[comp] = make([]*Reader, len(sizes))

		for size := range sizes {
			writerOpts.Compression = compressions[comp]
			f := &memFile{}
			w := NewWriter(f, writerOpts)
			for i := 0; i < sizes[size]; i++ {
				binary.BigEndian.PutUint64(key[:8], 123) // 16-byte shared prefix
				binary.BigEndian.PutUint64(key[8:16], 456)
				binary.BigEndian.PutUint64(key[16:], uint64(i))
				if err := w.Set(key, key); err != nil {
					b.Fatal(err)
				}
			}

			if err := w.Close(); err != nil {
				b.Fatal(err)
			}
			r, err := NewMemReader(f.Bytes(), ReaderOptions{
				Comparer: test4bSuffixComparer,
				Filters:  map[string]base.FilterPolicy{writerOpts.FilterPolicy.Name(): writerOpts.FilterPolicy},
			})
			if err != nil {
				b.Fatal(err)
			}
			files[comp][size] = r
		}
	}

	b.ResetTimer()
	for comp := range compressions {
		b.Run(compressions[comp].String(), func(b *testing.B) {
			for sz := range sizes {
				r := files[comp][sz]
				b.Run(fmt.Sprintf("keys=%d", sizes[sz]), func(b *testing.B) {
					b.Run("ReaderWriterLoop", func(b *testing.B) {
						stat, _ := r.file.Stat()
						b.SetBytes(stat.Size())
						for i := 0; i < b.N; i++ {
							if _, err := iterateReaderIntoWriter(r, &discardFile{}, writerOpts, []byte("_123"), []byte("_456")); err != nil {
								b.Fatal(err)
							}
						}
					})
				})
			}
		})
	}
}
