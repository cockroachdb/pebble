package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
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
	files := make([]*Reader, len(sizes))
	for size := range sizes {
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
		files[size] = r
	}

	b.ResetTimer()
	for sz := range sizes {
		r := files[sz]
		b.Run(fmt.Sprintf("keys=%d", sizes[sz]), func(b *testing.B) {
			for _, concurrency := range []int{1, 2, 4, 8, 16} {
				b.Run(fmt.Sprintf("RewriteKeySuffixes,concurrency=%d", concurrency), func(b *testing.B) {
					stat, _ := r.file.Stat()
					b.SetBytes(stat.Size())
					for i := 0; i < b.N; i++ {
						if _, err := RewriteKeySuffixes(r, discardFile{}, writerOpts, []byte("_123"), []byte("_456"), concurrency); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
			b.Run("ReaderWriterLoop", func(b *testing.B) {
				stat, _ := r.file.Stat()
				b.SetBytes(stat.Size())
				for i := 0; i < b.N; i++ {
					if err := readWriteRewriteKeySuffixes(r, discardFile{}, writerOpts, []byte("_123"), []byte("_456")); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func readWriteRewriteKeySuffixes(
	r *Reader, out writeCloseSyncer, o WriterOptions, from, to []byte,
) error {
	w := NewWriter(out, o)
	i, err := r.NewIter(nil, nil)
	if err != nil {
		return err
	}
	k, v := i.First()
	var scratch []byte
	for k != nil {
		if !bytes.HasSuffix(k.UserKey, from) {
			return errors.New("mismatched suffix")
		}
		scratch = append(scratch[:0], k.UserKey[:len(k.UserKey)-len(from)]...)
		scratch = append(scratch, to...)

		w.Set(scratch, v)
		k, v = i.Next()
	}
	if err := w.Close(); err != nil {
		return err
	}
	return nil
}

func NewMemReader(sst []byte, o ReaderOptions) (*Reader, error) {
	return NewReader(memReader{bytes.NewReader(sst), sizeOnlyStat(int64(len(sst)))}, o)
}

type memReader struct {
	r *bytes.Reader
	s sizeOnlyStat
}

var _ ReadableFile = memReader{}

func (m memReader) ReadAt(p []byte, off int64) (n int, err error) { return m.r.ReadAt(p, off) }
func (memReader) Close() error                                    { return nil }
func (m memReader) Stat() (os.FileInfo, error)                    { return m.s, nil }

type sizeOnlyStat int64

func (s sizeOnlyStat) Size() int64      { return int64(s) }
func (sizeOnlyStat) IsDir() bool        { panic(errors.AssertionFailedf("unimplemented")) }
func (sizeOnlyStat) ModTime() time.Time { panic(errors.AssertionFailedf("unimplemented")) }
func (sizeOnlyStat) Mode() os.FileMode  { panic(errors.AssertionFailedf("unimplemented")) }
func (sizeOnlyStat) Name() string       { panic(errors.AssertionFailedf("unimplemented")) }
func (sizeOnlyStat) Sys() interface{}   { panic(errors.AssertionFailedf("unimplemented")) }
