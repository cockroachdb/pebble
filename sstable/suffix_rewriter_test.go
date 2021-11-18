package sstable

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/humanize"
	"github.com/cockroachdb/pebble/vfs"
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

func BenchmarkRewriter(b *testing.B) {
	writerOpts := WriterOptions{
		FilterPolicy: bloom.FilterPolicy(10),
		Comparer:     test4bSuffixComparer,
	}

	f := &memFile{}
	w := NewWriter(f, writerOpts)
	key := make([]byte, 28)
	copy(key[24:], "_123")
	for i := 0; i < 1e6; i++ {
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
	r, err := NewReader(vfs.NewMemFile(f.Bytes()), ReaderOptions{
		Comparer: test4bSuffixComparer,
		Filters:  map[string]base.FilterPolicy{writerOpts.FilterPolicy.Name(): writerOpts.FilterPolicy},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Logf("rewriting a %s SSTable", humanize.IEC.Int64(int64(len(f.Bytes()))))

	b.ResetTimer()
	b.Run("RewriteKeySuffixes,concurrency=1", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			fNew := &memFile{}
			if _, err := RewriteKeySuffixes(r, fNew, writerOpts, []byte("_123"), []byte("_456"), 1); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("RewriteKeySuffixes,concurrency=8", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			fNew := &memFile{}
			if _, err := RewriteKeySuffixes(r, fNew, writerOpts, []byte("_123"), []byte("_456"), 8); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("read-and-write-new-sst", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			fNew := &memFile{}
			if err := readWriteRewriteKeySuffixes(r, fNew, writerOpts, []byte("_123"), []byte("_456")); err != nil {
				b.Fatal(err)
			}
		}
	})
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
	for k != nil {
		w.Set(k.UserKey, v)
		k, v = i.Next()
	}
	if err := w.Close(); err != nil {
		return err
	}
	return nil
}
