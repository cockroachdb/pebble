package sstable

import (
	"bytes"
	"os"
	"time"

	"github.com/cockroachdb/errors"
)

// RewriteKeySuffixes copies the sstable in the passed reader to out, but with
// each key updated to have the byte suffix `from` replaced with `to`. It is
// required that the SST consist of only SETs, and that every key have `from` as
// its suffix, or an error will be returned.
func RewriteKeySuffixes(
	r *Reader,
	out writeCloseSyncer, o WriterOptions,
	from, to []byte,
) (*WriterMetadata, error) {
	return iterateReaderIntoWriter(r, out, o, from, to)
}

func iterateReaderIntoWriter(
	r *Reader, out writeCloseSyncer, o WriterOptions, from, to []byte,
) (*WriterMetadata, error) {
	w := NewWriter(out, o)
	i, err := r.NewIter(nil, nil)
	if err != nil {
		return nil, err
	}
	k, v := i.First()
	var scratch []byte
	for k != nil {
		if k.Kind() != InternalKeyKindSet {
			return nil, errors.New("invalid key type")
		}
		if !bytes.HasSuffix(k.UserKey, from) {
			return nil, errors.New("mismatched key suffix")
		}
		scratch = append(scratch[:0], k.UserKey[:len(k.UserKey)-len(from)]...)
		scratch = append(scratch, to...)
		w.Set(scratch, v)
		k, v = i.Next()
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return &w.meta, nil
}

// NewMemReader opens a reader over the SST stored in the passed []byte.
func NewMemReader(sst []byte, o ReaderOptions) (*Reader, error) {
	return NewReader(memReader{sst, bytes.NewReader(sst), sizeOnlyStat(int64(len(sst)))}, o)
}

// memReader is a thin wrapper around a []byte such that it can be passed to an
// sstable.Reader. It supports concurrent use, and does so without locking in
// contrast to the heavier read/write vfs.MemFile.
type memReader struct {
	b []byte
	r *bytes.Reader
	s sizeOnlyStat
}

var _ ReadableFile = memReader{}

// ReadAt implements io.ReaderAt.
func (m memReader) ReadAt(p []byte, off int64) (n int, err error) { return m.r.ReadAt(p, off) }

// Close implements io.Closer.
func (memReader) Close() error { return nil }

// Stat implements ReadableFile.
func (m memReader) Stat() (os.FileInfo, error) { return m.s, nil }

type sizeOnlyStat int64

func (s sizeOnlyStat) Size() int64      { return int64(s) }
func (sizeOnlyStat) IsDir() bool        { panic(errors.AssertionFailedf("unimplemented")) }
func (sizeOnlyStat) ModTime() time.Time { panic(errors.AssertionFailedf("unimplemented")) }
func (sizeOnlyStat) Mode() os.FileMode  { panic(errors.AssertionFailedf("unimplemented")) }
func (sizeOnlyStat) Name() string       { panic(errors.AssertionFailedf("unimplemented")) }
func (sizeOnlyStat) Sys() interface{}   { panic(errors.AssertionFailedf("unimplemented")) }
