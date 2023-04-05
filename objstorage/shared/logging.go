// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package shared

import (
	"fmt"
	"io"
	"sort"
)

// WithLogging wraps the given Storage implementation and emits logs for various
// operations.
func WithLogging(wrapped Storage, logf func(fmt string, args ...interface{})) Storage {
	return &loggingStore{
		logf:    logf,
		wrapped: wrapped,
	}
}

// loggingStore wraps a shared.Storage implementation and emits logs of the
// operations.
type loggingStore struct {
	logf    func(fmt string, args ...interface{})
	wrapped Storage
}

var _ Storage = (*loggingStore)(nil)

func (l *loggingStore) Close() error {
	l.logf("close")
	return l.wrapped.Close()
}

func (l *loggingStore) ReadObjectAt(
	basename string, offset int64,
) (_ io.ReadCloser, totalSize int64, _ error) {
	r, totalSize, err := l.wrapped.ReadObjectAt(basename, offset)
	l.logf("read object %q at %d: %s", basename, offset, errOrPrintf(err, "%d bytes", totalSize))
	if err != nil {
		return nil, 0, err
	}
	return &loggingReader{
		l:          l,
		name:       basename,
		ReadCloser: r,
	}, totalSize, nil
}

type loggingReader struct {
	l    *loggingStore
	name string
	io.ReadCloser
}

func (l *loggingReader) Close() error {
	l.l.logf("close reader for %q", l.name)
	return l.ReadCloser.Close()
}

func (l *loggingStore) CreateObject(basename string) (io.WriteCloser, error) {
	l.logf("create object %q", basename)
	writer, err := l.wrapped.CreateObject(basename)
	if err != nil {
		return nil, err
	}
	return &loggingWriter{
		l:           l,
		name:        basename,
		WriteCloser: writer,
	}, nil
}

type loggingWriter struct {
	l            *loggingStore
	name         string
	bytesWritten int64
	io.WriteCloser
}

func (l *loggingWriter) Write(p []byte) (n int, err error) {
	n, err = l.WriteCloser.Write(p)
	l.bytesWritten += int64(n)
	return n, err
}

func (l *loggingWriter) Close() error {
	l.l.logf("close writer for %q after %d bytes", l.name, l.bytesWritten)
	return l.WriteCloser.Close()
}

func (l *loggingStore) List(prefix, delimiter string) ([]string, error) {
	l.logf("list (prefix=%q, delimiter=%q)", prefix, delimiter)
	res, err := l.wrapped.List(prefix, delimiter)
	if err != nil {
		return nil, err
	}
	sorted := append([]string(nil), res...)
	sort.Strings(sorted)
	for _, s := range sorted {
		l.logf(" - %s", s)
	}
	return res, nil
}

func (l *loggingStore) Delete(basename string) error {
	l.logf("delete object %q", basename)
	return l.wrapped.Delete(basename)
}

func (l *loggingStore) Size(basename string) (int64, error) {
	size, err := l.wrapped.Size(basename)
	l.logf("size of object %q: %s", basename, errOrPrintf(err, "%d", size))
	return size, err
}

func errOrPrintf(err error, format string, args ...interface{}) string {
	if err != nil {
		return fmt.Sprintf("error: %v", err)
	}
	return fmt.Sprintf(format, args...)
}

func (l *loggingStore) IsNotExistError(err error) bool {
	return l.wrapped.IsNotExistError(err)
}
