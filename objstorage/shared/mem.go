// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package shared

import (
	"bytes"
	"io"
	"os"
	"strings"
	"sync"
)

// NewInMem returns an in-memory implementation of the shared.Storage
// interface (for testing).
func NewInMem() Storage {
	store := &inMemStore{}
	store.mu.objects = make(map[string]*inMemObj)
	return store
}

// inMemStore is an in-memory implementation of the shared.Storage interface
// (for testing).
type inMemStore struct {
	mu struct {
		sync.Mutex
		objects map[string]*inMemObj
	}
}

var _ Storage = (*inMemStore)(nil)

type inMemObj struct {
	name string
	data []byte
}

func (s *inMemStore) Close() error {
	*s = inMemStore{}
	return nil
}

func (s *inMemStore) ReadObjectAt(basename string, offset int64) (io.ReadCloser, int64, error) {
	obj, err := s.getObj(basename)
	if err != nil {
		return nil, 0, err
	}
	remaining := int64(len(obj.data)) - offset
	if remaining < 0 {
		return nil, 0, io.EOF
	}
	return newInMemReader(obj.data[int(offset):]), remaining, nil
}

type inMemReader struct {
	bytes.Reader
}

func newInMemReader(b []byte) *inMemReader {
	r := &inMemReader{}
	r.Reset(b)
	return r
}

var _ io.ReadCloser = (*inMemReader)(nil)

func (r *inMemReader) Close() error {
	r.Reset(nil)
	return nil
}

func (s *inMemStore) CreateObject(basename string) (io.WriteCloser, error) {
	return &inMemWriter{
		store: s,
		name:  basename,
	}, nil
}

type inMemWriter struct {
	store *inMemStore
	name  string
	buf   bytes.Buffer
}

var _ io.WriteCloser = (*inMemWriter)(nil)

func (o *inMemWriter) Write(p []byte) (n int, err error) {
	if o.store == nil {
		panic("Write after Close")
	}
	return o.buf.Write(p)
}

func (o *inMemWriter) Close() error {
	if o.store != nil {
		o.store.addObj(&inMemObj{
			name: o.name,
			data: o.buf.Bytes(),
		})
		o.store = nil
	}
	return nil
}

func (s *inMemStore) List(prefix, delimiter string) ([]string, error) {
	if delimiter != "" {
		panic("delimiter unimplemented")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	res := make([]string, 0, len(s.mu.objects))
	for name := range s.mu.objects {
		if strings.HasPrefix(name, prefix) {
			res = append(res, name)
		}
	}
	return res, nil
}

func (s *inMemStore) Delete(basename string) error {
	s.rmObj(basename)
	return nil
}

// Size returns the length of the named object in bytesWritten.
func (s *inMemStore) Size(basename string) (int64, error) {
	obj, err := s.getObj(basename)
	if err != nil {
		return 0, err
	}
	return int64(len(obj.data)), nil
}

func (s *inMemStore) IsNotExistError(err error) bool {
	return err == os.ErrNotExist
}

func (s *inMemStore) getObj(name string) (*inMemObj, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	obj, ok := s.mu.objects[name]
	if !ok {
		return nil, os.ErrNotExist
	}
	return obj, nil
}

func (s *inMemStore) addObj(o *inMemObj) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.objects[o.name] = o
}

func (s *inMemStore) rmObj(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.mu.objects, name)
}
