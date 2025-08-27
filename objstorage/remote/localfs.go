// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package remote

import (
	"context"
	"io"
	"path"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// NewLocalFS returns a vfs-backed implementation of the remote.Storage
// interface (for testing). All objects will be stored at the directory
// dirname.
func NewLocalFS(dirname string, fs vfs.FS) Storage {
	store := &localFSStore{
		dirname: dirname,
		vfs:     fs,
	}
	return store
}

// localFSStore is a vfs-backed implementation of the remote.Storage
// interface (for testing).
type localFSStore struct {
	dirname string
	vfs     vfs.FS
}

var _ Storage = (*localFSStore)(nil)

// Close is part of the remote.Storage interface.
func (s *localFSStore) Close() error {
	*s = localFSStore{}
	return nil
}

// ReadObject is part of the remote.Storage interface.
func (s *localFSStore) ReadObject(
	ctx context.Context, objName string,
) (_ ObjectReader, objSize int64, _ error) {
	f, err := s.vfs.Open(path.Join(s.dirname, objName))
	if err != nil {
		return nil, 0, err
	}
	stat, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}

	return &localFSReader{f}, stat.Size(), nil
}

type localFSReader struct {
	file vfs.File
}

var _ ObjectReader = (*localFSReader)(nil)

// ReadAt is part of the shared.ObjectReader interface.
func (r *localFSReader) ReadAt(_ context.Context, p []byte, offset int64) error {
	n, err := r.file.ReadAt(p, offset)
	// https://pkg.go.dev/io#ReaderAt
	if err == io.EOF && n == len(p) {
		return nil
	}
	return err
}

// Close is part of the shared.ObjectReader interface.
func (r *localFSReader) Close() error {
	r.file.Close()
	r.file = nil
	return nil
}

func (f *localFSStore) sync() error {
	file, err := f.vfs.OpenDir(f.dirname)
	if err != nil {
		return err
	}
	return errors.CombineErrors(file.Sync(), file.Close())
}

type objWriter struct {
	vfs.File
	store *localFSStore
}

func (w *objWriter) Close() error {
	if w.File == nil {
		return nil
	}
	err := w.File.Sync()
	err = errors.CombineErrors(err, w.File.Close())
	err = errors.CombineErrors(err, w.store.sync())
	*w = objWriter{}
	return err
}

// CreateObject is part of the remote.Storage interface.
func (s *localFSStore) CreateObject(objName string) (io.WriteCloser, error) {
	file, err := s.vfs.Create(path.Join(s.dirname, objName), vfs.WriteCategoryUnspecified)
	if err != nil {
		return nil, err
	}
	return &objWriter{
		File:  file,
		store: s,
	}, nil
}

// List is part of the remote.Storage interface.
func (s *localFSStore) List(prefix, delimiter string) ([]string, error) {
	if delimiter != "" {
		panic("delimiter unimplemented")
	}
	files, err := s.vfs.List(s.dirname)
	if err != nil {
		return nil, err
	}
	res := make([]string, 0, len(files))
	for _, name := range files {
		if strings.HasPrefix(name, prefix) {
			res = append(res, name)
		}
	}
	return res, nil
}

// Delete is part of the remote.Storage interface.
func (s *localFSStore) Delete(objName string) error {
	if err := s.vfs.Remove(path.Join(s.dirname, objName)); err != nil {
		return err
	}
	return s.sync()
}

// Size is part of the remote.Storage interface.
func (s *localFSStore) Size(objName string) (int64, error) {
	f, err := s.vfs.Open(path.Join(s.dirname, objName))
	if err != nil {
		return 0, err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

// IsNotExistError is part of the remote.Storage interface.
func (s *localFSStore) IsNotExistError(err error) bool {
	return oserror.IsNotExist(err)
}
