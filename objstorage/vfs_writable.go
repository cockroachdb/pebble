// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorage

import (
	"bufio"

	"github.com/cockroachdb/pebble/vfs"
)

type fileBufferedWritable struct {
	file vfs.File
	bw   *bufio.Writer
}

var _ Writable = (*fileBufferedWritable)(nil)

func newFileBufferedWritable(file vfs.File) *fileBufferedWritable {
	return &fileBufferedWritable{
		file: file,
		bw:   bufio.NewWriter(file),
	}
}

// Write is part of the objstorage.Writable interface.
func (w *fileBufferedWritable) Write(p []byte) (n int, err error) {
	return w.bw.Write(p)
}

// Sync is part of the objstorage.Writable interface.
func (w *fileBufferedWritable) Sync() error {
	if err := w.bw.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

// Close is part of the objstorage.Writable interface.
func (w *fileBufferedWritable) Close() error {
	return w.file.Close()
}
