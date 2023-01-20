// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfsbackend

import (
	"bufio"

	"github.com/cockroachdb/pebble/sststorage"
	"github.com/cockroachdb/pebble/vfs"
)

type bufferedWritable struct {
	file vfs.File
	bw   *bufio.Writer
}

var _ sststorage.Writable = (*bufferedWritable)(nil)

func newBufferedWritable(file vfs.File) *bufferedWritable {
	return &bufferedWritable{
		file: file,
		bw:   bufio.NewWriter(file),
	}
}

// Write is part of the sststorage.Writable interface.
func (w *bufferedWritable) Write(p []byte) (n int, err error) {
	return w.bw.Write(p)
}

// Sync is part of the sststorage.Writable interface.
func (w *bufferedWritable) Sync() error {
	if err := w.bw.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

// Close is part of the sststorage.Writable interface.
func (w *bufferedWritable) Close() error {
	return w.file.Close()
}
