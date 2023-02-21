// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorage

import (
	"io"

	"github.com/cockroachdb/errors"
)

// sharedWritable is a very simple implementation of Writable on top of the
// WriteCloser returned by shared.Storage.CreateObject.
type sharedWritable struct {
	storageWriter io.WriteCloser
}

var _ Writable = (*sharedWritable)(nil)

func (w *sharedWritable) Write(p []byte) (n int, err error) {
	if w.storageWriter == nil {
		return 0, errors.AssertionFailedf("Write after Sync or Close")
	}
	return w.storageWriter.Write(p)
}

func (w *sharedWritable) Close() error {
	if w.storageWriter == nil {
		return nil
	}
	err := w.storageWriter.Close()
	w.storageWriter = nil
	return err
}

func (w *sharedWritable) Sync() error {
	if w.storageWriter == nil {
		return errors.AssertionFailedf("Sync after Sync or Close")
	}
	return w.Close()
}
