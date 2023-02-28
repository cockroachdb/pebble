// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorage

import "io"

// sharedWritable is a very simple implementation of Writable on top of the
// WriteCloser returned by shared.Storage.CreateObject.
type sharedWritable struct {
	storageWriter io.WriteCloser
}

var _ Writable = (*sharedWritable)(nil)

// Write is part of the Writable interface.
func (w *sharedWritable) Write(p []byte) error {
	_, err := w.storageWriter.Write(p)
	return err
}

// Finish is part of the Writable interface.
func (w *sharedWritable) Finish() error {
	err := w.storageWriter.Close()
	w.storageWriter = nil
	return err
}

// Abort is part of the Writable interface.
func (w *sharedWritable) Abort() {
	_ = w.storageWriter.Close()
	w.storageWriter = nil
}
