// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorage

import (
	"bytes"
	"context"

	"github.com/pkg/errors"
)

// MemObj is an in-memory implementation of the Writable and Readable that holds
// all data in memory.
//
// A zero MemObj can be populated with data through its Writable methods, and
// then can be repeatedly used as a Readable.
type MemObj struct {
	buf bytes.Buffer
}

var _ Writable = (*MemObj)(nil)
var _ Readable = (*MemObj)(nil)

// Finish is part of the Writable interface.
func (f *MemObj) Finish() error { return nil }

// Abort is part of the Writable interface.
func (f *MemObj) Abort() { f.buf.Reset() }

// Write is part of the Writable interface.
func (f *MemObj) Write(p []byte) error {
	_, err := f.buf.Write(p)
	return err
}

// Data returns the in-memory buffer behind this MemObj.
func (f *MemObj) Data() []byte {
	return f.buf.Bytes()
}

// ReadAt is part of the Readable interface.
func (f *MemObj) ReadAt(ctx context.Context, p []byte, off int64) error {
	if f.Size() < off+int64(len(p)) {
		return errors.Errorf("read past the end of object")
	}
	copy(p, f.Data()[off:off+int64(len(p))])
	return nil
}

// Close is part of the Readable interface.
func (f *MemObj) Close() error { return nil }

// Size is part of the Readable interface.
func (f *MemObj) Size() int64 {
	return int64(f.buf.Len())
}

// NewReadHandle is part of the Readable interface.
func (f *MemObj) NewReadHandle(readBeforeSize ReadBeforeSize) ReadHandle {
	return &NoopReadHandle{readable: f}
}
