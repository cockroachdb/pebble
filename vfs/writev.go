// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

import (
	"io"
	"os"
)

// VectorWriter is a Writer with a WriteV method, which can efficiently
// write collections of discontinuous buffers in a single operation.
type VectorWriter interface {
	io.Writer

	// WriteV writes each of the byte slices to the underlying data stream, in
	// order. It returns the number of bytes written across all bufs (0 <= n <=
	// len(bufs)*len(bufs[..])) and any error encountered that caused the write
	// to stop early. WriteV must return a non-nil error if it returns n <
	// len(bufs)*len(bufs[..]). All retryable errors are dealt with in the
	// implementation of WriteV.
	//
	// As with the vfs.File.Write() method, the vfs.VectorWriter.WriteV() method
	// *is* allowed to modify the slices passed in, whether temporarily or
	// permanently. Callers of WriteV() need to take this into account.
	WriteV(bufs [][]byte) (n int, err error)
}

// WriteV writes each of the byte slices to the provided Writer, in
// order. It returns the number of bytes written across all bufs (0 <= n
// <= len(bufs)*len(bufs[..])) and any error encountered that caused the
// write to stop early. WriteV must return a non-nil error if it returns
// n < len(bufs)*len(bufs[..]).
func WriteV(w io.Writer, bufs [][]byte) (n int, err error) {
	if vw, ok := w.(VectorWriter); ok {
		return vw.WriteV(bufs)
	}
	if f, ok := w.(*os.File); ok && writevSupported {
		return writevFile(f, bufs)
	}
	for _, buf := range bufs {
		var m int
		m, err = w.Write(buf)
		n += m
		if err != nil {
			return n, err
		}
		if m != len(buf) {
			return n, io.ErrShortWrite
		}
	}
	return n, nil
}
