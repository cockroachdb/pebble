// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// +build darwin dragonfly freebsd linux netbsd openbsd

package vfs

import (
	"io"
	"os"
	"syscall"
)

const writevSupported = true

// writevFile repeatedly issues writev calls to the provided file until the byte
// buffers have been written in their entirety, or until an error is reached.
//
// Adapted from src/internal/poll/writev.go.
func writevFile(f *os.File, bufs [][]byte) (n int, err error) {
	fd := int(f.Fd())

	// We need to convert [][]byte to []syscall.Iovec before each syscall. To
	// avoid heap allocations in the common case, we place a small array of
	// syscall.Iovec objects on the stack. We tune the size to be just large
	// enough to fit the maximum number of discontinuous buffers written by
	// LogWriter.
	var iovecArr [17]syscall.Iovec
	iovecs := iovecArr[:]
	for len(bufs) > 0 {
		// Prepare the []syscall.Iovec.
		iovecs = iovecs[:0]
		for _, chunk := range bufs {
			if len(chunk) == 0 {
				continue
			}
			iovecs = append(iovecs, syscall.Iovec{
				Base: &chunk[0],
				Len:  uint64(len(chunk)),
			})
		}
		if len(iovecs) == 0 {
			break
		}

		// Issue the WRITEV syscall.
		var wrote uintptr
		wrote, err = writev(fd, iovecs)
		if wrote == ^uintptr(0) {
			wrote = 0
		}
		n += int(wrote)
		bufs = consume(bufs, int(wrote))
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			break
		}
		if n == 0 {
			err = io.ErrUnexpectedEOF
			break
		}
	}
	return n, err
}

// consume removes data from a slice of byte slices and returns the result.
func consume(bufs [][]byte, n int) [][]byte {
	for len(bufs) > 0 {
		ln0 := len(bufs[0])
		if ln0 > n {
			bufs[0] = bufs[0][n:]
			break
		}
		n -= ln0
		bufs = bufs[1:]
	}
	return bufs
}
