// Copyright 2014 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package db

import (
	"io"
	"os"
	"syscall"
	"unsafe"
)

// lockCloser hides all of an os.File's methods, except for Close.
type lockCloser struct {
	f *os.File
}

func (l lockCloser) Close() error {
	return l.f.Close()
}

func (defFS) Lock(name string) (io.Closer, error) {
	f, err := os.Create(name)
	if err != nil {
		return nil, err
	}

	// This type matches C's "struct flock" defined in /usr/include/fcntl.h.
	// TODO: move this into the standard syscall package.
	k := struct {
		Start  int64 /* off_t starting offset */
		Len    int64 /* off_t len = 0 means until end of file */
		Pid    int32 /* pid_t lock owner */
		Type   int16 /* short lock type: read/write, etc. */
		Whence int16 /* short type of l_start */
		Sysid  int32 /* int   remote system id or zero for local */
	}{
		Start:  0,
		Len:    0, // 0 means to lock the entire file.
		Pid:    int32(os.Getpid()),
		Type:   syscall.F_WRLCK,
		Whence: int16(os.SEEK_SET),
		Sysid:  0,
	}

	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), uintptr(syscall.F_SETLK), uintptr(unsafe.Pointer(&k)))
	if errno != 0 {
		f.Close()
		return nil, errno
	}
	return lockCloser{f}, nil
}
