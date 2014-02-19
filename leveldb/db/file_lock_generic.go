// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build !linux,!amd64
// +build !darwin,!amd64
// +build !openbsd,!amd64
// +build !freebsd
// +build !windows

package db

import (
	"fmt"
	"io"
	"runtime"
)

func (defFS) Lock(name string) (io.Closer, error) {
	return nil, fmt.Errorf("leveldb/db: file locking is not implemented on %s/%s", runtime.GOOS, runtime.GOARCH)
}
