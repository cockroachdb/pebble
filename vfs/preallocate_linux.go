// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// +build linux

package vfs

import (
	"syscall"
)

func preallocExtend(fd uintptr, offset, length int64) error {
	err := syscall.Fallocate(int(fd), 0 /* mode */, offset, length)
	if err != nil {
		errno, ok := err.(syscall.Errno)
		// not supported; fallback
		// fallocate EINTRs frequently in some environments; fallback
		if ok && (errno == syscall.ENOTSUP || errno == syscall.EINTR) {
			return syscall.Ftruncate(int(fd), offset+length)
		}
	}
	return err
}
