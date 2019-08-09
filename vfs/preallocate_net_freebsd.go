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

// +build freebsd netbsd

package vfs

import "syscall"

// #include <fcntl.h>
import "C"

// Note: openbsd has a syscall in the kernel but no libc wrapper.
// If/when implementing specific support for openbsd, it will need a
// custom caller.

func preallocExtend(fd uintptr, offset, length int64) error {
	res := C.posix_fallocate(C.int(fd), C.off_t(offset), C.off_t(length))
	errno := syscall.Errno(res)
	if errno == syscall.ENOTSUP || errno == syscall.EINTR {
		// fallocate EINTRs frequently in some environments; fallback
		return syscall.Ftruncate(int(fd), offset+length)
	}
	return nil
}
