// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// +build windows

package vfs

import "golang.org/x/sys/windows"

func (defaultFS) GetFreeSpace(path string) (uint64, error) {
	var freeSpace uint64
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	err = windows.GetDiskFreeSpaceEx(p, &freeSpace, nil, nil)
	return freeSpace, err
}
