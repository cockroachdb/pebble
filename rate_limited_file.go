// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"github.com/petermattis/pebble/storage"
)

type rateLimitedFile struct {
	storage.File
	controller *controller
}

func newRateLimitedFile(f storage.File, c *controller) *rateLimitedFile {
	return &rateLimitedFile{
		File:       f,
		controller: c,
	}
}

func (f *rateLimitedFile) Write(b []byte) (int, error) {
	f.controller.WaitN(len(b))
	return f.File.Write(b)
}
