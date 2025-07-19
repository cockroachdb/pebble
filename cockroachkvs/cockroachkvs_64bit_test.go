// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build arm64 || amd64

package cockroachkvs

import (
	"unsafe"

	"github.com/cockroachdb/pebble/sstable/colblk"
)

// Assert that KeySeekerMetadata isn't larger than it needs to be to fit a
// cockroachKeySeeker on a 64-bit platform.
var _ uint = uint(unsafe.Sizeof(cockroachKeySeeker{})) - colblk.KeySeekerMetadataSize
