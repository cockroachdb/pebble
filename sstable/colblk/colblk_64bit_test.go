// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build arm64 || amd64

package colblk

import "github.com/cockroachdb/pebble/sstable/block"

// Assert that block.MetadataSize is not larger than it needs to be on a 64-bit
// platform.
const _ uint = uint(dataBlockDecoderSize) + KeySeekerMetadataSize - block.MetadataSize
