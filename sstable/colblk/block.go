// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package colblk implements a columnar block format.
//
// TODO(jackson): Describe the format once we start merging the higher-level
// block format.
//
// # Alignment
//
// Block buffers are required to be word-aligned, during encoding and decoding.
// This ensures that if any individual column or piece of data requires
// word-alignment, the writer can align the offset into the block buffer
// appropriately to ensure that the data is word-aligned.
package colblk
