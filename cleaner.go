// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import "github.com/cockroachdb/pebble/vfs"

// Cleaner aliases the vfs.Cleaner type.
type Cleaner = vfs.Cleaner

// DeleteCleaner aliases the vfs.DeleteCleaner type.
type DeleteCleaner = vfs.DeleteCleaner

// ArchiveCleaner aliases the vfs.ArchiveCleaner type.
type ArchiveCleaner = vfs.ArchiveCleaner
