// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package private

import (
	"github.com/cockroachdb/pebble/internal/manifest"
)

// FlushExternalTable is a hook for linking files into L0 without assigning a
// global sequence number, mimicking a flush. FlushExternalTable takes a
// *pebble.DB, the path and the metadata of a sstable to flush directly into
// L0.
//
// Calls to flush a sstable may fail if the file's sequence numbers are not
// greater than the current commit pipeline's sequence number. On success the
// commit pipeline's published sequence number will be moved to the file's
// highest sequence number.
//
// This function is wrapped in a safer, more ergonomic API in the
// internal/replay package. Clients should use the replay package rather than
// calling this private hook directly.
var FlushExternalTable func(interface{}, string, *manifest.FileMetadata) error
