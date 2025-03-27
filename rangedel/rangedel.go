// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package rangedel provides functionality for working with range deletions.
package rangedel

import (
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/rangedel"
	"github.com/cockroachdb/pebble/sstable"
)

// Fragmenter exports the keyspan.Fragmenter type.
type Fragmenter = keyspan.Fragmenter

// Key exports the keyspan.Key type.
type Key = keyspan.Key

// Span exports the keyspan.Span type.
type Span = keyspan.Span

// Decode decodes an InternalKey representing a range deletion into a Span. If
// keysDst is provided, keys will be appended to keysDst to reduce allocations.
func Decode(ik sstable.InternalKey, val []byte, keysDst []Key) Span {
	return rangedel.Decode(ik, val, keysDst)
}
