// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package batchrepr

import (
	"encoding/binary"

	"github.com/cockroachdb/pebble/v2/internal/base"
)

// SetSeqNum mutates the provided batch representation, storing the provided
// sequence number in its header. The provided byte slice must already be at
// least HeaderLen bytes long or else SetSeqNum will panic.
func SetSeqNum(repr []byte, seqNum base.SeqNum) {
	binary.LittleEndian.PutUint64(repr[:countOffset], uint64(seqNum))
}

// SetCount mutates the provided batch representation, storing the provided
// count in its header. The provided byte slice must already be at least
// HeaderLen bytes long or else SetCount will panic.
func SetCount(repr []byte, count uint32) {
	binary.LittleEndian.PutUint32(repr[countOffset:HeaderLen], count)
}
