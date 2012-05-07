// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package leveldb

import (
	"encoding/binary"
)

const batchHeaderLen = 12

// Batch is a sequence of Sets and/or Deletes that are applied atomically.
type Batch struct {
	// Data is the wire format of a batch's log entry:
	//   - 8 bytes for a sequence number of the first batch element,
	//   - 4 bytes for the count: the number of elements in the batch,
	//   - count elements, being:
	//     - one byte for the kind: delete (0) or set (1),
	//     - the varint-string user key,
	//     - the varint-string value (if kind == set).
	data []byte
}

func (b *Batch) Set(key, value []byte) {
	panic("unimplemented")
}

func (b *Batch) Delete(key []byte) {
	panic("unimplemented")
}

func (b *Batch) seqNum() uint64 {
	return binary.LittleEndian.Uint64(b.data[:8])
}

func (b *Batch) count() uint32 {
	return binary.LittleEndian.Uint32(b.data[8:12])
}

func (b *Batch) iter() batchIter {
	return b.data[12:]
}

type batchIter []byte

// next returns the next operation in this batch.
// The final return value is false if the batch is corrupt.
func (t *batchIter) next() (kind internalKeyKind, key []byte, value []byte, ok bool) {
	p := *t
	if len(p) == 0 {
		return 0, nil, nil, false
	}
	kind, *t = internalKeyKind(p[0]), p[1:]
	if kind > internalKeyKindMax {
		return 0, nil, nil, false
	}
	key, ok = t.nextStr()
	if !ok {
		return 0, nil, nil, false
	}
	if kind != internalKeyKindDelete {
		value, ok = t.nextStr()
		if !ok {
			return 0, nil, nil, false
		}
	}
	return kind, key, value, true
}

func (t *batchIter) nextStr() (s []byte, ok bool) {
	p := *t
	u, numBytes := binary.Uvarint(p)
	if numBytes <= 0 {
		return nil, false
	}
	p = p[numBytes:]
	if u > uint64(len(p)) {
		return nil, false
	}
	s, *t = p[:u], p[u:]
	return s, true
}
