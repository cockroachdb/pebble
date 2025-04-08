// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package rowblk defines facilities for row-oriented sstable blocks.
package rowblk

import (
	"encoding/binary"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/sstable/block"
)

const (
	// MaximumRestartOffset indicates the maximum offset that we can encode
	// within a restart point of a row-oriented block. The last bit is reserved
	// for the setHasSameKeyPrefixSinceLastRestart flag within a restart point.
	// If a block exceeds this size and we attempt to add another KV pair, the
	// restart points table will be unable to express the position of the pair,
	// resulting in undefined behavior and arbitrary corruption.
	MaximumRestartOffset = 1 << 31
	// EmptySize holds the size of an empty block. Every block ends in a uint32
	// trailer encoding the number of restart points within the block.
	EmptySize = 4
)

const (
	// TrailerObsoleteBit is a bit within the internal key trailer that's used
	// by the row-oriented block format to signify when a key is obsolete. It's
	// internal to the row-oriented block format, set when writing a block and
	// unset by blockIter, so no code outside block writing/reading code ever
	// sees it.
	//
	// TODO(jackson): Unexport once the blockIter is also in this package.
	TrailerObsoleteBit = base.InternalKeyTrailer(base.InternalKeyKindSSTableInternalObsoleteBit)
	// TrailerObsoleteMask defines a mask for the obsolete bit in the internal
	// key trailer.
	TrailerObsoleteMask = (base.InternalKeyTrailer(base.SeqNumMax) << 8) | base.InternalKeyTrailer(base.InternalKeyKindSSTableInternalObsoleteMask)
)

// ErrBlockTooBig is surfaced when a block exceeds the maximum size.
var ErrBlockTooBig = errors.New("rowblk: block size exceeds maximum size")

// Writer buffers and serializes key/value pairs into a row-oriented block.
type Writer struct {
	// RestartInterval configures the interval at which the writer will write a
	// full key without prefix compression, and encode a corresponding restart
	// point.
	RestartInterval int
	nEntries        int
	nextRestart     int
	buf             []byte
	// For datablocks in TableFormatPebblev3, we steal the most significant bit
	// in restarts for encoding setHasSameKeyPrefixSinceLastRestart. This leaves
	// us with 31 bits, which is more than enough (no one needs > 2GB blocks).
	// Typically, restarts occur every 16 keys, and by storing this bit with the
	// restart, we can optimize for the case where a user wants to skip to the
	// next prefix which happens to be in the same data block, but is > 16 keys
	// away. We have seen production situations with 100+ versions per MVCC key
	// (which share the same prefix). Additionally, for such writers, the prefix
	// compression of the key, that shares the key with the preceding key, is
	// limited to the prefix part of the preceding key -- this ensures that when
	// doing NPrefix (see blockIter) we don't need to assemble the full key
	// for each step since by limiting the length of the shared key we are
	// ensuring that any of the keys with the same prefix can be used to
	// assemble the full key when the prefix does change.
	restarts []uint32
	// Do not read curKey directly from outside blockWriter since it can have
	// the InternalKeyKindSSTableInternalObsoleteBit set. Use getCurKey() or
	// getCurUserKey() instead.
	curKey []byte
	// curValue excludes the optional prefix provided to
	// storeWithOptionalValuePrefix.
	curValue []byte
	prevKey  []byte
	tmp      [4]byte
	// We don't know the state of the sets that were at the end of the previous
	// block, so this is initially 0. It may be true for the second and later
	// restarts in a block. Not having inter-block information is fine since we
	// will optimize by stepping through restarts only within the same block.
	// Note that the first restart is the first key in the block.
	setHasSameKeyPrefixSinceLastRestart bool
}

// Reset resets the block writer to empty, preserving buffers for reuse.
func (w *Writer) Reset() {
	*w = Writer{
		buf:      w.buf[:0],
		restarts: w.restarts[:0],
		curKey:   w.curKey[:0],
		curValue: w.curValue[:0],
		prevKey:  w.prevKey[:0],
	}
}

const setHasSameKeyPrefixRestartMask uint32 = 1 << 31

// EntryCount returns the count of entries written to the writer.
func (w *Writer) EntryCount() int {
	return w.nEntries
}

// CurKey returns the most recently written key.
func (w *Writer) CurKey() base.InternalKey {
	k := base.DecodeInternalKey(w.curKey)
	k.Trailer = k.Trailer & TrailerObsoleteMask
	return k
}

// CurValue returns the most recently written value.
func (w *Writer) CurValue() []byte {
	return w.curValue
}

// CurUserKey returns the most recently written user key.
func (w *Writer) CurUserKey() []byte {
	n := len(w.curKey) - base.InternalTrailerLen
	if n < 0 {
		panic(errors.AssertionFailedf("corrupt key in blockWriter buffer"))
	}
	return w.curKey[:n:n]
}

// If !addValuePrefix, the valuePrefix is ignored.
func (w *Writer) storeWithOptionalValuePrefix(
	keySize int,
	value []byte,
	maxSharedKeyLen int,
	addValuePrefix bool,
	valuePrefix block.ValuePrefix,
	setHasSameKeyPrefix bool,
) error {
	// Check that the block does not already exceed MaximumRestartOffset. If it
	// does and we append the additional key-value pair, the new key-value pair's
	// offset in the block will be inexpressible as a restart point.
	if len(w.buf) >= MaximumRestartOffset {
		return errors.WithDetailf(ErrBlockTooBig, "block is %d bytes long", len(w.buf))
	}

	shared := 0
	if !setHasSameKeyPrefix {
		w.setHasSameKeyPrefixSinceLastRestart = false
	}
	if w.nEntries == w.nextRestart {
		w.nextRestart = w.nEntries + w.RestartInterval
		restart := uint32(len(w.buf))
		if w.setHasSameKeyPrefixSinceLastRestart {
			restart = restart | setHasSameKeyPrefixRestartMask
		}
		w.setHasSameKeyPrefixSinceLastRestart = true
		w.restarts = append(w.restarts, restart)
	} else {
		// TODO(peter): Manually inlined version of base.SharedPrefixLen(). This
		// is 3% faster on BenchmarkWriter on go1.16. Remove if future versions
		// show this to not be a performance win. For now, functions that use of
		// unsafe cannot be inlined.
		n := maxSharedKeyLen
		if n > len(w.prevKey) {
			n = len(w.prevKey)
		}
		asUint64 := func(b []byte, i int) uint64 {
			return binary.LittleEndian.Uint64(b[i:])
		}
		for shared < n-7 && asUint64(w.curKey, shared) == asUint64(w.prevKey, shared) {
			shared += 8
		}
		for shared < n && w.curKey[shared] == w.prevKey[shared] {
			shared++
		}
	}

	lenValuePlusOptionalPrefix := len(value)
	if addValuePrefix {
		lenValuePlusOptionalPrefix++
	}
	needed := 3*binary.MaxVarintLen32 + len(w.curKey[shared:]) + lenValuePlusOptionalPrefix
	n := len(w.buf)
	if cap(w.buf) < n+needed {
		newCap := 2 * cap(w.buf)
		if newCap == 0 {
			newCap = 1024
		}
		for newCap < n+needed {
			newCap *= 2
		}
		newBuf := make([]byte, n, newCap)
		copy(newBuf, w.buf)
		w.buf = newBuf
	}
	w.buf = w.buf[:n+needed]

	// TODO(peter): Manually inlined versions of binary.PutUvarint(). This is 15%
	// faster on BenchmarkWriter on go1.13. Remove if go1.14 or future versions
	// show this to not be a performance win.
	{
		x := uint32(shared)
		for x >= 0x80 {
			w.buf[n] = byte(x) | 0x80
			x >>= 7
			n++
		}
		w.buf[n] = byte(x)
		n++
	}

	{
		x := uint32(keySize - shared)
		for x >= 0x80 {
			w.buf[n] = byte(x) | 0x80
			x >>= 7
			n++
		}
		w.buf[n] = byte(x)
		n++
	}

	{
		x := uint32(lenValuePlusOptionalPrefix)
		for x >= 0x80 {
			w.buf[n] = byte(x) | 0x80
			x >>= 7
			n++
		}
		w.buf[n] = byte(x)
		n++
	}

	n += copy(w.buf[n:], w.curKey[shared:])
	if addValuePrefix {
		w.buf[n : n+1][0] = byte(valuePrefix)
		n++
	}
	n += copy(w.buf[n:], value)
	w.buf = w.buf[:n]

	w.curValue = w.buf[n-len(value):]

	w.nEntries++
	return nil
}

// Add adds a key value pair to the block without a value prefix.
func (w *Writer) Add(key base.InternalKey, value []byte) error {
	return w.AddWithOptionalValuePrefix(
		key, false, value, len(key.UserKey), false, 0, false)
}

// AddWithOptionalValuePrefix adds a key value pair to the block, optionally
// including a value prefix.
//
// Callers that always set addValuePrefix to false should use add() instead.
//
// isObsolete indicates whether this key-value pair is obsolete in this
// sstable (only applicable when writing data blocks) -- see the comment in
// table.go and the longer one in format.go. addValuePrefix adds a 1 byte
// prefix to the value, specified in valuePrefix -- this is used for data
// blocks in TableFormatPebblev3 onwards for SETs (see the comment in
// format.go, with more details in value_block.go). setHasSameKeyPrefix is
// also used in TableFormatPebblev3 onwards for SETs.
func (w *Writer) AddWithOptionalValuePrefix(
	key base.InternalKey,
	isObsolete bool,
	value []byte,
	maxSharedKeyLen int,
	addValuePrefix bool,
	valuePrefix block.ValuePrefix,
	setHasSameKeyPrefix bool,
) error {
	w.curKey, w.prevKey = w.prevKey, w.curKey

	size := key.Size()
	if cap(w.curKey) < size {
		w.curKey = make([]byte, 0, size*2)
	}
	w.curKey = w.curKey[:size]
	if isObsolete {
		key.Trailer = key.Trailer | TrailerObsoleteBit
	}
	key.Encode(w.curKey)

	return w.storeWithOptionalValuePrefix(
		size, value, maxSharedKeyLen, addValuePrefix, valuePrefix, setHasSameKeyPrefix)
}

// Finish finalizes the block, serializes it and returns the serialized data.
func (w *Writer) Finish() []byte {
	// Write the restart points to the buffer.
	if w.nEntries == 0 {
		// Every block must have at least one restart point.
		if cap(w.restarts) > 0 {
			w.restarts = w.restarts[:1]
			w.restarts[0] = 0
		} else {
			w.restarts = append(w.restarts, 0)
		}
	}
	tmp4 := w.tmp[:4]
	for _, x := range w.restarts {
		binary.LittleEndian.PutUint32(tmp4, x)
		w.buf = append(w.buf, tmp4...)
	}
	binary.LittleEndian.PutUint32(tmp4, uint32(len(w.restarts)))
	w.buf = append(w.buf, tmp4...)
	result := w.buf

	// Reset the block state.
	w.nEntries = 0
	w.nextRestart = 0
	w.buf = w.buf[:0]
	w.restarts = w.restarts[:0]
	return result
}

// EstimatedSize returns the estimated size of the block in bytes.
func (w *Writer) EstimatedSize() int {
	return len(w.buf) + 4*len(w.restarts) + EmptySize
}

// AddRaw adds a key value pair to the block.
func (w *Writer) AddRaw(key, value []byte) error {
	w.curKey, w.prevKey = w.prevKey, w.curKey

	size := len(key)
	if cap(w.curKey) < size {
		w.curKey = make([]byte, 0, size*2)
	}
	w.curKey = w.curKey[:size]
	copy(w.curKey, key)
	return w.storeWithOptionalValuePrefix(
		size, value, len(key), false, 0, false)
}

// AddRawString is AddRaw but with a string key.
func (w *Writer) AddRawString(key string, value []byte) error {
	return w.AddRaw(unsafe.Slice(unsafe.StringData(key), len(key)), value)
}
