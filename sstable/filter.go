// Copyright 2011 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sstable

import (
	"encoding/binary"
	"errors"

	"github.com/petermattis/pebble/db"
)

type filterReader interface {
	mayContain(blockOffset uint64, key []byte) bool
}

type filterWriter interface {
	addKey(key []byte)
	finishBlock(blockOffset uint64) error
	finish() ([]byte, error)
	policyName() string
}

type blockFilterReader struct {
	data    []byte
	offsets []byte // len(offsets) must be a multiple of 4.
	policy  db.FilterPolicy
	shift   uint32
}

func (f *blockFilterReader) valid() bool {
	return f.data != nil
}

func (f *blockFilterReader) init(data []byte, policy db.FilterPolicy) (ok bool) {
	if len(data) < 5 {
		return false
	}
	lastOffset := binary.LittleEndian.Uint32(data[len(data)-5:])
	if uint64(lastOffset) > uint64(len(data)-5) {
		return false
	}
	data, offsets, shift := data[:lastOffset], data[lastOffset:len(data)-1], uint32(data[len(data)-1])
	if len(offsets)&3 != 0 {
		return false
	}
	f.data = data
	f.offsets = offsets
	f.policy = policy
	f.shift = shift
	return true
}

func (f *blockFilterReader) mayContain(blockOffset uint64, key []byte) bool {
	index := blockOffset >> f.shift
	if index >= uint64(len(f.offsets)/4-1) {
		return true
	}
	i := binary.LittleEndian.Uint32(f.offsets[4*index+0:])
	j := binary.LittleEndian.Uint32(f.offsets[4*index+4:])
	if i >= j || uint64(j) > uint64(len(f.data)) {
		return true
	}
	return f.policy.MayContain(db.BlockFilter, f.data[i:j], key)
}

// filterBaseLog being 11 means that we generate a new filter for every 2KiB of
// data.
//
// It's a little unfortunate that this is 11, whilst the default db.Options
// BlockSize is 1<<12 or 4KiB, so that in practice, every second filter is
// empty, but both values match the C++ code.
const filterBaseLog = 11

type blockFilterWriter struct {
	policy db.FilterPolicy
	writer db.FilterWriter
	// count is the count of the number of keys in the current block.
	count int
	// data and offsets are the per-block filters for the overall table.
	data    []byte
	offsets []uint32
}

func (f *blockFilterWriter) hasKeys() bool {
	return f.count != 0
}

func (f *blockFilterWriter) addKey(key []byte) {
	f.count++
	f.writer.AddKey(key)
}

func (f *blockFilterWriter) appendOffset() error {
	o := len(f.data)
	if uint64(o) > 1<<32-1 {
		return errors.New("pebble/table: filter data is too long")
	}
	f.offsets = append(f.offsets, uint32(o))
	return nil
}

func (f *blockFilterWriter) emit() error {
	if err := f.appendOffset(); err != nil {
		return err
	}
	if !f.hasKeys() {
		return nil
	}
	f.data = f.writer.Finish(f.data)
	f.count = 0
	return nil
}

func (f *blockFilterWriter) finishBlock(blockOffset uint64) error {
	for i := blockOffset >> filterBaseLog; i > uint64(len(f.offsets)); {
		if err := f.emit(); err != nil {
			return err
		}
	}
	return nil
}

func (f *blockFilterWriter) finish() ([]byte, error) {
	if f.hasKeys() {
		if err := f.emit(); err != nil {
			return nil, err
		}
	}
	if err := f.appendOffset(); err != nil {
		return nil, err
	}

	var b [4]byte
	for _, x := range f.offsets {
		binary.LittleEndian.PutUint32(b[:], x)
		f.data = append(f.data, b[0], b[1], b[2], b[3])
	}
	f.data = append(f.data, filterBaseLog)
	return f.data, nil
}

func (f *blockFilterWriter) policyName() string {
	return f.policy.Name()
}
