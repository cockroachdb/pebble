// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pebble

import (
	"encoding/binary"
	"errors"
	"fmt"
	"unsafe"

	"github.com/petermattis/pebble/batchskl"
	"github.com/petermattis/pebble/db"
)

const (
	batchHeaderLen    = 12
	invalidBatchCount = 1<<32 - 1
)

// ErrNotIndexed means that a read operation on a batch failed because the
// batch is not indexed and thus doesn't support reads.
var ErrNotIndexed = errors.New("pebble: batch not indexed")

type batchStorage struct {
	// Data is the wire format of a batch's log entry:
	//   - 8 bytes for a sequence number of the first batch element,
	//     or zeroes if the batch has not yet been applied,
	//   - 4 bytes for the count: the number of elements in the batch,
	//     or "\xff\xff\xff\xff" if the batch is invalid,
	//   - count elements, being:
	//     - one byte for the kind
	//     - the varint-string user key,
	//     - the varint-string value (if kind != delete).
	// The sequence number and count are stored in little-endian order.
	data []byte
	cmp  db.Compare
}

// Get implements Storage.Get, as documented in the pebble/batchskl package.
func (s *batchStorage) Get(offset uint32) []byte {
	_, key, ok := batchDecodeStr(s.data[offset+1:])
	if !ok {
		panic("corrupted batch entry")
	}
	return key
}

// InlineKey implements Storage.InlineKey, as documented in the pebble/batchskl
// package.
func (s *batchStorage) InlineKey(key []byte) batchskl.InlineKey {
	// TODO(peter): This needs to be part of the db.Comparer package as the
	// specifics of the comparison routine affect how a fixed prefix can be
	// extracted.
	var v batchskl.InlineKey
	n := int(unsafe.Sizeof(batchskl.InlineKey(0)))
	if n > len(key) {
		n = len(key)
	}
	for _, b := range key[:n] {
		v <<= 8
		v |= batchskl.InlineKey(b)
	}
	return v
}

// Compare implements Storage.Compare, as documented in the pebble/batchskl
// package.
func (s *batchStorage) Compare(a []byte, b uint32) int {
	// The key "a" is always the search key or the newer key being inserted. If
	// it is equal to the existing key consider it smaller so that it sorts
	// first.
	c := s.cmp(a, s.Get(b))
	if c <= 0 {
		return -1
	}
	return 1
}

// Batch is a sequence of Sets and/or Deletes that are applied atomically.
type Batch struct {
	batchStorage

	// The parent writer to which the batch will be committed.
	parent db.Writer

	// An optional skiplist keyed by offset into data of the entry.
	index *batchskl.Skiplist
}

// Batch implements the db.Reader interface.
var _ db.Reader = (*Batch)(nil)

func newIndexedBatch(parent db.Writer, cmp db.Compare) *Batch {
	var b struct {
		batch Batch
		index batchskl.Skiplist
	}
	b.batch.cmp = cmp
	b.batch.parent = parent
	b.batch.index = &b.index
	b.batch.index.Reset(&b.batch.batchStorage, 0)
	return &b.batch
}

// Apply implements DB.Apply, as documented in the pebble/db package.
func (b *Batch) Apply(repr []byte) error {
	if len(repr) == 0 {
		return nil
	}
	if len(repr) < batchHeaderLen {
		return errors.New("pebble: invalid batch")
	}

	offset := len(b.data)
	if offset == 0 {
		b.init(offset)
		offset = batchHeaderLen
	}
	b.data = append(b.data, repr[batchHeaderLen:]...)

	count := binary.LittleEndian.Uint32(repr[8:12])
	b.setCount(b.count() + count)

	if b.index != nil {
		start := batchReader(b.data[offset:])
		for iter := batchReader(start); ; {
			if _, _, _, ok := iter.next(); !ok {
				break
			}
			offset := uintptr(unsafe.Pointer(&iter[0])) - uintptr(unsafe.Pointer(&start[0]))
			if err := b.index.Add(uint32(offset)); err != nil {
				panic(err)
			}
		}
	}
	return nil
}

// Get implements DB.Get, as documented in the pebble/db package.
func (b *Batch) Get(key []byte, o *db.ReadOptions) (value []byte, err error) {
	if b.index == nil {
		return nil, ErrNotIndexed
	}
	// Loop over the entries with keys >= the target key. The indexing of the
	// entries returns equal keys in reverse order of insertion. That is, the
	// last key added will be seen first.
	iter := b.index.NewIter()
	iter.SeekGE(key)
	for ; iter.Valid(); iter.Next() {
		_, ekey, value, ok := b.decode(iter.KeyOffset())
		if !ok {
			return nil, fmt.Errorf("corrupted batch")
		}
		if b.cmp(key, ekey) > 0 {
			break
		}
		// Invariant: b.cmp(key, ekey) == 0.
		return value, nil
	}
	return nil, db.ErrNotFound
}

// Set adds an action to the batch that sets the key to map to the value.
func (b *Batch) Set(key, value []byte) {
	if len(b.data) == 0 {
		b.init(len(key) + len(value) + 2*binary.MaxVarintLen64 + batchHeaderLen)
	}
	if b.increment() {
		offset := uint32(len(b.data))
		b.data = append(b.data, byte(db.InternalKeyKindSet))
		b.appendStr(key)
		b.appendStr(value)
		if b.index != nil {
			if err := b.index.Add(offset); err != nil {
				// We never add duplicate entries, so an error should never occur.
				panic(err)
			}
		}
	}
}

// Merge adds an action to the batch that merges the value at key with the new
// value. The details of the merge are dependent upon the configured merge
// operator.
func (b *Batch) Merge(key, value []byte) {
	if len(b.data) == 0 {
		b.init(len(key) + len(value) + 2*binary.MaxVarintLen64 + batchHeaderLen)
	}
	if b.increment() {
		offset := uint32(len(b.data))
		b.data = append(b.data, byte(db.InternalKeyKindMerge))
		b.appendStr(key)
		b.appendStr(value)
		if b.index != nil {
			if err := b.index.Add(offset); err != nil {
				// We never add duplicate entries, so an error should never occur.
				panic(err)
			}
		}
	}
}

// Delete adds an action to the batch that deletes the entry for key.
func (b *Batch) Delete(key []byte) {
	if len(b.data) == 0 {
		b.init(len(key) + binary.MaxVarintLen64 + batchHeaderLen)
	}
	if b.increment() {
		offset := uint32(len(b.data))
		b.data = append(b.data, byte(db.InternalKeyKindDelete))
		b.appendStr(key)
		if b.index != nil {
			if err := b.index.Add(offset); err != nil {
				// We never add duplicate entries, so an error should never occur.
				panic(err)
			}
		}
	}
}

// DeleteRange implements DB.DeleteRange, as documented in the pebble/db
// package.
func (b *Batch) DeleteRange(start, end []byte) {
	if len(b.data) == 0 {
		b.init(len(start) + len(end) + 2*binary.MaxVarintLen64 + batchHeaderLen)
	}
	if b.increment() {
		offset := uint32(len(b.data))
		b.data = append(b.data, byte(db.InternalKeyKindRangeDelete))
		b.appendStr(start)
		b.appendStr(end)
		if b.index != nil {
			if err := b.index.Add(offset); err != nil {
				// We never add duplicate entries, so an error should never occur.
				panic(err)
			}
		}
	}
}

// NewIter implements DB.NewIter, as documented in the pebble/db package.
func (b *Batch) NewIter(o *db.ReadOptions) db.Iterator {
	if b.index == nil {
		return newErrorIter(ErrNotIndexed)
	}
	panic("pebble.Batch: NewIter unimplemented")
}

// Commit applies the batch to its parent writer.
func (b *Batch) Commit(o *db.WriteOptions) error {
	return b.parent.Apply(b.data, o)
}

// Close implements DB.Close, as documented in the pebble/db package.
func (b *Batch) Close() error {
	return nil
}

// Indexed returns true if the batch is indexed (i.e. supports read
// operations).
func (b *Batch) Indexed() bool {
	return b.index != nil
}

func (b *Batch) init(cap int) {
	n := 256
	for n < cap {
		n *= 2
	}
	b.data = make([]byte, batchHeaderLen, n)
}

// seqNumData returns the 8 byte little-endian sequence number. Zero means that
// the batch has not yet been applied.
func (b *Batch) seqNumData() []byte {
	return b.data[:8]
}

// countData returns the 4 byte little-endian count data. "\xff\xff\xff\xff"
// means that the batch is invalid.
func (b *Batch) countData() []byte {
	return b.data[8:12]
}

func (b *Batch) increment() (ok bool) {
	p := b.countData()
	for i := range p {
		p[i]++
		if p[i] != 0x00 {
			return true
		}
	}
	// The countData was "\xff\xff\xff\xff". Leave it as it was.
	p[0] = 0xff
	p[1] = 0xff
	p[2] = 0xff
	p[3] = 0xff
	return false
}

func (b *Batch) appendStr(s []byte) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(len(s)))
	b.data = append(b.data, buf[:n]...)
	b.data = append(b.data, s...)
}

func (b *Batch) setSeqNum(seqNum uint64) {
	binary.LittleEndian.PutUint64(b.seqNumData(), seqNum)
}

func (b *Batch) seqNum() uint64 {
	return binary.LittleEndian.Uint64(b.seqNumData())
}

func (b *Batch) setCount(v uint32) {
	binary.LittleEndian.PutUint32(b.countData(), v)
}

func (b *Batch) count() uint32 {
	return binary.LittleEndian.Uint32(b.countData())
}

func (b *Batch) iter() batchReader {
	return b.data[batchHeaderLen:]
}

func (b *Batch) decode(offset uint32) (kind db.InternalKeyKind, ukey []byte, value []byte, ok bool) {
	p := b.data[offset:]
	if len(p) == 0 {
		return 0, nil, nil, false
	}
	kind, p = db.InternalKeyKind(p[0]), p[1:]
	if kind > db.InternalKeyKindMax {
		return 0, nil, nil, false
	}
	p, ukey, ok = batchDecodeStr(p)
	if !ok {
		return 0, nil, nil, false
	}
	switch kind {
	case db.InternalKeyKindSet,
		db.InternalKeyKindMerge,
		db.InternalKeyKindRangeDelete:
		_, value, ok = batchDecodeStr(p)
		if !ok {
			return 0, nil, nil, false
		}
	}
	return kind, ukey, value, true
}

func batchDecodeStr(data []byte) (odata []byte, s []byte, ok bool) {
	v, n := binary.Uvarint(data)
	if n <= 0 {
		return nil, nil, false
	}
	data = data[n:]
	if v > uint64(len(data)) {
		return nil, nil, false
	}
	return data[v:], data[:v], true
}

type batchReader []byte

// next returns the next operation in this batch.
// The final return value is false if the batch is corrupt.
func (r *batchReader) next() (kind db.InternalKeyKind, ukey []byte, value []byte, ok bool) {
	p := *r
	if len(p) == 0 {
		return 0, nil, nil, false
	}
	kind, *r = db.InternalKeyKind(p[0]), p[1:]
	if kind > db.InternalKeyKindMax {
		return 0, nil, nil, false
	}
	ukey, ok = r.nextStr()
	if !ok {
		return 0, nil, nil, false
	}
	switch kind {
	case db.InternalKeyKindSet, db.InternalKeyKindRangeDelete:
		value, ok = r.nextStr()
		if !ok {
			return 0, nil, nil, false
		}
	}
	return kind, ukey, value, true
}

func (r *batchReader) nextStr() (s []byte, ok bool) {
	p := *r
	u, numBytes := binary.Uvarint(p)
	if numBytes <= 0 {
		return nil, false
	}
	p = p[numBytes:]
	if u > uint64(len(p)) {
		return nil, false
	}
	s, *r = p[:u], p[u:]
	return s, true
}

// TODO(peter): Flesh out the implementation here. It should be similar to
// memTableIter. The sequence number for batch entries is the
// key-offset|db.InternalSeqNumBatch.
type batchIter struct {
}

// batchIter implements the db.InternalIterator interface.
var _ db.InternalIterator = (*batchIter)(nil)

func (i *batchIter) SeekGE(key *db.InternalKey) {
	panic("pebble.batchIter: SeekGE unimplemented")
}

func (i *batchIter) SeekLT(key *db.InternalKey) {
	panic("pebble.batchIter: SeekLT unimplemented")
}

func (i *batchIter) First() {
	panic("pebble.batchIter: First unimplemented")
}

func (i *batchIter) Last() {
	panic("pebble.batchIter: Last unimplemented")
}

func (i *batchIter) Next() bool {
	return false
}

func (i *batchIter) NextUserKey() bool {
	return false
}

func (i *batchIter) Prev() bool {
	return false
}

func (i *batchIter) PrevUserKey() bool {
	return false
}

func (i *batchIter) Key() *db.InternalKey {
	return nil
}

func (i *batchIter) Value() []byte {
	return nil
}

func (i *batchIter) Valid() bool {
	return false
}

func (i *batchIter) Error() error {
	return nil
}

func (i *batchIter) Close() error {
	return nil
}
