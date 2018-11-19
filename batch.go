// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/internal/batchskl"
)

const (
	batchHeaderLen    = 12
	invalidBatchCount = 1<<32 - 1
)

// ErrNotIndexed means that a read operation on a batch failed because the
// batch is not indexed and thus doesn't support reads.
var ErrNotIndexed = errors.New("pebble: batch not indexed")

// ErrInvalidBatch indicates that a batch is invalid or otherwise corrupted.
var ErrInvalidBatch = errors.New("pebble: invalid batch")

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
	data      []byte
	cmp       db.Compare
	inlineKey db.InlineKey
}

// Get implements Storage.Get, as documented in the pebble/batchskl package.
func (s *batchStorage) Get(offset uint32) db.InternalKey {
	kind := db.InternalKeyKind(s.data[offset])
	_, key, ok := batchDecodeStr(s.data[offset+1:])
	if !ok {
		panic(fmt.Sprintf("corrupted batch entry: %d", offset))
	}
	return db.MakeInternalKey(key, uint64(offset)|db.InternalKeySeqNumBatch, kind)
}

// InlineKey implements Storage.InlineKey, as documented in the pebble/batchskl
// package.
func (s *batchStorage) InlineKey(key []byte) uint64 {
	return s.inlineKey(key)
}

// Compare implements Storage.Compare, as documented in the pebble/batchskl
// package.
func (s *batchStorage) Compare(a []byte, b uint32) int {
	// The key "a" is always the search key or the newer key being inserted. If
	// it is equal to the existing key consider it smaller so that it sorts
	// first.
	if s.cmp(a, s.Get(b).UserKey) <= 0 {
		return -1
	}
	return 1
}

// Batch is a sequence of Sets and/or Deletes that are applied atomically.
type Batch struct {
	batchStorage

	memTableSize uint32

	// The db to which the batch will be committed.
	db *DB

	// An optional skiplist keyed by offset into data of the entry.
	index         *batchskl.Skiplist
	rangeDelIndex *batchskl.Skiplist

	// The flushableBatch wrapper if the batch is too large to fit in the
	// memtable.
	flushable *flushableBatch

	commit  sync.WaitGroup
	applied uint32 // updated atomically
}

var _ Reader = (*Batch)(nil)
var _ Writer = (*Batch)(nil)

var batchPool = sync.Pool{
	New: func() interface{} {
		return &Batch{}
	},
}

type indexedBatch struct {
	batch Batch
	index batchskl.Skiplist
}

var indexedBatchPool = sync.Pool{
	New: func() interface{} {
		return &indexedBatch{}
	},
}

func newBatch(db *DB) *Batch {
	b := batchPool.Get().(*Batch)
	b.db = db
	return b
}

func newIndexedBatch(db *DB, comparer *db.Comparer) *Batch {
	i := indexedBatchPool.Get().(*indexedBatch)
	i.batch.cmp = comparer.Compare
	i.batch.inlineKey = comparer.InlineKey
	i.batch.db = db
	i.batch.index = &i.index
	i.batch.index.Reset(&i.batch.batchStorage, 0)
	return &i.batch
}

func (b *Batch) release() {
	// NB: This is ugly, but necessary so that we can use atomic.StoreUint32 for
	// the Batch.applied field. Without using an atomic to clear that field the
	// Go race detector complains.
	b.batchStorage = batchStorage{}
	b.memTableSize = 0
	b.db = nil
	b.flushable = nil
	b.commit = sync.WaitGroup{}
	atomic.StoreUint32(&b.applied, 0)

	if b.index == nil {
		batchPool.Put(b)
	} else {
		*b.index = batchskl.Skiplist{}
		b.index, b.rangeDelIndex = nil, nil
		indexedBatchPool.Put(b)
	}
}

func (b *Batch) refreshMemTableSize() {
	b.memTableSize = 0
	for iter := b.iter(); ; {
		_, key, value, ok := iter.next()
		if !ok {
			break
		}
		b.memTableSize += memTableEntrySize(len(key), len(value))
	}
}

// Apply the operations contained in the batch to the receiver batch.
//
// It is safe to modify the contents of the arguments after Apply returns.
func (b *Batch) Apply(batch *Batch, _ *db.WriteOptions) error {
	if len(batch.data) == 0 {
		return nil
	}
	if len(batch.data) < batchHeaderLen {
		return errors.New("pebble: invalid batch")
	}

	offset := len(b.data)
	if offset == 0 {
		b.init(offset)
		offset = batchHeaderLen
	}
	b.data = append(b.data, batch.data[batchHeaderLen:]...)

	count := binary.LittleEndian.Uint32(batch.data[8:12])
	b.setCount(b.count() + count)

	for iter := batchReader(b.data[offset:]); len(iter) > 0; {
		offset := uintptr(unsafe.Pointer(&iter[0])) - uintptr(unsafe.Pointer(&b.data[0]))
		_, key, value, ok := iter.next()
		if !ok {
			break
		}
		if b.index != nil {
			if err := b.index.Add(uint32(offset)); err != nil {
				panic(err)
			}
		}
		b.memTableSize += memTableEntrySize(len(key), len(value))
	}
	return nil
}

// Get gets the value for the given key. It returns ErrNotFound if the DB
// does not contain the key.
//
// The caller should not modify the contents of the returned slice, but
// it is safe to modify the contents of the argument after Get returns.
func (b *Batch) Get(key []byte) (value []byte, err error) {
	if b.index == nil {
		return nil, ErrNotIndexed
	}
	// Loop over the entries with keys >= the target key. The indexing of the
	// entries returns equal keys in reverse order of insertion. That is, the
	// last key added will be seen firsi.
	iter := b.index.NewIter()
	iter.SeekGE(key)
	if iter.Valid() {
		_, ekey, value, ok := b.decode(iter.KeyOffset())
		if !ok {
			return nil, fmt.Errorf("corrupted batch")
		}
		if b.cmp(key, ekey) <= 0 {
			// Invariant: b.cmp(key, ekey) == 0.
			return value, nil
		}
	}
	return nil, db.ErrNotFound
}

// Set adds an action to the batch that sets the key to map to the value.
//
// It is safe to modify the contents of the arguments after Set returns.
func (b *Batch) Set(key, value []byte, _ *db.WriteOptions) error {
	if len(b.data) == 0 {
		b.init(len(key) + len(value) + 2*binary.MaxVarintLen64 + batchHeaderLen)
	}
	if !b.increment() {
		return ErrInvalidBatch
	}
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
	b.memTableSize += memTableEntrySize(len(key), len(value))
	return nil
}

// Merge adds an action to the batch that merges the value at key with the new
// value. The details of the merge are dependent upon the configured merge
// operator.
//
// It is safe to modify the contents of the arguments after Merge returns.
func (b *Batch) Merge(key, value []byte, _ *db.WriteOptions) error {
	if len(b.data) == 0 {
		b.init(len(key) + len(value) + 2*binary.MaxVarintLen64 + batchHeaderLen)
	}
	if !b.increment() {
		return ErrInvalidBatch
	}
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
	b.memTableSize += memTableEntrySize(len(key), len(value))
	return nil
}

// Delete adds an action to the batch that deletes the entry for key.
//
// It is safe to modify the contents of the arguments after Delete returns.
func (b *Batch) Delete(key []byte, _ *db.WriteOptions) error {
	if len(b.data) == 0 {
		b.init(len(key) + binary.MaxVarintLen64 + batchHeaderLen)
	}
	if !b.increment() {
		return ErrInvalidBatch
	}
	offset := uint32(len(b.data))
	b.data = append(b.data, byte(db.InternalKeyKindDelete))
	b.appendStr(key)
	if b.index != nil {
		if err := b.index.Add(offset); err != nil {
			// We never add duplicate entries, so an error should never occur.
			panic(err)
		}
	}
	b.memTableSize += memTableEntrySize(len(key), 0)
	return nil
}

// DeleteRange deletes all of the keys (and values) in the range [start,end)
// (inclusive on start, exclusive on end).
//
// It is safe to modify the contents of the arguments after DeleteRange
// returns.
func (b *Batch) DeleteRange(start, end []byte, _ *db.WriteOptions) error {
	if len(b.data) == 0 {
		b.init(len(start) + len(end) + 2*binary.MaxVarintLen64 + batchHeaderLen)
	}
	if !b.increment() {
		return ErrInvalidBatch
	}
	offset := uint32(len(b.data))
	b.data = append(b.data, byte(db.InternalKeyKindRangeDelete))
	b.appendStr(start)
	b.appendStr(end)
	if b.index != nil {
		// Range deletions are rare, so we lazily allocate the index for them.
		if b.rangeDelIndex == nil {
			b.rangeDelIndex = batchskl.NewSkiplist(&b.batchStorage, 0)
		}
		if err := b.rangeDelIndex.Add(offset); err != nil {
			// We never add duplicate entries, so an error should never occur.
			panic(err)
		}
	}
	b.memTableSize += memTableEntrySize(len(start), len(end))
	return nil
}

// Repr returns the underlying batch representation. It is not safe to modify
// the contents.
func (b *Batch) Repr() []byte {
	return b.data
}

// NewIter returns an iterator that is unpositioned (Iterator.Valid() will
// return false). The iterator can be positioned via a call to SeekGE, SeekLT,
// First or Last. Only indexed batches support iterators.
func (b *Batch) NewIter(o *db.IterOptions) db.Iterator {
	if b.index == nil {
		return &dbIter{err: ErrNotIndexed}
	}
	return b.db.newIterInternal(b.newInternalIter(o), nil /* snapshot */, o)
}

// newInternalIter creates a new InternalIterator that iterates over the
// contents of the batch.
func (b *Batch) newInternalIter(o *db.IterOptions) internalIterator {
	if b.index == nil {
		return newErrorIter(ErrNotIndexed)
	}
	return &batchIter{
		cmp:   b.cmp,
		batch: b,
		iter:  b.index.NewIter(),
	}
}

func (b *Batch) newRangeDelIter(o *db.IterOptions) internalIterator {
	if b.index == nil {
		return newErrorIter(ErrNotIndexed)
	}
	if b.rangeDelIndex == nil {
		return newErrorIter(nil)
	}
	return &batchIter{
		cmp:   b.cmp,
		batch: b,
		iter:  b.rangeDelIndex.NewIter(),
	}
}

// Commit applies the batch to its parent writer.
func (b *Batch) Commit(o *db.WriteOptions) error {
	return b.db.Apply(b, o)
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
	case db.InternalKeyKindSet, db.InternalKeyKindMerge, db.InternalKeyKindRangeDelete:
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
// The final return value is false if the batch is corrupi.
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
	case db.InternalKeyKindSet, db.InternalKeyKindMerge, db.InternalKeyKindRangeDelete:
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

// Note: batchIter mirrors the implementation of flushableBatchIter. Keep the
// two in sync.
type batchIter struct {
	cmp     db.Compare
	batch   *Batch
	reverse bool
	iter    batchskl.Iterator
	err     error
}

// batchIter implements the internalIterator interface.
var _ internalIterator = (*batchIter)(nil)

func (i *batchIter) SeekGE(key []byte) {
	i.iter.SeekGE(key)
}

func (i *batchIter) SeekLT(key []byte) {
	i.iter.SeekLT(key)
}

func (i *batchIter) First() {
	i.iter.First()
}

func (i *batchIter) Last() {
	i.iter.Last()
}

func (i *batchIter) Next() bool {
	return i.iter.Next()
}

func (i *batchIter) Prev() bool {
	return i.iter.Prev()
}

func (i *batchIter) Key() db.InternalKey {
	return i.iter.Key()
}

func (i *batchIter) Value() []byte {
	_, _, value, ok := i.batch.decode(i.iter.KeyOffset())
	if !ok {
		i.err = fmt.Errorf("corrupted batch")
	}
	return value
}

func (i *batchIter) Valid() bool {
	return i.iter.Valid()
}

func (i *batchIter) Error() error {
	return i.err
}

func (i *batchIter) Close() error {
	_ = i.iter.Close()
	return i.err
}

type flushableBatchEntry struct {
	offset uint32
	index  uint32
}

// flushableBatch wraps an existing batch and provides the interfaces needed
// for making the batch flushable (i.e. able to mimic a memtable).
type flushableBatch struct {
	batch *Batch
	cmp   db.Compare

	// The base sequence number for the entries in the batch. This is the same
	// value as Batch.seqNum() and is cached here for performance.
	seqNum uint64

	// A slice of offsets and indices for the entries in the batch. Used to
	// implement flushableBatchIter. Unlike the indexing on a normal batch, a
	// flushable batch is indexed such that batch entry i will be given the
	// sequence number flushableBatch.seqNum+i.
	offsets         []flushableBatchEntry
	rangeDelOffsets []flushableBatchEntry

	flushedCh chan struct{}
}

var _ flushable = (*flushableBatch)(nil)

func newFlushableBatch(batch *Batch, comparer *db.Comparer) *flushableBatch {
	b := &flushableBatch{
		batch:           batch,
		cmp:             comparer.Compare,
		offsets:         make([]flushableBatchEntry, 0, batch.count()),
		rangeDelOffsets: nil, // NB: assume no range deletions need indexing
		flushedCh:       make(chan struct{}),
	}

	var index uint32
	for iter := batchReader(batch.data[batchHeaderLen:]); len(iter) > 0; index++ {
		offset := uintptr(unsafe.Pointer(&iter[0])) - uintptr(unsafe.Pointer(&batch.data[0]))
		kind, _, _, ok := iter.next()
		if !ok {
			break
		}
		entry := flushableBatchEntry{
			offset: uint32(offset),
			index:  uint32(index),
		}
		if kind == db.InternalKeyKindRangeDelete {
			b.rangeDelOffsets = append(b.rangeDelOffsets, entry)
		} else {
			b.offsets = append(b.offsets, entry)
		}
	}

	// Sort both offsets and rangeDelOffsets.
	sort.Sort(b)
	b.rangeDelOffsets, b.offsets = b.offsets, b.rangeDelOffsets
	sort.Sort(b)
	b.rangeDelOffsets, b.offsets = b.offsets, b.rangeDelOffsets
	return b
}

func (b *flushableBatch) Len() int {
	return len(b.offsets)
}

func (b *flushableBatch) Less(i, j int) bool {
	return db.InternalCompare(b.cmp,
		b.batch.batchStorage.Get(b.offsets[i].offset),
		b.batch.batchStorage.Get(b.offsets[j].offset)) < 0
}

func (b *flushableBatch) Swap(i, j int) {
	b.offsets[i], b.offsets[j] = b.offsets[j], b.offsets[i]
}

func (b *flushableBatch) newIter(o *db.IterOptions) internalIterator {
	return &flushableBatchIter{
		batch:   b,
		offsets: b.offsets,
		cmp:     b.cmp,
		index:   -1,
	}
}

func (b *flushableBatch) newRangeDelIter(o *db.IterOptions) internalIterator {
	return &flushableBatchIter{
		batch:   b,
		offsets: b.rangeDelOffsets,
		cmp:     b.cmp,
		index:   -1,
	}
}

func (b *flushableBatch) flushed() chan struct{} {
	return b.flushedCh
}

func (b *flushableBatch) readyForFlush() bool {
	return true
}

// Note: flushableBatchIter mirrors the implementation of batchIter. Keep the
// two in sync.
type flushableBatchIter struct {
	batch   *flushableBatch
	offsets []flushableBatchEntry
	cmp     db.Compare
	reverse bool
	index   int
	err     error
}

// flushableBatchIter implements the internalIterator interface.
var _ internalIterator = (*flushableBatchIter)(nil)

func (i *flushableBatchIter) SeekGE(key []byte) {
	ikey := db.MakeSearchKey(key)
	i.index = sort.Search(len(i.offsets), func(j int) bool {
		return db.InternalCompare(i.cmp, ikey, i.getKey(j)) < 0
	})
}

func (i *flushableBatchIter) SeekLT(key []byte) {
	ikey := db.MakeSearchKey(key)
	i.index = sort.Search(len(i.offsets), func(j int) bool {
		return db.InternalCompare(i.cmp, ikey, i.getKey(j)) <= 0
	})
	i.index--
}

func (i *flushableBatchIter) First() {
	i.index = 0
}

func (i *flushableBatchIter) Last() {
	i.index = len(i.offsets) - 1
}

func (i *flushableBatchIter) Next() bool {
	if i.index == len(i.offsets) {
		return false
	}
	i.index++
	return i.index < len(i.offsets)
}

func (i *flushableBatchIter) Prev() bool {
	if i.index < 0 {
		return false
	}
	i.index--
	return i.index >= 0
}

func (i *flushableBatchIter) getKey(index int) db.InternalKey {
	entry := i.offsets[index]
	kind := db.InternalKeyKind(i.batch.batch.data[entry.offset])
	_, key, ok := batchDecodeStr(i.batch.batch.data[entry.offset+1:])
	if !ok {
		panic(fmt.Sprintf("corrupted batch entry: %d", entry.offset))
	}
	return db.MakeInternalKey(key, i.batch.seqNum+uint64(entry.index), kind)
}

func (i *flushableBatchIter) Key() db.InternalKey {
	return i.getKey(i.index)
}

func (i *flushableBatchIter) Value() []byte {
	offset := i.offsets[i.index].offset
	_, _, value, ok := i.batch.batch.decode(offset)
	if !ok {
		i.err = fmt.Errorf("corrupted batch")
	}
	return value
}

func (i *flushableBatchIter) Valid() bool {
	return i.index >= 0 && i.index < len(i.offsets)
}

func (i *flushableBatchIter) Error() error {
	return i.err
}

func (i *flushableBatchIter) Close() error {
	return i.err
}
