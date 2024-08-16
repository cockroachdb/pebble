// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sync"
	"unsafe"

	"github.com/cockroachdb/crlib/crbytes"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/treeprinter"
	"github.com/cockroachdb/pebble/sstable/block"
)

// KeySchema defines the schema of a user key, as defined by the user's
// application.
//
// TODO(jackson): Consider making this KVSchema. It feels like there's an
// opportunity to generalize the ShortAttribute so that when a value is stored
// out-of-band, the DataBlockWriter calls user-provided code to store the short
// attributes inlined within the data block. For inlined-values, the
// user-defined value columns would be implicitly null.
type KeySchema struct {
	ColumnTypes  []DataType
	NewKeyWriter func() KeyWriter
	NewKeySeeker func() KeySeeker
}

// A KeyWriter maintains ColumnWriters for a data block for writing user keys
// into the database-specific key schema. Users may define their own key schema
// and implement KeyWriter to encode keys into custom columns that are aware of
// the structure of user keys.
type KeyWriter interface {
	ColumnWriter
	// ComparePrev compares the provided user to the previously-written user
	// key. The returned KeyComparison's UserKeyComparison field is equivalent
	// to Compare(key, prevKey) where prevKey is the last key passed to
	// WriteKey.
	ComparePrev(key []byte) KeyComparison
	// WriteKey writes a user key into the KeyWriter's columns. The
	// keyPrefixLenSharedWithPrev parameter takes the number of bytes prefixing
	// the key's logical prefix (as defined by (base.Comparer).Split) that the
	// previously-written key's prefix shares.
	//
	// WriteKey is guaranteed to be called sequentially with increasing row
	// indexes, beginning at zero.
	WriteKey(row int, key []byte, keyPrefixLen, keyPrefixLenSharedWithPrev int32)
}

// KeyComparison holds information about a key and its comparison to another a
// key.
type KeyComparison struct {
	// PrefixLen is the length of the prefix of the key. It's the outcome of
	// calling base.Split on the key.
	PrefixLen int32
	// CommonPrefixLen is the length of the physical (byte-wise) prefix of the
	// logical prefix that is shared with the other key. For example, for
	// "apple@1" and "applied@3" the value is 4 (the length of "appl"). For
	// "apple@1" and "apple@10" the value is 5 (the length of "apple"), because
	// the shared bytes within the suffix are not included.
	CommonPrefixLen int32
	// UserKeyComparison is the comparison of the user keys of the two keys.
	// Should be equivalent to
	//
	//   Compare(key, otherKey)
	UserKeyComparison int32
}

// PrefixEqual returns true if the key comparison determined that the keys have
// equal prefixes.
func (kcmp KeyComparison) PrefixEqual() bool { return kcmp.PrefixLen == kcmp.CommonPrefixLen }

// KeySeeker iterates over the keys in a columnar data block.
//
// Users of Pebble who define their own key schema must implement KeySeeker to
// seek over their decomposed keys.
//
// KeySeeker implementations must be safe for concurrent use by multiple
// goroutines. In practice, multiple DataBlockIterators may use the same
// KeySeeker.
type KeySeeker interface {
	// Init initializes the iterator to read from the provided DataBlockReader.
	Init(b *DataBlockReader) error
	// SeekGE returns the index of the first row with a key greater than or
	// equal to [key].
	//
	// If the caller externally knows a bound on where the key is located, it
	// may indicate it through [boundRow] and [searchDir]. A [searchDir] value
	// of -1 indicates that the sought row must be at an index ≤ [boundRow]. A
	// [searchDir] value of +1 indicates that the sought row must be at an index
	// ≥ [boundRow]. Implementations may use this information to constrain the
	// search. See (base.SeekGEFlags).TrySeekUsingNext for context on when this
	// may be set in practice.
	SeekGE(key []byte, boundRow int, searchDir int8) (row int)
	// MaterializeUserKey materializes the user key of the specified row,
	// returning a slice of the materialized user key.  The caller may use the
	// provided keyIter and its buffer to avoid allocations and reduce work. The
	// prevRow parameter is the row MaterializeUserKey was last invoked with.
	// Implementations may take advantage of that knowledge to reduce work.
	MaterializeUserKey(keyIter *PrefixBytesIter, prevRow, row int) []byte
	// Release releases the KeySeeker. It's called when the seeker is no longer
	// in use. Implementations may pool KeySeeker objects.
	Release()
}

const (
	defaultKeySchemaColumnPrefix int = iota
	defaultKeySchemaColumnSuffix
)

var defaultSchemaColumnTypes = []DataType{
	defaultKeySchemaColumnPrefix: DataTypePrefixBytes,
	defaultKeySchemaColumnSuffix: DataTypeBytes,
}

var defaultKeySeekerPool = sync.Pool{
	New: func() interface{} {
		return &defaultKeySeeker{}
	},
}

// DefaultKeySchema returns the default key schema that decomposes a user key
// into its prefix and suffix. Prefixes are sorted in lexicographical order.
func DefaultKeySchema(comparer *base.Comparer, prefixBundleSize int) KeySchema {
	return KeySchema{
		ColumnTypes: defaultSchemaColumnTypes,
		NewKeyWriter: func() KeyWriter {
			kw := &defaultKeyWriter{comparer: comparer}
			kw.prefixes.Init(prefixBundleSize)
			kw.suffixes.Init()
			return kw
		},
		NewKeySeeker: func() KeySeeker {
			ks := defaultKeySeekerPool.Get().(*defaultKeySeeker)
			ks.comparer = comparer
			return ks
		},
	}
}

// Assert that *defaultKeyWriter implements the KeyWriter interface.
var _ KeyWriter = (*defaultKeyWriter)(nil)

type defaultKeyWriter struct {
	comparer *base.Comparer
	prefixes PrefixBytesBuilder
	suffixes RawBytesBuilder
}

func (w *defaultKeyWriter) ComparePrev(key []byte) KeyComparison {
	lp := w.prefixes.LastKey()

	var cmpv KeyComparison
	cmpv.PrefixLen = int32(w.comparer.Split(key))
	cmpv.CommonPrefixLen = int32(crbytes.CommonPrefix(lp, key[:cmpv.PrefixLen]))
	if len(lp) == 0 {
		// The first key has no previous key to compare to.
		return cmpv
	}

	if invariants.Enabled && bytes.Compare(lp, key[:cmpv.PrefixLen]) > 0 {
		panic(errors.AssertionFailedf("keys are not in order: %q > %q", lp, key[:cmpv.PrefixLen]))
	}
	// Keys are written in order and prefixes must be sorted lexicograpgically,
	// so CommonPrefixLen == PrefixLen implies that the keys share the same
	// logical prefix. (If the previous key had a prefix longer than
	// CommonPrefixLen, it would sort after [key].)
	if cmpv.CommonPrefixLen == cmpv.PrefixLen {
		// The keys share the same MVCC prefix. Compare the suffixes.
		cmpv.UserKeyComparison = int32(w.comparer.CompareSuffixes(key[cmpv.PrefixLen:], w.suffixes.LastSlice()))
		if invariants.Enabled {
			if !w.comparer.Equal(lp, key[:cmpv.PrefixLen]) {
				panic(errors.AssertionFailedf("keys have different logical prefixes: %q != %q", lp, key[:cmpv.PrefixLen]))
			}
		}
		return cmpv
	}

	// The keys have different MVCC prefixes. We haven't determined which is
	// greater, but we know the index at which they diverge. The base.Comparer
	// contract dictates that prefixes must be lexicographically ordered.
	if len(lp) == int(cmpv.CommonPrefixLen) {
		// cmpv.PrefixLen > cmpv.PrefixLenShared; key is greater.
		cmpv.UserKeyComparison = +1
	} else {
		// Both keys have at least 1 additional byte at which they diverge.
		// Compare the diverging byte.
		cmpv.UserKeyComparison = int32(cmp.Compare(key[cmpv.CommonPrefixLen], lp[cmpv.CommonPrefixLen]))
	}
	if invariants.Enabled {
		// In this case we've determined that the keys have different prefixes,
		// so the UserKeyComparison should be equal to the result of comparing
		// the prefixes and nonzero.
		if cmpv.UserKeyComparison == 0 {
			panic(errors.AssertionFailedf("user keys should not be equal: %q+%q, %q", lp, w.suffixes.LastSlice(), key))
		}
		if v := w.comparer.Compare(key, lp); v != int(cmpv.UserKeyComparison) {
			panic(errors.AssertionFailedf("user key comparison mismatch: Compare(%q, %q) = %d ≠ %d",
				key, lp, v, cmpv.UserKeyComparison))
		}
	}
	return cmpv
}

func (w *defaultKeyWriter) WriteKey(
	row int, key []byte, keyPrefixLen, keyPrefixLenSharedWithPrev int32,
) {
	w.prefixes.Put(key[:keyPrefixLen], int(keyPrefixLenSharedWithPrev))
	w.suffixes.Put(key[keyPrefixLen:])
}

func (w *defaultKeyWriter) NumColumns() int {
	return 2
}

func (w *defaultKeyWriter) DataType(col int) DataType {
	return defaultSchemaColumnTypes[col]
}

func (w *defaultKeyWriter) Reset() {
	w.prefixes.Reset()
	w.suffixes.Reset()
}

func (w *defaultKeyWriter) WriteDebug(dst io.Writer, rows int) {
	fmt.Fprint(dst, "0: prefixes:       ")
	w.prefixes.WriteDebug(dst, rows)
	fmt.Fprintln(dst)
	fmt.Fprint(dst, "1: suffixes:       ")
	w.suffixes.WriteDebug(dst, rows)
	fmt.Fprintln(dst)
}

func (w *defaultKeyWriter) Size(rows int, offset uint32) uint32 {
	offset = w.prefixes.Size(rows, offset)
	offset = w.suffixes.Size(rows, offset)
	return offset
}

func (w *defaultKeyWriter) Finish(col, rows int, offset uint32, buf []byte) (nextOffset uint32) {
	switch col {
	case defaultKeySchemaColumnPrefix:
		return w.prefixes.Finish(0, rows, offset, buf)
	case defaultKeySchemaColumnSuffix:
		return w.suffixes.Finish(0, rows, offset, buf)
	default:
		panic(fmt.Sprintf("unknown default key column: %d", col))
	}
}

// Assert that *defaultKeySeeker implements KeySeeker.
var _ KeySeeker = (*defaultKeySeeker)(nil)

type defaultKeySeeker struct {
	comparer     *base.Comparer
	reader       *DataBlockReader
	prefixes     PrefixBytes
	suffixes     RawBytes
	sharedPrefix []byte
}

func (ks *defaultKeySeeker) Init(r *DataBlockReader) error {
	ks.reader = r
	ks.prefixes = r.r.PrefixBytes(defaultKeySchemaColumnPrefix)
	ks.suffixes = r.r.RawBytes(defaultKeySchemaColumnSuffix)
	ks.sharedPrefix = ks.prefixes.SharedPrefix()
	return nil
}

func (ks *defaultKeySeeker) SeekGE(key []byte, currRow int, dir int8) (row int) {
	si := ks.comparer.Split(key)
	row, eq := ks.prefixes.Search(key[:si])
	if eq {
		return ks.seekGEOnSuffix(row, key[si:])
	}
	return row
}

// seekGEOnSuffix is a helper function for SeekGE when a seek key's prefix
// exactly matches a row. seekGEOnSuffix finds the first row at index or later
// with the same prefix as index and a suffix greater than or equal to [suffix],
// or if no such row exists, the next row with a different prefix.
func (ks *defaultKeySeeker) seekGEOnSuffix(index int, suffix []byte) (row int) {
	// The search key's prefix exactly matches the prefix of the row at index.
	// If the row at index has a suffix >= [suffix], then return the row.
	if ks.comparer.CompareSuffixes(ks.suffixes.At(index), suffix) >= 0 {
		return index
	}
	// Otherwise, the row at [index] sorts before the search key and we need to
	// search forward. Binary search between [index+1, prefixChanged.Successor(index+1)].
	//
	// Define f(l-1) == false and f(u) == true.
	// Invariant: f(l-1) == false, f(u) == true.
	l := index + 1
	u := ks.reader.prefixChanged.Successor(index + 1)
	for l < u {
		h := int(uint(l+u) >> 1) // avoid overflow when computing h
		// l ≤ h < u
		if ks.comparer.CompareSuffixes(ks.suffixes.At(h), suffix) >= 0 {
			u = h // preserves f(u) == true
		} else {
			l = h + 1 // preserves f(l-1) == false
		}
	}
	return l
}

func (ks *defaultKeySeeker) MaterializeUserKey(keyIter *PrefixBytesIter, prevRow, row int) []byte {
	if row == prevRow+1 && prevRow >= 0 {
		ks.prefixes.SetNext(keyIter)
	} else {
		ks.prefixes.SetAt(keyIter, row)
	}
	suffix := ks.suffixes.At(row)
	memmove(unsafe.Pointer(uintptr(keyIter.ptr)+uintptr(keyIter.len)), unsafe.Pointer(unsafe.SliceData(suffix)), uintptr(len(suffix)))
	return unsafe.Slice((*byte)(keyIter.ptr), keyIter.len+len(suffix))
}

func (ks *defaultKeySeeker) Release() {
	*ks = defaultKeySeeker{}
	defaultKeySeekerPool.Put(ks)
}

// DataBlockWriter writes columnar data blocks, encoding keys using a
// user-defined schema.
type DataBlockWriter struct {
	Schema    KeySchema
	KeyWriter KeyWriter
	// trailers is the column writer for InternalKey uint64 trailers.
	trailers UintBuilder
	// prefixSame is the column writer for the prefix-changed bitmap that
	// indicates when a new key prefix begins. During block building, the bitmap
	// represents when the prefix stays the same, which is expected to be a
	// rarer case. Before Finish-ing the column, we invert the bitmap.
	prefixSame BitmapBuilder
	// values is the column writer for values. Iff the isValueExternal bitmap
	// indicates a value is external, the value is prefixed with a ValuePrefix
	// byte.
	values RawBytesBuilder
	// isValueExternal is the column writer for the is-value-external bitmap
	// that indicates when a value is stored out-of-band in a value block.
	isValueExternal BitmapBuilder

	enc              blockEncoder
	rows             int
	maximumKeyLength int
	valuePrefixTmp   [1]byte
}

// TODO(jackson): Add an isObsolete bitmap column.

const (
	dataBlockColumnTrailer = iota
	dataBlockColumnPrefixChanged
	dataBlockColumnValue
	dataBlockColumnIsValueExternal
	dataBlockColumnMax
)

// The data block header is a 4-byte uint32 encoding the maximum length of a key
// contained within the block. This is used by iterators to avoid the need to
// grow key buffers while iterating over the block, ensuring that the key buffer
// is always sufficiently large.
const dataBlockCustomHeaderSize = 4

// Init initializes the data block writer.
func (w *DataBlockWriter) Init(schema KeySchema) {
	w.Schema = schema
	w.KeyWriter = schema.NewKeyWriter()
	w.trailers.Init()
	w.prefixSame.Reset()
	w.values.Init()
	w.isValueExternal.Reset()
	w.rows = 0
	w.maximumKeyLength = 0
}

// Reset resets the data block writer to its initial state, retaining buffers.
func (w *DataBlockWriter) Reset() {
	w.KeyWriter.Reset()
	w.trailers.Reset()
	w.prefixSame.Reset()
	w.values.Reset()
	w.isValueExternal.Reset()
	w.rows = 0
	w.maximumKeyLength = 0
	w.enc.reset()
}

// String outputs a human-readable summary of internal DataBlockWriter state.
func (w *DataBlockWriter) String() string {
	var buf bytes.Buffer
	size := uint32(w.Size())
	fmt.Fprintf(&buf, "size=%d:\n", size)
	w.KeyWriter.WriteDebug(&buf, w.rows)

	fmt.Fprintf(&buf, "%d: trailers:       ", len(w.Schema.ColumnTypes)+dataBlockColumnTrailer)
	w.trailers.WriteDebug(&buf, w.rows)
	fmt.Fprintln(&buf)

	fmt.Fprintf(&buf, "%d: prefix changed: ", len(w.Schema.ColumnTypes)+dataBlockColumnPrefixChanged)
	w.prefixSame.WriteDebug(&buf, w.rows)
	fmt.Fprintln(&buf)

	fmt.Fprintf(&buf, "%d: values:         ", len(w.Schema.ColumnTypes)+dataBlockColumnValue)
	w.values.WriteDebug(&buf, w.rows)
	fmt.Fprintln(&buf)

	fmt.Fprintf(&buf, "%d: is-value-ext:   ", len(w.Schema.ColumnTypes)+dataBlockColumnIsValueExternal)
	w.isValueExternal.WriteDebug(&buf, w.rows)
	fmt.Fprintln(&buf)

	return buf.String()
}

// Add adds the provided key to the data block. Keys must be added in order. The
// caller must supply a KeyComparison containing the comparison of the key to
// the previously-added key, obtainable through
//
//	KeyWriter.ComparePrev(ikey.UserKey)
//
// The caller is required to pass this in because in expected use cases, the
// caller will also require the same information.
func (w *DataBlockWriter) Add(
	ikey base.InternalKey, value []byte, valuePrefix block.ValuePrefix, kcmp KeyComparison,
) {
	w.KeyWriter.WriteKey(w.rows, ikey.UserKey, kcmp.PrefixLen, kcmp.CommonPrefixLen)
	if kcmp.PrefixEqual() {
		w.prefixSame.Set(w.rows, true)
	}
	w.trailers.Set(w.rows, uint64(ikey.Trailer))
	if valuePrefix.IsValueHandle() {
		w.isValueExternal.Set(w.rows, true)
		// Write the value with the value prefix byte preceding the value.
		w.valuePrefixTmp[0] = byte(valuePrefix)
		w.values.PutConcat(w.valuePrefixTmp[:], value)
	} else {
		// Elide the value prefix. Readers will examine the isValueExternal
		// bitmap and know there is no value prefix byte if !isValueExternal.
		w.values.Put(value)
	}
	if len(ikey.UserKey) > int(w.maximumKeyLength) {
		w.maximumKeyLength = len(ikey.UserKey)
	}
	w.rows++
}

// Rows returns the number of rows in the current pending data block.
func (w *DataBlockWriter) Rows() int {
	return w.rows
}

// Size returns the size of the current pending data block.
func (w *DataBlockWriter) Size() int {
	off := blockHeaderSize(len(w.Schema.ColumnTypes)+dataBlockColumnMax, dataBlockCustomHeaderSize)
	off = w.KeyWriter.Size(w.rows, off)
	off = w.trailers.Size(w.rows, off)
	off = w.prefixSame.Size(w.rows, off)
	off = w.values.Size(w.rows, off)
	off = w.isValueExternal.Size(w.rows, off)
	off++ // trailer padding byte
	return int(off)
}

// Finish serializes the pending data block.
func (w *DataBlockWriter) Finish() []byte {
	cols := len(w.Schema.ColumnTypes) + dataBlockColumnMax
	h := Header{
		Version: Version1,
		Columns: uint16(cols),
		Rows:    uint32(w.rows),
	}
	w.enc.init(w.Size(), h, dataBlockCustomHeaderSize)

	// Write the max key length in the custom header.
	binary.LittleEndian.PutUint32(w.enc.data()[:dataBlockCustomHeaderSize], uint32(w.maximumKeyLength))

	// Write the user-defined key columns.
	w.enc.encode(w.rows, w.KeyWriter)

	// Write the internal key trailers.
	w.enc.encode(w.rows, &w.trailers)

	// Invert the prefix-same bitmap before writing it out, because we want it
	// to represent when the prefix changes.
	w.prefixSame.Invert(w.rows)
	w.enc.encode(w.rows, &w.prefixSame)

	// Write the value columns.
	w.enc.encode(w.rows, &w.values)
	w.enc.encode(w.rows, &w.isValueExternal)
	return w.enc.finish()
}

// DataBlockReaderSize is the size of a DataBlockReader struct. If allocating
// memory for a data block, the caller may want to additionally allocate memory
// for the corresponding DataBlockReader.
const DataBlockReaderSize = unsafe.Sizeof(DataBlockReader{})

// A DataBlockReader holds state for reading a columnar data block. It may be
// shared among multiple DataBlockIters.
type DataBlockReader struct {
	r BlockReader
	// trailers holds an array of the InternalKey trailers, encoding the key
	// kind and sequence number of each key.
	trailers UnsafeUints
	// prefixChanged is a bitmap indicating when the prefix (as defined by
	// Split) of a key changes, relative to the preceding key. This is used to
	// bound seeks within a prefix, and to optimize NextPrefix.
	prefixChanged Bitmap
	// values is the column reader for values. If the isValueExternal bitmap
	// indicates a value is external, the value is prefixed with a ValuePrefix
	// byte.
	values RawBytes
	// isValueExternal is the column reader for the is-value-external bitmap
	// that indicates whether a value is stored out-of-band in a value block. If
	// true, the value contains a ValuePrefix byte followed by an encoded value
	// handle indicating the value's location within the value block(s).
	isValueExternal Bitmap
	// maximumKeyLength is the maximum length of a user key in the block.
	// Iterators may use it to allocate a sufficiently large buffer up front,
	// and elide size checks during iteration.
	maximumKeyLength uint32
}

// BlockReader returns a pointer to the underlying BlockReader.
func (r *DataBlockReader) BlockReader() *BlockReader {
	return &r.r
}

// Init initializes the data block reader with the given serialized data block.
func (r *DataBlockReader) Init(schema KeySchema, data []byte) {
	r.r.Init(data, dataBlockCustomHeaderSize)
	r.trailers = r.r.Uints(len(schema.ColumnTypes) + dataBlockColumnTrailer)
	r.prefixChanged = r.r.Bitmap(len(schema.ColumnTypes) + dataBlockColumnPrefixChanged)
	r.values = r.r.RawBytes(len(schema.ColumnTypes) + dataBlockColumnValue)
	r.isValueExternal = r.r.Bitmap(len(schema.ColumnTypes) + dataBlockColumnIsValueExternal)
	r.maximumKeyLength = binary.LittleEndian.Uint32(data[:dataBlockCustomHeaderSize])
}

func (r *DataBlockReader) toFormatter(f *binfmt.Formatter) {
	f.CommentLine("data block header")
	f.HexBytesln(4, "maximum key length: %d", r.maximumKeyLength)
	r.r.headerToBinFormatter(f)
	for i := 0; i < int(r.r.header.Columns); i++ {
		r.r.columnToBinFormatter(f, i, int(r.r.header.Rows))
	}
}

// DataBlockIter iterates over a columnar data block.
type DataBlockIter struct {
	// configuration
	r            *DataBlockReader
	maxRow       int
	keySeeker    KeySeeker
	getLazyValue func([]byte) base.LazyValue

	// state
	keyIter PrefixBytesIter
	row     int
	kv      base.InternalKV
	kvRow   int // the row currently held in kv
}

var _ base.InternalIterator = (*DataBlockIter)(nil)

// Init initializes the data block iterator, configuring it to read from the
// provided reader.
func (i *DataBlockIter) Init(
	r *DataBlockReader, keyIterator KeySeeker, getLazyValue func([]byte) base.LazyValue,
) error {
	*i = DataBlockIter{
		r:            r,
		maxRow:       int(r.r.header.Rows) - 1,
		keySeeker:    keyIterator,
		getLazyValue: getLazyValue,
		row:          -1,
		kvRow:        math.MinInt,
		kv:           base.InternalKV{},
		keyIter:      PrefixBytesIter{},
	}
	// Allocate a keyIter buffer that's large enough to hold the largest user
	// key in the block.
	i.keyIter.Alloc(int(r.maximumKeyLength))
	return i.keySeeker.Init(r)
}

// SeekGE implements the base.InternalIterator interface.
func (i *DataBlockIter) SeekGE(key []byte, flags base.SeekGEFlags) *base.InternalKV {
	searchDir := int8(0)
	if flags.TrySeekUsingNext() {
		searchDir = +1
	}
	return i.decodeRow(i.keySeeker.SeekGE(key, i.row, searchDir))
}

// SeekPrefixGE implements the base.InternalIterator interface.
func (i *DataBlockIter) SeekPrefixGE(prefix, key []byte, flags base.SeekGEFlags) *base.InternalKV {
	// This should never be called as prefix iteration is handled by
	// sstable.Iterator.

	// TODO(jackson): We can implement this and avoid propagating keys without
	// the prefix up to the merging iterator. It will avoid unnecessary key
	// comparisons fixing up the merging iterator heap. We can also short
	// circuit the search if the prefix isn't found within the prefix column.
	// There's some subtlety around ensuring we continue to benefit from the
	// TrySeekUsingNext optimization.
	panic("pebble: SeekPrefixGE unimplemented")
}

// SeekLT implements the base.InternalIterator interface.
func (i *DataBlockIter) SeekLT(key []byte, _ base.SeekLTFlags) *base.InternalKV {
	row := i.keySeeker.SeekGE(key, i.row, 0 /* searchDir */)
	return i.decodeRow(row - 1)
}

// First implements the base.InternalIterator interface.
func (i *DataBlockIter) First() *base.InternalKV {
	return i.decodeRow(0)
}

// Last implements the base.InternalIterator interface.
func (i *DataBlockIter) Last() *base.InternalKV {
	return i.decodeRow(i.r.BlockReader().Rows() - 1)
}

// Next advances to the next KV pair in the block.
func (i *DataBlockIter) Next() *base.InternalKV {
	// Inline decodeRow, but avoiding unnecessary checks against i.row.
	if i.row >= i.maxRow {
		i.row = i.maxRow + 1
		return nil
	}
	i.row++
	i.kv.K = base.InternalKey{
		UserKey: i.keySeeker.MaterializeUserKey(&i.keyIter, i.kvRow, i.row),
		Trailer: base.InternalKeyTrailer(i.r.trailers.At(i.row)),
	}
	// Inline i.r.values.At(row).
	startOffset := i.r.values.offsets.At(i.row)
	v := unsafe.Slice((*byte)(i.r.values.ptr(startOffset)), i.r.values.offsets.At(i.row+1)-startOffset)
	if i.r.isValueExternal.At(i.row) {
		i.kv.V = i.getLazyValue(v)
	} else {
		i.kv.V = base.MakeInPlaceValue(v)
	}
	i.kvRow = i.row
	return &i.kv
}

// NextPrefix moves the iterator to the next row with a different prefix than
// the key at the current iterator position.
//
// The columnar block implementation uses a newPrefix bitmap to identify the
// next row with a differing prefix from the current row's key. If newPrefix[i]
// is set then row's i key prefix is different that row i-1. The bitmap is
// organized as a slice of 64-bit words. If a 64-bit word in the bitmap is zero
// then all of the rows corresponding to the bits in that word have the same
// prefix and we can skip ahead. If a row is non-zero a small bit of bit
// shifting and masking combined with bits.TrailingZeros64 can identify the the
// next bit that is set after the current row. The bitmap uses 1 bit/row (0.125
// bytes/row). A 32KB block containing 1300 rows (25 bytes/row) would need a
// bitmap of 21 64-bit words. Even in the worst case where every word is 0 this
// bitmap can be scanned in ~20 ns (1 ns/word) leading to a total NextPrefix
// time of ~30 ns if a row is found and decodeRow are called. In more normal
// cases, NextPrefix takes ~15% longer that a single Next call.
//
// For comparision, the rowblk nextPrefixV3 optimizations work by setting a bit
// in the value prefix byte that indicates that the current key has the same
// prefix as the previous key. Additionally, a bit is stolen from the restart
// table entries indicating whether a restart table entry has the same key
// prefix as the previous entry. Checking the value prefix byte bit requires
// locating that byte which requires decoding 3 varints per key/value pair.
func (i *DataBlockIter) NextPrefix(_ []byte) *base.InternalKV {
	return i.decodeRow(i.r.prefixChanged.Successor(i.row + 1))
}

// Prev moves the iterator to the previous KV pair in the block.
func (i *DataBlockIter) Prev() *base.InternalKV {
	return i.decodeRow(i.row - 1)
}

// Error implements the base.InternalIterator interface. A DataBlockIter is
// infallible and always returns a nil error.
func (i *DataBlockIter) Error() error {
	return nil // infallible
}

// Close implements the base.InternalIterator interface.
func (i *DataBlockIter) Close() error {
	*i = DataBlockIter{}
	return nil
}

// SetBounds implements the base.InternalIterator interface.
func (i *DataBlockIter) SetBounds(lower, upper []byte) {
	// This should never be called as bounds are handled by sstable.Iterator.
	panic("pebble: SetBounds unimplemented")
}

// SetContext implements the base.InternalIterator interface.
func (i *DataBlockIter) SetContext(_ context.Context) {}

var dataBlockTypeString string = fmt.Sprintf("%T", (*DataBlockIter)(nil))

// String implements the base.InternalIterator interface.
func (i *DataBlockIter) String() string {
	return dataBlockTypeString
}

// DebugTree is part of the InternalIterator interface.
func (i *DataBlockIter) DebugTree(tp treeprinter.Node) {
	tp.Childf("%T(%p)", i, i)
}

func (i *DataBlockIter) decodeRow(row int) *base.InternalKV {
	switch {
	case row < 0:
		i.row = -1
		return nil
	case row >= i.r.BlockReader().Rows():
		i.row = i.r.BlockReader().Rows()
		return nil
	case i.kvRow == row:
		i.row = row
		// Already synthesized the kv at row.
		return &i.kv
	default:
		i.kv.K = base.InternalKey{
			UserKey: i.keySeeker.MaterializeUserKey(&i.keyIter, i.kvRow, row),
			Trailer: base.InternalKeyTrailer(i.r.trailers.At(row)),
		}
		// Inline i.r.values.At(row).
		startOffset := i.r.values.offsets.At(row)
		v := unsafe.Slice((*byte)(i.r.values.ptr(startOffset)), i.r.values.offsets.At(row+1)-startOffset)
		if i.r.isValueExternal.At(row) {
			i.kv.V = i.getLazyValue(v)
		} else {
			i.kv.V = base.MakeInPlaceValue(v)
		}
		i.row = row
		i.kvRow = row
		return &i.kv
	}
}
