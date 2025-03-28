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
	"unsafe"

	"github.com/cockroachdb/crlib/crbytes"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/bytealloc"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/treeprinter"
	"github.com/cockroachdb/pebble/sstable/block"
)

// KeySchema defines the schema of a user key, as defined by the user's
// application.
//
// TODO(jackson): Consider making this KVSchema. It feels like there's an
// opportunity to generalize the ShortAttribute so that when a value is stored
// out-of-band, the DataBlockEncoder calls user-provided code to store the short
// attributes inlined within the data block. For inlined-values, the
// user-defined value columns would be implicitly null.
type KeySchema struct {
	Name string
	// KeySchema implementations can optionally make use a fixed-sized custom
	// header inside each block.
	HeaderSize   uint32
	ColumnTypes  []DataType
	NewKeyWriter func() KeyWriter

	// InitKeySeekerMetadata initializes the provided KeySeekerMetadata. This
	// happens once when a block enters the block cache and can be used to save
	// computation in NewKeySeeker.
	InitKeySeekerMetadata func(meta *KeySeekerMetadata, d *DataBlockDecoder)

	// KeySeeker returns a KeySeeker using metadata that was previously
	// initialized with InitKeySeekerMetadata. The returned key seeker can be an
	// unsafe cast of the metadata itself.
	KeySeeker func(meta *KeySeekerMetadata) KeySeeker
}

// KeySeekerMetadata is an in-memory buffer that stores metadata for a block. It
// is allocated together with the buffer storing the block and is initialized
// once when the block is read from disk. It is always 8-byte aligned.
//
// Portions of this buffer can be cast to the structures we need (through
// unsafe.Pointer), but note that any pointers in these structures will be
// invisible to the GC. Pointers to the block's data buffer are ok, since the
// metadata and the data have the same lifetime (sharing the underlying
// allocation).
//
// KeySeekerMetadata is stored inside block.Metadata.
type KeySeekerMetadata [KeySeekerMetadataSize]byte

// KeySeekerMetadataSize is chosen to fit the CockroachDB key seeker
// implementation.
const KeySeekerMetadataSize = 176

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
	//
	// If no key has been written yet, ComparePrev returns a KeyComparison with
	// PrefixLen set and UserKeyComparison=1.
	ComparePrev(key []byte) KeyComparison
	// WriteKey writes a user key into the KeyWriter's columns. The
	// keyPrefixLenSharedWithPrev parameter takes the number of bytes prefixing
	// the key's logical prefix (as defined by (base.Comparer).Split) that the
	// previously-written key's prefix shares.
	//
	// WriteKey is guaranteed to be called sequentially with increasing row
	// indexes, beginning at zero.
	WriteKey(row int, key []byte, keyPrefixLen, keyPrefixLenSharedWithPrev int32)
	// MaterializeKey appends the zero-indexed row'th key written to dst,
	// returning the result.
	MaterializeKey(dst []byte, row int) []byte
	// FinishHeader serializes an internal header of exactly KeySchema.HeaderSize bytes.
	FinishHeader(dst []byte)
}

// AssertKeyCompare compares two keys using the provided comparer, ensuring the
// provided KeyComparison accurately describing the result. Panics if the
// assertion does not hold.
func AssertKeyCompare(comparer *base.Comparer, a, b []byte, kcmp KeyComparison) {
	bi := comparer.Split(b)
	var recomputed KeyComparison
	recomputed.PrefixLen = int32(comparer.Split(a))
	recomputed.CommonPrefixLen = int32(crbytes.CommonPrefix(a[:recomputed.PrefixLen], b[:bi]))
	recomputed.UserKeyComparison = int32(comparer.Compare(a, b))
	if recomputed.PrefixEqual() != bytes.Equal(a[:recomputed.PrefixLen], b[:bi]) {
		panic(errors.AssertionFailedf("PrefixEqual()=%t doesn't hold: %q, %q", kcmp.PrefixEqual(), a, b))
	}
	if recomputed != kcmp {
		panic(errors.AssertionFailedf("KeyComparison of (%q, %q) = %s, ComparePrev gave %s",
			a, b, recomputed, kcmp))
	}
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

// String returns a string representation of the KeyComparison.
func (kcmp KeyComparison) String() string {
	return fmt.Sprintf("(prefix={%d,common=%d} cmp=%d)",
		kcmp.PrefixLen, kcmp.CommonPrefixLen, kcmp.UserKeyComparison)
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
	// IsLowerBound returns true if all keys in the data block (after suffix
	// replacement if syntheticSuffix is not empty) are >= the given key. If the
	// data block contains no keys, returns true.
	IsLowerBound(k []byte, syntheticSuffix []byte) bool
	// SeekGE returns the index of the first row with a key greater than or equal
	// to [key], and whether that row has the same prefix as [key].
	//
	// If the caller externally knows a bound on where the key is located, it
	// may indicate it through [boundRow] and [searchDir]. A [searchDir] value
	// of -1 indicates that the sought row must be at an index ≤ [boundRow]. A
	// [searchDir] value of +1 indicates that the sought row must be at an index
	// ≥ [boundRow]. Implementations may use this information to constrain the
	// search. See (base.SeekGEFlags).TrySeekUsingNext for context on when this
	// may be set in practice.
	SeekGE(key []byte, boundRow int, searchDir int8) (row int, equalPrefix bool)
	// MaterializeUserKey materializes the user key of the specified row,
	// returning a slice of the materialized user key.
	//
	// The provided keyIter must have a buffer large enough to hold the key.
	//
	// The prevRow parameter is the row MaterializeUserKey was last invoked with
	// (or a negative number if not applicable). Implementations may take
	// advantage of that knowledge to reduce work.
	MaterializeUserKey(keyIter *PrefixBytesIter, prevRow, row int) []byte
	// MaterializeUserKeyWithSyntheticSuffix is a variant of MaterializeUserKey
	// where the suffix is replaced.
	//
	// The provided keyIter must have a buffer large enough to hold the key after
	// suffix replacement.
	//
	// The prevRow parameter is the row MaterializeUserKeyWithSyntheticSuffix was
	// last invoked with (or a negative number if not applicable). Implementations
	// may take advantage of that knowledge to reduce work.
	MaterializeUserKeyWithSyntheticSuffix(
		keyIter *PrefixBytesIter, syntheticSuffix []byte, prevRow, row int,
	) []byte
}

const (
	defaultKeySchemaColumnPrefix int = iota
	defaultKeySchemaColumnSuffix
)

var defaultSchemaColumnTypes = []DataType{
	defaultKeySchemaColumnPrefix: DataTypePrefixBytes,
	defaultKeySchemaColumnSuffix: DataTypeBytes,
}

// DefaultKeySchema returns the default key schema that decomposes a user key
// into its prefix and suffix. Prefixes are sorted in lexicographical order.
func DefaultKeySchema(comparer *base.Comparer, prefixBundleSize int) KeySchema {
	return KeySchema{
		Name:        fmt.Sprintf("DefaultKeySchema(%s,%d)", comparer.Name, prefixBundleSize),
		HeaderSize:  0,
		ColumnTypes: defaultSchemaColumnTypes,
		NewKeyWriter: func() KeyWriter {
			kw := &defaultKeyWriter{comparer: comparer}
			kw.prefixes.Init(prefixBundleSize)
			kw.suffixes.Init()
			return kw
		},
		InitKeySeekerMetadata: func(meta *KeySeekerMetadata, d *DataBlockDecoder) {
			ks := (*defaultKeySeeker)(unsafe.Pointer(&meta[0]))
			ks.comparer = comparer
			ks.init(d)
		},
		KeySeeker: func(meta *KeySeekerMetadata) KeySeeker {
			ks := (*defaultKeySeeker)(unsafe.Pointer(&meta[0]))
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
	var cmpv KeyComparison
	cmpv.PrefixLen = int32(w.comparer.Split(key))
	if w.prefixes.nKeys == 0 {
		// The first key has no previous key to compare to.
		cmpv.UserKeyComparison = 1
		return cmpv
	}
	lp := w.prefixes.UnsafeGet(w.prefixes.nKeys - 1)
	cmpv.CommonPrefixLen = int32(crbytes.CommonPrefix(lp, key[:cmpv.PrefixLen]))

	// Keys are written in order and prefixes must be sorted lexicograpgically,
	// so CommonPrefixLen == PrefixLen implies that the keys share the same
	// logical prefix. (If the previous key had a prefix longer than
	// CommonPrefixLen, it would sort after [key].)
	if cmpv.CommonPrefixLen == cmpv.PrefixLen {
		// The keys share the same MVCC prefix. Compare the suffixes.
		cmpv.UserKeyComparison = int32(w.comparer.ComparePointSuffixes(key[cmpv.PrefixLen:],
			w.suffixes.UnsafeGet(w.suffixes.rows-1)))
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
			panic(errors.AssertionFailedf("user keys should not be equal: %q+%q, %q",
				lp, w.suffixes.UnsafeGet(w.suffixes.rows-1), key))
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

func (w *defaultKeyWriter) MaterializeKey(dst []byte, row int) []byte {
	dst = append(dst, w.prefixes.UnsafeGet(row)...)
	dst = append(dst, w.suffixes.UnsafeGet(row)...)
	return dst
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

func (w *defaultKeyWriter) FinishHeader([]byte) {}

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

// Assert that the metadata fits the defalut key seeker.
var _ uint = KeySeekerMetadataSize - uint(unsafe.Sizeof(defaultKeySeeker{}))

type defaultKeySeeker struct {
	comparer     *base.Comparer
	decoder      *DataBlockDecoder
	prefixes     PrefixBytes
	suffixes     RawBytes
	sharedPrefix []byte
}

func (ks *defaultKeySeeker) init(d *DataBlockDecoder) {
	ks.decoder = d
	ks.prefixes = d.d.PrefixBytes(defaultKeySchemaColumnPrefix)
	ks.suffixes = d.d.RawBytes(defaultKeySchemaColumnSuffix)
	ks.sharedPrefix = ks.prefixes.SharedPrefix()
}

// IsLowerBound is part of the KeySeeker interface.
func (ks *defaultKeySeeker) IsLowerBound(k []byte, syntheticSuffix []byte) bool {
	si := ks.comparer.Split(k)
	if v := ks.comparer.Compare(ks.prefixes.UnsafeFirstSlice(), k[:si]); v != 0 {
		return v > 0
	}
	suffix := syntheticSuffix
	if len(suffix) == 0 {
		suffix = ks.suffixes.At(0)
	}
	return ks.comparer.Compare(suffix, k[si:]) >= 0
}

// SeekGE is part of the KeySeeker interface.
func (ks *defaultKeySeeker) SeekGE(
	key []byte, boundRow int, searchDir int8,
) (row int, equalPrefix bool) {
	si := ks.comparer.Split(key)
	row, eq := ks.prefixes.Search(key[:si])
	if eq {
		return ks.seekGEOnSuffix(row, key[si:]), true
	}
	return row, false
}

// seekGEOnSuffix is a helper function for SeekGE when a seek key's prefix
// exactly matches a row. seekGEOnSuffix finds the first row at index or later
// with the same prefix as index and a suffix greater than or equal to [suffix],
// or if no such row exists, the next row with a different prefix.
func (ks *defaultKeySeeker) seekGEOnSuffix(index int, suffix []byte) (row int) {
	// The search key's prefix exactly matches the prefix of the row at index.
	// If the row at index has a suffix >= [suffix], then return the row.
	if ks.comparer.ComparePointSuffixes(ks.suffixes.At(index), suffix) >= 0 {
		return index
	}
	// Otherwise, the row at [index] sorts before the search key and we need to
	// search forward. Binary search between [index+1, prefixChanged.SeekSetBitGE(index+1)].
	//
	// Define f(l-1) == false and f(u) == true.
	// Invariant: f(l-1) == false, f(u) == true.
	l := index + 1
	u := ks.decoder.prefixChanged.SeekSetBitGE(index + 1)
	for l < u {
		h := int(uint(l+u) >> 1) // avoid overflow when computing h
		// l ≤ h < u
		if ks.comparer.ComparePointSuffixes(ks.suffixes.At(h), suffix) >= 0 {
			u = h // preserves f(u) == true
		} else {
			l = h + 1 // preserves f(l-1) == false
		}
	}
	return l
}

// MaterializeUserKey is part of the colblk.KeySeeker interface.
func (ks *defaultKeySeeker) MaterializeUserKey(keyIter *PrefixBytesIter, prevRow, row int) []byte {
	if row == prevRow+1 && prevRow >= 0 {
		ks.prefixes.SetNext(keyIter)
	} else {
		ks.prefixes.SetAt(keyIter, row)
	}
	suffix := ks.suffixes.At(row)
	res := keyIter.Buf[:len(keyIter.Buf)+len(suffix)]
	memmove(
		unsafe.Pointer(uintptr(unsafe.Pointer(unsafe.SliceData(keyIter.Buf)))+uintptr(len(keyIter.Buf))),
		unsafe.Pointer(unsafe.SliceData(suffix)),
		uintptr(len(suffix)),
	)
	return res
}

// MaterializeUserKeyWithSyntheticSuffix is part of the colblk.KeySeeker interface.
func (ks *defaultKeySeeker) MaterializeUserKeyWithSyntheticSuffix(
	keyIter *PrefixBytesIter, suffix []byte, prevRow, row int,
) []byte {
	if row == prevRow+1 && prevRow >= 0 {
		ks.prefixes.SetNext(keyIter)
	} else {
		ks.prefixes.SetAt(keyIter, row)
	}
	res := keyIter.Buf[:len(keyIter.Buf)+len(suffix)]
	memmove(
		unsafe.Pointer(uintptr(unsafe.Pointer(unsafe.SliceData(keyIter.Buf)))+uintptr(len(keyIter.Buf))),
		unsafe.Pointer(unsafe.SliceData(suffix)),
		uintptr(len(suffix)),
	)
	return res
}

// DataBlockEncoder encodes columnar data blocks using a user-defined schema.
type DataBlockEncoder struct {
	Schema    *KeySchema
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
	// isObsolete is the column writer for the is-obsolete bitmap that indicates
	// when a key is known to be obsolete/non-live (i.e., shadowed by another
	// identical point key or range deletion with a higher sequence number).
	isObsolete BitmapBuilder

	enc              blockEncoder
	rows             int
	maximumKeyLength int
	valuePrefixTmp   [1]byte
	lastUserKeyTmp   []byte
}

const (
	dataBlockColumnTrailer = iota
	dataBlockColumnPrefixChanged
	dataBlockColumnValue
	dataBlockColumnIsValueExternal
	dataBlockColumnIsObsolete
	dataBlockColumnMax
)

// The data block header is a 4-byte uint32 encoding the maximum length of a key
// contained within the block. This is used by iterators to avoid the need to
// grow key buffers while iterating over the block, ensuring that the key buffer
// is always sufficiently large.
// This is serialized immediately after the KeySchema specific header.
const dataBlockCustomHeaderSize = 4

// Init initializes the data block writer.
func (w *DataBlockEncoder) Init(schema *KeySchema) {
	w.Schema = schema
	w.KeyWriter = schema.NewKeyWriter()
	w.trailers.Init()
	w.prefixSame.Reset()
	w.values.Init()
	w.isValueExternal.Reset()
	w.isObsolete.Reset()
	w.rows = 0
	w.maximumKeyLength = 0
	w.lastUserKeyTmp = w.lastUserKeyTmp[:0]
	w.enc.reset()
}

// Reset resets the data block writer to its initial state, retaining buffers.
func (w *DataBlockEncoder) Reset() {
	w.KeyWriter.Reset()
	w.trailers.Reset()
	w.prefixSame.Reset()
	w.values.Reset()
	w.isValueExternal.Reset()
	w.isObsolete.Reset()
	w.rows = 0
	w.maximumKeyLength = 0
	w.lastUserKeyTmp = w.lastUserKeyTmp[:0]
	w.enc.reset()
}

// String outputs a human-readable summary of internal DataBlockEncoder state.
func (w *DataBlockEncoder) String() string {
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

	fmt.Fprintf(&buf, "%d: is-obsolete:    ", len(w.Schema.ColumnTypes)+dataBlockColumnIsObsolete)
	w.isObsolete.WriteDebug(&buf, w.rows)
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
func (w *DataBlockEncoder) Add(
	ikey base.InternalKey,
	value []byte,
	valuePrefix block.ValuePrefix,
	kcmp KeyComparison,
	isObsolete bool,
) {
	w.KeyWriter.WriteKey(w.rows, ikey.UserKey, kcmp.PrefixLen, kcmp.CommonPrefixLen)
	if kcmp.PrefixEqual() {
		w.prefixSame.Set(w.rows)
	}
	if isObsolete {
		w.isObsolete.Set(w.rows)
	}
	w.trailers.Set(w.rows, uint64(ikey.Trailer))
	if !valuePrefix.IsInPlaceValue() {
		w.isValueExternal.Set(w.rows)
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
func (w *DataBlockEncoder) Rows() int {
	return w.rows
}

// Size returns the size of the current pending data block.
func (w *DataBlockEncoder) Size() int {
	off := blockHeaderSize(len(w.Schema.ColumnTypes)+dataBlockColumnMax, dataBlockCustomHeaderSize+w.Schema.HeaderSize)
	off = w.KeyWriter.Size(w.rows, off)
	off = w.trailers.Size(w.rows, off)
	off = w.prefixSame.InvertedSize(w.rows, off)
	off = w.values.Size(w.rows, off)
	off = w.isValueExternal.Size(w.rows, off)
	off = w.isObsolete.Size(w.rows, off)
	off++ // trailer padding byte
	return int(off)
}

// MaterializeLastUserKey materializes the last added user key.
func (w *DataBlockEncoder) MaterializeLastUserKey(appendTo []byte) []byte {
	return w.KeyWriter.MaterializeKey(appendTo, w.rows-1)
}

// Finish serializes the pending data block, including the first [rows] rows.
// The value of [rows] must be Rows() or Rows()-1. The provided size must be the
// size of the data block with the provided row count (i.e., the return value of
// [Size] when DataBlockEncoder.Rows() = [rows]).
//
// Finish the returns the serialized, uncompressed data block and the
// InternalKey of the last key contained within the data block. The memory of
// the lastKey's UserKey is owned by the DataBlockEncoder. The caller must
// copy it if they require it to outlive a Reset of the writer.
func (w *DataBlockEncoder) Finish(rows, size int) (finished []byte, lastKey base.InternalKey) {
	if invariants.Enabled && rows != w.rows && rows != w.rows-1 {
		panic(errors.AssertionFailedf("data block has %d rows; asked to finish %d", w.rows, rows))
	}

	cols := len(w.Schema.ColumnTypes) + dataBlockColumnMax
	h := Header{
		Version: Version1,
		Columns: uint16(cols),
		Rows:    uint32(rows),
	}

	// Invert the prefix-same bitmap before writing it out, because we want it
	// to represent when the prefix changes.
	w.prefixSame.Invert(rows)

	w.enc.init(size, h, dataBlockCustomHeaderSize+w.Schema.HeaderSize)

	// Write the key schema custom header.
	w.KeyWriter.FinishHeader(w.enc.data()[:w.Schema.HeaderSize])
	// Write the max key length in the data block custom header.
	binary.LittleEndian.PutUint32(w.enc.data()[w.Schema.HeaderSize:w.Schema.HeaderSize+dataBlockCustomHeaderSize], uint32(w.maximumKeyLength))
	w.enc.encode(rows, w.KeyWriter)
	w.enc.encode(rows, &w.trailers)
	w.enc.encode(rows, &w.prefixSame)
	w.enc.encode(rows, &w.values)
	w.enc.encode(rows, &w.isValueExternal)
	w.enc.encode(rows, &w.isObsolete)
	finished = w.enc.finish()

	w.lastUserKeyTmp = w.lastUserKeyTmp[:0]
	w.lastUserKeyTmp = w.KeyWriter.MaterializeKey(w.lastUserKeyTmp[:0], rows-1)
	lastKey = base.InternalKey{
		UserKey: w.lastUserKeyTmp,
		Trailer: base.InternalKeyTrailer(w.trailers.Get(rows - 1)),
	}
	return finished, lastKey
}

// DataBlockRewriter rewrites data blocks. See RewriteSuffixes.
type DataBlockRewriter struct {
	KeySchema *KeySchema

	encoder   DataBlockEncoder
	decoder   DataBlockDecoder
	iter      DataBlockIter
	keySeeker KeySeeker
	comparer  *base.Comparer
	keyBuf    []byte
	// keyAlloc grown throughout the lifetime of the rewriter.
	keyAlloc        bytealloc.A
	prefixBytesIter PrefixBytesIter
	initialized     bool
}

// NewDataBlockRewriter creates a block rewriter.
func NewDataBlockRewriter(keySchema *KeySchema, comparer *base.Comparer) *DataBlockRewriter {
	return &DataBlockRewriter{
		KeySchema: keySchema,
		comparer:  comparer,
	}
}

type assertNoExternalValues struct{}

var _ block.GetInternalValueForPrefixAndValueHandler = assertNoExternalValues{}

func (assertNoExternalValues) GetInternalValueForPrefixAndValueHandle(
	value []byte,
) base.InternalValue {
	panic(errors.AssertionFailedf("pebble: sstable contains values in value blocks"))
}

// RewriteSuffixes rewrites the input block. It expects the input block to only
// contain keys with the suffix `from`. It rewrites the block to contain the
// same keys with the suffix `to`.
//
// RewriteSuffixes returns the start and end keys of the rewritten block, and
// the finished rewritten block. The returned start and end keys have indefinite
// lifetimes. The returned rewritten block is owned by the DataBlockRewriter. If
// it must be retained beyond the next call to RewriteSuffixes, the caller
// should make a copy.
//
// Note that the input slice must be 8-byte aligned.
func (rw *DataBlockRewriter) RewriteSuffixes(
	input []byte, from []byte, to []byte,
) (start, end base.InternalKey, rewritten []byte, err error) {
	if !rw.initialized {
		rw.iter.InitOnce(rw.KeySchema, rw.comparer, assertNoExternalValues{})
		rw.encoder.Init(rw.KeySchema)
		rw.initialized = true
	}

	// TODO(jackson): RewriteSuffixes performs a naïve rewrite of the block,
	// iterating over the input block while building a new block, KV-by-KV.
	// Since key columns are stored separately from other data, we could copy
	// columns that are unchanged (at least all the non-key columns, and in
	// practice a PrefixBytes column) wholesale without retrieving rows
	// one-by-one. In practice, there a few obstacles to making this work:
	//
	// - Only the beginning of a data block is assumed to be aligned. Columns
	//   then add padding as necessary to align data that needs to be aligned.
	//   If we copy a column, we have no guarantee that the alignment of the
	//   column start in the old block matches the alignment in the new block.
	//   We'd have to add padding to between columns to match the original
	//   alignment. It's a bit subtle.
	// - We still need to read all the key columns in order to synthesize
	//   [start] and [end].
	//
	// The columnar format is designed to support fast IterTransforms at read
	// time, including IterTransforms.SyntheticSuffix. Our effort might be
	// better spent dropping support for the physical rewriting of data blocks
	// we're performing here and instead use a read-time IterTransform.

	rw.decoder.Init(rw.KeySchema, input)
	meta := &KeySeekerMetadata{}
	rw.KeySchema.InitKeySeekerMetadata(meta, &rw.decoder)
	rw.keySeeker = rw.KeySchema.KeySeeker(meta)
	rw.encoder.Reset()
	if err = rw.iter.Init(&rw.decoder, block.IterTransforms{}); err != nil {
		return base.InternalKey{}, base.InternalKey{}, nil, err
	}

	// Allocate a keyIter buffer that's large enough to hold the largest user
	// key in the block with 1 byte to spare (so that pointer arithmetic is
	// never pointing beyond the allocation, which would violate Go rules).
	if cap(rw.prefixBytesIter.Buf) < int(rw.decoder.maximumKeyLength)+1 {
		rw.prefixBytesIter.Buf = make([]byte, rw.decoder.maximumKeyLength+1)
	}
	if newMax := int(rw.decoder.maximumKeyLength) - len(from) + len(to) + 1; cap(rw.keyBuf) < newMax {
		rw.keyBuf = make([]byte, newMax)
	}

	// Rewrite each key-value pair one-by-one.
	for i, kv := 0, rw.iter.First(); kv != nil; i, kv = i+1, rw.iter.Next() {
		value := kv.V.LazyValue().ValueOrHandle
		valuePrefix := block.InPlaceValuePrefix(false /* setHasSamePrefix (unused) */)
		isValueExternal := rw.decoder.isValueExternal.At(i)
		if isValueExternal {
			valuePrefix = block.ValuePrefix(value[0])
			value = value[1:]
		}
		kcmp := rw.encoder.KeyWriter.ComparePrev(kv.K.UserKey)
		if !bytes.Equal(kv.K.UserKey[kcmp.PrefixLen:], from) {
			return base.InternalKey{}, base.InternalKey{}, nil,
				errors.Newf("key %s has suffix 0x%x; require 0x%x", kv.K, kv.K.UserKey[kcmp.PrefixLen:], from)
		}
		rw.keyBuf = append(rw.keyBuf[:0], kv.K.UserKey[:kcmp.PrefixLen]...)
		rw.keyBuf = append(rw.keyBuf, to...)
		if i == 0 {
			start.UserKey, rw.keyAlloc = rw.keyAlloc.Copy(rw.keyBuf)
			start.Trailer = kv.K.Trailer
		}
		k := base.InternalKey{UserKey: rw.keyBuf, Trailer: kv.K.Trailer}
		rw.encoder.Add(k, value, valuePrefix, kcmp, rw.decoder.isObsolete.At(i))
	}
	rewritten, end = rw.encoder.Finish(int(rw.decoder.d.header.Rows), rw.encoder.Size())
	end.UserKey, rw.keyAlloc = rw.keyAlloc.Copy(end.UserKey)
	return start, end, rewritten, nil
}

// dataBlockDecoderSize is the size of DataBlockDecoder, round up to 8 bytes.
const dataBlockDecoderSize = (unsafe.Sizeof(DataBlockDecoder{}) + 7) &^ 7

// Assert that dataBlockDecoderSize is a multiple of 8 bytes (so that
// KeySeekerMetadata is also aligned).
const _ uint = uint(-(dataBlockDecoderSize % 8))

// Assert that a DataBlockDecoder and a KeySeekerMetadata can fit inside block.Metadata.
const _ uint = block.MetadataSize - uint(dataBlockDecoderSize) - KeySeekerMetadataSize

// InitDataBlockMetadata initializes the metadata for a data block.
func InitDataBlockMetadata(schema *KeySchema, md *block.Metadata, data []byte) (err error) {
	if uintptr(unsafe.Pointer(md))%8 != 0 {
		return errors.AssertionFailedf("metadata is not 8-byte aligned")
	}
	d := (*DataBlockDecoder)(unsafe.Pointer(md))
	// Initialization can panic; convert panics to corruption errors (so higher
	// layers can add file number and offset information).
	defer func() {
		if r := recover(); r != nil {
			err = base.CorruptionErrorf("error initializing data block metadata: %v", r)
		}
	}()
	d.Init(schema, data)
	keySchemaMeta := (*KeySeekerMetadata)(unsafe.Pointer(&md[dataBlockDecoderSize]))
	schema.InitKeySeekerMetadata(keySchemaMeta, d)
	return nil
}

// Assert that an IndexBlockDecoder can fit inside block.Metadata.
const _ uint = block.MetadataSize - uint(unsafe.Sizeof(IndexBlockDecoder{}))

// InitIndexBlockMetadata initializes the metadata for an index block.
func InitIndexBlockMetadata(md *block.Metadata, data []byte) (err error) {
	if uintptr(unsafe.Pointer(md))%8 != 0 {
		return errors.AssertionFailedf("metadata is not 8-byte aligned")
	}
	d := (*IndexBlockDecoder)(unsafe.Pointer(md))
	// Initialization can panic; convert panics to corruption errors (so higher
	// layers can add file number and offset information).
	defer func() {
		if r := recover(); r != nil {
			err = base.CorruptionErrorf("error initializing index block metadata: %v", r)
		}
	}()
	d.Init(data)
	return nil
}

// Assert that a IndexBlockDecoder can fit inside block.Metadata.
const _ uint = block.MetadataSize - uint(unsafe.Sizeof(KeyspanDecoder{}))

// InitKeyspanBlockMetadata initializes the metadata for a rangedel or range key block.
func InitKeyspanBlockMetadata(md *block.Metadata, data []byte) (err error) {
	if uintptr(unsafe.Pointer(md))%8 != 0 {
		return errors.AssertionFailedf("metadata is not 8-byte aligned")
	}
	d := (*KeyspanDecoder)(unsafe.Pointer(md))
	// Initialization can panic; convert panics to corruption errors (so higher
	// layers can add file number and offset information).
	defer func() {
		if r := recover(); r != nil {
			err = base.CorruptionErrorf("error initializing keyspan block metadata: %v", r)
		}
	}()
	d.Init(data)
	return nil
}

// A DataBlockDecoder holds state for interpreting a columnar data block. It may
// be shared among multiple DataBlockIters.
type DataBlockDecoder struct {
	d BlockDecoder
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
	// isObsolete is the column reader for the is-obsolete bitmap
	// that indicates whether a key is obsolete/non-live.
	isObsolete Bitmap
	// maximumKeyLength is the maximum length of a user key in the block.
	// Iterators may use it to allocate a sufficiently large buffer up front,
	// and elide size checks during iteration. Note that iterators should add +1
	// to the key length to ensure pointer arithmetric that computes a pointer
	// to the tail of the key does not point to memory beyond the allocation
	// (prohibited by Go pointer rules).
	maximumKeyLength uint32
}

// BlockDecoder returns a pointer to the underlying BlockDecoder.
func (d *DataBlockDecoder) BlockDecoder() *BlockDecoder {
	return &d.d
}

// PrefixChanged returns the prefix-changed bitmap.
func (d *DataBlockDecoder) PrefixChanged() Bitmap {
	return d.prefixChanged
}

// KeySchemaHeader returns the KeySchema-specific header.
func (d *DataBlockDecoder) KeySchemaHeader() []byte {
	return d.d.data[:d.d.customHeaderSize-dataBlockCustomHeaderSize]
}

// Init initializes the data block reader with the given serialized data block.
func (d *DataBlockDecoder) Init(schema *KeySchema, data []byte) {
	if uintptr(unsafe.Pointer(unsafe.SliceData(data)))&7 != 0 {
		panic("data buffer not 8-byte aligned")
	}
	d.d.Init(data, dataBlockCustomHeaderSize+schema.HeaderSize)
	d.trailers = d.d.Uints(len(schema.ColumnTypes) + dataBlockColumnTrailer)
	d.prefixChanged = d.d.Bitmap(len(schema.ColumnTypes) + dataBlockColumnPrefixChanged)
	d.values = d.d.RawBytes(len(schema.ColumnTypes) + dataBlockColumnValue)
	d.isValueExternal = d.d.Bitmap(len(schema.ColumnTypes) + dataBlockColumnIsValueExternal)
	d.isObsolete = d.d.Bitmap(len(schema.ColumnTypes) + dataBlockColumnIsObsolete)
	d.maximumKeyLength = binary.LittleEndian.Uint32(data[schema.HeaderSize:])
}

// Describe descirbes the binary format of the data block, assuming f.Offset()
// is positioned at the beginning of the same data block described by d.
func (d *DataBlockDecoder) Describe(f *binfmt.Formatter, tp treeprinter.Node) {
	// Set the relative offset. When loaded into memory, the beginning of blocks
	// are aligned. Padding that ensures alignment is done relative to the
	// current offset. Setting the relative offset ensures that if we're
	// describing this block within a larger structure (eg, f.Offset()>0), we
	// compute padding appropriately assuming the current byte f.Offset() is
	// aligned.
	f.SetAnchorOffset()

	n := tp.Child("data block header")
	if keySchemaHeaderSize := int(d.d.customHeaderSize - 4); keySchemaHeaderSize > 0 {
		f.HexBytesln(keySchemaHeaderSize, "key schema header")
	}
	f.HexBytesln(4, "maximum key length: %d", d.maximumKeyLength)
	d.d.headerToBinFormatter(f, n)
	for i := 0; i < int(d.d.header.Columns); i++ {
		d.d.columnToBinFormatter(f, n, i, int(d.d.header.Rows))
	}
	f.HexBytesln(1, "block padding byte")
	f.ToTreePrinter(n)
}

// A DataBlockValidator validates invariants that should hold across all data
// blocks. It may be used multiple times and will reuse allocations across
// Validate invocations when possible.
type DataBlockValidator struct {
	dec            DataBlockDecoder
	keySeekerMeta  KeySeekerMetadata
	curKeyIter     PrefixBytesIter
	prevUserKeyBuf []byte
}

// Validate validates the provided block. It returns an error if the block is
// invalid.
func (v *DataBlockValidator) Validate(
	data []byte, comparer *base.Comparer, keySchema *KeySchema,
) error {
	v.dec.Init(keySchema, data)
	n := v.dec.d.header.Rows
	keySchema.InitKeySeekerMetadata(&v.keySeekerMeta, &v.dec)
	keySeeker := keySchema.KeySeeker(&v.keySeekerMeta)

	if cap(v.prevUserKeyBuf) < int(v.dec.maximumKeyLength)+1 {
		v.prevUserKeyBuf = make([]byte, 0, v.dec.maximumKeyLength+1)
	}
	prevKey := base.InternalKey{UserKey: v.prevUserKeyBuf[:0]}
	v.curKeyIter.Init(int(v.dec.maximumKeyLength), nil)

	for i := 0; i < int(n); i++ {
		k := base.InternalKey{
			UserKey: keySeeker.MaterializeUserKey(&v.curKeyIter, i-1, i),
			Trailer: base.InternalKeyTrailer(v.dec.trailers.At(i)),
		}
		// Ensure the keys are ordered.
		ucmp := comparer.Compare(k.UserKey, prevKey.UserKey)
		if ucmp < 0 || (ucmp == 0 && k.Trailer >= prevKey.Trailer) {
			return errors.AssertionFailedf("key %s (row %d) and key %s (row %d) are out of order",
				prevKey, i-1, k, i)
		}
		// Ensure the obsolete bit is set if the key is definitively obsolete.
		// Not all sources of obsolescence are evident with only a data block
		// available (range deletions or point keys in previous blocks may cause
		// a key to be obsolete).
		if ucmp == 0 && prevKey.Kind() != base.InternalKeyKindMerge && !v.dec.isObsolete.At(i) {
			return errors.AssertionFailedf("key %s (row %d) is shadowed by previous key %s but is not marked as obsolete",
				k, i, prevKey)
		}
		// Ensure that the prefix-changed bit is set correctly.
		if i > 0 {
			currPrefix := comparer.Split.Prefix(k.UserKey)
			prevPrefix := comparer.Split.Prefix(prevKey.UserKey)
			prefixChanged := !bytes.Equal(prevPrefix, currPrefix)
			if prefixChanged != v.dec.prefixChanged.At(i) {
				return errors.AssertionFailedf("prefix changed bit for key %q (row %d) is %t, expected %t [prev key was %q]",
					k.UserKey, i, v.dec.prefixChanged.At(i), prefixChanged, prevKey.UserKey)
			}
		}

		prevKey.CopyFrom(k)
	}
	return nil
}

// Assert that *DataBlockIter implements block.DataBlockIterator.
var _ block.DataBlockIterator = (*DataBlockIter)(nil)

// DataBlockIter iterates over a columnar data block.
type DataBlockIter struct {
	// -- Fields that are initialized once --
	// For any changes to these fields, InitOnce should be updated.

	// keySchema configures the DataBlockIterConfig to use the provided
	// KeySchema when initializing the DataBlockIter for iteration over a new
	// block.
	keySchema *KeySchema
	suffixCmp base.ComparePointSuffixes
	split     base.Split
	// getLazyValuer configures the DataBlockIterConfig to initialize the
	// DataBlockIter to use the provided handler for retrieving lazy values.
	getLazyValuer block.GetInternalValueForPrefixAndValueHandler

	// -- Fields that are initialized for each block --
	// For any changes to these fields, InitHandle should be updated.

	d            *DataBlockDecoder
	h            block.BufferHandle
	maxRow       int
	transforms   block.IterTransforms
	noTransforms bool
	keySeeker    KeySeeker

	// -- State --
	// For any changes to these fields, InitHandle (which resets them) should be
	// updated.

	keyIter PrefixBytesIter
	row     int
	kv      base.InternalKV
	kvRow   int // the row currently held in kv

	// nextObsoletePoint is the row index of the first obsolete point after i.row.
	// It is used to optimize skipping of obsolete points during forward
	// iteration.
	nextObsoletePoint int
}

// InitOnce configures the data block iterator's key schema and lazy value
// handler. The iterator must be initialized with a block before it can be used.
// It may be reinitialized with new blocks without calling InitOnce again.
func (i *DataBlockIter) InitOnce(
	keySchema *KeySchema,
	comparer *base.Comparer,
	getLazyValuer block.GetInternalValueForPrefixAndValueHandler,
) {
	i.keySchema = keySchema
	i.suffixCmp = comparer.ComparePointSuffixes
	i.split = comparer.Split
	i.getLazyValuer = getLazyValuer
}

// Init initializes the data block iterator, configuring it to read from the
// provided decoder.
func (i *DataBlockIter) Init(d *DataBlockDecoder, transforms block.IterTransforms) error {
	i.d = d
	// Leave i.h unchanged.
	numRows := int(d.d.header.Rows)
	i.maxRow = numRows - 1
	i.transforms = transforms
	if i.transforms.HideObsoletePoints && d.isObsolete.SeekSetBitGE(0) == numRows {
		// There are no obsolete points in the block; don't bother checking.
		i.transforms.HideObsoletePoints = false
	}
	i.noTransforms = i.transforms.NoTransforms()

	// TODO(radu): see if this allocation can be a problem for the suffix rewriter.
	meta := &KeySeekerMetadata{}
	i.keySchema.InitKeySeekerMetadata(meta, d)
	i.keySeeker = i.keySchema.KeySeeker(meta)

	// The worst case is when the largest key in the block has no suffix.
	maxKeyLength := int(i.transforms.SyntheticPrefixAndSuffix.PrefixLen() + d.maximumKeyLength + i.transforms.SyntheticPrefixAndSuffix.SuffixLen())
	i.keyIter.Init(maxKeyLength, i.transforms.SyntheticPrefix())
	i.row = -1
	i.kv = base.InternalKV{}
	i.kvRow = math.MinInt
	i.nextObsoletePoint = 0
	return nil
}

// InitHandle initializes the block from the provided buffer handle. InitHandle
// assumes that the block's metadata was initialized using
// InitDataBlockMetadata().
func (i *DataBlockIter) InitHandle(
	comparer *base.Comparer, h block.BufferHandle, transforms block.IterTransforms,
) error {
	i.suffixCmp = comparer.ComparePointSuffixes
	i.split = comparer.Split
	blockMeta := h.BlockMetadata()
	i.d = (*DataBlockDecoder)(unsafe.Pointer(blockMeta))
	keySeekerMeta := (*KeySeekerMetadata)(blockMeta[unsafe.Sizeof(DataBlockDecoder{}):])
	i.h.Release()
	i.h = h

	numRows := int(i.d.d.header.Rows)
	i.maxRow = numRows - 1

	i.transforms = transforms
	if i.transforms.HideObsoletePoints && i.d.isObsolete.SeekSetBitGE(0) == numRows {
		// There are no obsolete points in the block; don't bother checking.
		i.transforms.HideObsoletePoints = false
	}
	i.noTransforms = i.transforms.NoTransforms()

	// The worst case is when the largest key in the block has no suffix.
	maxKeyLength := int(i.transforms.SyntheticPrefixAndSuffix.PrefixLen() + i.d.maximumKeyLength + i.transforms.SyntheticPrefixAndSuffix.SuffixLen())
	i.keyIter.Init(maxKeyLength, i.transforms.SyntheticPrefix())
	i.row = -1
	i.kv = base.InternalKV{}
	i.kvRow = math.MinInt
	i.nextObsoletePoint = 0
	i.keySeeker = i.keySchema.KeySeeker(keySeekerMeta)
	return nil
}

// Handle returns the handle to the block.
func (i *DataBlockIter) Handle() block.BufferHandle {
	return i.h
}

// Valid returns true if the iterator is currently positioned at a valid KV.
func (i *DataBlockIter) Valid() bool {
	return i.row >= 0 && i.row <= i.maxRow && !i.IsDataInvalidated()
}

// KV returns the key-value pair at the current iterator position. The
// iterator must be positioned over a valid KV.
func (i *DataBlockIter) KV() *base.InternalKV {
	return &i.kv
}

// Invalidate invalidates the block iterator, removing references to the block
// it was initialized with. The iterator may continue to be used after
// a call to Invalidate, but all positioning methods should return false.
// Valid() must also return false.
func (i *DataBlockIter) Invalidate() {
	i.d = nil
}

// IsDataInvalidated returns true when the iterator has been invalidated
// using an Invalidate call.
func (i *DataBlockIter) IsDataInvalidated() bool {
	return i.d == nil
}

// IsLowerBound implements the block.DataBlockIterator interface.
func (i *DataBlockIter) IsLowerBound(k []byte) bool {
	if i.transforms.HasSyntheticPrefix() {
		var keyPrefix []byte
		keyPrefix, k = splitKey(k, len(i.transforms.SyntheticPrefix()))
		if cmp := bytes.Compare(keyPrefix, i.transforms.SyntheticPrefix()); cmp != 0 {
			return cmp < 0
		}
	}
	// If we are hiding obsolete points, it is possible that all points < k are
	// hidden.
	// Note: we ignore HideObsoletePoints, but false negatives are allowed.
	return i.keySeeker.IsLowerBound(k, i.transforms.SyntheticSuffix())
}

// splitKey splits a key into k[:at] and k[at:].
func splitKey(k []byte, at int) (before, after []byte) {
	if len(k) <= at {
		return k, nil
	}
	return k[:at], k[at:]
}

// seekGEInternal is a wrapper around keySeeker.SeekGE which takes into account
// the synthetic prefix and suffix.
func (i *DataBlockIter) seekGEInternal(key []byte, boundRow int, searchDir int8) (row int) {
	if i.transforms.HasSyntheticPrefix() {
		var keyPrefix []byte
		keyPrefix, key = splitKey(key, len(i.transforms.SyntheticPrefix()))
		if cmp := bytes.Compare(keyPrefix, i.transforms.SyntheticPrefix()); cmp != 0 {
			if cmp < 0 {
				return 0
			}
			return i.maxRow + 1
		}
	}
	if i.transforms.HasSyntheticSuffix() {
		n := i.split(key)
		row, eq := i.keySeeker.SeekGE(key[:n], boundRow, searchDir)
		if eq && i.suffixCmp(key[n:], i.transforms.SyntheticSuffix()) > 0 {
			row = i.d.prefixChanged.SeekSetBitGE(row + 1)
		}
		return row
	}
	row, _ = i.keySeeker.SeekGE(key, boundRow, searchDir)
	return row
}

// SeekGE implements the base.InternalIterator interface.
func (i *DataBlockIter) SeekGE(key []byte, flags base.SeekGEFlags) *base.InternalKV {
	if i.d == nil {
		return nil
	}
	searchDir := int8(0)
	if flags.TrySeekUsingNext() {
		searchDir = +1
	}
	if i.noTransforms {
		// Fast path.
		i.row, _ = i.keySeeker.SeekGE(key, i.row, searchDir)
		return i.decodeRow()
	}
	i.row = i.seekGEInternal(key, i.row, searchDir)
	if i.transforms.HideObsoletePoints {
		i.nextObsoletePoint = i.d.isObsolete.SeekSetBitGE(i.row)
		if i.atObsoletePointForward() {
			i.skipObsoletePointsForward()
			if i.row > i.maxRow {
				return nil
			}
		}
	}
	return i.decodeRow()
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
	if i.d == nil {
		return nil
	}
	i.row = i.seekGEInternal(key, i.row, 0 /* searchDir */) - 1
	if i.transforms.HideObsoletePoints {
		i.nextObsoletePoint = i.d.isObsolete.SeekSetBitGE(max(i.row, 0))
		if i.atObsoletePointBackward() {
			i.skipObsoletePointsBackward()
			if i.row < 0 {
				return nil
			}
		}
	}
	return i.decodeRow()
}

// First implements the base.InternalIterator interface.
func (i *DataBlockIter) First() *base.InternalKV {
	if i.d == nil {
		return nil
	}
	i.row = 0
	if i.transforms.HideObsoletePoints {
		i.nextObsoletePoint = i.d.isObsolete.SeekSetBitGE(0)
		if i.atObsoletePointForward() {
			i.skipObsoletePointsForward()
			if i.row > i.maxRow {
				return nil
			}
		}
	}
	return i.decodeRow()
}

// Last implements the base.InternalIterator interface.
func (i *DataBlockIter) Last() *base.InternalKV {
	if i.d == nil {
		return nil
	}
	i.row = i.maxRow
	if i.transforms.HideObsoletePoints {
		i.nextObsoletePoint = i.maxRow + 1
		if i.atObsoletePointBackward() {
			i.skipObsoletePointsBackward()
			if i.row < 0 {
				return nil
			}
		}
	}
	return i.decodeRow()
}

// Next advances to the next KV pair in the block.
func (i *DataBlockIter) Next() *base.InternalKV {
	if i.d == nil {
		return nil
	}
	// Inline decodeRow, but avoiding unnecessary checks against i.row.
	if i.row >= i.maxRow {
		i.row = i.maxRow + 1
		return nil
	}
	i.row++
	// Inline decodeKey(), adding obsolete points logic.
	if i.noTransforms {
		// Fast path.
		i.kv.K = base.InternalKey{
			UserKey: i.keySeeker.MaterializeUserKey(&i.keyIter, i.kvRow, i.row),
			Trailer: base.InternalKeyTrailer(i.d.trailers.At(i.row)),
		}
	} else {
		if i.transforms.HideObsoletePoints && i.atObsoletePointForward() {
			i.skipObsoletePointsForward()
			if i.row > i.maxRow {
				return nil
			}
		}
		if i.transforms.HasSyntheticSuffix() {
			i.kv.K.UserKey = i.keySeeker.MaterializeUserKeyWithSyntheticSuffix(
				&i.keyIter, i.transforms.SyntheticSuffix(), i.kvRow, i.row,
			)
		} else {
			i.kv.K.UserKey = i.keySeeker.MaterializeUserKey(&i.keyIter, i.kvRow, i.row)
		}
		i.kv.K.Trailer = base.InternalKeyTrailer(i.d.trailers.At(i.row))
		if n := i.transforms.SyntheticSeqNum; n != 0 {
			i.kv.K.SetSeqNum(base.SeqNum(n))
		}
	}
	// Inline i.d.values.At(row).
	v := i.d.values.slice(i.d.values.offsets.At2(i.row))
	if i.d.isValueExternal.At(i.row) {
		i.kv.V = i.getLazyValuer.GetInternalValueForPrefixAndValueHandle(v)
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
// shifting and masking combined with bits.TrailingZeros64 can identify the
// next bit that is set after the current row. The bitmap uses 1 bit/row (0.125
// bytes/row). A 32KB block containing 1300 rows (25 bytes/row) would need a
// bitmap of 21 64-bit words. Even in the worst case where every word is 0 this
// bitmap can be scanned in ~20 ns (1 ns/word) leading to a total NextPrefix
// time of ~30 ns if a row is found and decodeRow are called. In more normal
// cases, NextPrefix takes ~15% longer that a single Next call.
//
// For comparison, the rowblk nextPrefixV3 optimizations work by setting a bit
// in the value prefix byte that indicates that the current key has the same
// prefix as the previous key. Additionally, a bit is stolen from the restart
// table entries indicating whether a restart table entry has the same key
// prefix as the previous entry. Checking the value prefix byte bit requires
// locating that byte which requires decoding 3 varints per key/value pair.
func (i *DataBlockIter) NextPrefix(_ []byte) *base.InternalKV {
	if i.d == nil {
		return nil
	}
	i.row = i.d.prefixChanged.SeekSetBitGE(i.row + 1)
	if i.transforms.HideObsoletePoints {
		i.nextObsoletePoint = i.d.isObsolete.SeekSetBitGE(i.row)
		if i.atObsoletePointForward() {
			i.skipObsoletePointsForward()
		}
	}

	return i.decodeRow()
}

// Prev moves the iterator to the previous KV pair in the block.
func (i *DataBlockIter) Prev() *base.InternalKV {
	if i.d == nil {
		return nil
	}
	i.row--
	if i.transforms.HideObsoletePoints && i.atObsoletePointBackward() {
		i.skipObsoletePointsBackward()
		if i.row < 0 {
			return nil
		}
	}
	return i.decodeRow()
}

// atObsoletePointForward returns true if i.row is an obsolete point. It is
// separate from skipObsoletePointsForward() because that method does not
// inline. It can only be used during forward iteration (i.e. i.row was
// incremented).
//
//gcassert:inline
func (i *DataBlockIter) atObsoletePointForward() bool {
	if invariants.Enabled && i.row > i.nextObsoletePoint {
		panic("invalid nextObsoletePoint")
	}
	return i.row == i.nextObsoletePoint && i.row <= i.maxRow
}

func (i *DataBlockIter) skipObsoletePointsForward() {
	if invariants.Enabled {
		i.atObsoletePointCheck()
	}
	i.row = i.d.isObsolete.SeekUnsetBitGE(i.row)
	i.nextObsoletePoint = i.d.isObsolete.SeekSetBitGE(i.row)
}

// atObsoletePointBackward returns true if i.row is an obsolete point. It is
// separate from skipObsoletePointsBackward() because that method does not
// inline. It can only be used during reverse iteration (i.e. i.row was
// decremented).
//
//gcassert:inline
func (i *DataBlockIter) atObsoletePointBackward() bool {
	return i.row >= 0 && i.d.isObsolete.At(i.row)
}

func (i *DataBlockIter) skipObsoletePointsBackward() {
	if invariants.Enabled {
		i.atObsoletePointCheck()
	}
	i.row = i.d.isObsolete.SeekUnsetBitLE(i.row)
	i.nextObsoletePoint = i.row + 1
}

func (i *DataBlockIter) atObsoletePointCheck() {
	// We extract this code into a separate function to avoid getting a spurious
	// error from GCAssert about At not being inlined because it is compiled out
	// altogether in non-invariant builds.
	if !i.transforms.HideObsoletePoints || !i.d.isObsolete.At(i.row) {
		panic("expected obsolete point")
	}
}

// Error implements the base.InternalIterator interface. A DataBlockIter is
// infallible and always returns a nil error.
func (i *DataBlockIter) Error() error {
	return nil // infallible
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

// decodeRow decodes i.row into i.kv. If i.row is invalid, it returns nil.
func (i *DataBlockIter) decodeRow() *base.InternalKV {
	switch {
	case i.row < 0 || i.row > i.maxRow:
		return nil
	case i.kvRow == i.row:
		// Already synthesized the kv at row.
		return &i.kv
	default:
		// Inline decodeKey().
		if i.noTransforms {
			// Fast path.
			i.kv.K = base.InternalKey{
				UserKey: i.keySeeker.MaterializeUserKey(&i.keyIter, i.kvRow, i.row),
				Trailer: base.InternalKeyTrailer(i.d.trailers.At(i.row)),
			}
		} else {
			if i.transforms.HasSyntheticSuffix() {
				i.kv.K.UserKey = i.keySeeker.MaterializeUserKeyWithSyntheticSuffix(
					&i.keyIter, i.transforms.SyntheticSuffix(), i.kvRow, i.row,
				)
			} else {
				i.kv.K.UserKey = i.keySeeker.MaterializeUserKey(&i.keyIter, i.kvRow, i.row)
			}
			i.kv.K.Trailer = base.InternalKeyTrailer(i.d.trailers.At(i.row))
			if n := i.transforms.SyntheticSeqNum; n != 0 {
				i.kv.K.SetSeqNum(base.SeqNum(n))
			}
		}
		// Inline i.d.values.At(row).
		startOffset := i.d.values.offsets.At(i.row)
		v := unsafe.Slice((*byte)(i.d.values.ptr(startOffset)), i.d.values.offsets.At(i.row+1)-startOffset)
		if i.d.isValueExternal.At(i.row) {
			i.kv.V = i.getLazyValuer.GetInternalValueForPrefixAndValueHandle(v)
		} else {
			i.kv.V = base.MakeInPlaceValue(v)
		}
		i.kvRow = i.row
		return &i.kv
	}
}

// decodeKey updates i.kv.K to the key for i.row (which must be valid).
// This function does not inline, so we copy its code verbatim. For any updates
// to this code, all code preceded by "Inline decodeKey" must be updated.
func (i *DataBlockIter) decodeKey() {
	if i.noTransforms {
		// Fast path.
		i.kv.K = base.InternalKey{
			UserKey: i.keySeeker.MaterializeUserKey(&i.keyIter, i.kvRow, i.row),
			Trailer: base.InternalKeyTrailer(i.d.trailers.At(i.row)),
		}
	} else {
		if i.transforms.HasSyntheticSuffix() {
			i.kv.K.UserKey = i.keySeeker.MaterializeUserKeyWithSyntheticSuffix(
				&i.keyIter, i.transforms.SyntheticSuffix(), i.kvRow, i.row,
			)
		} else {
			i.kv.K.UserKey = i.keySeeker.MaterializeUserKey(&i.keyIter, i.kvRow, i.row)
		}
		i.kv.K.Trailer = base.InternalKeyTrailer(i.d.trailers.At(i.row))
		if n := i.transforms.SyntheticSeqNum; n != 0 {
			i.kv.K.SetSeqNum(base.SeqNum(n))
		}
	}
}

var _ = (*DataBlockIter).decodeKey

// Close implements the base.InternalIterator interface.
func (i *DataBlockIter) Close() error {
	i.keySeeker = nil
	i.d = nil
	i.h.Release()
	i.h = block.BufferHandle{}
	i.transforms = block.IterTransforms{}
	i.kv = base.InternalKV{}
	return nil
}
