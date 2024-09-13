// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/cockroachdb/crlib/crbytes"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/crdbtest"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

const (
	cockroachColPrefix int = iota
	cockroachColMVCCWallTime
	cockroachColMVCCLogical
	cockroachColUntypedSuffix
	cockroachColCount
)

var cockroachKeySchema = KeySchema{
	ColumnTypes: []DataType{
		cockroachColPrefix:        DataTypePrefixBytes,
		cockroachColMVCCWallTime:  DataTypeUint,
		cockroachColMVCCLogical:   DataTypeUint,
		cockroachColUntypedSuffix: DataTypeBytes,
	},
	NewKeyWriter: func() KeyWriter {
		kw := &cockroachKeyWriter{}
		kw.prefixes.Init(16)
		kw.wallTimes.Init()
		kw.logicalTimes.InitWithDefault()
		kw.untypedSuffixes.Init()
		return kw
	},
	NewKeySeeker: func() KeySeeker {
		return &cockroachKeySeeker{}
	},
}

type cockroachKeyWriter struct {
	prefixes        PrefixBytesBuilder
	wallTimes       UintBuilder
	logicalTimes    UintBuilder
	untypedSuffixes RawBytesBuilder
	prevSuffix      []byte
}

func (kw *cockroachKeyWriter) ComparePrev(key []byte) KeyComparison {
	lp := kw.prefixes.UnsafeGet(kw.prefixes.Rows() - 1)
	var cmpv KeyComparison
	cmpv.PrefixLen = int32(crdbtest.Split(key)) // TODO(jackson): Inline
	cmpv.CommonPrefixLen = int32(crbytes.CommonPrefix(lp, key[:cmpv.PrefixLen]))
	if cmpv.CommonPrefixLen == cmpv.PrefixLen {
		cmpv.UserKeyComparison = int32(crdbtest.CompareSuffixes(key[cmpv.PrefixLen:], kw.prevSuffix))
		return cmpv
	}
	// The keys have different MVCC prefixes. We haven't determined which is
	// greater, but we know the index at which they diverge. The base.Comparer
	// contract dictates that prefixes must be lexicograrphically ordered.
	if len(lp) == int(cmpv.CommonPrefixLen) {
		// cmpv.PrefixLen > cmpv.PrefixLenShared; key is greater.
		cmpv.UserKeyComparison = +1
	} else {
		// Both keys have at least 1 additional byte at which they diverge.
		// Compare the diverging byte.
		cmpv.UserKeyComparison = int32(cmp.Compare(key[cmpv.CommonPrefixLen], lp[cmpv.CommonPrefixLen]))
	}
	return cmpv
}

func (kw *cockroachKeyWriter) WriteKey(
	row int, key []byte, keyPrefixLen, keyPrefixLenSharedWithPrev int32,
) {
	// TODO(jackson): Avoid copying the previous suffix.
	// TODO(jackson): Use keyPrefixLen to speed up decoding.
	prefix, untypedSuffix, wallTime, logicalTime := crdbtest.DecodeTimestamp(key)
	kw.prevSuffix = append(kw.prevSuffix[:0], key[keyPrefixLen:]...)
	kw.prefixes.Put(prefix, int(keyPrefixLenSharedWithPrev))
	kw.wallTimes.Set(row, wallTime)
	// The w.logicalTimes builder was initialized with InitWithDefault, so if we
	// don't set a value, the column value is implicitly zero. We only need to
	// Set anything for non-zero values.
	if logicalTime > 0 {
		kw.logicalTimes.Set(row, uint64(logicalTime))
	}
	kw.untypedSuffixes.Put(untypedSuffix)
}

func (kw *cockroachKeyWriter) MaterializeKey(dst []byte, i int) []byte {
	dst = append(dst, kw.prefixes.UnsafeGet(i)...)
	if untypedSuffixed := kw.untypedSuffixes.UnsafeGet(i); len(untypedSuffixed) > 0 {
		return append(dst, untypedSuffixed...)
	}
	return crdbtest.AppendTimestamp(dst, kw.wallTimes.Get(i), uint32(kw.logicalTimes.Get(i)))
}

func (kw *cockroachKeyWriter) Reset() {
	kw.prefixes.Reset()
	kw.wallTimes.Reset()
	kw.logicalTimes.Reset()
	kw.untypedSuffixes.Reset()
}

func (kw *cockroachKeyWriter) WriteDebug(dst io.Writer, rows int) {
	fmt.Fprint(dst, "prefixes: ")
	kw.prefixes.WriteDebug(dst, rows)
	fmt.Fprintln(dst)
	fmt.Fprint(dst, "wall times: ")
	kw.wallTimes.WriteDebug(dst, rows)
	fmt.Fprintln(dst)
	fmt.Fprint(dst, "logical times: ")
	kw.logicalTimes.WriteDebug(dst, rows)
	fmt.Fprintln(dst)
	fmt.Fprint(dst, "untyped suffixes: ")
	kw.untypedSuffixes.WriteDebug(dst, rows)
	fmt.Fprintln(dst)
}

func (kw *cockroachKeyWriter) NumColumns() int {
	return cockroachColCount
}

func (kw *cockroachKeyWriter) DataType(col int) DataType {
	return cockroachKeySchema.ColumnTypes[col]
}

func (kw *cockroachKeyWriter) Size(rows int, offset uint32) uint32 {
	offset = kw.prefixes.Size(rows, offset)
	offset = kw.wallTimes.Size(rows, offset)
	offset = kw.logicalTimes.Size(rows, offset)
	offset = kw.untypedSuffixes.Size(rows, offset)
	return offset
}

func (kw *cockroachKeyWriter) Finish(
	col int, rows int, offset uint32, buf []byte,
) (endOffset uint32) {
	switch col {
	case cockroachColPrefix:
		return kw.prefixes.Finish(0, rows, offset, buf)
	case cockroachColMVCCWallTime:
		return kw.wallTimes.Finish(0, rows, offset, buf)
	case cockroachColMVCCLogical:
		return kw.logicalTimes.Finish(0, rows, offset, buf)
	case cockroachColUntypedSuffix:
		return kw.untypedSuffixes.Finish(0, rows, offset, buf)
	default:
		panic(fmt.Sprintf("unknown default key column: %d", col))
	}
}

var cockroachKeySeekerPool = sync.Pool{
	New: func() interface{} { return &cockroachKeySeeker{} },
}

type cockroachKeySeeker struct {
	reader          *DataBlockReader
	prefixes        PrefixBytes
	mvccWallTimes   UnsafeUints
	mvccLogical     UnsafeUints
	untypedSuffixes RawBytes
	sharedPrefix    []byte
}

var _ KeySeeker = (*cockroachKeySeeker)(nil)

// Init is part of the KeySeeker interface.
func (ks *cockroachKeySeeker) Init(r *DataBlockReader) error {
	ks.reader = r
	ks.prefixes = r.r.PrefixBytes(cockroachColPrefix)
	ks.mvccWallTimes = r.r.Uints(cockroachColMVCCWallTime)
	ks.mvccLogical = r.r.Uints(cockroachColMVCCLogical)
	ks.untypedSuffixes = r.r.RawBytes(cockroachColUntypedSuffix)
	ks.sharedPrefix = ks.prefixes.SharedPrefix()
	return nil
}

// SeekGE is part of the KeySeeker interface.
func (ks *cockroachKeySeeker) SeekGE(key []byte, currRow int, dir int8) (row int) {
	// TODO(jackson): Inline crdbtest.Split.
	si := crdbtest.Split(key)
	row, eq := ks.prefixes.Search(key[:si-1])
	if eq {
		return ks.seekGEOnSuffix(row, key[si:])
	}
	return row
}

// seekGEOnSuffix is a helper function for SeekGE when a seek key's prefix
// exactly matches a row. seekGEOnSuffix finds the first row at index or later
// with the same prefix as index and a suffix greater than or equal to [suffix],
// or if no such row exists, the next row with a different prefix.
func (ks *cockroachKeySeeker) seekGEOnSuffix(index int, seekSuffix []byte) (row int) {
	// The search key's prefix exactly matches the prefix of the row at index.
	const withWall = 9
	const withLogical = withWall + 4
	const withSynthetic = withLogical + 1
	var seekWallTime uint64
	var seekLogicalTime uint32
	switch len(seekSuffix) {
	case 0:
		// The search key has no suffix, so it's the smallest possible key with
		// its prefix. Return the row. This is a common case where the user is
		// seeking to the most-recent row and just wants the smallest key with
		// the prefix.
		return index
	case withLogical, withSynthetic:
		seekWallTime = binary.LittleEndian.Uint64(seekSuffix)
		seekLogicalTime = binary.LittleEndian.Uint32(seekSuffix[8:])
	case withWall:
		seekWallTime = binary.LittleEndian.Uint64(seekSuffix)
	default:
		// The suffix is untyped. Compare the untyped suffixes.
		// Binary search between [index, prefixChanged.Successor(index)].
		//
		// Define f(l-1) == false and f(u) == true.
		// Invariant: f(l-1) == false, f(u) == true.
		l := index
		u := ks.reader.prefixChanged.Successor(index)
		for l < u {
			h := int(uint(l+u) >> 1) // avoid overflow when computing h
			// l ≤ h < u
			if bytes.Compare(ks.untypedSuffixes.At(h), seekSuffix) >= 0 {
				u = h // preserves f(u) == true
			} else {
				l = h + 1 // preserves f(l-1) == false
			}
		}
		return l
	}
	// Seeking among MVCC versions using a MVCC timestamp.

	// TODO(jackson): What if the row has an untyped suffix?

	// Binary search between [index, prefixChanged.Successor(index)].
	//
	// Define f(l-1) == false and f(u) == true.
	// Invariant: f(l-1) == false, f(u) == true.
	l := index
	u := ks.reader.prefixChanged.Successor(index)
	for l < u {
		h := int(uint(l+u) >> 1) // avoid overflow when computing h
		// l ≤ h < u
		switch cmp.Compare(ks.mvccWallTimes.At(h), seekWallTime) {
		case -1:
			l = h + 1 // preserves f(l-1) == false
		case +1:
			u = h // preserves f(u) == true
		}
		if cmp.Compare(uint32(ks.mvccLogical.At(h)), seekLogicalTime) >= 0 {
			u = h // preserves f(u) == true
		} else {
			l = h + 1 // preserves f(l-1) == false
		}
	}
	return l
}

// MaterializeUserKey is part of the KeySeeker interface.
func (ks *cockroachKeySeeker) MaterializeUserKey(ki *PrefixBytesIter, prevRow, row int) []byte {
	if prevRow+1 == row && prevRow >= 0 {
		ks.prefixes.SetNext(ki)
	} else {
		ks.prefixes.SetAt(ki, row)
	}

	ptr := unsafe.Pointer(uintptr(unsafe.Pointer(unsafe.SliceData(ki.buf))) + uintptr(len(ki.buf)))
	mvccWall := ks.mvccWallTimes.At(row)
	mvccLogical := uint32(ks.mvccLogical.At(row))
	if mvccWall == 0 && mvccLogical == 0 {
		// This is not an MVCC key. Use the untyped suffix.
		untypedSuffixed := ks.untypedSuffixes.At(row)
		res := ki.buf[:len(ki.buf)+len(untypedSuffixed)]
		memmove(ptr, unsafe.Pointer(unsafe.SliceData(untypedSuffixed)), uintptr(len(untypedSuffixed)))
		return res
	}

	// Inline binary.BigEndian.PutUint64. Note that this code is converted into
	// word-size instructions by the compiler.
	*(*byte)(ptr) = byte(mvccWall >> 56)
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 1)) = byte(mvccWall >> 48)
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 2)) = byte(mvccWall >> 40)
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 3)) = byte(mvccWall >> 32)
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 4)) = byte(mvccWall >> 24)
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 5)) = byte(mvccWall >> 16)
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 6)) = byte(mvccWall >> 8)
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 7)) = byte(mvccWall)

	ptr = unsafe.Pointer(uintptr(ptr) + 8)
	// This is an MVCC key.
	if mvccLogical == 0 {
		*(*byte)(ptr) = 9
		return ki.buf[:len(ki.buf)+9]
	}

	// Inline binary.BigEndian.PutUint32.
	*(*byte)(ptr) = byte(mvccWall >> 24)
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 1)) = byte(mvccWall >> 16)
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 2)) = byte(mvccWall >> 8)
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 3)) = byte(mvccWall)
	*(*byte)(unsafe.Pointer(uintptr(ptr) + 4)) = 13
	return ki.buf[:len(ki.buf)+13]
}

// Release is part of the KeySeeker interface.
func (ks *cockroachKeySeeker) Release() {
	*ks = cockroachKeySeeker{}
	cockroachKeySeekerPool.Put(ks)
}

func TestCockroachDataBlock(t *testing.T) {
	const targetBlockSize = 32 << 10
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewSource(seed))
	keys, values := crdbtest.RandomKVs(rng, targetBlockSize/100, crdbtest.KeyConfig{
		PrefixAlphabetLen: 26,
		PrefixLen:         12,
		BaseWallTime:      seed,
	}, 100)

	var w DataBlockWriter
	w.Init(cockroachKeySchema)
	var count int
	for w.Size() < targetBlockSize {
		ik := base.MakeInternalKey(keys[count], base.SeqNum(rng.Uint64n(uint64(base.SeqNumMax))), base.InternalKeyKindSet)
		kcmp := w.KeyWriter.ComparePrev(ik.UserKey)
		w.Add(ik, values[count], block.InPlaceValuePrefix(kcmp.PrefixEqual()), kcmp, false /* isObsolete */)
		count++
	}
	serializedBlock, _ := w.Finish(w.Rows(), w.Size())
	var reader DataBlockReader
	var it DataBlockIter
	reader.Init(cockroachKeySchema, serializedBlock)
	it.Init(&reader, cockroachKeySchema.NewKeySeeker(), func([]byte) base.LazyValue {
		return base.LazyValue{ValueOrHandle: []byte("mock external value")}
	})

	t.Run("Next", func(t *testing.T) {
		// Scan the block using Next and ensure that all the keys values match.
		i := 0
		for kv := it.First(); kv != nil; i, kv = i+1, it.Next() {
			if !bytes.Equal(kv.K.UserKey, keys[i]) {
				t.Fatalf("expected %q, but found %q", keys[i], kv.K.UserKey)
			}
			if !bytes.Equal(kv.V.InPlaceValue(), values[i]) {
				t.Fatalf("expected %x, but found %x", values[i], kv.V.InPlaceValue())
			}
		}
		require.Equal(t, count, i)
	})
	t.Run("SeekGE", func(t *testing.T) {
		rng := rand.New(rand.NewSource(seed))
		for _, i := range rng.Perm(count) {
			kv := it.SeekGE(keys[i], base.SeekGEFlagsNone)
			if kv == nil {
				t.Fatalf("%q not found", keys[i])
			}
			if !bytes.Equal(kv.V.InPlaceValue(), values[i]) {
				t.Fatalf("expected %x, but found %x", values[i], kv.V.InPlaceValue())
			}
		}
	})
}

func BenchmarkCockroachDataBlockWriter(b *testing.B) {
	for _, alphaLen := range []int{4, 8, 26} {
		for _, lenSharedPct := range []float64{0.25, 0.5} {
			for _, prefixLen := range []int{8, 32, 128} {
				lenShared := int(float64(prefixLen) * lenSharedPct)
				for _, valueLen := range []int{8, 128, 1024} {
					keyConfig := crdbtest.KeyConfig{
						PrefixAlphabetLen: alphaLen,
						PrefixLen:         prefixLen,
						PrefixLenShared:   lenShared,
						Logical:           0,
						BaseWallTime:      uint64(time.Now().UnixNano()),
					}
					b.Run(fmt.Sprintf("%s,valueLen=%d", keyConfig, valueLen), func(b *testing.B) {
						benchmarkCockroachDataBlockWriter(b, keyConfig, valueLen)
					})
				}
			}
		}
	}
}

func benchmarkCockroachDataBlockWriter(b *testing.B, keyConfig crdbtest.KeyConfig, valueLen int) {
	const targetBlockSize = 32 << 10
	seed := uint64(time.Now().UnixNano())
	rng := rand.New(rand.NewSource(seed))
	keys, values := crdbtest.RandomKVs(rng, targetBlockSize/valueLen, keyConfig, valueLen)

	var w DataBlockWriter
	w.Init(cockroachKeySchema)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.Reset()
		var count int
		for w.Size() < targetBlockSize {
			ik := base.MakeInternalKey(keys[count], base.SeqNum(rng.Uint64n(uint64(base.SeqNumMax))), base.InternalKeyKindSet)
			kcmp := w.KeyWriter.ComparePrev(ik.UserKey)
			w.Add(ik, values[count], block.InPlaceValuePrefix(kcmp.PrefixEqual()), kcmp, false /* isObsolete */)
			count++
		}
		_, _ = w.Finish(w.Rows(), w.Size())
	}
}

func BenchmarkCockroachDataBlockIter(b *testing.B) {
	for _, alphaLen := range []int{4, 8, 26} {
		for _, lenSharedPct := range []float64{0.25, 0.5} {
			for _, prefixLen := range []int{8, 32, 128} {
				lenShared := int(float64(prefixLen) * lenSharedPct)
				for _, logical := range []uint32{0, 1} {
					for _, valueLen := range []int{8, 128, 1024} {
						keyConfig := crdbtest.KeyConfig{
							PrefixAlphabetLen: alphaLen,
							PrefixLen:         prefixLen,
							PrefixLenShared:   lenShared,
							Logical:           logical,
							BaseWallTime:      uint64(time.Now().UnixNano()),
						}
						b.Run(fmt.Sprintf("%s,value=%d", keyConfig, valueLen),
							func(b *testing.B) {
								benchmarkCockroachDataBlockIter(b, keyConfig, valueLen)
							})
					}
				}
			}
		}
	}
}

func benchmarkCockroachDataBlockIter(b *testing.B, keyConfig crdbtest.KeyConfig, valueLen int) {
	const targetBlockSize = 32 << 10
	seed := uint64(time.Now().UnixNano())
	rng := rand.New(rand.NewSource(seed))
	keys, values := crdbtest.RandomKVs(rng, targetBlockSize/valueLen, keyConfig, valueLen)

	var w DataBlockWriter
	w.Init(cockroachKeySchema)
	var count int
	for w.Size() < targetBlockSize {
		ik := base.MakeInternalKey(keys[count], base.SeqNum(rng.Uint64n(uint64(base.SeqNumMax))), base.InternalKeyKindSet)
		kcmp := w.KeyWriter.ComparePrev(ik.UserKey)
		w.Add(ik, values[count], block.InPlaceValuePrefix(kcmp.PrefixEqual()), kcmp, false /* isObsolete */)
		count++
	}
	serializedBlock, _ := w.Finish(w.Rows(), w.Size())
	var reader DataBlockReader
	var it DataBlockIter
	reader.Init(cockroachKeySchema, serializedBlock)
	it.Init(&reader, cockroachKeySchema.NewKeySeeker(), func([]byte) base.LazyValue {
		return base.LazyValue{ValueOrHandle: []byte("mock external value")}
	})
	avgRowSize := float64(len(serializedBlock)) / float64(count)

	b.Run("Next", func(b *testing.B) {
		kv := it.First()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if kv == nil {
				kv = it.First()
			} else {
				kv = it.Next()
			}
		}
		b.StopTimer()
		b.ReportMetric(avgRowSize, "bytes/row")
	})
	b.Run("SeekGE", func(b *testing.B) {
		rng := rand.New(rand.NewSource(seed))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			k := keys[rng.Intn(count)]
			if kv := it.SeekGE(k, base.SeekGEFlagsNone); kv == nil {
				b.Fatalf("%q not found", k)
			}
		}
		b.StopTimer()
		b.ReportMetric(avgRowSize, "bytes/row")
	})
}
