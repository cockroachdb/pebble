// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"slices"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/bytealloc"
	"github.com/cockroachdb/pebble/internal/sstableinternal"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/colblk"
	"github.com/cockroachdb/pebble/sstable/rowblk"
)

// Layout describes the block organization of an sstable.
type Layout struct {
	// NOTE: changes to fields in this struct should also be reflected in
	// ValidateBlockChecksums, which validates a static list of BlockHandles
	// referenced in this struct.

	Data       []block.HandleWithProperties
	Index      []block.Handle
	TopIndex   block.Handle
	Filter     []NamedBlockHandle
	RangeDel   block.Handle
	RangeKey   block.Handle
	ValueBlock []block.Handle
	ValueIndex block.Handle
	Properties block.Handle
	MetaIndex  block.Handle
	Footer     block.Handle
	Format     TableFormat
}

// NamedBlockHandle holds a block.Handle and corresponding name.
type NamedBlockHandle struct {
	block.Handle
	Name string
}

// FilterByName retrieves the block handle of the named filter, if it exists.
// The provided the name should be the name as it appears in the metaindex
// block.
func (l *Layout) FilterByName(name string) (block.Handle, bool) {
	for i := range l.Filter {
		if l.Filter[i].Name == name {
			return l.Filter[i].Handle, true
		}
	}
	return block.Handle{}, false
}

func (l *Layout) orderedBlocks() []NamedBlockHandle {
	var blocks []NamedBlockHandle
	for i := range l.Data {
		blocks = append(blocks, NamedBlockHandle{l.Data[i].Handle, "data"})
	}
	for i := range l.Index {
		blocks = append(blocks, NamedBlockHandle{l.Index[i], "index"})
	}
	if l.TopIndex.Length != 0 {
		blocks = append(blocks, NamedBlockHandle{l.TopIndex, "top-index"})
	}
	blocks = append(blocks, l.Filter...)
	if l.RangeDel.Length != 0 {
		blocks = append(blocks, NamedBlockHandle{l.RangeDel, "range-del"})
	}
	if l.RangeKey.Length != 0 {
		blocks = append(blocks, NamedBlockHandle{l.RangeKey, "range-key"})
	}
	for i := range l.ValueBlock {
		blocks = append(blocks, NamedBlockHandle{l.ValueBlock[i], "value-block"})
	}
	if l.ValueIndex.Length != 0 {
		blocks = append(blocks, NamedBlockHandle{l.ValueIndex, "value-index"})
	}
	if l.Properties.Length != 0 {
		blocks = append(blocks, NamedBlockHandle{l.Properties, "properties"})
	}
	if l.MetaIndex.Length != 0 {
		blocks = append(blocks, NamedBlockHandle{l.MetaIndex, "meta-index"})
	}
	if l.Footer.Length != 0 {
		if l.Footer.Length == levelDBFooterLen {
			blocks = append(blocks, NamedBlockHandle{l.Footer, "leveldb-footer"})
		} else {
			blocks = append(blocks, NamedBlockHandle{l.Footer, "footer"})
		}
	}
	slices.SortFunc(blocks, func(a, b NamedBlockHandle) int {
		return cmp.Compare(a.Offset, b.Offset)
	})
	return blocks
}

// Describe returns a description of the layout. If the verbose parameter is
// true, details of the structure of each block are returned as well.
func (l *Layout) Describe(
	w io.Writer, verbose bool, r *Reader, fmtRecord func(key *base.InternalKey, value []byte),
) {
	ctx := context.TODO()

	blocks := l.orderedBlocks()
	// TODO(jackson): This function formats offsets within blocks by adding the
	// block's offset. A block's offset is an offset in the physical, compressed
	// file whereas KV pairs offsets are within the uncompressed block. This is
	// confusing and can result in blocks' KVs offsets overlapping one another.
	// We should just print offsets relative to the block start.

	formatting := rowblkFormatting
	if l.Format.BlockColumnar() {
		formatting = colblkFormatting
	}

	for i := range blocks {
		b := &blocks[i]
		fmt.Fprintf(w, "%10d  %s (%d)\n", b.Offset, b.Name, b.Length)

		if !verbose {
			continue
		}
		if b.Name == "filter" {
			continue
		}

		if b.Name == "footer" || b.Name == "leveldb-footer" {
			trailer, offset := make([]byte, b.Length), b.Offset
			_ = r.readable.ReadAt(ctx, trailer, int64(offset))

			if b.Name == "footer" {
				checksumType := block.ChecksumType(trailer[0])
				fmt.Fprintf(w, "%10d    checksum type: %s\n", offset, checksumType)
				trailer, offset = trailer[1:], offset+1
			}

			metaHandle, n := binary.Uvarint(trailer)
			metaLen, m := binary.Uvarint(trailer[n:])
			fmt.Fprintf(w, "%10d    meta: offset=%d, length=%d\n", offset, metaHandle, metaLen)
			trailer, offset = trailer[n+m:], offset+uint64(n+m)

			indexHandle, n := binary.Uvarint(trailer)
			indexLen, m := binary.Uvarint(trailer[n:])
			fmt.Fprintf(w, "%10d    index: offset=%d, length=%d\n", offset, indexHandle, indexLen)
			trailer, offset = trailer[n+m:], offset+uint64(n+m)

			fmt.Fprintf(w, "%10d    [padding]\n", offset)

			trailing := 12
			if b.Name == "leveldb-footer" {
				trailing = 8
			}

			offset += uint64(len(trailer) - trailing)
			trailer = trailer[len(trailer)-trailing:]

			if b.Name == "footer" {
				version := trailer[:4]
				fmt.Fprintf(w, "%10d    version: %d\n", offset, binary.LittleEndian.Uint32(version))
				trailer, offset = trailer[4:], offset+4
			}

			magicNumber := trailer
			fmt.Fprintf(w, "%10d    magic number: 0x%x\n", offset, magicNumber)

			continue
		}

		h, err := r.readBlock(
			context.Background(), b.Handle, nil /* transform */, nil /* readHandle */, nil /* stats */, nil /* iterStats */, nil /* buffer pool */)
		if err != nil {
			fmt.Fprintf(w, "  [err: %s]\n", err)
			continue
		}
		err = func() error {
			// Defer release of the block handle so we always return the handle
			// to the cache.
			defer h.Release()

			formatTrailer := func() {
				trailer := make([]byte, block.TrailerLen)
				offset := int64(b.Offset + b.Length)
				_ = r.readable.ReadAt(ctx, trailer, offset)
				algo := block.CompressionIndicator(trailer[0])
				checksum := binary.LittleEndian.Uint32(trailer[1:])
				fmt.Fprintf(w, "%10d    [trailer compression=%s checksum=0x%04x]\n", offset, algo, checksum)
			}

			var lastKey InternalKey
			switch b.Name {
			case "data":
				formatting.formatDataBlock(w, r, *b, h.Get(), func(key *base.InternalKey, value []byte) {
					if fmtRecord != nil {
						fmtRecord(key, value)
					}
					if base.InternalCompare(r.Compare, lastKey, *key) >= 0 {
						fmt.Fprintf(w, "              WARNING: OUT OF ORDER KEYS!\n")
					}
					lastKey.Trailer = key.Trailer
					lastKey.UserKey = append(lastKey.UserKey[:0], key.UserKey...)
				})
				formatTrailer()
			case "range-del", "range-key":
				// TODO(jackson): colblk ignores fmtRecord, because it doesn't
				// make sense in the context.
				formatting.formatKeyspanBlock(w, r, *b, h.Get(), fmtRecord)
				formatTrailer()
			case "index", "top-index":
				formatting.formatIndexBlock(w, r, *b, h.Get())
				formatTrailer()
			case "properties":
				iter, _ := rowblk.NewRawIter(r.Compare, h.Get())
				iter.Describe(w, b.Offset,
					func(w io.Writer, key *base.InternalKey, value []byte, enc rowblk.KVEncoding) {
						fmt.Fprintf(w, "%10d    %s (%d)", b.Offset+uint64(enc.Offset), key.UserKey, enc.Length)
					})
				formatTrailer()
			case "meta-index":
				iter, _ := rowblk.NewRawIter(r.Compare, h.Get())
				iter.Describe(w, b.Offset,
					func(w io.Writer, key *base.InternalKey, value []byte, enc rowblk.KVEncoding) {
						var bh block.Handle
						var n int
						var vbih valueBlocksIndexHandle
						isValueBlocksIndexHandle := false
						if bytes.Equal(iter.Key().UserKey, []byte(metaValueIndexName)) {
							vbih, n, err = decodeValueBlocksIndexHandle(value)
							bh = vbih.h
							isValueBlocksIndexHandle = true
						} else {
							bh, n = block.DecodeHandle(value)
						}
						if n == 0 || n != len(value) {
							fmt.Fprintf(w, "%10d    [err: %s]\n", enc.Offset, err)
							return
						}
						var vbihStr string
						if isValueBlocksIndexHandle {
							vbihStr = fmt.Sprintf(" value-blocks-index-lengths: %d(num), %d(offset), %d(length)",
								vbih.blockNumByteLength, vbih.blockOffsetByteLength, vbih.blockLengthByteLength)
						}
						fmt.Fprintf(w, "%10d    %s block:%d/%d%s",
							b.Offset+uint64(enc.Offset), iter.Key().UserKey, bh.Offset, bh.Length, vbihStr)
					})
				formatTrailer()
			case "value-block":
				// We don't peer into the value-block since it can't be interpreted
				// without the valueHandles.
			case "value-index":
				// We have already read the value-index to construct the list of
				// value-blocks, so no need to do it again.
			}
			return nil
		}()
		if err != nil {
			fmt.Fprintf(w, "  [err: %s]\n", err)
		}
	}

	last := blocks[len(blocks)-1]
	fmt.Fprintf(w, "%10d  EOF\n", last.Offset+last.Length)
}

type blockFormatting struct {
	formatIndexBlock   formatBlockFunc
	formatDataBlock    formatBlockFuncKV
	formatKeyspanBlock formatBlockFuncKV
}

type (
	formatBlockFunc   func(io.Writer, *Reader, NamedBlockHandle, []byte) error
	formatBlockFuncKV func(io.Writer, *Reader, NamedBlockHandle, []byte, func(*base.InternalKey, []byte)) error
)

var (
	rowblkFormatting = blockFormatting{
		formatIndexBlock:   formatRowblkIndexBlock,
		formatDataBlock:    formatRowblkDataBlock,
		formatKeyspanBlock: formatRowblkDataBlock,
	}
	colblkFormatting = blockFormatting{
		formatIndexBlock:   formatColblkIndexBlock,
		formatDataBlock:    formatColblkDataBlock,
		formatKeyspanBlock: formatColblkKeyspanBlock,
	}
)

func formatColblkIndexBlock(w io.Writer, r *Reader, b NamedBlockHandle, data []byte) error {
	iter := new(colblk.IndexIter)
	if err := iter.Init(r.Compare, r.Split, data, NoTransforms); err != nil {
		return err
	}
	defer iter.Close()
	i := 0
	for v := iter.First(); v; v = iter.Next() {
		bh, err := iter.BlockHandleWithProperties()
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "%10d    block:%d/%d\n", i, bh.Offset, bh.Length)
		i++
	}
	return nil
}

func formatColblkDataBlock(
	w io.Writer,
	r *Reader,
	b NamedBlockHandle,
	data []byte,
	fmtRecord func(key *base.InternalKey, value []byte),
) error {
	var reader colblk.DataBlockDecoder
	reader.Init(r.keySchema, data)
	f := binfmt.New(data)
	f.SetLinePrefix("               ")
	reader.Describe(f)
	fmt.Fprint(w, f.String())

	if fmtRecord != nil {
		var iter colblk.DataBlockIter
		iter.InitOnce(r.keySchema, r.Compare, r.Split, describingLazyValueHandler{})
		if err := iter.Init(&reader, block.IterTransforms{}); err != nil {
			return err
		}
		defer iter.Close()
		for kv := iter.First(); kv != nil; kv = iter.Next() {
			fmtRecord(&kv.K, kv.V.ValueOrHandle)
		}
	}
	return nil
}

// describingLazyValueHandler is a block.GetLazyValueForPrefixAndValueHandler
// that replaces a value handle with an in-place value describing the handle.
type describingLazyValueHandler struct{}

// Assert that debugLazyValueHandler implements the
// block.GetLazyValueForPrefixAndValueHandler interface.
var _ block.GetLazyValueForPrefixAndValueHandler = describingLazyValueHandler{}

func (describingLazyValueHandler) GetLazyValueForPrefixAndValueHandle(
	handle []byte,
) base.LazyValue {
	vh := decodeValueHandle(handle[1:])
	return base.LazyValue{ValueOrHandle: []byte(fmt.Sprintf("value handle %+v", vh))}
}

func formatColblkKeyspanBlock(
	w io.Writer, r *Reader, b NamedBlockHandle, data []byte, _ func(*base.InternalKey, []byte),
) error {
	var reader colblk.KeyspanReader
	reader.Init(data)
	f := binfmt.New(data)
	f.SetLinePrefix("               ")
	reader.Describe(f)
	fmt.Fprint(w, f.String())
	return nil
}

func formatRowblkIndexBlock(w io.Writer, r *Reader, b NamedBlockHandle, data []byte) error {
	iter, err := rowblk.NewIter(r.Compare, r.Split, data, NoTransforms)
	if err != nil {
		return err
	}
	iter.Describe(w, b.Offset, func(w io.Writer, key *base.InternalKey, value []byte, enc rowblk.KVEncoding) {
		bh, err := block.DecodeHandleWithProperties(value)
		if err != nil {
			fmt.Fprintf(w, "%10d    [err: %s]\n", b.Offset+uint64(enc.Offset), err)
			return
		}
		fmt.Fprintf(w, "%10d    block:%d/%d",
			b.Offset+uint64(enc.Offset), bh.Offset, bh.Length)
		if enc.IsRestart {
			fmt.Fprintf(w, " [restart]\n")
		} else {
			fmt.Fprintf(w, "\n")
		}
	})
	return nil
}

func formatRowblkDataBlock(
	w io.Writer,
	r *Reader,
	b NamedBlockHandle,
	data []byte,
	fmtRecord func(key *base.InternalKey, value []byte),
) error {
	iter, err := rowblk.NewIter(r.Compare, r.Split, data, NoTransforms)
	if err != nil {
		return err
	}
	iter.Describe(w, b.Offset, func(w io.Writer, key *base.InternalKey, value []byte, enc rowblk.KVEncoding) {
		// The format of the numbers in the record line is:
		//
		//   (<total> = <length> [<shared>] + <unshared> + <value>)
		//
		// <total>    is the total number of bytes for the record.
		// <length>   is the size of the 3 varint encoded integers for <shared>,
		//            <unshared>, and <value>.
		// <shared>   is the number of key bytes shared with the previous key.
		// <unshared> is the number of unshared key bytes.
		// <value>    is the number of value bytes.
		fmt.Fprintf(w, "%10d    record (%d = %d [%d] + %d + %d)",
			b.Offset+uint64(enc.Offset), enc.Length,
			enc.Length-int32(enc.KeyUnshared+enc.ValueLen), enc.KeyShared, enc.KeyUnshared, enc.ValueLen)
		if enc.IsRestart {
			fmt.Fprintf(w, " [restart]\n")
		} else {
			fmt.Fprintf(w, "\n")
		}
		if fmtRecord != nil {
			fmt.Fprintf(w, "              ")
			if r.tableFormat < TableFormatPebblev3 {
				fmtRecord(key, value)
			} else {
				if key.Kind() != InternalKeyKindSet {
					fmtRecord(key, value)
				} else if !block.ValuePrefix(value[0]).IsValueHandle() {
					fmtRecord(key, value[1:])
				} else {
					vh := decodeValueHandle(value[1:])
					fmtRecord(key, []byte(fmt.Sprintf("value handle %+v", vh)))
				}
			}
		}
	})
	return nil
}

func decodeLayout(comparer *base.Comparer, data []byte) (Layout, error) {
	foot, err := parseFooter(data, 0, int64(len(data)))
	if err != nil {
		return Layout{}, err
	}
	decompressedMeta, err := decompressInMemory(data, foot.metaindexBH)
	if err != nil {
		return Layout{}, errors.Wrap(err, "decompressing metaindex")
	}
	meta, vbih, err := decodeMetaindex(decompressedMeta)
	if err != nil {
		return Layout{}, err
	}
	layout := Layout{
		MetaIndex:  foot.metaindexBH,
		Properties: meta[metaPropertiesName],
		RangeDel:   meta[metaRangeDelV2Name],
		RangeKey:   meta[metaRangeKeyName],
		ValueIndex: vbih.h,
		Footer:     foot.footerBH,
		Format:     foot.format,
	}
	var props Properties
	decompressedProps, err := decompressInMemory(data, layout.Properties)
	if err != nil {
		return Layout{}, errors.Wrap(err, "decompressing properties")
	}
	if err := props.load(decompressedProps, map[string]struct{}{}); err != nil {
		return Layout{}, err
	}

	if props.IndexType == twoLevelIndex {
		decompressed, err := decompressInMemory(data, foot.indexBH)
		if err != nil {
			return Layout{}, errors.Wrap(err, "decompressing two-level index")
		}
		layout.TopIndex = foot.indexBH
		topLevelIter, err := newIndexIter(foot.format, comparer, decompressed)
		if err != nil {
			return Layout{}, err
		}
		err = forEachIndexEntry(topLevelIter, func(bhp block.HandleWithProperties) {
			layout.Index = append(layout.Index, bhp.Handle)
		})
		if err != nil {
			return Layout{}, err
		}
	} else {
		layout.Index = append(layout.Index, foot.indexBH)
	}
	for _, indexBH := range layout.Index {
		decompressed, err := decompressInMemory(data, indexBH)
		if err != nil {
			return Layout{}, errors.Wrap(err, "decompressing index block")
		}
		indexIter, err := newIndexIter(foot.format, comparer, decompressed)
		if err != nil {
			return Layout{}, err
		}
		err = forEachIndexEntry(indexIter, func(bhp block.HandleWithProperties) {
			layout.Data = append(layout.Data, bhp)
		})
		if err != nil {
			return Layout{}, err
		}
	}

	if layout.ValueIndex.Length > 0 {
		vbiBlock, err := decompressInMemory(data, layout.ValueIndex)
		if err != nil {
			return Layout{}, errors.Wrap(err, "decompressing value index")
		}
		layout.ValueBlock, err = decodeValueBlockIndex(vbiBlock, vbih)
		if err != nil {
			return Layout{}, err
		}
	}

	return layout, nil
}

func decompressInMemory(data []byte, bh block.Handle) ([]byte, error) {
	typ := block.CompressionIndicator(data[bh.Offset+bh.Length])
	var decompressed []byte
	if typ == block.NoCompressionIndicator {
		return data[bh.Offset : bh.Offset+bh.Length], nil
	}
	// Decode the length of the decompressed value.
	decodedLen, prefixLen, err := block.DecompressedLen(typ, data[bh.Offset:bh.Offset+bh.Length])
	if err != nil {
		return nil, err
	}
	decompressed = make([]byte, decodedLen)
	if err := block.DecompressInto(typ, data[int(bh.Offset)+prefixLen:bh.Offset+bh.Length], decompressed); err != nil {
		return nil, err
	}
	return decompressed, nil
}

func newIndexIter(
	tableFormat TableFormat, comparer *base.Comparer, data []byte,
) (block.IndexBlockIterator, error) {
	var iter block.IndexBlockIterator
	var err error
	if tableFormat <= TableFormatPebblev4 {
		iter = new(rowblk.IndexIter)
		err = iter.Init(comparer.Compare, comparer.Split, data, block.NoTransforms)
	} else {
		iter = new(colblk.IndexIter)
		err = iter.Init(comparer.Compare, comparer.Split, data, block.NoTransforms)
	}
	if err != nil {
		return nil, err
	}
	return iter, nil
}

func forEachIndexEntry(
	indexIter block.IndexBlockIterator, fn func(block.HandleWithProperties),
) error {
	for v := indexIter.First(); v; v = indexIter.Next() {
		bhp, err := indexIter.BlockHandleWithProperties()
		if err != nil {
			return err
		}
		fn(bhp)
	}
	return indexIter.Close()
}

func decodeMetaindex(
	data []byte,
) (meta map[string]block.Handle, vbih valueBlocksIndexHandle, err error) {
	i, err := rowblk.NewRawIter(bytes.Compare, data)
	if err != nil {
		return nil, valueBlocksIndexHandle{}, err
	}
	defer func() { err = firstError(err, i.Close()) }()

	meta = map[string]block.Handle{}
	for valid := i.First(); valid; valid = i.Next() {
		value := i.Value()
		if bytes.Equal(i.Key().UserKey, []byte(metaValueIndexName)) {
			var n int
			vbih, n, err = decodeValueBlocksIndexHandle(i.Value())
			if err != nil {
				return nil, vbih, err
			}
			if n == 0 || n != len(value) {
				return nil, vbih, base.CorruptionErrorf("pebble/table: invalid table (bad value blocks index handle)")
			}
		} else {
			bh, n := block.DecodeHandle(value)
			if n == 0 || n != len(value) {
				return nil, vbih, base.CorruptionErrorf("pebble/table: invalid table (bad block handle)")
			}
			meta[string(i.Key().UserKey)] = bh
		}
	}
	return meta, vbih, nil
}

func decodeValueBlockIndex(data []byte, vbih valueBlocksIndexHandle) ([]block.Handle, error) {
	var valueBlocks []block.Handle
	indexEntryLen := int(vbih.blockNumByteLength + vbih.blockOffsetByteLength +
		vbih.blockLengthByteLength)
	i := 0
	for len(data) != 0 {
		if len(data) < indexEntryLen {
			return nil, errors.Errorf(
				"remaining value index block %d does not contain a full entry of length %d",
				len(data), indexEntryLen)
		}
		n := int(vbih.blockNumByteLength)
		bn := int(littleEndianGet(data, n))
		if bn != i {
			return nil, errors.Errorf("unexpected block num %d, expected %d",
				bn, i)
		}
		i++
		data = data[n:]
		n = int(vbih.blockOffsetByteLength)
		blockOffset := littleEndianGet(data, n)
		data = data[n:]
		n = int(vbih.blockLengthByteLength)
		blockLen := littleEndianGet(data, n)
		data = data[n:]
		valueBlocks = append(valueBlocks, block.Handle{Offset: blockOffset, Length: blockLen})
	}
	return valueBlocks, nil
}

// layoutWriter writes the structure of an sstable to durable storage. It
// accepts serialized blocks, writes them to storage and returns a block handle
// describing the offset and length of the block.
type layoutWriter struct {
	writable objstorage.Writable

	// cacheOpts are used to remove blocks written to the sstable from the cache,
	// providing a defense in depth against bugs which cause cache collisions.
	cacheOpts sstableinternal.CacheOptions

	// options copied from WriterOptions
	tableFormat  TableFormat
	compression  block.Compression
	checksumType block.ChecksumType

	// offset tracks the current write offset within the writable.
	offset uint64
	// lastIndexBlockHandle holds the handle to the most recently-written index
	// block.  It's updated by writeIndexBlock. When writing sstables with a
	// single-level index, this field will be updated once. When writing
	// sstables with a two-level index, the last update will set the two-level
	// index.
	lastIndexBlockHandle block.Handle
	handles              []metaIndexHandle
	handlesBuf           bytealloc.A
	tmp                  [blockHandleLikelyMaxLen]byte
	buf                  blockBuf
}

func makeLayoutWriter(w objstorage.Writable, opts WriterOptions) layoutWriter {
	return layoutWriter{
		writable:     w,
		cacheOpts:    opts.internal.CacheOpts,
		tableFormat:  opts.TableFormat,
		compression:  opts.Compression,
		checksumType: opts.Checksum,
		buf: blockBuf{
			checksummer: block.Checksummer{Type: opts.Checksum},
		},
	}
}

type metaIndexHandle struct {
	key                string
	encodedBlockHandle []byte
}

// Abort aborts writing the table, aborting the underlying writable too. Abort
// is idempotent.
func (w *layoutWriter) Abort() {
	if w.writable != nil {
		w.writable.Abort()
		w.writable = nil
	}
}

// WriteDataBlock constructs a trailer for the provided data block and writes
// the block and trailer to the writer. It returns the block's handle.
func (w *layoutWriter) WriteDataBlock(b []byte, buf *blockBuf) (block.Handle, error) {
	return w.writeBlock(b, w.compression, buf)
}

// WritePrecompressedDataBlock writes a pre-compressed data block and its
// pre-computed trailer to the writer, returning it's block handle.
func (w *layoutWriter) WritePrecompressedDataBlock(blk block.PhysicalBlock) (block.Handle, error) {
	return w.writePrecompressedBlock(blk)
}

// WriteIndexBlock constructs a trailer for the provided index (first or
// second-level) and writes the block and trailer to the writer. It remembers
// the last-written index block's handle and adds it to the file's meta index
// when the writer is finished.
func (w *layoutWriter) WriteIndexBlock(b []byte) (block.Handle, error) {
	h, err := w.writeBlock(b, w.compression, &w.buf)
	if err == nil {
		w.lastIndexBlockHandle = h
	}
	return h, err
}

// WriteFilterBlock finishes the provided filter, constructs a trailer and
// writes the block and trailer to the writer. It automatically adds the filter
// block to the file's meta index when the writer is finished.
func (w *layoutWriter) WriteFilterBlock(f filterWriter) (bh block.Handle, err error) {
	b, err := f.finish()
	if err != nil {
		return block.Handle{}, err
	}
	return w.writeNamedBlock(b, f.metaName())
}

// WritePropertiesBlock constructs a trailer for the provided properties block
// and writes the block and trailer to the writer. It automatically adds the
// properties block to the file's meta index when the writer is finished.
func (w *layoutWriter) WritePropertiesBlock(b []byte) (block.Handle, error) {
	return w.writeNamedBlock(b, metaPropertiesName)
}

// WriteRangeKeyBlock constructs a trailer for the provided range key block and
// writes the block and trailer to the writer. It automatically adds the range
// key block to the file's meta index when the writer is finished.
func (w *layoutWriter) WriteRangeKeyBlock(b []byte) (block.Handle, error) {
	return w.writeNamedBlock(b, metaRangeKeyName)
}

// WriteRangeDeletionBlock constructs a trailer for the provided range deletion
// block and writes the block and trailer to the writer. It automatically adds
// the range deletion block to the file's meta index when the writer is
// finished.
func (w *layoutWriter) WriteRangeDeletionBlock(b []byte) (block.Handle, error) {
	return w.writeNamedBlock(b, metaRangeDelV2Name)
}

func (w *layoutWriter) writeNamedBlock(b []byte, name string) (bh block.Handle, err error) {
	bh, err = w.writeBlock(b, block.NoCompression, &w.buf)
	if err == nil {
		w.recordToMetaindex(name, bh)
	}
	return bh, err
}

// WriteValueBlock writes a pre-finished value block (with the trailer) to the
// writer.
func (w *layoutWriter) WriteValueBlock(blk block.PhysicalBlock) (block.Handle, error) {
	return w.writePrecompressedBlock(blk)
}

func (w *layoutWriter) WriteValueIndexBlock(
	blk []byte, vbih valueBlocksIndexHandle,
) (block.Handle, error) {
	// NB: value index blocks are already finished and contain the block
	// trailer.
	// TODO(jackson): can this be refactored to make value blocks less
	// of a snowflake?
	off := w.offset
	w.clearFromCache(off)
	// Write the bytes to the file.
	if err := w.writable.Write(blk); err != nil {
		return block.Handle{}, err
	}
	l := uint64(len(blk))
	w.offset += l

	n := encodeValueBlocksIndexHandle(w.tmp[:], vbih)
	w.recordToMetaindexRaw(metaValueIndexName, w.tmp[:n])

	return block.Handle{Offset: off, Length: l}, nil
}

func (w *layoutWriter) writeBlock(
	b []byte, compression block.Compression, buf *blockBuf,
) (block.Handle, error) {
	return w.writePrecompressedBlock(block.CompressAndChecksum(
		&buf.compressedBuf, b, compression, &buf.checksummer))
}

// writePrecompressedBlock writes a pre-compressed block and its
// pre-computed trailer to the writer, returning it's block handle.
func (w *layoutWriter) writePrecompressedBlock(blk block.PhysicalBlock) (block.Handle, error) {
	w.clearFromCache(w.offset)
	// Write the bytes to the file.
	n, err := blk.WriteTo(w.writable)
	if err != nil {
		return block.Handle{}, err
	}
	bh := block.Handle{Offset: w.offset, Length: uint64(blk.LengthWithoutTrailer())}
	w.offset += uint64(n)
	return bh, nil
}

// Write implements io.Writer. This is analogous to writePrecompressedBlock for
// blocks that already incorporate the trailer, and don't need the callee to
// return a BlockHandle.
func (w *layoutWriter) Write(blockWithTrailer []byte) (n int, err error) {
	offset := w.offset
	w.clearFromCache(offset)
	w.offset += uint64(len(blockWithTrailer))
	if err := w.writable.Write(blockWithTrailer); err != nil {
		return 0, err
	}
	return len(blockWithTrailer), nil
}

// clearFromCache removes the block at the provided offset from the cache. This provides defense in
// depth against bugs which cause cache collisions.
func (w *layoutWriter) clearFromCache(offset uint64) {
	if w.cacheOpts.Cache != nil {
		// TODO(peter): Alternatively, we could add the uncompressed value to the
		// cache.
		w.cacheOpts.Cache.Delete(w.cacheOpts.CacheID, w.cacheOpts.FileNum, offset)
	}
}

func (w *layoutWriter) recordToMetaindex(key string, h block.Handle) {
	n := h.EncodeVarints(w.tmp[:])
	w.recordToMetaindexRaw(key, w.tmp[:n])
}

func (w *layoutWriter) recordToMetaindexRaw(key string, h []byte) {
	var encodedHandle []byte
	w.handlesBuf, encodedHandle = w.handlesBuf.Alloc(len(h))
	copy(encodedHandle, h)
	w.handles = append(w.handles, metaIndexHandle{key: key, encodedBlockHandle: encodedHandle})
}

func (w *layoutWriter) IsFinished() bool { return w.writable == nil }

// Finish serializes the sstable, writing out the meta index block and sstable
// footer and closing the file. It returns the total size of the resulting
// ssatable.
func (w *layoutWriter) Finish() (size uint64, err error) {
	// Sort the meta index handles by key and write the meta index block.
	slices.SortFunc(w.handles, func(a, b metaIndexHandle) int {
		return cmp.Compare(a.key, b.key)
	})
	bw := rowblk.Writer{RestartInterval: 1}
	for _, h := range w.handles {
		bw.AddRaw(unsafe.Slice(unsafe.StringData(h.key), len(h.key)), h.encodedBlockHandle)
	}
	metaIndexHandle, err := w.writeBlock(bw.Finish(), block.NoCompression, &w.buf)
	if err != nil {
		return 0, err
	}

	// Write the table footer.
	footer := footer{
		format:      w.tableFormat,
		checksum:    w.checksumType,
		metaindexBH: metaIndexHandle,
		indexBH:     w.lastIndexBlockHandle,
	}
	encodedFooter := footer.encode(w.tmp[:])
	if err := w.writable.Write(encodedFooter); err != nil {
		return 0, err
	}
	w.offset += uint64(len(encodedFooter))

	err = w.writable.Finish()
	w.writable = nil
	return w.offset, err
}
