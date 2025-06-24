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
	"github.com/cockroachdb/pebble/internal/crc"
	"github.com/cockroachdb/pebble/internal/sstableinternal"
	"github.com/cockroachdb/pebble/internal/treeprinter"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/block/blockkind"
	"github.com/cockroachdb/pebble/sstable/colblk"
	"github.com/cockroachdb/pebble/sstable/rowblk"
	"github.com/cockroachdb/pebble/sstable/valblk"
)

// Layout describes the block organization of an sstable.
type Layout struct {
	// NOTE: changes to fields in this struct should also be reflected in
	// ValidateBlockChecksums, which validates a static list of BlockHandles
	// referenced in this struct.

	Data               []block.HandleWithProperties
	Index              []block.Handle
	TopIndex           block.Handle
	Filter             []NamedBlockHandle
	RangeDel           block.Handle
	RangeKey           block.Handle
	ValueBlock         []block.Handle
	ValueIndex         block.Handle
	Properties         block.Handle
	MetaIndex          block.Handle
	BlobReferenceIndex block.Handle
	Footer             block.Handle
	Format             TableFormat
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
	if l.BlobReferenceIndex.Length != 0 {
		blocks = append(blocks, NamedBlockHandle{l.BlobReferenceIndex, "blob-reference-index"})
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
// If verbose is true and fmtKV is non-nil, the output includes the KVs (as formatted by this function).
func (l *Layout) Describe(
	verbose bool, r *Reader, fmtKV func(key *base.InternalKey, value []byte) string,
) string {
	ctx := context.TODO()

	blocks := l.orderedBlocks()
	formatting := rowblkFormatting
	if l.Format.BlockColumnar() {
		formatting = colblkFormatting
	}

	tp := treeprinter.New()
	root := tp.Child("sstable")

	for i := range blocks {
		b := &blocks[i]
		tpNode := root.Childf("%s  offset: %d  length: %d", b.Name, b.Offset, b.Length)

		if !verbose {
			continue
		}
		if b.Name == "filter" {
			continue
		}

		if b.Name == "footer" || b.Name == "leveldb-footer" {
			trailer, offset := make([]byte, b.Length), 0
			_ = r.blockReader.Readable().ReadAt(ctx, trailer, int64(b.Offset))

			// In all cases, we know the version is right before the magic.
			version := binary.LittleEndian.Uint32(trailer[len(trailer)-magicLen-versionLen:])
			magicNumber := trailer[len(trailer)-magicLen:]
			format, err := parseTableFormat(magicNumber, version)
			if err != nil {
				panic("Error parsing table format.")
			}

			var attributes Attributes
			if format >= TableFormatPebblev7 {
				attributes = Attributes(binary.LittleEndian.Uint32(trailer[pebbleDBV7FooterAttributesOffset:]))
			}

			var computedChecksum uint32
			var encodedChecksum uint32
			if format >= TableFormatPebblev6 {
				checksumOffset := checkedPebbleDBChecksumOffset
				if format >= TableFormatPebblev7 {
					checksumOffset = pebbleDBv7FooterChecksumOffset
				}
				computedChecksum = crc.CRC(0).
					Update(trailer[:checksumOffset]).
					Update(trailer[checksumOffset+checksumLen:]).
					Value()
				encodedChecksum = binary.LittleEndian.Uint32(trailer[checksumOffset:])
			}

			if b.Name == "footer" {
				checksumType := block.ChecksumType(trailer[0])
				tpNode.Childf("%03d  checksum type: %s", offset, checksumType)
				trailer, offset = trailer[1:], offset+1
			}

			metaHandle, n := binary.Uvarint(trailer)
			metaLen, m := binary.Uvarint(trailer[n:])
			tpNode.Childf("%03d  meta: offset=%d, length=%d", offset, metaHandle, metaLen)
			trailer, offset = trailer[n+m:], offset+n+m

			indexHandle, n := binary.Uvarint(trailer)
			indexLen, m := binary.Uvarint(trailer[n:])
			tpNode.Childf("%03d  index: offset=%d, length=%d", offset, indexHandle, indexLen)
			trailer, offset = trailer[n+m:], offset+n+m

			// Set the offset to the start of footer's remaining trailing fields.
			trailing := magicLen
			if b.Name != "leveldb-footer" {
				trailing += versionLen
			}
			if format >= TableFormatPebblev6 {
				trailing += checksumLen
			}
			offset += len(trailer) - trailing

			if format >= TableFormatPebblev7 {
				// Attributes should be just prior to the checksum.
				tpNode.Childf("%03d  attributes: %s", offset-attributesLen, attributes.String())
			}

			if format >= TableFormatPebblev6 {
				if computedChecksum == encodedChecksum {
					tpNode.Childf("%03d  footer checksum: 0x%04x", offset, encodedChecksum)
				} else {
					tpNode.Childf("%03d  invalid footer checksum: 0x%04x, expected: 0x%04x", offset, encodedChecksum, computedChecksum)
				}
				offset += checksumLen
			}

			tpNode.Childf("%03d  version: %d", offset, version)
			offset = offset + 4
			tpNode.Childf("%03d  magic number: 0x%x", offset, magicNumber)
			continue
		}
		// Read the block and format it. Returns an error if we couldn't read the
		// block.
		err := func() error {
			var err error
			var h block.BufferHandle
			// Defer release of any block handle that will have been read.
			defer func() { h.Release() }()

			switch b.Name {
			case "data":
				h, err = r.readDataBlock(ctx, block.NoReadEnv, noReadHandle, b.Handle)
				if err != nil {
					return err
				}
				if fmtKV == nil {
					err = formatting.formatDataBlock(tpNode, r, *b, h.BlockData(), nil)
				} else {
					var lastKey InternalKey
					err = formatting.formatDataBlock(tpNode, r, *b, h.BlockData(), func(key *base.InternalKey, value []byte) string {
						v := fmtKV(key, value)
						if base.InternalCompare(r.Comparer.Compare, lastKey, *key) >= 0 {
							v += " WARNING: OUT OF ORDER KEYS!"
						}
						lastKey.Trailer = key.Trailer
						lastKey.UserKey = append(lastKey.UserKey[:0], key.UserKey...)
						return v
					})
				}

			case "range-del":
				h, err = r.readRangeDelBlock(ctx, block.NoReadEnv, noReadHandle, b.Handle)
				if err != nil {
					return err
				}
				// TODO(jackson): colblk ignores fmtKV, because it doesn't
				// make sense in the context.
				err = formatting.formatKeyspanBlock(tpNode, r, *b, h.BlockData(), fmtKV)

			case "range-key":
				h, err = r.readRangeKeyBlock(ctx, block.NoReadEnv, noReadHandle, b.Handle)
				if err != nil {
					return err
				}
				// TODO(jackson): colblk ignores fmtKV, because it doesn't
				// make sense in the context.
				err = formatting.formatKeyspanBlock(tpNode, r, *b, h.BlockData(), fmtKV)

			case "index", "top-index":
				h, err = r.readIndexBlock(ctx, block.NoReadEnv, noReadHandle, b.Handle)
				if err != nil {
					return err
				}
				err = formatting.formatIndexBlock(tpNode, r, *b, h.BlockData())

			case "properties":
				h, err = r.blockReader.Read(ctx, block.NoReadEnv, noReadHandle, b.Handle, blockkind.Metadata, noInitBlockMetadataFn)
				if err != nil {
					return err
				}
				if r.tableFormat >= TableFormatPebblev7 {
					var decoder colblk.KeyValueBlockDecoder
					decoder.Init(h.BlockData())
					offset := 0
					for i := 0; i < decoder.BlockDecoder().Rows(); i++ {
						key := decoder.KeyAt(i)
						value := decoder.ValueAt(i)
						length := len(key) + len(value)
						tpNode.Childf("%05d    %s (%d)", offset, key, length)
						offset += length
					}
				} else {
					iter, _ := rowblk.NewRawIter(r.Comparer.Compare, h.BlockData())
					iter.Describe(tpNode, func(w io.Writer, key *base.InternalKey, value []byte, enc rowblk.KVEncoding) {
						fmt.Fprintf(w, "%05d    %s (%d)", enc.Offset, key.UserKey, enc.Length)
					})
				}

			case "meta-index":
				if b.Handle != r.metaindexBH {
					return base.AssertionFailedf("range-del block handle does not match rangeDelBH")
				}
				h, err = r.readMetaindexBlock(ctx, block.NoReadEnv, noReadHandle)
				if err != nil {
					return err
				}

				if r.tableFormat >= TableFormatPebblev6 {
					var decoder colblk.KeyValueBlockDecoder
					decoder.Init(h.BlockData())
					for i := 0; i < decoder.BlockDecoder().Rows(); i++ {
						key := decoder.KeyAt(i)
						value := decoder.ValueAt(i)
						var bh block.Handle
						var n int
						var vbih valblk.IndexHandle
						isValueBlocksIndexHandle := false
						if bytes.Equal(key, []byte(metaValueIndexName)) {
							vbih, n, err = valblk.DecodeIndexHandle(value)
							bh = vbih.Handle
							isValueBlocksIndexHandle = true
						} else {
							bh, n = block.DecodeHandle(value)
						}
						if n == 0 || n != len(value) {
							tpNode.Childf("%04d    [err: %s]\n", i, err)
							continue
						}
						var vbihStr string
						if isValueBlocksIndexHandle {
							vbihStr = fmt.Sprintf(" value-blocks-index-lengths: %d(num), %d(offset), %d(length)",
								vbih.BlockNumByteLength, vbih.BlockOffsetByteLength, vbih.BlockLengthByteLength)
						}
						tpNode.Childf("%04d    %s block:%d/%d%s\n",
							i, key, bh.Offset, bh.Length, vbihStr)
					}
				} else {
					iter, _ := rowblk.NewRawIter(r.Comparer.Compare, h.BlockData())
					iter.Describe(tpNode, func(w io.Writer, key *base.InternalKey, value []byte, enc rowblk.KVEncoding) {
						var bh block.Handle
						var n int
						var vbih valblk.IndexHandle
						isValueBlocksIndexHandle := false
						if bytes.Equal(iter.Key().UserKey, []byte(metaValueIndexName)) {
							vbih, n, err = valblk.DecodeIndexHandle(value)
							bh = vbih.Handle
							isValueBlocksIndexHandle = true
						} else {
							bh, n = block.DecodeHandle(value)
						}
						if n == 0 || n != len(value) {
							fmt.Fprintf(w, "%04d    [err: %s]\n", enc.Offset, err)
							return
						}
						var vbihStr string
						if isValueBlocksIndexHandle {
							vbihStr = fmt.Sprintf(" value-blocks-index-lengths: %d(num), %d(offset), %d(length)",
								vbih.BlockNumByteLength, vbih.BlockOffsetByteLength, vbih.BlockLengthByteLength)
						}
						fmt.Fprintf(w, "%04d    %s block:%d/%d%s",
							uint64(enc.Offset), iter.Key().UserKey, bh.Offset, bh.Length, vbihStr)
					})
				}

			case "value-block":
				// We don't peer into the value-block since it can't be interpreted
				// without the valueHandles.
			case "value-index":
				// We have already read the value-index to construct the list of
				// value-blocks, so no need to do it again.
			case "blob-reference-index":
				h, err = r.blockReader.Read(ctx, block.NoReadEnv, noReadHandle, b.Handle, blockkind.BlobReferenceValueLivenessIndex, noInitBlockMetadataFn)
				if err != nil {
					return err
				}
				var decoder colblk.ReferenceLivenessBlockDecoder
				decoder.Init(h.BlockData())
				offset := 0
				for i := range decoder.BlockDecoder().Rows() {
					value := decoder.LivenessAtReference(i)
					encs := DecodeBlobRefLivenessEncoding(value)
					length := len(value)
					parent := tpNode.Childf("%05d (%d)", offset, length)
					for _, enc := range encs {
						parent.Childf("block: %d, values size: %d, bitmap size: %d byte(s), bitmap: %08b",
							enc.BlockID, enc.ValuesSize, enc.BitmapSize, enc.Bitmap)
					}
					offset += length
				}
			}

			// Format the trailer.
			trailer := make([]byte, block.TrailerLen)
			_ = r.blockReader.Readable().ReadAt(ctx, trailer, int64(b.Offset+b.Length))
			algo := block.CompressionIndicator(trailer[0])
			checksum := binary.LittleEndian.Uint32(trailer[1:])
			tpNode.Childf("trailer [compression=%s checksum=0x%04x]", algo, checksum)
			return nil
		}()
		if err != nil {
			tpNode.Childf("error reading block: %v", err)
		}
	}
	return tp.String()
}

type blockFormatting struct {
	formatIndexBlock   formatBlockFunc
	formatDataBlock    formatBlockFuncKV
	formatKeyspanBlock formatBlockFuncKV
}

type (
	formatBlockFunc   func(treeprinter.Node, *Reader, NamedBlockHandle, []byte) error
	formatBlockFuncKV func(treeprinter.Node, *Reader, NamedBlockHandle, []byte, func(*base.InternalKey, []byte) string) error
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

func formatColblkIndexBlock(tp treeprinter.Node, r *Reader, b NamedBlockHandle, data []byte) error {
	var iter colblk.IndexIter
	if err := iter.Init(r.Comparer, data, NoTransforms); err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()
	i := 0
	for v := iter.First(); v; v = iter.Next() {
		bh, err := iter.BlockHandleWithProperties()
		if err != nil {
			return err
		}
		tp.Childf("%05d    block:%d/%d\n", i, bh.Offset, bh.Length)
		i++
	}
	return nil
}

func formatColblkDataBlock(
	tp treeprinter.Node,
	r *Reader,
	b NamedBlockHandle,
	data []byte,
	fmtKV func(key *base.InternalKey, value []byte) string,
) error {
	var decoder colblk.DataBlockDecoder
	decoder.Init(r.keySchema, data)
	f := binfmt.New(data)
	decoder.Describe(f, tp)

	if fmtKV != nil {
		var iter colblk.DataBlockIter
		iter.InitOnce(r.keySchema, r.Comparer, describingLazyValueHandler{})
		if err := iter.Init(&decoder, block.IterTransforms{}); err != nil {
			return err
		}
		defer func() { _ = iter.Close() }()
		for kv := iter.First(); kv != nil; kv = iter.Next() {
			tp.Child(fmtKV(&kv.K, kv.V.LazyValue().ValueOrHandle))
		}
	}
	return nil
}

// describingLazyValueHandler is a block.GetInternalValueForPrefixAndValueHandler
// that replaces a value handle with an in-place value describing the handle.
type describingLazyValueHandler struct{}

// Assert that debugLazyValueHandler implements the
// block.GetInternalValueForPrefixAndValueHandler interface.
var _ block.GetInternalValueForPrefixAndValueHandler = describingLazyValueHandler{}

func (describingLazyValueHandler) GetInternalValueForPrefixAndValueHandle(
	handle []byte,
) base.InternalValue {
	vp := block.ValuePrefix(handle[0])
	var result string
	switch {
	case vp.IsValueBlockHandle():
		vh := valblk.DecodeHandle(handle[1:])
		result = fmt.Sprintf("value handle %+v", vh)
	case vp.IsBlobValueHandle():
		handlePreface, remainder := blob.DecodeInlineHandlePreface(handle[1:])
		handleSuffix := blob.DecodeHandleSuffix(remainder)
		ih := blob.InlineHandle{
			InlineHandlePreface: handlePreface,
			HandleSuffix:        handleSuffix,
		}
		result = fmt.Sprintf("blob handle %+v", ih)
	default:
		result = "unknown value type"
	}
	return base.MakeInPlaceValue([]byte(result))
}

func formatColblkKeyspanBlock(
	tp treeprinter.Node,
	r *Reader,
	b NamedBlockHandle,
	data []byte,
	_ func(*base.InternalKey, []byte) string,
) error {
	var decoder colblk.KeyspanDecoder
	decoder.Init(data)
	f := binfmt.New(data)
	decoder.Describe(f, tp)
	return nil
}

func formatRowblkIndexBlock(tp treeprinter.Node, r *Reader, b NamedBlockHandle, data []byte) error {
	iter, err := rowblk.NewIter(r.Comparer.Compare, r.Comparer.ComparePointSuffixes, r.Comparer.Split, data, NoTransforms)
	if err != nil {
		return err
	}
	iter.Describe(tp, func(w io.Writer, key *base.InternalKey, value []byte, enc rowblk.KVEncoding) {
		bh, err := block.DecodeHandleWithProperties(value)
		if err != nil {
			fmt.Fprintf(w, "%05d    [err: %s]\n", enc.Offset, err)
			return
		}
		fmt.Fprintf(w, "%05d    block:%d/%d", enc.Offset, bh.Offset, bh.Length)
		if enc.IsRestart {
			fmt.Fprintf(w, " [restart]")
		}
	})
	return nil
}

func formatRowblkDataBlock(
	tp treeprinter.Node,
	r *Reader,
	b NamedBlockHandle,
	data []byte,
	fmtRecord func(key *base.InternalKey, value []byte) string,
) error {
	iter, err := rowblk.NewIter(r.Comparer.Compare, r.Comparer.ComparePointSuffixes, r.Comparer.Split, data, NoTransforms)
	if err != nil {
		return err
	}
	iter.Describe(tp, func(w io.Writer, key *base.InternalKey, value []byte, enc rowblk.KVEncoding) {
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
		fmt.Fprintf(w, "%05d    record (%d = %d [%d] + %d + %d)",
			uint64(enc.Offset), enc.Length,
			enc.Length-int32(enc.KeyUnshared+enc.ValueLen), enc.KeyShared, enc.KeyUnshared, enc.ValueLen)
		if enc.IsRestart {
			fmt.Fprint(w, " [restart]")
		}
		if fmtRecord != nil {
			if r.tableFormat < TableFormatPebblev3 || key.Kind() != InternalKeyKindSet {
				fmt.Fprintf(w, "\n         %s", fmtRecord(key, value))
				return
			}
			vp := block.ValuePrefix(value[0])
			if vp.IsInPlaceValue() {
				fmt.Fprintf(w, "\n         %s", fmtRecord(key, value[1:]))
			} else if vp.IsValueBlockHandle() {
				vh := valblk.DecodeHandle(value[1:])
				fmt.Fprintf(w, "\n         %s", fmtRecord(key, []byte(fmt.Sprintf("value handle %+v", vh))))
			} else {
				panic(fmt.Sprintf("unknown value prefix: %d", value[0]))
			}
		}
	})
	return nil
}

func decodeLayout(comparer *base.Comparer, data []byte, tableFormat TableFormat) (Layout, error) {
	foot, err := parseFooter(data, 0, int64(len(data)))
	if err != nil {
		return Layout{}, err
	}
	decompressedMeta, err := decompressInMemory(data, foot.metaindexBH)
	if err != nil {
		return Layout{}, errors.Wrap(err, "decompressing metaindex")
	}
	var meta map[string]block.Handle
	var vbih valblk.IndexHandle
	if tableFormat >= TableFormatPebblev6 {
		meta, vbih, err = decodeColumnarMetaIndex(decompressedMeta)
	} else {
		meta, vbih, err = decodeMetaindex(decompressedMeta)
	}
	if err != nil {
		return Layout{}, err
	}
	layout := Layout{
		MetaIndex:  foot.metaindexBH,
		Properties: meta[metaPropertiesName],
		RangeDel:   meta[metaRangeDelV2Name],
		RangeKey:   meta[metaRangeKeyName],
		ValueIndex: vbih.Handle,
		Footer:     foot.footerBH,
		Format:     foot.format,
	}
	decompressedProps, err := decompressInMemory(data, layout.Properties)
	if err != nil {
		return Layout{}, errors.Wrap(err, "decompressing properties")
	}
	props, err := decodePropertiesBlock(tableFormat, decompressedProps)
	if err != nil {
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
		layout.ValueBlock, err = valblk.DecodeIndex(vbiBlock, vbih)
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
	decodedLen, err := block.DecompressedLen(typ, data[bh.Offset:bh.Offset+bh.Length])
	if err != nil {
		return nil, err
	}
	decompressed = make([]byte, decodedLen)
	if err := block.DecompressInto(typ, data[int(bh.Offset):bh.Offset+bh.Length], decompressed); err != nil {
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
		err = iter.Init(comparer, data, block.NoTransforms)
	} else {
		iter = new(colblk.IndexIter)
		err = iter.Init(comparer, data, block.NoTransforms)
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

// decodeMetaindex decodes a row-based meta index block. The returned map owns
// all its memory and can outlive the provided data slice.
func decodeMetaindex(
	data []byte,
) (meta map[string]block.Handle, vbih valblk.IndexHandle, err error) {
	i, err := rowblk.NewRawIter(bytes.Compare, data)
	if err != nil {
		return nil, valblk.IndexHandle{}, err
	}
	defer func() { err = firstError(err, i.Close()) }()

	var keysAlloc bytealloc.A
	meta = map[string]block.Handle{}
	for valid := i.First(); valid; valid = i.Next() {
		value := i.Value()
		var bh block.Handle
		if bytes.Equal(i.Key().UserKey, []byte(metaValueIndexName)) {
			var n int
			vbih, n, err = valblk.DecodeIndexHandle(i.Value())
			if err != nil {
				return nil, vbih, err
			}
			if n == 0 || n != len(value) {
				return nil, vbih, base.CorruptionErrorf("pebble/table: invalid table (bad value blocks index handle)")
			}
			bh = vbih.Handle
		} else {
			var n int
			bh, n = block.DecodeHandle(value)
			if n == 0 || n != len(value) {
				return nil, vbih, base.CorruptionErrorf("pebble/table: invalid table (bad block handle)")
			}
		}
		var key []byte
		keysAlloc, key = keysAlloc.Copy(i.Key().UserKey)
		keyStr := unsafe.String(unsafe.SliceData(key), len(key))
		meta[keyStr] = bh
	}
	return meta, vbih, nil
}

// decodeColumnarMetaIndex decodes a columnar meta index block. The returned map
// owns all its memory and can outlive the provided data slice.
func decodeColumnarMetaIndex(
	data []byte,
) (meta map[string]block.Handle, vbih valblk.IndexHandle, err error) {
	var decoder colblk.KeyValueBlockDecoder
	decoder.Init(data)
	var keysAlloc bytealloc.A
	meta = map[string]block.Handle{}
	for i := 0; i < decoder.BlockDecoder().Rows(); i++ {
		key := decoder.KeyAt(i)
		value := decoder.ValueAt(i)

		var bh block.Handle
		if bytes.Equal(key, []byte(metaValueIndexName)) {
			var n int
			vbih, n, err = valblk.DecodeIndexHandle(value)
			if err != nil {
				return nil, vbih, err
			}
			if n == 0 || n != len(value) {
				return nil, vbih, base.CorruptionErrorf("pebble/table: invalid table (bad value blocks index handle)")
			}
			bh = vbih.Handle
		} else {
			var n int
			bh, n = block.DecodeHandle(value)
			if n == 0 || n != len(value) {
				return nil, vbih, base.CorruptionErrorf("pebble/table: invalid table (bad block handle)")
			}
		}
		var keyCopy []byte
		keysAlloc, keyCopy = keysAlloc.Copy(key)
		keyStr := unsafe.String(unsafe.SliceData(keyCopy), len(keyCopy))
		meta[keyStr] = bh
	}
	return meta, vbih, nil
}

// layoutWriter writes the structure of an sstable to durable storage. It
// accepts serialized blocks, writes them to storage, and returns a block handle
// describing the offset and length of the block.
type layoutWriter struct {
	writable objstorage.Writable

	// cacheOpts are used to remove blocks written to the sstable from the cache,
	// providing a defense in depth against bugs which cause cache collisions.
	cacheOpts sstableinternal.CacheOptions

	// options copied from WriterOptions
	tableFormat  TableFormat
	compressor   block.Compressor
	checksumType block.ChecksumType

	// Attribute bitset of the sstable, derived from sstable Properties at the time
	// of writing.
	attributes Attributes

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
		compressor:   block.MakeCompressor(opts.Compression),
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
		w.compressor.Close()
	}
}

// WriteDataBlock constructs a trailer for the provided data block and writes
// the block and trailer to the writer. It returns the block's handle.
func (w *layoutWriter) WriteDataBlock(b []byte, buf *blockBuf) (block.Handle, error) {
	return w.writeBlock(b, blockkind.SSTableData, &w.compressor, buf)
}

// WritePrecompressedDataBlock writes a pre-compressed data block and its
// pre-computed trailer to the writer, returning its block handle. It can mangle
// the block data.
func (w *layoutWriter) WritePrecompressedDataBlock(blk block.PhysicalBlock) (block.Handle, error) {
	return w.writePrecompressedBlock(blk)
}

// WriteIndexBlock constructs a trailer for the provided index (first or
// second-level) and writes the block and trailer to the writer. It remembers
// the last-written index block's handle and adds it to the file's meta index
// when the writer is finished.
func (w *layoutWriter) WriteIndexBlock(b []byte) (block.Handle, error) {
	h, err := w.writeBlock(b, blockkind.SSTableIndex, &w.compressor, &w.buf)
	if err == nil {
		w.lastIndexBlockHandle = h
	}
	return h, err
}

// WriteFilterBlock finishes the provided filter, constructs a trailer, and
// writes the block and trailer to the writer. It automatically adds the filter
// block to the file's meta index when the writer is finished.
func (w *layoutWriter) WriteFilterBlock(f filterWriter) (bh block.Handle, err error) {
	b, err := f.finish()
	if err != nil {
		return block.Handle{}, err
	}
	return w.writeNamedBlock(b, blockkind.Filter, block.NoopCompressor, f.metaName())
}

// WritePropertiesBlock constructs a trailer for the provided properties block
// and writes the block and trailer to the writer. It automatically adds the
// properties block to the file's meta index when the writer is finished.
func (w *layoutWriter) WritePropertiesBlock(b []byte) (block.Handle, error) {
	compressor := &w.compressor
	// In v6 and earlier, we use a row oriented block with an infinite restart
	// interval, which provides very good prefix compression. Since v7, we use the
	// columnar format without prefix compression for this block; we enable block
	// compression to compensate.
	if w.tableFormat < TableFormatPebblev7 {
		compressor = block.NoopCompressor
	}
	return w.writeNamedBlock(b, blockkind.Metadata, compressor, metaPropertiesName)
}

// WriteRangeKeyBlock constructs a trailer for the provided range key block and
// writes the block and trailer to the writer. It automatically adds the range
// key block to the file's meta index when the writer is finished.
func (w *layoutWriter) WriteRangeKeyBlock(b []byte) (block.Handle, error) {
	return w.writeNamedBlock(b, blockkind.RangeKey, block.NoopCompressor, metaRangeKeyName)
}

// WriteBlobRefIndexBlock constructs a trailer for the provided blob reference
// index block and writes the block and trailer to the writer. It automatically
// adds the blob reference index block to the file's meta index when the writer
// is finished.
func (w *layoutWriter) WriteBlobRefIndexBlock(b []byte) (block.Handle, error) {
	return w.writeNamedBlock(b, blockkind.BlobReferenceValueLivenessIndex, &w.compressor, metaBlobRefIndexName)
}

// WriteRangeDeletionBlock constructs a trailer for the provided range deletion
// block and writes the block and trailer to the writer. It automatically adds
// the range deletion block to the file's meta index when the writer is
// finished.
func (w *layoutWriter) WriteRangeDeletionBlock(b []byte) (block.Handle, error) {
	return w.writeNamedBlock(b, blockkind.RangeDel, block.NoopCompressor, metaRangeDelV2Name)
}

func (w *layoutWriter) writeNamedBlock(
	b []byte, kind block.Kind, compressor *block.Compressor, name string,
) (bh block.Handle, err error) {
	bh, err = w.writeBlock(b, kind, compressor, &w.buf)
	if err == nil {
		w.recordToMetaindex(name, bh)
	}
	return bh, err
}

// WriteValueBlock writes a pre-finished value block (with the trailer) to the
// writer. It can mangle the block data.
func (w *layoutWriter) WriteValueBlock(blk block.PhysicalBlock) (block.Handle, error) {
	return w.writePrecompressedBlock(blk)
}

// WriteValueIndexBlock writes a value index block and adds it to the meta
// index. It can mangle the block data.
func (w *layoutWriter) WriteValueIndexBlock(
	blk block.PhysicalBlock, vbih valblk.IndexHandle,
) (block.Handle, error) {
	h, err := w.writePrecompressedBlock(blk)
	if err != nil {
		return block.Handle{}, err
	}
	n := valblk.EncodeIndexHandle(w.tmp[:], vbih)
	w.recordToMetaindexRaw(metaValueIndexName, w.tmp[:n])
	return h, nil
}

// writeBlock checksums, compresses, and writes out a block.
func (w *layoutWriter) writeBlock(
	b []byte, kind block.Kind, compressor *block.Compressor, buf *blockBuf,
) (block.Handle, error) {
	pb := block.CompressAndChecksum(&buf.dataBuf, b, kind, compressor, &buf.checksummer)
	h, err := w.writePrecompressedBlock(pb)
	return h, err
}

// writePrecompressedBlock writes a pre-compressed block and its
// pre-computed trailer to the writer, returning its block handle.
//
// writePrecompressedBlock might mangle the block data.
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

// Write implements io.Writer (with the caveat that it can mangle the block
// data). This is analogous to writePrecompressedBlock for blocks that already
// incorporate the trailer, and don't need the callee to return a BlockHandle.
func (w *layoutWriter) Write(blockWithTrailer []byte) (n int, err error) {
	offset := w.offset
	w.clearFromCache(offset)
	w.offset += uint64(len(blockWithTrailer))
	// This call can mangle blockWithTrailer.
	if err := w.writable.Write(blockWithTrailer); err != nil {
		return 0, err
	}
	return len(blockWithTrailer), nil
}

// clearFromCache removes the block at the provided offset from the cache. This provides defense in
// depth against bugs which cause cache collisions.
func (w *layoutWriter) clearFromCache(offset uint64) {
	if w.cacheOpts.CacheHandle != nil {
		// TODO(peter): Alternatively, we could add the uncompressed value to the
		// cache.
		w.cacheOpts.CacheHandle.Delete(w.cacheOpts.FileNum, offset)
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
// sstable.
func (w *layoutWriter) Finish() (size uint64, err error) {
	// Sort the meta index handles by key and write the meta index block.
	slices.SortFunc(w.handles, func(a, b metaIndexHandle) int {
		return cmp.Compare(a.key, b.key)
	})
	var b []byte
	if w.tableFormat >= TableFormatPebblev6 {
		var cw colblk.KeyValueBlockWriter
		cw.Init()
		for _, h := range w.handles {
			cw.AddKV(unsafe.Slice(unsafe.StringData(h.key), len(h.key)), h.encodedBlockHandle)
		}
		b = cw.Finish(cw.Rows())
	} else {
		bw := rowblk.Writer{RestartInterval: 1}
		for _, h := range w.handles {
			if err := bw.AddRaw(unsafe.Slice(unsafe.StringData(h.key), len(h.key)), h.encodedBlockHandle); err != nil {
				return 0, err
			}
		}
		b = bw.Finish()
	}
	metaIndexHandle, err := w.writeBlock(b, blockkind.Metadata, block.NoopCompressor, &w.buf)
	if err != nil {
		return 0, err
	}

	// Write the table footer.
	footer := footer{
		format:      w.tableFormat,
		checksum:    w.checksumType,
		metaindexBH: metaIndexHandle,
		indexBH:     w.lastIndexBlockHandle,
		attributes:  w.attributes,
	}
	encodedFooter := footer.encode(w.tmp[:])
	if err := w.writable.Write(encodedFooter); err != nil {
		return 0, err
	}
	w.offset += uint64(len(encodedFooter))

	err = w.writable.Finish()
	w.writable = nil
	w.compressor.Close()
	return w.offset, err
}
