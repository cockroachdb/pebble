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

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/rowblk"
)

// Layout describes the block organization of an sstable.
type Layout struct {
	// NOTE: changes to fields in this struct should also be reflected in
	// ValidateBlockChecksums, which validates a static list of BlockHandles
	// referenced in this struct.

	Data       []BlockHandleWithProperties
	Index      []block.Handle
	TopIndex   block.Handle
	Filter     block.Handle
	RangeDel   block.Handle
	RangeKey   block.Handle
	ValueBlock []block.Handle
	ValueIndex block.Handle
	Properties block.Handle
	MetaIndex  block.Handle
	Footer     block.Handle
	Format     TableFormat
}

// Describe returns a description of the layout. If the verbose parameter is
// true, details of the structure of each block are returned as well.
func (l *Layout) Describe(
	w io.Writer, verbose bool, r *Reader, fmtRecord func(key *base.InternalKey, value []byte),
) {
	ctx := context.TODO()
	type namedBlockHandle struct {
		block.Handle
		name string
	}
	var blocks []namedBlockHandle

	for i := range l.Data {
		blocks = append(blocks, namedBlockHandle{l.Data[i].Handle, "data"})
	}
	for i := range l.Index {
		blocks = append(blocks, namedBlockHandle{l.Index[i], "index"})
	}
	if l.TopIndex.Length != 0 {
		blocks = append(blocks, namedBlockHandle{l.TopIndex, "top-index"})
	}
	if l.Filter.Length != 0 {
		blocks = append(blocks, namedBlockHandle{l.Filter, "filter"})
	}
	if l.RangeDel.Length != 0 {
		blocks = append(blocks, namedBlockHandle{l.RangeDel, "range-del"})
	}
	if l.RangeKey.Length != 0 {
		blocks = append(blocks, namedBlockHandle{l.RangeKey, "range-key"})
	}
	for i := range l.ValueBlock {
		blocks = append(blocks, namedBlockHandle{l.ValueBlock[i], "value-block"})
	}
	if l.ValueIndex.Length != 0 {
		blocks = append(blocks, namedBlockHandle{l.ValueIndex, "value-index"})
	}
	if l.Properties.Length != 0 {
		blocks = append(blocks, namedBlockHandle{l.Properties, "properties"})
	}
	if l.MetaIndex.Length != 0 {
		blocks = append(blocks, namedBlockHandle{l.MetaIndex, "meta-index"})
	}
	if l.Footer.Length != 0 {
		if l.Footer.Length == levelDBFooterLen {
			blocks = append(blocks, namedBlockHandle{l.Footer, "leveldb-footer"})
		} else {
			blocks = append(blocks, namedBlockHandle{l.Footer, "footer"})
		}
	}

	slices.SortFunc(blocks, func(a, b namedBlockHandle) int {
		return cmp.Compare(a.Offset, b.Offset)
	})
	// TODO(jackson): This function formats offsets within blocks by adding the
	// block's offset. A block's offset is an offset in the physical, compressed
	// file whereas KV pairs offsets are within the uncompressed block. This is
	// confusing and can result in blocks' KVs offsets overlapping one another.
	// We should just print offsets relative to the block start.

	for i := range blocks {
		b := &blocks[i]
		fmt.Fprintf(w, "%10d  %s (%d)\n", b.Offset, b.name, b.Length)

		if !verbose {
			continue
		}
		if b.name == "filter" {
			continue
		}

		if b.name == "footer" || b.name == "leveldb-footer" {
			trailer, offset := make([]byte, b.Length), b.Offset
			_ = r.readable.ReadAt(ctx, trailer, int64(offset))

			if b.name == "footer" {
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
			if b.name == "leveldb-footer" {
				trailing = 8
			}

			offset += uint64(len(trailer) - trailing)
			trailer = trailer[len(trailer)-trailing:]

			if b.name == "footer" {
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

		formatTrailer := func() {
			trailer := make([]byte, block.TrailerLen)
			offset := int64(b.Offset + b.Length)
			_ = r.readable.ReadAt(ctx, trailer, offset)
			bt := blockType(trailer[0])
			checksum := binary.LittleEndian.Uint32(trailer[1:])
			fmt.Fprintf(w, "%10d    [trailer compression=%s checksum=0x%04x]\n", offset, bt, checksum)
		}

		var lastKey InternalKey
		switch b.name {
		case "data", "range-del", "range-key":
			iter, _ := rowblk.NewIter(r.Compare, r.Split, h.Get(), NoTransforms)
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
					if l.Format < TableFormatPebblev3 {
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

				if b.name == "data" {
					if base.InternalCompare(r.Compare, lastKey, *key) >= 0 {
						fmt.Fprintf(w, "              WARNING: OUT OF ORDER KEYS!\n")
					}
					lastKey.Trailer = key.Trailer
					lastKey.UserKey = append(lastKey.UserKey[:0], key.UserKey...)
				}
			})
			formatTrailer()
		case "index", "top-index":
			iter, _ := rowblk.NewIter(r.Compare, r.Split, h.Get(), NoTransforms)
			iter.Describe(w, b.Offset, func(w io.Writer, key *base.InternalKey, value []byte, enc rowblk.KVEncoding) {
				bh, err := decodeBlockHandleWithProperties(value)
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
						bh, n = decodeBlockHandle(value)
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

		h.Release()
	}

	last := blocks[len(blocks)-1]
	fmt.Fprintf(w, "%10d  EOF\n", last.Offset+last.Length)
}
