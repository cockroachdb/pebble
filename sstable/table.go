// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

/*
Package sstable implements readers and writers of pebble tables.

Tables are either opened for reading or created for writing but not both.

A reader can create iterators, which allow seeking and next/prev
iteration. There may be multiple key/value pairs that have the same key and
different sequence numbers.

A reader can be used concurrently. Multiple goroutines can call NewIter
concurrently, and each iterator can run concurrently with other iterators.
However, any particular iterator should not be used concurrently, and iterators
should not be used once a reader is closed.

A writer writes key/value pairs in increasing key order, and cannot be used
concurrently. A table cannot be read until the writer has finished.

Readers and writers can be created with various options. Passing a nil
Options pointer is valid and means to use the default values.

One such option is to define the 'less than' ordering for keys. The default
Comparer uses the natural ordering consistent with bytes.Compare. The same
ordering should be used for reading and writing a table.

To return the value for a key:

	r := table.NewReader(file, options)
	defer r.Close()
	return r.Get(key)

To count the number of entries in a table:

	i, n := r.NewIter(ropts), 0
	for valid := i.First(); valid; valid = i.Next() {
		n++
	}
	if err := i.Close(); err != nil {
		return 0, err
	}
	return n, nil

To write a table with three entries:

	w := table.NewWriter(file, options)
	if err := w.Set([]byte("apple"), []byte("red"), wopts); err != nil {
		w.Close()
		return err
	}
	if err := w.Set([]byte("banana"), []byte("yellow"), wopts); err != nil {
		w.Close()
		return err
	}
	if err := w.Set([]byte("cherry"), []byte("red"), wopts); err != nil {
		w.Close()
		return err
	}
	return w.Close()
*/
package sstable // import "github.com/petermattis/pebble/sstable"
import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/petermattis/pebble/vfs"
)

/*
The table file format looks like:

<start_of_file>
[data block 0]
[data block 1]
...
[data block N-1]
[meta block 0]
[meta block 1]
...
[meta block K-1]
[metaindex block]
[index block]
[footer]
<end_of_file>

Each block consists of some data and a 5 byte trailer: a 1 byte block type and
a 4 byte checksum of the compressed data. The block type gives the per-block
compression used; each block is compressed independently. The checksum
algorithm is described in the pebble/crc package.

The decompressed block data consists of a sequence of key/value entries
followed by a trailer. Each key is encoded as a shared prefix length and a
remainder string. For example, if two adjacent keys are "tweedledee" and
"tweedledum", then the second key would be encoded as {8, "um"}. The shared
prefix length is varint encoded. The remainder string and the value are
encoded as a varint-encoded length followed by the literal contents. To
continue the example, suppose that the key "tweedledum" mapped to the value
"socks". The encoded key/value entry would be: "\x08\x02\x05umsocks".

Every block has a restart interval I. Every I'th key/value entry in that block
is called a restart point, and shares no key prefix with the previous entry.
Continuing the example above, if the key after "tweedledum" was "two", but was
part of a restart point, then that key would be encoded as {0, "two"} instead
of {2, "o"}. If a block has P restart points, then the block trailer consists
of (P+1)*4 bytes: (P+1) little-endian uint32 values. The first P of these
uint32 values are the block offsets of each restart point. The final uint32
value is P itself. Thus, when seeking for a particular key, one can use binary
search to find the largest restart point whose key is <= the key sought.

An index block is a block with N key/value entries. The i'th value is the
encoded block handle of the i'th data block. The i'th key is a separator for
i < N-1, and a successor for i == N-1. The separator between blocks i and i+1
is a key that is >= every key in block i and is < every key i block i+1. The
successor for the final block is a key that is >= every key in block N-1. The
index block restart interval is 1: every entry is a restart point.

A block handle is an offset and a length; the length does not include the 5
byte trailer. Both numbers are varint-encoded, with no padding between the two
values. The maximum size of an encoded block handle is therefore 20 bytes.
*/

const (
	blockTrailerLen   = 5
	blockHandleMaxLen = 10 + 10

	levelDBFooterLen   = 48
	levelDBMagic       = "\x57\xfb\x80\x8b\x24\x75\x47\xdb"
	levelDBMagicOffset = levelDBFooterLen - len(levelDBMagic)

	rocksDBFooterLen     = 1 + 2*blockHandleMaxLen + 4 + 8
	rocksDBMagic         = "\xf7\xcf\xf4\x85\xb7\x41\xe2\x88"
	rocksDBMagicOffset   = rocksDBFooterLen - len(rocksDBMagic)
	rocksDBVersionOffset = rocksDBMagicOffset - 4

	minFooterLen = levelDBFooterLen
	maxFooterLen = rocksDBFooterLen

	levelDBFormatVersion  = 0
	rocksDBFormatVersion2 = 2

	noChecksum     = 0
	checksumCRC32c = 1
	checksumXXHash = 2

	// The block type gives the per-block compression format.
	// These constants are part of the file format and should not be changed.
	// They are different from the Compression constants because the latter
	// are designed so that the zero value of the Compression type means to
	// use the default compression (which is snappy).
	noCompressionBlockType     byte = 0
	snappyCompressionBlockType byte = 1

	metaPropertiesName = "rocksdb.properties"
	metaRangeDelName   = "rocksdb.range_del"
	metaRangeDelV2Name = "rocksdb.range_del2"

	// Index Types.
	// A space efficient index block that is optimized for binary-search-based
	// index.
	binarySearchIndex = 0
	// hashSearchIndex               = 1
	// A two-level index implementation. Both levels are binary search indexes.
	twoLevelIndex = 2
	// binarySearchWithFirstKeyIndex = 3

	// RocksDB always includes this in the properties block. Since Pebble
	// doesn't use zstd compression, the string will always be the same.
	// This should be removed if we ever decide to diverge from the RocksDB
	// properties block.
	rocksDBCompressionOptions = "window_bits=-14; level=32767; strategy=0; max_dict_bytes=0; zstd_max_train_bytes=0; enabled=0; "
)

// legacy (LevelDB) footer format:
//    metaindex handle (varint64 offset, varint64 size)
//    index handle     (varint64 offset, varint64 size)
//    <padding> to make the total size 2 * BlockHandle::kMaxEncodedLength
//    table_magic_number (8 bytes)
// new (RocksDB) footer format:
//    checksum type (char, 1 byte)
//    metaindex handle (varint64 offset, varint64 size)
//    index handle     (varint64 offset, varint64 size)
//    <padding> to make the total size 2 * BlockHandle::kMaxEncodedLength + 1
//    footer version (4 bytes)
//    table_magic_number (8 bytes)
type footer struct {
	format      TableFormat
	checksum    uint8
	metaindexBH BlockHandle
	indexBH     BlockHandle
	footerBH    BlockHandle
}

func readFooter(f vfs.File) (footer, error) {
	var footer footer
	stat, err := f.Stat()
	if err != nil {
		return footer, fmt.Errorf("pebble/table: invalid table (could not stat file): %v", err)
	}
	if stat.Size() < minFooterLen {
		return footer, errors.New("pebble/table: invalid table (file size is too small)")
	}

	buf := make([]byte, maxFooterLen)
	off := stat.Size() - maxFooterLen
	if off < 0 {
		off = 0
	}
	n, err := f.ReadAt(buf, off)
	if err != nil && err != io.EOF {
		return footer, fmt.Errorf("pebble/table: invalid table (could not read footer): %v", err)
	}
	buf = buf[:n]

	switch string(buf[len(buf)-len(rocksDBMagic):]) {
	case levelDBMagic:
		if len(buf) < levelDBFooterLen {
			return footer, fmt.Errorf("pebble/table: invalid table (footer too short): %d", len(buf))
		}
		footer.footerBH.Offset = uint64(off+int64(len(buf))) - levelDBFooterLen
		buf = buf[len(buf)-levelDBFooterLen:]
		footer.footerBH.Length = uint64(len(buf))
		footer.format = TableFormatLevelDB
		footer.checksum = checksumCRC32c

	case rocksDBMagic:
		if len(buf) < rocksDBFooterLen {
			return footer, fmt.Errorf("pebble/table: invalid table (footer too short): %d", len(buf))
		}
		footer.footerBH.Offset = uint64(off+int64(len(buf))) - rocksDBFooterLen
		buf = buf[len(buf)-rocksDBFooterLen:]
		footer.footerBH.Length = uint64(len(buf))
		version := binary.LittleEndian.Uint32(buf[rocksDBVersionOffset:rocksDBMagicOffset])
		if version != rocksDBFormatVersion2 {
			return footer, fmt.Errorf("pebble/table: unsupported format version %d", version)
		}
		footer.format = TableFormatRocksDBv2
		footer.checksum = uint8(buf[0])
		if footer.checksum != checksumCRC32c {
			return footer, fmt.Errorf("pebble/table: unsupported checksum type %d", footer.checksum)
		}
		buf = buf[1:]

	default:
		return footer, errors.New("pebble/table: invalid table (bad magic number)")
	}

	{
		var n int
		footer.metaindexBH, n = decodeBlockHandle(buf)
		if n == 0 {
			return footer, errors.New("pebble/table: invalid table (bad metaindex block handle)")
		}
		buf = buf[n:]

		footer.indexBH, n = decodeBlockHandle(buf)
		if n == 0 {
			return footer, errors.New("pebble/table: invalid table (bad index block handle)")
		}
	}

	return footer, nil
}

func (f footer) encode(buf []byte) []byte {
	switch f.format {
	case TableFormatLevelDB:
		buf = buf[:levelDBFooterLen]
		for i := range buf {
			buf[i] = 0
		}
		n := encodeBlockHandle(buf[0:], f.metaindexBH)
		n += encodeBlockHandle(buf[n:], f.indexBH)
		copy(buf[len(buf)-len(levelDBMagic):], levelDBMagic)

	case TableFormatRocksDBv2:
		buf = buf[:rocksDBFooterLen]
		for i := range buf {
			buf[i] = 0
		}
		buf[0] = f.checksum
		n := 1
		n += encodeBlockHandle(buf[n:], f.metaindexBH)
		n += encodeBlockHandle(buf[n:], f.indexBH)
		binary.LittleEndian.PutUint32(buf[rocksDBVersionOffset:], rocksDBFormatVersion2)
		copy(buf[len(buf)-len(rocksDBMagic):], rocksDBMagic)
	}

	return buf
}
