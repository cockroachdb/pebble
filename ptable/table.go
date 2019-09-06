// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package ptable // import "github.com/cockroachdb/pebble/ptable"

// The ptable file format is similar to the sstable format except that data
// blocks are formatted differently.
//
// <start_of_file>
// [data block 0]
// [data block 1]
// ...
// [data block N-1]
// [meta block 0]
// [meta block 1]
// ...
// [meta block K-1]
// [metaindex block]
// [index block]
// [footer]
// <end_of_file>
//
// The block consists of some data and a 5 byte trailer: a 1 byte block type
// and a 4 byte checksum of the (optionally) compressed data. The block type
// gives the per-block compression used; each block is compressed
// independently. The checksum algorithm is described in the pebble/crc
// package.
//
// The decompressed block consists of structured row data in a columnar
// layout. The schema for rows is fixed for an entire table.
//
// An index block consists of a fixed 2 column schema of keys and block
// handles. The i'th value is the encoded block handle of the i'th data
// block. The i'th key is a separator for i < N-1, and a successor for i ==
// N-1. The separator between blocks i and i+1 is a key that is >= every key in
// block i and is < every key i block i+1. The successor for the final block is
// a key that is >= every key in block N-1. Note that the keys in the index
// block are not stored as such in data blocks.
//
// A block handle is an offset and a length. In the index block, the block
// handle length is not stored directly but is instead calculated using the
// offset of the following block. In the index block, the block handle offset
// is stored as an 8-byte 64-bit integer.

// TODO(peter):
//
// - We likely need to allow different block schemas within the same
//   table. This is needed for both interleaved tables and for L0 tables which
//   will likely hold blocks for many different SQL tables at once.
//
// - Do we need to store which columns the rows are sorted on? How to store
//   sort order? Yes, we need to be able to merge tables in order to perform
//   compactions and the fundamental operation here is comparison. We need to
//   know the key columns.
//
// - Iteration iterates over blocks. Every row has an implicit timestamp column
//   containing the hlc timestamp. Need to be able to filter to get only the
//   desired version of a row.
//
// - How to integrate iteration with the memtable? The memtable contains
//   relatively little data. Do we convert to columnar data on the fly?
//
// - How to specify the schema for a given key? The number of schemas is the
//   number of indexes in all of the tables. The /table/index/ prefix is a
//   unique prefix. Perhaps there should be a callback from key to schema and
//   the "schema key" can be stored in each block.
//
// - Define scan operation which takes a start and end key and an operator tree
//   composed of projections and filters and returns an iterator over the data.
//
// - How to handle changes to the schema? This happens for the primary row data
//   only and is is due to the addition or deletion of columns. The schema
//   needs to be stored in the table and when merging tables columns need to be
//   added and dropped appropriately.
//
// - What to do about column families where row data is spread across multiple
//   key/value pairs? Column families are logically part of the same row. In
//   some ways, they are similar to the Merge operation, allowing partial
//   updates to a row.
//
// - How to handle the MVCC metadata keys? The intent data (txn, etc) logically
//   belongs on the row, yet no other version of the row has that data. Is this
//   a hidden "intent" column that is NULL for other versions of the row?
//
// - What to do about Merge operations? Simplest approach would be to disallow
//   them on the structured data portion of the key space.
//
// - Need to add a per-row "deleted" bitmap in order to support deletion
//   tombstones.
//
// - How to handle range deletion tombstones?
//
// - How to support UNIQUE indexes with NULL-able keys? Such indexes have a
//   fixed set of columns in the key, but also a unique-ifier suffix in case
//   one of the columns is NULL. Perhaps just model such columns exactly like
//   that, with an implicit hidden column that is part of the key and only
//   non-empty if one of the key column values is NULL.
//
// - How to perform level merging iteration? This is sort of like a full-outer
//   merge-join. For equality on the key columns we keep the version in the
//   higher level.
//
// - How to perform most version selection? There seems to be some similarity
//   with a sorted distinct operation, but instead of keeping the most recent
//   value, we choose the one which meets some criteria on the timestamp
//   column. Or perhaps the op first filters out versions which are newer than
//   the read timestamp, and then does a sorted distinct to only keep the most
//   recent version of the versions remaining.
//
// - How to support interleaved tables? The naive approach of switching blocks
//   whenever we switch from parent to child keys would result in a lot of
//   small blocks for the parent rows (perhaps 1 row per block). Seems better
//   to interleave at the block level, though this will make iteration through
//   a table more complex.

// RowWriter provides an interface for writing the column data for a row.
type RowWriter interface {
	PutBool(col int, v bool)
	PutInt8(col int, v int8)
	PutInt16(col int, v int16)
	PutInt32(col int, v int32)
	PutInt64(col int, v int64)
	PutFloat32(col int, v float32)
	PutFloat64(col int, v float64)
	PutBytes(col int, v []byte)
	PutNull(col int)
}

// RowReader provides an interface for reading the column data from a row.
type RowReader interface {
	Null(col int) bool
	Bool(col int) bool
	Int8(col int) int8
	Int16(col int) int16
	Int32(col int) int32
	Int64(col int) int64
	Float32(col int) float32
	Float64(col int) float64
	Bytes(col int) []byte
}

// Env holds a set of functions used to convert key/value data to and from
// structured column data.
type Env struct {
	// Schema specifies the columns for a table. The order of the columns in the
	// schema matters. Columns that are part of the key need to occur in the same
	// order as they are present in the key and all key columns need to specify a
	// direction.
	Schema []ColumnDef
	// Decode the columns from a key/value pair, outputting the column data to
	// writer. Buf can tbe used for temporary storage during decoding. The column
	// data written to writer is copied.
	Decode func(key, value, buf []byte, writer RowWriter)
	// Encode the columns from the specified row into a key/value pair. Buf can
	// be used to store the encoded key/value data or for temporary storage.
	Encode func(row RowReader, buf []byte) (key, value []byte)
}
