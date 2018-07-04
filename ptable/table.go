package ptable

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
