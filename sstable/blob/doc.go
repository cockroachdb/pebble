// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package blob implements mechanics for encoding and decoding values into blob
// files.
//
// # Blob file format
//
// A blob file consists of a sequence of blob-value blocks containing values,
// followed by an index block describing the location of the blob-value blocks.
// At the tail of the file is a fixed-size footer encoding the exact offset and
// length of the index block.
//
// Semantically, a blob file implements an array of blob values. SSTables that
// reference separated blob values encode a tuple of a (blockID, blockValueID)
// to identify the value within the blob file. The blockID identifies the
// blob-value block that contains the value. The blockValueID identifies the
// value within the block. A reader retrieving a particular value uses the index
// block to identify the offset and length of the blob-value block containing
// the referenced value. It loads the identified blob-value block and then uses
// the block's internal structure to retrieve the value based on the
// blockValueID.
//
// A blob file may be rewritten (without rewriting referencing sstables) to
// remove unused values. Extant handles within sstables must continue to work.
// See the Sparseness section below for more details.
//
// ## Index Block
//
// The index block is used to determine which blob-value block contains a
// particular value and the block's physical offset and length within the file.
// The index block uses a columnar encoding (see pkg colblk) to encode two
// columns:
//
// **Virtual Blocks**:
// an array of uints that is only non-empty for blob files that have been
// rewritten. The length of the array is identified by the first 4 bytes of the
// index block as a custom block header. Within the array each 64-bit uint
// value's least significant 32 bits encode the index of the physical block
// containing the original block's data. This index can be used to look up the
// byte offset and length of the physical block within the index block's offsets
// column. The most significant 32 bits of each uint value encode a BlockValueID
// offset that remaps the original BlockValueID to the corresponding
// BlockValueID in the new physical block. A reader adds this BlockValueID
// offset to a handle's BlockValueID to get the index of the value within the
// physical block.
//
// TODO(jackson,radu): Consider interleaving the encoding of the uints so that
// in the common case of <64K blocks and <64K values per-block, the uint column
// can be encoded in 32-bits.
// See related issue https://github.com/cockroachdb/pebble/issues/4426.
//
// **Offsets**:
// an array of uints encoding the offset in the blob file at which each block
// begins. There are <num-physical-blocks>+1 offsets. The last offset points to
// the first byte after the last block. The length of each block is inferred
// through the difference between consecutive offsets.
//
// ## Blob Value Blocks
//
// A blob value block is a columnar block encoding blob values. It encodes a
// single column: a RawBytes of values. The colblk.RawBytes encoding allows
// constant-time access to the i'th value within the block.
//
// ## Sparseness
//
// A rewrite of a blob file elides values that are no longer referenced,
// conserving disk space. Within a value block, an absent value is represented
// as an empty byte slice within the RawBytes column. This requires the overhead
// of 1 additional offset within the RawBytes encoding (typically 2-4 bytes).
//
// If a wide swath of values are no longer referenced, entire blocks may elided.
// When this occurs, the index block's virtual blocks column will map multiple
// of the original blockIDs to the same physical block.
//
// We expect significant locality to gaps in referenced values. Compactions will
// remove swaths of references all at once, typically all the values of keys
// that fall within a narrow keyspan. This locality allows us to represent most
// sparseness using the gaps between blocks, without suffering the 2-4 bytes of
// overhead for absent values internally within a block.
//
// Note: If we find this locality not hold for some reason, we can extend the
// blob-value block format to encode a NullBitmap. This would allow us to
// represent missing values using 2-bits per missing value.
//
// ## Diagram
//
// +------------------------------------------------------------------------------+
// |                             BLOB FILE FORMAT                                 |
// +------------------------------------------------------------------------------+
// |                              Value Block #0                                  |
// |   +----------------------------------------------------------------------+   |
// |   | RawBytes[...]                                                        |   |
// |   +----------------------------------------------------------------------+   |
// |                              Value Block #1                                  |
// |   +----------------------------------------------------------------------+   |
// |   | RawBytes[...]                                                        |   |
// |   +----------------------------------------------------------------------+   |
// |                                 ...                                          |
// |                              Value Block #N                                  |
// |   +----------------------------------------------------------------------+   |
// |   | RawBytes[...]                                                        |   |
// |   +----------------------------------------------------------------------+   |
// |                                                                              |
// +------------------------------- Index Block ----------------------------------+
// | Custom Header (4 bytes)                                                      |
// |   Num virtual blocks: M                                                      |
// |   +---------Virtual blocks (M)--------+    +--------Offsets(N+1)---------+   |
// |   | idx    block index  valueIDoffset |    | idx         offset          |   |
// |   | 0      0            0             |    | 0           0               |   |
// |   | 1      0            0             |    | 1           32952           |   |
// |   | 2      0            32            |    | 2           65904           |   |
// |   | 3      1            0             |    | 3           92522           |   |
// |   | 4      2            0             |    | 4           125474          |   |
// |   | 5      3            0             |    +-----------------------------+   |
// |   +-----------------------------------+                                      |
// +----------------------------- Footer (30 bytes) ------------------------------+
// | CRC Checksum (4 bytes)                                                       |
// | Index Block Offset (8 bytes)                                                 |
// | Index Block Length (8 bytes)                                                 |
// | Checksum Type (1 byte)                                                       |
// | Format (1 byte)                                                              |
// | Magic String (8 bytes)                                                       |
// +------------------------------------------------------------------------------+
package blob
