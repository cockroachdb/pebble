// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package record reads and writes sequences of records. Each record is a stream
// of bytes that completes before the next record starts.
//
// When reading, call Next to obtain an io.Reader for the next record. Next will
// return io.EOF when there are no more records. It is valid to call Next
// without reading the current record to exhaustion.
//
// When writing, call Next to obtain an io.Writer for the next record. Calling
// Next finishes the current record. Call Close to finish the final record.
//
// Optionally, call Flush to finish the current record and flush the underlying
// writer without starting a new record. To start a new record after flushing,
// call Next.
//
// Neither Readers or Writers are safe to use concurrently.
//
// Example code:
//
//	func read(r io.Reader) ([]string, error) {
//		var ss []string
//		records := record.NewReader(r)
//		for {
//			rec, err := records.Next()
//			if err == io.EOF {
//				break
//			}
//			s, err := io.ReadAll(rec)
//			ss = append(ss, string(s))
//		}
//		return ss, nil
//	}
//
//	func write(w io.Writer, ss []string) error {
//		records := record.NewWriter(w)
//		for _, s := range ss {
//			rec, err := records.Next()
//			if err != nil {
//				return err
//			}
//			if _, err := rec.Write([]byte(s)), err != nil {
//				return err
//			}
//		}
//		return records.Close()
//	}
//
// The wire format is that the stream is divided into 32KiB blocks, and each
// block contains a number of tightly packed chunks. Chunks cannot cross block
// boundaries. The last block may be shorter than 32 KiB. Any unused bytes in a
// block must be zero.
//
// A record maps to one or more chunks. There are two chunk formats: legacy and
// recyclable. The legacy chunk format:
//
//	+----------+-----------+-----------+--- ... ---+
//	| CRC (4B) | Size (2B) | Type (1B) | Payload   |
//	+----------+-----------+-----------+--- ... ---+
//
// CRC is computed over the type and payload
// Size is the length of the payload in bytes
// Type is the chunk type
//
// There are four chunk types: whether the chunk is the full record, or the
// first, middle or last chunk of a multi-chunk record. A multi-chunk record
// has one first chunk, zero or more middle chunks, and one last chunk.
//
// The recyclable chunk format is similar to the legacy format, but extends
// the chunk header with an additional log number field. This allows reuse
// (recycling) of log files which can provide significantly better performance
// when syncing frequently as it avoids needing to update the file
// metadata. Additionally, recycling log files is a prequisite for using direct
// IO with log writing. The recyclable format is:
//
//	+----------+-----------+-----------+----------------+--- ... ---+
//	| CRC (4B) | Size (2B) | Type (1B) | Log number (4B)| Payload   |
//	+----------+-----------+-----------+----------------+--- ... ---+
//
// Recyclable chunks are distinguished from legacy chunks by the addition of 4
// extra "recyclable" chunk types that map directly to the legacy chunk types
// (i.e. full, first, middle, last). The CRC is computed over the type, log
// number, and payload.
//
// The WAL sync chunk format allows for detection of data corruption in some
// circumstances. The WAL sync format extends the recyclable header with an
// additional offset field. This allows "reading ahead" to be done in order to
// decipher whether an invalid or zeroed chunk was an artifact of corruption or the
// logical end of the log. SyncOffset is a promise that the log should have been
// synced up until the offset. A promised synced offset is needed because cloud
// providers  may write blocks out of order, rendering "read aheads" scanning for
// logNum inaccurate.
// The WAL sync format is:
//	+----------+-----------+-----------+----------------+------------------+--- ... ---+
//	| CRC (4B) | Size (2B) | Type (1B) | Log number (4B)| Sync Offset (8B) | Payload   |
//	+----------+-----------+-----------+----------------+------------------+--- ... ---+
//

package record

// The C++ Level-DB code calls this the log, but it has been renamed to record
// to avoid clashing with the standard log package, and because it is generally
// useful outside of logging. The C++ code also uses the term "physical record"
// instead of "chunk", but "chunk" is shorter and less confusing.

import (
	"encoding/binary"
	"io"
	"math"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/crc"
)

// These constants are part of the wire format and should not be changed.
const (
	invalidChunkEncoding = 0

	fullChunkEncoding   = 1
	firstChunkEncoding  = 2
	middleChunkEncoding = 3
	lastChunkEncoding   = 4

	recyclableFullChunkEncoding   = 5
	recyclableFirstChunkEncoding  = 6
	recyclableMiddleChunkEncoding = 7
	recyclableLastChunkEncoding   = 8

	walSyncFullChunkEncoding   = 9
	walSyncFirstChunkEncoding  = 10
	walSyncMiddleChunkEncoding = 11
	walSyncLastChunkEncoding   = 12
)

const (
	blockSize            = 32 * 1024
	blockSizeMask        = blockSize - 1
	legacyHeaderSize     = 7
	recyclableHeaderSize = legacyHeaderSize + 4
	walSyncHeaderSize    = recyclableHeaderSize + 8
)

// chunkPosition represents the type of a chunk in the log.
// Records can be split into multiple chunks and marked
// by the following position types:
// - invalidChunkPosition: an invalid chunk
// - fullChunkPosition: a complete record stored in a single chunk
// - firstChunkPosition: first chunk of a multi-chunk record
// - middleChunkPosition: intermediate chunk in a multi-chunk record
// - lastChunkPosition: final chunk of a multi-chunk record
type chunkPosition int

const (
	invalidChunkPosition chunkPosition = iota
	fullChunkPosition
	firstChunkPosition
	middleChunkPosition
	lastChunkPosition
)

// wireFormat specifies the encoding format used for chunks.
// wireFormat is used for backwards compatibility and new
// wire formats may be introduced to support additional
// WAL chunks.
type wireFormat int

const (
	invalidWireFormat wireFormat = iota
	legacyWireFormat
	recyclableWireFormat
	walSyncWireFormat
)

// headerFormat represents the format of a chunk which has
// a chunkPosition, wireFormat, and a headerSize.
type headerFormat struct {
	chunkPosition
	wireFormat
	headerSize int
}

// headerFormatMappings translates encodings to headerFormats
var headerFormatMappings = [...]headerFormat{
	invalidChunkEncoding:          {chunkPosition: invalidChunkPosition, wireFormat: invalidWireFormat, headerSize: 0},
	fullChunkEncoding:             {chunkPosition: fullChunkPosition, wireFormat: legacyWireFormat, headerSize: legacyHeaderSize},
	firstChunkEncoding:            {chunkPosition: firstChunkPosition, wireFormat: legacyWireFormat, headerSize: legacyHeaderSize},
	middleChunkEncoding:           {chunkPosition: middleChunkPosition, wireFormat: legacyWireFormat, headerSize: legacyHeaderSize},
	lastChunkEncoding:             {chunkPosition: lastChunkPosition, wireFormat: legacyWireFormat, headerSize: legacyHeaderSize},
	recyclableFullChunkEncoding:   {chunkPosition: fullChunkPosition, wireFormat: recyclableWireFormat, headerSize: recyclableHeaderSize},
	recyclableFirstChunkEncoding:  {chunkPosition: firstChunkPosition, wireFormat: recyclableWireFormat, headerSize: recyclableHeaderSize},
	recyclableMiddleChunkEncoding: {chunkPosition: middleChunkPosition, wireFormat: recyclableWireFormat, headerSize: recyclableHeaderSize},
	recyclableLastChunkEncoding:   {chunkPosition: lastChunkPosition, wireFormat: recyclableWireFormat, headerSize: recyclableHeaderSize},
	walSyncFullChunkEncoding:      {chunkPosition: fullChunkPosition, wireFormat: walSyncWireFormat, headerSize: walSyncHeaderSize},
	walSyncFirstChunkEncoding:     {chunkPosition: firstChunkPosition, wireFormat: walSyncWireFormat, headerSize: walSyncHeaderSize},
	walSyncMiddleChunkEncoding:    {chunkPosition: middleChunkPosition, wireFormat: walSyncWireFormat, headerSize: walSyncHeaderSize},
	walSyncLastChunkEncoding:      {chunkPosition: lastChunkPosition, wireFormat: walSyncWireFormat, headerSize: walSyncHeaderSize},
}

var (
	// ErrNotAnIOSeeker is returned if the io.Reader underlying a Reader does not implement io.Seeker.
	ErrNotAnIOSeeker = errors.New("pebble/record: reader does not implement io.Seeker")

	// ErrNoLastRecord is returned if LastRecordOffset is called and there is no previous record.
	ErrNoLastRecord = errors.New("pebble/record: no last record exists")

	// ErrZeroedChunk is returned if a chunk is encountered that is zeroed. This
	// usually occurs due to log file preallocation.
	ErrZeroedChunk = errors.New("pebble/record: zeroed chunk")

	// ErrInvalidChunk is returned if a chunk is encountered with an invalid
	// header, length, or checksum. This usually occurs when a log is recycled,
	// but can also occur due to corruption.
	ErrInvalidChunk = errors.New("pebble/record: invalid chunk")
)

// IsInvalidRecord returns true if the error matches one of the error types
// returned for invalid records. These are treated in a way similar to io.EOF
// in recovery code.
func IsInvalidRecord(err error) bool {
	return err == ErrZeroedChunk || err == ErrInvalidChunk || err == io.ErrUnexpectedEOF
}

// Reader reads records from an underlying io.Reader.
type Reader struct {
	// r is the underlying reader.
	r io.Reader
	// logNum is the low 32-bits of the log's file number. May be zero when used
	// with log files that do not have a file number (e.g. the MANIFEST).
	logNum uint32
	// blockNum is the zero based block number currently held in buf.
	blockNum int64
	// seq is the sequence number of the current record.
	seq int
	// buf[begin:end] is the unread portion of the current chunk's payload. The
	// low bound, begin, excludes the chunk header.
	begin, end int
	// n is the number of bytes of buf that are valid. Once reading has started,
	// only the final block can have n < blockSize.
	n int
	// last is whether the current chunk is the last chunk of the record.
	last bool
	// err is any accumulated error.
	err error
	// buf is the buffer.
	buf [blockSize]byte
	// invalidOffset is the first encountered chunk offset found during nextChunk()
	// that had garbage values. It is used to clarify whether or not a garbage chunk
	// encountered during WAL replay was the logical EOF or confirmed corruption.
	invalidOffset uint64

	// loggerForTesting is a logging helper used by the Reader to accumulate log messages.
	loggerForTesting loggerForTesting
}

type loggerForTesting interface {
	logf(format string, args ...interface{})
}

// NewReader returns a new reader. If the file contains records encoded using
// the recyclable record format, then the log number in those records must
// match the specified logNum.
func NewReader(r io.Reader, logNum base.DiskFileNum) *Reader {
	return &Reader{
		r:        r,
		logNum:   uint32(logNum),
		blockNum: -1,
		// invalidOffset is initialized as MaxUint64 so that reading ahead
		// with the old chunk wire formats results in io.ErrUnexpectedEOF.
		invalidOffset: math.MaxUint64,
	}
}

// nextChunk sets r.buf[r.i:r.j] to hold the next chunk's payload, reading the
// next block into the buffer if necessary.
func (r *Reader) nextChunk(wantFirst bool) error {
	for {
		if r.end+legacyHeaderSize <= r.n {
			checksum := binary.LittleEndian.Uint32(r.buf[r.end+0 : r.end+4])
			length := binary.LittleEndian.Uint16(r.buf[r.end+4 : r.end+6])
			chunkEncoding := r.buf[r.end+6]

			if int(chunkEncoding) >= len(headerFormatMappings) {
				r.invalidOffset = uint64(r.blockNum)*blockSize + uint64(r.begin)
				return ErrInvalidChunk
			}
			headerFormat := headerFormatMappings[chunkEncoding]
			chunkPosition, wireFormat, headerSize := headerFormat.chunkPosition, headerFormat.wireFormat, headerFormat.headerSize

			if checksum == 0 && length == 0 && chunkPosition == invalidChunkPosition {
				// remaining bytes < 11
				// The remaining bytes in the block is < 11 so regardless of which chunk format is
				// being written (Recyclable or walSync), we should skip to the next block.
				if r.end+recyclableHeaderSize > r.n {
					// Skip the rest of the block if the recyclable header size does not
					// fit within it. The end of a block will be zeroed out if the log writer
					// cannot fit another chunk into it, even a chunk with no payload like
					// the EOF Trailer.
					r.end = r.n
					continue
				}

				// check if 11 <= remaining bytes < 19
				// If so, the remaining bytes in the block can fit a recyclable header but not a
				// walSync header. In this case, check if the remainder of the chunk is all zeroes.
				//
				// If the remainder was all zeroes, then we tolerate this and continue to
				// the next block. However, if there was non-zero content in the remaining chunk,
				// then was possibly an artifact of corruption found and ErrZeroedChunk should be
				// returned.
				if r.end+walSyncHeaderSize > r.n {
					// Check that the remainder of the chunk is all zeroes.
					for i := r.end; i < r.n; i++ {
						if r.buf[i] != 0 {
							r.invalidOffset = uint64(r.blockNum)*blockSize + uint64(r.begin)
							return ErrZeroedChunk
						}
					}
					r.end = r.n
					continue
				}

				// The last case is when there was more than 19 bytes which means there shouldn't be
				// a zeroed header. Thus, this case should also return ErrZeroedChunk.
				r.invalidOffset = uint64(r.blockNum)*blockSize + uint64(r.begin)
				return ErrZeroedChunk
			}

			if wireFormat == invalidWireFormat {
				r.invalidOffset = uint64(r.blockNum)*blockSize + uint64(r.begin)
				return ErrInvalidChunk
			}
			if wireFormat == recyclableWireFormat || wireFormat == walSyncWireFormat {
				if r.end+headerSize > r.n {
					r.invalidOffset = uint64(r.blockNum)*blockSize + uint64(r.begin)
					return ErrInvalidChunk
				}

				logNum := binary.LittleEndian.Uint32(r.buf[r.end+7 : r.end+11])
				if logNum != r.logNum {
					// An EOF trailer encodes a log number that is 1 more than the
					// current log number.
					if logNum == 1+r.logNum && wantFirst {
						return io.EOF
					}
					// Otherwise, treat this chunk as invalid in order to prevent reading
					// of a partial record.
					r.invalidOffset = uint64(r.blockNum)*blockSize + uint64(r.begin)
					return ErrInvalidChunk
				}
			}

			r.begin = r.end + headerSize
			r.end = r.begin + int(length)
			if r.end > r.n {
				// The chunk straddles a 32KB boundary (or the end of file).
				r.invalidOffset = uint64(r.blockNum)*blockSize + uint64(r.begin)
				return ErrInvalidChunk
			}
			if checksum != crc.New(r.buf[r.begin-headerSize+6:r.end]).Value() {
				r.invalidOffset = uint64(r.blockNum)*blockSize + uint64(r.begin)
				return ErrInvalidChunk
			}
			if wantFirst {
				if chunkPosition != fullChunkPosition && chunkPosition != firstChunkPosition {
					continue
				}
			}
			r.last = chunkPosition == fullChunkPosition || chunkPosition == lastChunkPosition
			return nil
		}
		if r.n < blockSize && r.blockNum >= 0 {
			if !wantFirst || r.end != r.n {
				// This can happen if the previous instance of the log ended with a
				// partial block at the same blockNum as the new log but extended
				// beyond the partial block of the new log.
				r.invalidOffset = uint64(r.blockNum)*blockSize + uint64(r.begin)
				return ErrInvalidChunk
			}
			return io.EOF
		}
		n, err := io.ReadFull(r.r, r.buf[:])
		if err != nil && err != io.ErrUnexpectedEOF {
			if err == io.EOF && !wantFirst {
				r.invalidOffset = uint64(r.blockNum)*blockSize + uint64(r.begin)
				return io.ErrUnexpectedEOF
			}
			return err
		}
		r.begin, r.end, r.n = 0, 0, n
		r.blockNum++
	}
}

// Next returns a reader for the next record. It returns io.EOF if there are no
// more records. The reader returned becomes stale after the next Next call,
// and should no longer be used.
func (r *Reader) Next() (io.Reader, error) {
	r.seq++
	if r.err != nil {
		return nil, r.err
	}
	r.begin = r.end
	r.err = r.nextChunk(true)
	if errors.Is(r.err, ErrInvalidChunk) || errors.Is(r.err, ErrZeroedChunk) {
		readAheadResult := r.readAheadForCorruption()
		return nil, readAheadResult
	}
	if r.err != nil {
		return nil, r.err
	}
	return singleReader{r, r.seq}, nil
}

// readAheadForCorruption scans ahead in the log to detect corruption.
// It loads in blocks and reads chunks until it either detects corruption
// due to an offset (encoded in a chunk header) exceeding the invalid offset,
// or encountering end of file when loading a new block.
//
// This function is called from Reader.Read() and Reader.Next() after an error
// is recorded in r.err after a call to Reader.nextChunk(). Concretely, the function
// pre-conditions are that r.err has the error returned from nextChunk() when
// it is called from Read() or Next(); similarly, r.invalidOffset will have
// the first invalid offset encountered during a call to nextChunk().
//
// The function post-conditions are that the error stored in r.err is returned
// if there is confirmation of a corruption, otherwise ErrUnexpectedEOF is
// returned after reading all the blocks without corruption confirmation.
func (r *Reader) readAheadForCorruption() error {
	if r.loggerForTesting != nil {
		r.loggerForTesting.logf("Starting read ahead for corruption. Block corrupted %d.\n", r.blockNum)
	}

	for {
		// Load the next block into r.buf.
		n, err := io.ReadFull(r.r, r.buf[:])
		r.begin, r.end, r.n = 0, 0, n
		r.blockNum++
		if r.loggerForTesting != nil {
			r.loggerForTesting.logf("Read block %d with %d bytes\n", r.blockNum, n)
		}

		if errors.Is(err, io.EOF) {
			// io.ErrUnexpectedEOF is returned instead of
			// io.EOF because io library functions clear
			// an error when it is io.EOF. io.ErrUnexpectedEOF
			// is returned so that the error is not cleared
			// when the io library makes calls to Reader.Read().
			//
			// Since no sync offset was found to indicate that the
			// invalid chunk should have been valid, the chunk represents
			// an abrupt, unclean termination of the logical log. This
			// abrupt end of file represented by io.ErrUnexpectedEOF.
			if r.loggerForTesting != nil {
				r.loggerForTesting.logf("\tEncountered io.EOF; returning io.ErrUnexpectedEOF since no sync offset found.\n")
			}
			return io.ErrUnexpectedEOF
		}
		// The last block of a log can be less than 32KiB, which is
		// the length of r.buf. Thus, we should still parse the data in
		// the last block when io.ReadFull returns io.ErrUnexpectedEOF.
		// However, if the error is not ErrUnexpectedEOF, then this
		// error should be surfaced.
		if err != nil && err != io.ErrUnexpectedEOF {
			if r.loggerForTesting != nil {
				r.loggerForTesting.logf("\tError reading block %d: %v", r.blockNum, err)
			}
			return err
		}

		for r.end+legacyHeaderSize <= r.n {
			checksum := binary.LittleEndian.Uint32(r.buf[r.end+0 : r.end+4])
			length := binary.LittleEndian.Uint16(r.buf[r.end+4 : r.end+6])
			chunkEncoding := r.buf[r.end+6]

			if r.loggerForTesting != nil {
				r.loggerForTesting.logf("\tBlock %d: Processing chunk at offset %d, checksum=%d, length=%d, encoding=%d\n", r.blockNum, r.end, checksum, length, chunkEncoding)
			}

			if int(chunkEncoding) >= len(headerFormatMappings) {
				if r.loggerForTesting != nil {
					r.loggerForTesting.logf("\tInvalid chunk encoding encountered (value: %d); stopping chunk scan in block %d\n", chunkEncoding, r.blockNum)
				}
				break
			}

			headerFormat := headerFormatMappings[chunkEncoding]
			chunkPosition, wireFormat, headerSize := headerFormat.chunkPosition, headerFormat.wireFormat, headerFormat.headerSize
			if checksum == 0 && length == 0 && chunkPosition == invalidChunkPosition {
				if r.loggerForTesting != nil {
					r.loggerForTesting.logf("\tFound invalid chunk marker at block %d offset %d; aborting this block scan\n", r.blockNum, r.end)
				}
				break
			}
			if wireFormat == invalidWireFormat {
				if r.loggerForTesting != nil {
					r.loggerForTesting.logf("\tInvalid wire format detected in block %d at offset %d\n", r.blockNum, r.end)
				}
				break
			}
			if wireFormat == recyclableWireFormat || wireFormat == walSyncWireFormat {
				if r.end+headerSize > r.n {
					if r.loggerForTesting != nil {
						r.loggerForTesting.logf("\tIncomplete header in block %d at offset %d; breaking out\n", r.blockNum, r.end)
					}
					break
				}
				logNum := binary.LittleEndian.Uint32(r.buf[r.end+7 : r.end+11])
				if logNum != r.logNum {
					if r.loggerForTesting != nil {
						r.loggerForTesting.logf("\tMismatch log number in block %d at offset %d (expected %d, got %d)\n", r.blockNum, r.end, r.logNum, logNum)
					}
					break
				}
			}

			r.begin = r.end + headerSize
			r.end = r.begin + int(length)
			if r.end > r.n {
				// The chunk straddles a 32KB boundary (or the end of file).
				if r.loggerForTesting != nil {
					r.loggerForTesting.logf("\tChunk in block %d spans beyond block boundaries (begin=%d, end=%d, n=%d)\n", r.blockNum, r.begin, r.end, r.n)
				}
				break
			}
			if checksum != crc.New(r.buf[r.begin-headerSize+6:r.end]).Value() {
				if r.loggerForTesting != nil {
					r.loggerForTesting.logf("\tChecksum mismatch in block %d at offset %d; potential corruption\n", r.blockNum, r.end)
				}
				break
			}

			// Decode offset in header when chunk has the WAL Sync wire format.
			if wireFormat == walSyncWireFormat {
				syncedOffset := binary.LittleEndian.Uint64(r.buf[r.begin-headerSize+11 : r.begin-headerSize+19])
				if r.loggerForTesting != nil {
					r.loggerForTesting.logf("\tBlock %d: Found WAL sync chunk with syncedOffset=%d (invalidOffset=%d)\n", r.blockNum, syncedOffset, r.invalidOffset)
				}
				// If the encountered chunk offset promises durability beyond the invalid offset,
				// the invalid offset must have been corruption.
				if syncedOffset > r.invalidOffset {
					if r.loggerForTesting != nil {
						r.loggerForTesting.logf("\tCorruption confirmed: syncedOffset %d exceeds invalidOffset %d\n", syncedOffset, r.invalidOffset)
					}
					return r.err
				}
			}
		}
	}
}

// Offset returns the current offset within the file. If called immediately
// before a call to Next(), Offset() will return the record offset.
func (r *Reader) Offset() int64 {
	if r.blockNum < 0 {
		return 0
	}
	return int64(r.blockNum)*blockSize + int64(r.end)
}

// seekRecord seeks in the underlying io.Reader such that calling r.Next
// returns the record whose first chunk header starts at the provided offset.
// Its behavior is undefined if the argument given is not such an offset, as
// the bytes at that offset may coincidentally appear to be a valid header.
//
// It returns ErrNotAnIOSeeker if the underlying io.Reader does not implement
// io.Seeker.
//
// seekRecord will fail and return an error if the Reader previously
// encountered an error, including io.EOF.
//
// The offset is always relative to the start of the underlying io.Reader, so
// negative values will result in an error as per io.Seeker.
func (r *Reader) seekRecord(offset int64) error {
	r.seq++
	if r.err != nil {
		return r.err
	}

	s, ok := r.r.(io.Seeker)
	if !ok {
		return ErrNotAnIOSeeker
	}

	// Only seek to an exact block offset.
	c := int(offset & blockSizeMask)
	if _, r.err = s.Seek(offset&^blockSizeMask, io.SeekStart); r.err != nil {
		return r.err
	}

	// Clear the state of the internal reader.
	r.begin, r.end, r.n = 0, 0, 0
	r.blockNum, r.last = -1, false
	if r.err = r.nextChunk(false); r.err != nil {
		return r.err
	}

	// Now skip to the offset requested within the block. A subsequent
	// call to Next will return the block at the requested offset.
	r.begin, r.end = c, c

	return nil
}

type singleReader struct {
	r   *Reader
	seq int
}

func (x singleReader) Read(p []byte) (int, error) {
	r := x.r
	if r.seq != x.seq {
		return 0, errors.New("pebble/record: stale reader")
	}
	if r.err != nil {
		return 0, r.err
	}
	for r.begin == r.end {
		if r.last {
			return 0, io.EOF
		}
		r.err = r.nextChunk(false)
		if errors.Is(r.err, ErrInvalidChunk) || errors.Is(r.err, ErrZeroedChunk) {
			readAheadResult := r.readAheadForCorruption()
			return 0, readAheadResult
		}
		if r.err != nil {
			return 0, r.err
		}
	}
	n := copy(p, r.buf[r.begin:r.end])
	r.begin += n
	return n, nil
}

// Writer writes records to an underlying io.Writer.
type Writer struct {
	// w is the underlying writer.
	w io.Writer
	// seq is the sequence number of the current record.
	seq int
	// f is w as a flusher.
	f flusher
	// buf[i:j] is the bytes that will become the current chunk.
	// The low bound, i, includes the chunk header.
	i, j int
	// buf[:written] has already been written to w.
	// written is zero unless Flush has been called.
	written int
	// baseOffset is the base offset in w at which writing started. If
	// w implements io.Seeker, it's relative to the start of w, 0 otherwise.
	baseOffset int64
	// blockNumber is the zero based block number currently held in buf.
	blockNumber int64
	// lastRecordOffset is the offset in w where the last record was
	// written (including the chunk header). It is a relative offset to
	// baseOffset, thus the absolute offset of the last record is
	// baseOffset + lastRecordOffset.
	lastRecordOffset int64
	// first is whether the current chunk is the first chunk of the record.
	first bool
	// pending is whether a chunk is buffered but not yet written.
	pending bool
	// err is any accumulated error.
	err error
	// buf is the buffer.
	buf [blockSize]byte
}

// NewWriter returns a new Writer.
func NewWriter(w io.Writer) *Writer {
	f, _ := w.(flusher)

	var o int64
	if s, ok := w.(io.Seeker); ok {
		var err error
		if o, err = s.Seek(0, io.SeekCurrent); err != nil {
			o = 0
		}
	}
	return &Writer{
		w:                w,
		f:                f,
		baseOffset:       o,
		lastRecordOffset: -1,
	}
}

// fillHeader fills in the header for the pending chunk.
func (w *Writer) fillHeader(last bool) {
	if w.i+legacyHeaderSize > w.j || w.j > blockSize {
		panic("pebble/record: bad writer state")
	}
	if last {
		if w.first {
			w.buf[w.i+6] = fullChunkEncoding
		} else {
			w.buf[w.i+6] = lastChunkEncoding
		}
	} else {
		if w.first {
			w.buf[w.i+6] = firstChunkEncoding
		} else {
			w.buf[w.i+6] = middleChunkEncoding
		}
	}
	binary.LittleEndian.PutUint32(w.buf[w.i+0:w.i+4], crc.New(w.buf[w.i+6:w.j]).Value())
	binary.LittleEndian.PutUint16(w.buf[w.i+4:w.i+6], uint16(w.j-w.i-legacyHeaderSize))
}

// writeBlock writes the buffered block to the underlying writer, and reserves
// space for the next chunk's header.
func (w *Writer) writeBlock() {
	_, w.err = w.w.Write(w.buf[w.written:])
	w.i = 0
	w.j = legacyHeaderSize
	w.written = 0
	w.blockNumber++
}

// writePending finishes the current record and writes the buffer to the
// underlying writer.
func (w *Writer) writePending() {
	if w.err != nil {
		return
	}
	if w.pending {
		w.fillHeader(true)
		w.pending = false
	}
	_, w.err = w.w.Write(w.buf[w.written:w.j])
	w.written = w.j
}

// Close finishes the current record and closes the writer.
func (w *Writer) Close() error {
	w.seq++
	w.writePending()
	if w.err != nil {
		return w.err
	}
	w.err = errors.New("pebble/record: closed Writer")
	return nil
}

// Flush finishes the current record, writes to the underlying writer, and
// flushes it if that writer implements interface{ Flush() error }.
func (w *Writer) Flush() error {
	w.seq++
	w.writePending()
	if w.err != nil {
		return w.err
	}
	if w.f != nil {
		w.err = w.f.Flush()
		return w.err
	}
	return nil
}

// Next returns a writer for the next record. The writer returned becomes stale
// after the next Close, Flush or Next call, and should no longer be used.
func (w *Writer) Next() (io.Writer, error) {
	w.seq++
	if w.err != nil {
		return nil, w.err
	}
	if w.pending {
		w.fillHeader(true)
	}
	w.i = w.j
	w.j = w.j + legacyHeaderSize
	// Check if there is room in the block for the header.
	if w.j > blockSize {
		// Fill in the rest of the block with zeroes.
		clear(w.buf[w.i:])
		w.writeBlock()
		if w.err != nil {
			return nil, w.err
		}
	}
	w.lastRecordOffset = w.baseOffset + w.blockNumber*blockSize + int64(w.i)
	w.first = true
	w.pending = true
	return singleWriter{w, w.seq}, nil
}

// WriteRecord writes a complete record. Returns the offset just past the end
// of the record.
func (w *Writer) WriteRecord(p []byte) (int64, error) {
	if w.err != nil {
		return -1, w.err
	}
	t, err := w.Next()
	if err != nil {
		return -1, err
	}
	if _, err := t.Write(p); err != nil {
		return -1, err
	}
	w.writePending()
	offset := w.blockNumber*blockSize + int64(w.j)
	return offset, w.err
}

// Size returns the current size of the file.
func (w *Writer) Size() int64 {
	if w == nil {
		return 0
	}
	return w.blockNumber*blockSize + int64(w.j)
}

// LastRecordOffset returns the offset in the underlying io.Writer of the last
// record so far - the one created by the most recent Next call. It is the
// offset of the first chunk header, suitable to pass to Reader.SeekRecord.
//
// If that io.Writer also implements io.Seeker, the return value is an absolute
// offset, in the sense of io.SeekStart, regardless of whether the io.Writer
// was initially at the zero position when passed to NewWriter. Otherwise, the
// return value is a relative offset, being the number of bytes written between
// the NewWriter call and any records written prior to the last record.
//
// If there is no last record, i.e. nothing was written, LastRecordOffset will
// return ErrNoLastRecord.
func (w *Writer) LastRecordOffset() (int64, error) {
	if w.err != nil {
		return 0, w.err
	}
	if w.lastRecordOffset < 0 {
		return 0, ErrNoLastRecord
	}
	return w.lastRecordOffset, nil
}

type singleWriter struct {
	w   *Writer
	seq int
}

func (x singleWriter) Write(p []byte) (int, error) {
	w := x.w
	if w.seq != x.seq {
		return 0, errors.New("pebble/record: stale writer")
	}
	if w.err != nil {
		return 0, w.err
	}
	n0 := len(p)
	for len(p) > 0 {
		// Write a block, if it is full.
		if w.j == blockSize {
			w.fillHeader(false)
			w.writeBlock()
			if w.err != nil {
				return 0, w.err
			}
			w.first = false
		}
		// Copy bytes into the buffer.
		n := copy(w.buf[w.j:], p)
		w.j += n
		p = p[n:]
	}
	return n0, nil
}
