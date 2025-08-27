// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package wal

import (
	"bytes"
	"cmp"
	"io"
	"slices"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2/batchrepr"
	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/record"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/redact"
)

// A LogicalLog identifies a logical WAL and its consituent segment files.
type LogicalLog struct {
	Num NumWAL
	// segments contains the list of the consistuent physical segment files that
	// make up the single logical WAL file. segments is ordered by increasing
	// logIndex.
	segments []segment
}

// A segment represents an individual physical file that makes up a contiguous
// segment of a logical WAL. If a failover occurred during a WAL's lifetime, a
// WAL may be composed of multiple segments.
type segment struct {
	logNameIndex LogNameIndex
	dir          Dir
}

// String implements fmt.Stringer.
func (s segment) String() string {
	return redact.StringWithoutMarkers(s)
}

// SafeFormat implements redact.SafeFormatter.
func (s segment) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("(%s,%s)", errors.Safe(s.dir.Dirname), s.logNameIndex)
}

// NumSegments returns the number of constituent physical log files that make up
// the log.
func (ll LogicalLog) NumSegments() int { return len(ll.segments) }

// SegmentLocation returns the FS and path for the i-th physical segment file.
func (ll LogicalLog) SegmentLocation(i int) (vfs.FS, string) {
	s := ll.segments[i]
	path := s.dir.FS.PathJoin(s.dir.Dirname, makeLogFilename(ll.Num, s.logNameIndex))
	return s.dir.FS, path
}

// PhysicalSize stats each of the log's physical files, summing their sizes.
func (ll LogicalLog) PhysicalSize() (uint64, error) {
	var size uint64
	for i := range ll.segments {
		fs, path := ll.SegmentLocation(i)
		stat, err := fs.Stat(path)
		if err != nil {
			return 0, err
		}
		size += uint64(stat.Size())
	}
	return size, nil
}

// OpenForRead a logical WAL for reading.
func (ll LogicalLog) OpenForRead() Reader {
	return newVirtualWALReader(ll)
}

// String implements fmt.Stringer.
func (ll LogicalLog) String() string {
	return redact.StringWithoutMarkers(ll)
}

// SafeFormat implements redact.SafeFormatter.
func (ll LogicalLog) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("%s: {", base.DiskFileNum(ll.Num).String())
	for i := range ll.segments {
		if i > 0 {
			w.SafeString(", ")
		}
		w.Print(ll.segments[i])
	}
	w.SafeString("}")
}

// appendDeletableLogs appends all of the LogicalLog's constituent physical
// files as DeletableLogs to dst, returning the modified slice.
// AppendDeletableLogs will Stat physical files to determine physical sizes.
// AppendDeletableLogs does not make any judgmenet on whether a log file is
// obsolete, so callers must take care not to delete logs that are still
// unflushed.
func appendDeletableLogs(dst []DeletableLog, ll LogicalLog) ([]DeletableLog, error) {
	for i := range ll.segments {
		fs, path := ll.SegmentLocation(i)
		stat, err := fs.Stat(path)
		if err != nil {
			return dst, err
		}
		dst = append(dst, DeletableLog{
			FS:             fs,
			Path:           path,
			NumWAL:         ll.Num,
			ApproxFileSize: uint64(stat.Size()),
		})
	}
	return dst, nil
}

// Scan finds all log files in the provided directories. It returns an
// ordered list of WALs in increasing NumWAL order.
func Scan(dirs ...Dir) (Logs, error) {
	var fa FileAccumulator
	for _, d := range dirs {
		ls, err := d.FS.List(d.Dirname)
		if err != nil {
			return nil, errors.Wrapf(err, "reading %q", d.Dirname)
		}
		for _, name := range ls {
			_, err := fa.maybeAccumulate(d.FS, d.Dirname, name)
			if err != nil {
				return nil, err
			}
		}
	}
	return fa.wals, nil
}

// FileAccumulator parses and accumulates log files.
type FileAccumulator struct {
	wals []LogicalLog
}

// MaybeAccumulate parses the provided path's filename. If the filename
// indicates the file is a write-ahead log, MaybeAccumulate updates its internal
// state to remember the file and returns isLogFile=true. An error is returned
// if the file is a duplicate.
func (a *FileAccumulator) MaybeAccumulate(fs vfs.FS, path string) (isLogFile bool, err error) {
	filename := fs.PathBase(path)
	dirname := fs.PathDir(path)
	return a.maybeAccumulate(fs, dirname, filename)
}

// Finish returns a Logs constructed from the physical files observed through
// MaybeAccumulate.
func (a *FileAccumulator) Finish() Logs {
	wals := a.wals
	a.wals = nil
	return wals
}

func (a *FileAccumulator) maybeAccumulate(
	fs vfs.FS, dirname, name string,
) (isLogFile bool, err error) {
	dfn, li, ok := ParseLogFilename(name)
	if !ok {
		return false, nil
	}
	// Have we seen this logical log number yet?
	i, found := slices.BinarySearchFunc(a.wals, dfn, func(lw LogicalLog, n NumWAL) int {
		return cmp.Compare(lw.Num, n)
	})
	if !found {
		a.wals = slices.Insert(a.wals, i, LogicalLog{Num: dfn, segments: make([]segment, 0, 1)})
	}
	// Ensure we haven't seen this log index yet, and find where it
	// slots within this log's segments.
	j, found := slices.BinarySearchFunc(a.wals[i].segments, li, func(s segment, li LogNameIndex) int {
		return cmp.Compare(s.logNameIndex, li)
	})
	if found {
		return false, errors.Errorf("wal: duplicate logIndex=%s for WAL %s in %s and %s",
			li, dfn, dirname, a.wals[i].segments[j].dir.Dirname)
	}
	a.wals[i].segments = slices.Insert(a.wals[i].segments, j, segment{logNameIndex: li, dir: Dir{
		FS:      fs,
		Dirname: dirname,
	}})
	return true, nil
}

// Logs holds a collection of WAL files, in increasing order of NumWAL.
type Logs []LogicalLog

// Get retrieves the WAL with the given number if present. The second return
// value indicates whether or not the WAL was found.
func (l Logs) Get(num NumWAL) (LogicalLog, bool) {
	i, found := slices.BinarySearchFunc(l, num, func(lw LogicalLog, n NumWAL) int {
		return cmp.Compare(lw.Num, n)
	})
	if !found {
		return LogicalLog{}, false
	}
	return l[i], true
}

func newVirtualWALReader(wal LogicalLog) *virtualWALReader {
	return &virtualWALReader{
		LogicalLog: wal,
		currIndex:  -1,
	}
}

// A virtualWALReader takes an ordered sequence of physical WAL files
// ("segments") and implements the wal.Reader interface, providing a merged view
// of the WAL's logical contents. It's responsible for filtering duplicate
// records which may be shared by the tail of a segment file and the head of its
// successor.
type virtualWALReader struct {
	// VirtualWAL metadata.
	LogicalLog

	// State pertaining to the current position of the reader within the virtual
	// WAL and its constituent physical files.
	currIndex  int
	currFile   vfs.File
	currReader *record.Reader
	// off describes the current Offset within the WAL.
	off Offset
	// lastSeqNum is the sequence number of the batch contained within the last
	// record returned to the user. A virtual WAL may be split across a sequence
	// of several physical WAL files. The tail of one physical WAL may be
	// duplicated within the head of the next physical WAL file. We use
	// contained batches' sequence numbers to deduplicate. This lastSeqNum field
	// should monotonically increase as we iterate over the WAL files. If we
	// ever observe a batch encoding a sequence number <= lastSeqNum, we must
	// have already returned the batch and should skip it.
	lastSeqNum base.SeqNum
	// recordBuf is a buffer used to hold the latest record read from a physical
	// file, and then returned to the user. A pointer to this buffer is returned
	// directly to the caller of NextRecord.
	recordBuf bytes.Buffer
}

// *virtualWALReader implements wal.Reader.
var _ Reader = (*virtualWALReader)(nil)

// NextRecord returns a reader for the next record. It returns io.EOF if there
// are no more records. The reader returned becomes stale after the next
// NextRecord call, and should no longer be used.
func (r *virtualWALReader) NextRecord() (io.Reader, Offset, error) {
	// On the first call, we need to open the first file.
	if r.currIndex < 0 {
		err := r.nextFile()
		if err != nil {
			return nil, Offset{}, err
		}
	}

	for {
		// Update our current physical offset to match the current file offset.
		r.off.Physical = r.currReader.Offset()
		// Obtain a Reader for the next record within this log file.
		rec, err := r.currReader.Next()
		if errors.Is(err, io.EOF) {
			// This file is exhausted; continue to the next.
			err := r.nextFile()
			if err != nil {
				return nil, r.off, err
			}
			continue
		}

		// Copy the record into a buffer. This ensures we read its entirety so
		// that NextRecord returns the next record, even if the caller never
		// exhausts the previous record's Reader. The record.Reader requires the
		// record to be exhausted to read all of the record's chunks before
		// attempting to read the next record. Buffering also also allows us to
		// easily read the header of the batch down below for deduplication.
		r.recordBuf.Reset()
		if err == nil {
			_, err = io.Copy(&r.recordBuf, rec)
		}
		// The record may be malformed. This is expected during a WAL failover,
		// because the tail of a WAL may be only partially written or otherwise
		// unclean because of WAL recycling and the inability to write the EOF
		// trailer record. If this isn't the last file, we silently ignore the
		// invalid record at the tail and proceed to the next file. If it is
		// the last file, bubble the error up and let the client decide what to
		// do with it. If the virtual WAL is the most recent WAL, Open may also
		// decide to ignore it because it's consistent with an incomplete
		// in-flight write at the time of process exit/crash. See #453.
		if record.IsInvalidRecord(err) && r.currIndex < len(r.segments)-1 {
			if err := r.nextFile(); err != nil {
				return nil, r.off, err
			}
			continue
		} else if err != nil {
			return nil, r.off, err
		}

		// We may observe repeat records between the physical files that make up
		// a virtual WAL because inflight writes to a file on a stalled disk may
		// or may not end up completing. WAL records always contain encoded
		// batches, and batches that contain data can be uniquely identifed by
		// sequence number.
		//
		// Parse the batch header.
		h, ok := batchrepr.ReadHeader(r.recordBuf.Bytes())
		if !ok {
			// Failed to read the batch header because the record was smaller
			// than the length of a batch header. This is unexpected. The record
			// envelope successfully decoded and the checkums of the individual
			// record fragment(s) validated, so the writer truly wrote an
			// invalid batch. During Open WAL recovery treats this as
			// corruption. We could return the record to the caller, allowing
			// the caller to interpret it as corruption, but it seems safer to
			// be explicit and surface the corruption error here.
			return nil, r.off, base.CorruptionErrorf("pebble: corrupt log file logNum=%d, logNameIndex=%s: invalid batch",
				r.Num, errors.Safe(r.segments[r.currIndex].logNameIndex))
		}

		// There's a subtlety necessitated by LogData operations. A LogData
		// applied to a batch results in data appended to the WAL in a batch
		// format, but the data is never applied to the memtable or LSM. A batch
		// only containing LogData will repeat a sequence number. We skip these
		// batches because they're not relevant for recovery and we do not want
		// to mistakenly deduplicate the batch containing KVs at the same
		// sequence number. We can differentiate LogData-only batches through
		// their batch headers: they'll encode a count of zero.
		if h.Count == 0 {
			continue
		}

		// If we've already observed a sequence number >= this batch's sequence
		// number, we must've already returned this record to the client. Skip
		// it.
		if h.SeqNum <= r.lastSeqNum {
			continue
		}
		r.lastSeqNum = h.SeqNum
		return &r.recordBuf, r.off, nil
	}
}

// Close closes the reader, releasing open resources.
func (r *virtualWALReader) Close() error {
	if r.currFile != nil {
		if err := r.currFile.Close(); err != nil {
			return err
		}
	}
	return nil
}

// nextFile advances the internal state to the next physical segment file.
func (r *virtualWALReader) nextFile() error {
	if r.currFile != nil {
		err := r.currFile.Close()
		r.currFile = nil
		if err != nil {
			return err
		}
	}
	r.currIndex++
	if r.currIndex >= len(r.segments) {
		return io.EOF
	}

	fs, path := r.LogicalLog.SegmentLocation(r.currIndex)
	r.off.PreviousFilesBytes += r.off.Physical
	r.off.PhysicalFile = path
	r.off.Physical = 0
	var err error
	if r.currFile, err = fs.Open(path); err != nil {
		return errors.Wrapf(err, "opening WAL file segment %q", path)
	}
	r.currReader = record.NewReader(r.currFile, base.DiskFileNum(r.Num))
	return nil
}
