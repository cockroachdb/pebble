// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package wal

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/batchrepr"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadrivenutil"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
	"github.com/cockroachdb/pebble/vfs/vfstest"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestList(t *testing.T) {
	var buf bytes.Buffer
	filesystems := map[string]*vfs.MemFS{}
	getFS := func(name string) *vfs.MemFS {
		if _, ok := filesystems[name]; !ok {
			filesystems[name] = vfs.NewMem()
		}
		return filesystems[name]
	}

	datadriven.RunTest(t, "testdata/list", func(t *testing.T, td *datadriven.TestData) string {
		buf.Reset()
		switch td.Cmd {
		case "list":
			var dirs []Dir
			for _, arg := range td.CmdArgs {
				var dirname string
				if len(arg.Vals) > 1 {
					dirname = arg.Vals[1]
				}
				dirs = append(dirs, Dir{
					FS:      getFS(arg.Vals[0]),
					Dirname: dirname,
				})
			}
			logs, err := Scan(dirs...)
			if err != nil {
				return err.Error()
			}
			for i := range logs {
				fmt.Fprintln(&buf, logs[i].String())
			}
			return buf.String()
		case "reset":
			clear(filesystems)
			return ""
		case "touch":
			for l := range crstrings.LinesSeq(td.Input) {
				if l == "" {
					continue
				}
				fields := strings.Fields(l)
				fsName := fields[0]
				filename := fields[1]

				fs := getFS(fsName)
				require.NoError(t, fs.MkdirAll(fs.PathDir(filename), os.ModePerm))
				f, err := fs.Create(filename, vfs.WriteCategoryUnspecified)
				require.NoError(t, err)
				require.NoError(t, f.Close())
			}

			// Sort the filesystem names for determinism.
			var names []string
			for name := range filesystems {
				names = append(names, name)
			}
			slices.Sort(names)
			for _, name := range names {
				fmt.Fprintf(&buf, "%s:\n", name)
				fmt.Fprint(&buf, filesystems[name].String())
			}
			return buf.String()
		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}

// TestReader tests the virtual WAL reader that merges across multiple physical
// log files.
func TestReader(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rng := rand.New(rand.NewPCG(1, 1))
	var buf bytes.Buffer
	var fs vfs.FS
	var memFS *vfs.MemFS
	setFS := func(mem *vfs.MemFS) {
		memFS = mem
		fs = vfs.WithLogging(mem, func(format string, args ...interface{}) {
			s := fmt.Sprintf("# "+format, args...)
			fmt.Fprintln(&buf, strings.TrimRightFunc(s, unicode.IsSpace))
		})
	}
	setFS(vfs.NewCrashableMem())
	datadriven.RunTest(t, "testdata/reader", func(t *testing.T, td *datadriven.TestData) string {
		buf.Reset()
		switch td.Cmd {
		case "define":
			var logNum uint64
			var index int64
			var recycleFilename string
			td.ScanArgs(t, "logNum", &logNum)
			td.MaybeScanArgs(t, "logNameIndex", &index)
			td.MaybeScanArgs(t, "recycleFilename", &recycleFilename)

			filename := makeLogFilename(NumWAL(logNum), LogNameIndex(index))
			var f vfs.File
			var err error
			if recycleFilename != "" {
				f, err = fs.ReuseForWrite(recycleFilename, filename, vfs.WriteCategoryUnspecified)
				require.NoError(t, err)
				fmt.Fprintf(&buf, "recycled %q as %q\n", recycleFilename, filename)
			} else {
				f, err = fs.Create(filename, vfs.WriteCategoryUnspecified)
				require.NoError(t, err)
				fmt.Fprintf(&buf, "created %q\n", filename)
			}
			dir, err := fs.OpenDir("")
			require.NoError(t, err)
			require.NoError(t, dir.Sync())
			require.NoError(t, dir.Close())
			w := record.NewLogWriter(f, base.DiskFileNum(logNum), record.LogWriterConfig{
				WriteWALSyncOffsets: func() bool { return false },
			})

			lines := datadrivenutil.Lines(td.Input)
			var offset int64
			for len(lines) > 0 {
				fields := lines.Next().Fields()
				switch fields[0] {
				case "batch":
					// Fake a batch of the provided size.
					size := fields.MustKeyValue("size").Int()
					repr := make([]byte, size)
					var seq uint64
					if len(repr) >= batchrepr.HeaderLen {
						count := uint32(fields.MustKeyValue("count").Uint64())
						seq = fields.MustKeyValue("seq").Uint64()
						for i := range repr[batchrepr.HeaderLen:] {
							repr[i] = byte(rng.Uint32())
						}
						batchrepr.SetSeqNum(repr, base.SeqNum(seq))
						batchrepr.SetCount(repr, count)
					}

					var tailOffset int64
					if fields.HasValue("sync") {
						var wg sync.WaitGroup
						var writeErr, syncErr error
						wg.Add(1)
						tailOffset, writeErr = w.SyncRecord(repr, &wg, &syncErr)
						if writeErr != nil {
							return writeErr.Error()
						}
						wg.Wait()
						if syncErr != nil {
							return syncErr.Error()
						}
					} else {
						var writeErr error
						tailOffset, writeErr = w.WriteRecord(repr)
						if writeErr != nil {
							return writeErr.Error()
						}
					}

					fmt.Fprintf(&buf, "%d..%d: batch #%d\n", offset, tailOffset, seq)
					offset = tailOffset
				case "write-garbage":
					size := fields.MustKeyValue("size").Int()
					garbage := make([]byte, size)
					for i := range garbage {
						garbage[i] = byte(rng.Uint32())
					}
					_, err := f.Write(garbage)
					require.NoError(t, err)
					if fields.HasValue("sync") {
						require.NoError(t, f.Sync())
					}

					fmt.Fprintf(&buf, "%d..%d: write-garbage\n", offset, offset+int64(size))
				case "corrupt-tail":
					length := int64(fields.MustKeyValue("len").Int())
					garbage := make([]byte, length)
					for i := range garbage {
						garbage[i] = byte(rng.Uint32())
					}
					_, err := f.WriteAt(garbage, offset-length)
					require.NoError(t, err)

					fmt.Fprintf(&buf, "%d..%d: corrupt-tail\n", offset-length, offset)
				default:
					t.Fatalf("unrecognized command %q", fields[0])
				}
			}
			if td.HasArg("close-unclean") {
				crashFS := memFS.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 0})
				require.NoError(t, w.Close())
				setFS(crashFS)
			} else {
				require.NoError(t, w.Close())
			}
			return buf.String()
		case "copy":
			var logNum uint64
			var copyDir string
			var visibleSeqNum uint64
			td.ScanArgs(t, "logNum", &logNum)
			td.ScanArgs(t, "copyDir", &copyDir)
			td.ScanArgs(t, "visibleSeqNum", &visibleSeqNum)
			logs, err := Scan(Dir{FS: fs})
			require.NoError(t, err)
			log, ok := logs.Get(NumWAL(logNum))
			if !ok {
				return fmt.Sprintf("log with logNum %d not found", logNum)
			}
			require.NoError(t, fs.MkdirAll(copyDir, os.ModePerm))
			cfg := record.LogWriterConfig{
				WriteWALSyncOffsets: func() bool { return false },
			}
			err = Copy(fs, copyDir, log, base.SeqNum(visibleSeqNum), cfg)
			if err != nil {
				return err.Error()
			}
			return buf.String()
		case "read":
			var logNum uint64
			var forceLogNameIndexes []uint64
			dirname := ""
			td.ScanArgs(t, "logNum", &logNum)
			td.MaybeScanArgs(t, "forceLogNameIndexes", &forceLogNameIndexes)
			td.MaybeScanArgs(t, "dirname", &dirname)
			logs, err := Scan(Dir{Dirname: dirname, FS: fs})
			require.NoError(t, err)
			log, ok := logs.Get(NumWAL(logNum))
			if !ok {
				return fmt.Sprintf("log with logNum %d not found", logNum)
			}

			segments := log.segments
			// If forceLogNameIndexes is provided, pretend we found some
			// additional segments. This can be used to exercise the case where
			// opening the next physical segment file fails.
			if len(forceLogNameIndexes) > 0 {
				for _, li := range forceLogNameIndexes {
					j, found := slices.BinarySearchFunc(segments, LogNameIndex(li), func(s segment, li LogNameIndex) int {
						return cmp.Compare(s.logNameIndex, li)
					})
					require.False(t, found)
					segments = slices.Insert(segments, j, segment{logNameIndex: LogNameIndex(li), dir: Dir{FS: fs}})
				}
			}
			ll := LogicalLog{Num: log.Num, segments: segments}
			r := ll.OpenForRead()
			for {
				rr, off, err := r.NextRecord()
				fmt.Fprintf(&buf, "r.NextRecord() = (rr, %s, %v)\n", off, err)
				if err != nil {
					break
				}
				b, err := io.ReadAll(rr)
				fmt.Fprintf(&buf, "  io.ReadAll(rr) = (\"")
				if len(b) < 32 {
					fmt.Fprintf(&buf, "%x", b)
				} else {
					fmt.Fprintf(&buf, "%x... <%d-byte record>", b[:32], len(b))
				}
				fmt.Fprintf(&buf, "\", %v)\n", err)
				if h, ok := batchrepr.ReadHeader(b); !ok {
					fmt.Fprintln(&buf, "  failed to parse batch header")
				} else {
					fmt.Fprintf(&buf, "  BatchHeader: %s\n", h)
				}
			}
			if err := r.Close(); err != nil {
				fmt.Fprintf(&buf, "r.Close() = %q", err)
			}
			return buf.String()
		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}

// TestCopyClosesWriterOnError verifies that wal.Copy closes the destination
// file even when copying fails due to a corrupt WAL. This is a regression
// test for https://github.com/cockroachdb/cockroach/issues/164128 where Copy
// would leak the destination file handle on error.
func TestCopyClosesWriterOnError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	memFS := vfs.NewMem()
	fs, dumpOpenFiles := vfstest.WithOpenFileTracking(memFS)

	// Write a WAL containing a record shorter than batchrepr.HeaderLen.
	// The record will decode successfully at the record layer (valid
	// chunk and checksum), but virtualWALReader.nextRecord will fail
	// to parse the batch header, returning a CorruptionError that is
	// not io.EOF or ErrUnexpectedEOF.
	const logNum = 1
	filename := makeLogFilename(NumWAL(logNum), 0)
	f, err := fs.Create(filename, vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	w := record.NewLogWriter(f, base.DiskFileNum(logNum), record.LogWriterConfig{
		WriteWALSyncOffsets: func() bool { return false },
	})
	// Write a record that is too short to contain a valid batch header.
	_, err = w.WriteRecord([]byte("short"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	// Copy should fail.
	logs, err := Scan(Dir{FS: fs})
	require.NoError(t, err)
	ll, ok := logs.Get(NumWAL(logNum))
	require.True(t, ok)
	copyDir := "copy"
	require.NoError(t, fs.MkdirAll(copyDir, os.ModePerm))
	err = Copy(fs, copyDir, ll, base.SeqNum(100), record.LogWriterConfig{
		WriteWALSyncOffsets: func() bool { return false },
	})
	require.Error(t, err)
	t.Logf("Copy error (expected): %v", err)

	// Verify that no files are leaked.
	var buf bytes.Buffer
	dumpOpenFiles(&buf)
	if buf.Len() > 0 {
		t.Fatalf("leaked open files after Copy error:\n%s", buf.String())
	}
}

// newAppendingTestManager returns an initialized manager on fs that recycles
// WAL files: a StandaloneManager, or with failover, a failoverManager with a
// secondary directory whose monitor never switches segments on its own.
func newAppendingTestManager(t *testing.T, fs vfs.FS, failover bool) Manager {
	t.Helper()
	require.NoError(t, fs.MkdirAll("primary", os.ModePerm))
	opts := Options{
		Primary:              Dir{FS: fs, Dirname: "primary"},
		Logger:               testutils.Logger{T: t},
		MaxNumRecyclableLogs: 1,
		PreallocateSize:      func() int { return 0 },
		WriteWALSyncOffsets:  func() bool { return true },
	}
	var m Manager = &StandaloneManager{}
	if failover {
		require.NoError(t, fs.MkdirAll("secondary", os.ModePerm))
		opts.Secondary = Dir{FS: fs, Dirname: "secondary"}
		opts.FailoverOptions = FailoverOptions{
			UnhealthyOperationLatencyThreshold: func() (time.Duration, bool) {
				return time.Hour, false
			},
		}
		opts.FailoverWriteAndSyncLatency = prometheus.NewHistogram(prometheus.HistogramOpts{})
		m = &failoverManager{}
	}
	require.NoError(t, m.init(opts, nil /* initial logs */))
	return m
}

// writeSyncedBatches writes n records to w, each a batch of size bytes with the
// sequence number that follows *seqNum, and waits for each to be synced.
func writeSyncedBatches(t *testing.T, w Writer, seqNum *base.SeqNum, n, size int) {
	t.Helper()
	for range n {
		*seqNum++
		b := make([]byte, batchrepr.HeaderLen+size)
		batchrepr.SetSeqNum(b, *seqNum)
		batchrepr.SetCount(b, 1)
		var wg sync.WaitGroup
		var syncErr error
		wg.Add(1)
		_, err := w.WriteRecord(b, SyncOptions{Done: &wg, Err: &syncErr}, nil /* ref */)
		require.NoError(t, err)
		wg.Wait()
		require.NoError(t, syncErr)
	}
}

// readSeqNums reads ll, returning the sequence numbers of its batches.
func readSeqNums(ll LogicalLog) ([]base.SeqNum, error) {
	r := ll.OpenForRead()
	var seqNums []base.SeqNum
	for {
		rec, _, err := r.NextRecord()
		if errors.Is(err, io.EOF) {
			return seqNums, r.Close()
		}
		if err == nil {
			var b []byte
			if b, err = io.ReadAll(rec); err == nil {
				h, _ := batchrepr.ReadHeader(b)
				seqNums = append(seqNums, h.SeqNum)
				continue
			}
		}
		return seqNums, errors.CombineErrors(err, r.Close())
	}
}

// seqNumRange returns the sequence numbers from first to last, inclusive.
func seqNumRange(first, last base.SeqNum) []base.SeqNum {
	var seqNums []base.SeqNum
	for s := first; s <= last; s++ {
		seqNums = append(seqNums, s)
	}
	return seqNums
}

// lastLog returns the last WAL that m lists.
func lastLog(m Manager) LogicalLog {
	logs := m.List()
	return logs[len(logs)-1]
}

// createOnRecycledFile fills WAL 1 with several blocks' worth of batches and
// makes it obsolete, and then creates WAL 2, whose first segment reuses the file
// of WAL 1, and writes a small batch to it.
func createOnRecycledFile(t *testing.T, m Manager, seqNum *base.SeqNum) Writer {
	t.Helper()
	w, err := m.Create(1, 1 /* jobID */)
	require.NoError(t, err)
	writeSyncedBatches(t, w, seqNum, 150, 1000)
	_, err = w.Close()
	require.NoError(t, err)
	_, err = m.Obsolete(2, false /* noRecycle */)
	require.NoError(t, err)
	w, err = m.Create(2, 2 /* jobID */)
	require.NoError(t, err)
	// A failover manager creates the segment file in the background; the synced
	// batch has been written to it.
	writeSyncedBatches(t, w, seqNum, 1, 10)
	ll := lastLog(m)
	require.Equal(t, NumWAL(2), ll.Num)
	segFS, path := ll.SegmentLocation(0)
	info, err := segFS.Stat(path)
	require.NoError(t, err)
	require.Greater(t, info.Size(), int64(64<<10), "%s is not a recycled file", path)
	return w
}

// opHold describes an operation on a file that the injector of a test holds.
type opHold struct {
	path string
	kind errorfs.OpKind
	// n is the ordinal of the operation of that kind on path that is held,
	// counting from 1.
	n             int32
	count         atomic.Int32
	held, release chan struct{}
}

// holdingFS returns an FS that holds the operation that hold describes, if it
// is set.
func holdingFS(fs vfs.FS, hold *atomic.Pointer[opHold]) vfs.FS {
	return errorfs.Wrap(fs, errorfs.InjectorFunc(func(op errorfs.Op) error {
		if h := hold.Load(); h != nil && op.Path == h.path && op.Kind == h.kind &&
			h.count.Add(1) == h.n {
			close(h.held)
			<-h.release
		}
		return nil
	}))
}

// readWithHold reads ll in the background, and holds its nth operation of the
// given kind on the file at path, if it performs one, while it calls
// whileHeld. It returns whether the operation was held, and what readSeqNums
// returned.
func readWithHold(
	hold *atomic.Pointer[opHold],
	ll LogicalLog,
	path string,
	kind errorfs.OpKind,
	n int32,
	whileHeld func(),
) (bool, []base.SeqNum, error) {
	h := &opHold{path: path, kind: kind, n: n, held: make(chan struct{}), release: make(chan struct{})}
	hold.Store(h)
	defer hold.Store(nil)
	type result struct {
		seqNums []base.SeqNum
		err     error
	}
	done := make(chan result, 1)
	go func() {
		var res result
		res.seqNums, res.err = readSeqNums(ll)
		done <- res
	}()
	select {
	case <-h.held:
		whileHeld()
		close(h.release)
		res := <-done
		return true, res.seqNums, res.err
	case res := <-done:
		return false, res.seqNums, res.err
	}
}

// TestReadAppendingSegment verifies that a WAL returned by Manager.List can be
// read while its writer keeps appending to it: the reader reads the records
// the writer has written by the time the reader gets to them, and nothing
// past them.
//
// The segment being appended to is a recycled file, so past what the writer
// has written it holds the chunks of an earlier WAL. A reader that read some of
// those stale bytes, and then, once the writer had filled the rest of the block
// and synced into the next one, read the next block, would find chunks there
// whose synced offsets vouch for the stale bytes, which record.Reader takes for
// corruption.
func TestReadAppendingSegment(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, failover := range []bool{false, true} {
		t.Run(fmt.Sprintf("failover=%t", failover), func(t *testing.T) {
			var hold atomic.Pointer[opHold]
			m := newAppendingTestManager(t, holdingFS(vfs.NewMem(), &hold), failover)
			defer func() { require.NoError(t, m.Close()) }()
			var seqNum base.SeqNum
			w := createOnRecycledFile(t, m, &seqNum)
			// The manager can only be closed once its writer is.
			defer func() {
				if w != nil {
					_, _ = w.Close()
				}
			}()
			first := seqNum

			ll := lastLog(m)
			require.Equal(t, NumWAL(2), ll.Num)
			_, path := ll.SegmentLocation(ll.NumSegments() - 1)

			// The reader's first read of the segment ends within the first block,
			// at the end of the record. Should it read the segment again, the
			// read is held while the writer fills the rest of the block and syncs
			// into the next one.
			held, got, err := readWithHold(&hold, ll, path, errorfs.OpFileRead, 2 /* n */, func() {
				writeSyncedBatches(t, w, &seqNum, 40, 1000)
			})
			require.False(t, held, "the reader read past the end of what had been written")
			require.NoError(t, err)
			require.Equal(t, []base.SeqNum{first}, got)

			// Fill the rest of the first block and spill into the next one, and
			// list the WAL again. Its reader is held at its first read, of the first
			// block, while the writer adds records, one of them spanning blocks.
			// Each read is limited to what has been written by the time of that
			// read, so the reader reads those records too, and stops at their end,
			// short of the stale bytes that follow them.
			writeSyncedBatches(t, w, &seqNum, 40, 1000)
			ll = lastLog(m)
			held, got, err = readWithHold(&hold, ll, path, errorfs.OpFileRead, 1 /* n */, func() {
				writeSyncedBatches(t, w, &seqNum, 1, 40000)
				writeSyncedBatches(t, w, &seqNum, 20, 1000)
			})
			require.True(t, held)
			require.NoError(t, err)
			require.Equal(t, seqNumRange(first, seqNum), got)

			// Once the writer is closed, the WAL is read whole.
			_, err = w.Close()
			w = nil
			require.NoError(t, err)
			ll = lastLog(m)
			require.Equal(t, NumWAL(2), ll.Num)
			for i := range ll.NumSegments() {
				require.Nil(t, ll.segments[i].flushedOffset)
			}
			got, err = readSeqNums(ll)
			require.NoError(t, err)
			require.Equal(t, seqNumRange(first, seqNum), got)
		})
	}
}

// shortReadFS is an FS whose files opened for reading end at limit bytes,
// unless limit is zero.
type shortReadFS struct {
	vfs.FS
	limit atomic.Int64
}

func (fs *shortReadFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.FS.Open(name, opts...)
	if limit := fs.limit.Load(); err == nil && limit > 0 {
		f = &shortReadFile{File: f, left: limit}
	}
	return f, err
}

type shortReadFile struct {
	vfs.File
	left int64
}

func (f *shortReadFile) Read(p []byte) (int, error) {
	if f.left <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > f.left {
		p = p[:f.left]
	}
	n, err := f.File.Read(p)
	f.left -= int64(n)
	return n, err
}

// TestReadAppendingSegmentCorruption verifies that corruption in the records
// written to the segment being appended to is reported as such, including
// corruption that a reader of a log of unknown length takes for its end: the
// segment is read only as far as the records written to it, so it neither ends
// in a torn record, nor ends anywhere else than where those records end.
func TestReadAppendingSegmentCorruption(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const numRecords = 10
	// Each corruption is applied to the segment file at path, whose first
	// flushed bytes hold numRecords records of recordSize bytes each.
	corruptions := []struct {
		name    string
		corrupt func(t *testing.T, fs *shortReadFS, path string, flushed, recordSize int64)
	}{
		{
			// No later chunk can show this record to be corrupt rather than torn.
			name: "last-record-damaged",
			corrupt: func(t *testing.T, fs *shortReadFS, path string, flushed, _ int64) {
				f, err := fs.OpenReadWrite(path, vfs.WriteCategoryUnspecified)
				require.NoError(t, err)
				b := make([]byte, 1)
				_, err = f.ReadAt(b, flushed-1)
				require.NoError(t, err)
				b[0] ^= 0xff
				_, err = f.WriteAt(b, flushed-1)
				require.NoError(t, err)
				require.NoError(t, f.Close())
			},
		},
		{
			// The file ends after the fifth record, where a log can end.
			name: "file-reads-short",
			corrupt: func(t *testing.T, fs *shortReadFS, _ string, _, recordSize int64) {
				fs.limit.Store(5 * recordSize)
			},
		},
		{
			// The log number in the header of the sixth record reads as the one
			// that marks an EOF trailer, which is checked before the checksum.
			name: "header-reads-as-eof-trailer",
			corrupt: func(t *testing.T, fs *shortReadFS, path string, _, recordSize int64) {
				f, err := fs.OpenReadWrite(path, vfs.WriteCategoryUnspecified)
				require.NoError(t, err)
				b := binary.LittleEndian.AppendUint32(nil, 2 /* WAL 1's log number + 1 */)
				// The log number follows the checksum, length and chunk type.
				_, err = f.WriteAt(b, 5*recordSize+7)
				require.NoError(t, err)
				require.NoError(t, f.Close())
			},
		},
	}
	for _, failover := range []bool{false, true} {
		for _, c := range corruptions {
			t.Run(fmt.Sprintf("failover=%t,%s", failover, c.name), func(t *testing.T) {
				fs := &shortReadFS{FS: vfs.NewMem()}
				m := newAppendingTestManager(t, fs, failover)
				defer func() { require.NoError(t, m.Close()) }()
				w, err := m.Create(1, 1 /* jobID */)
				require.NoError(t, err)
				defer func() {
					_, err := w.Close()
					require.NoError(t, err)
				}()
				var seqNum base.SeqNum
				writeSyncedBatches(t, w, &seqNum, numRecords, 100)

				ll := lastLog(m)
				got, err := readSeqNums(ll)
				require.NoError(t, err)
				require.Equal(t, seqNumRange(1, seqNum), got)

				s := ll.segments[ll.NumSegments()-1]
				_, path := ll.SegmentLocation(ll.NumSegments() - 1)
				flushed := s.flushedOffset()
				// The records are all the same size, and fit in the first block.
				require.Zero(t, flushed%numRecords)
				c.corrupt(t, fs, path, flushed, flushed/numRecords)

				_, err = readSeqNums(ll)
				require.Truef(t, base.IsCorruptionError(err), "%+v", err)
				require.NoError(t, fs.MkdirAll("copy", os.ModePerm))
				err = Copy(fs, "copy", ll, seqNum+1, record.LogWriterConfig{
					WriteWALSyncOffsets: func() bool { return true },
				})
				require.Truef(t, base.IsCorruptionError(err), "%+v", err)
			})
		}
	}
}

// TestReadAppendingSegmentEndingInPadding verifies that the segment being
// appended to reads cleanly when its last record ends close enough to the end
// of a block that the rest of the block is zero padding. A record.Reader does
// not count a tail shorter than a chunk header in the offset at which it finds
// the end of the log, so the prefix that is read must end with the record, not
// with the padding.
func TestReadAppendingSegmentEndingInPadding(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const blockSize = 32 << 10
	const walSyncHeaderSize = 19
	for _, failover := range []bool{false, true} {
		for gap := range walSyncHeaderSize + 1 {
			t.Run(fmt.Sprintf("failover=%t,gap=%d", failover, gap), func(t *testing.T) {
				m := newAppendingTestManager(t, vfs.NewMem(), failover)
				defer func() { require.NoError(t, m.Close()) }()
				w, err := m.Create(1, 1 /* jobID */)
				require.NoError(t, err)
				defer func() {
					_, err := w.Close()
					require.NoError(t, err)
				}()
				// A record that ends gap bytes short of the end of the first block.
				var seqNum base.SeqNum
				writeSyncedBatches(t, w, &seqNum, 1, blockSize-gap-walSyncHeaderSize-batchrepr.HeaderLen)
				got, err := readSeqNums(lastLog(m))
				require.NoError(t, err)
				require.Equal(t, seqNumRange(1, 1), got)
				// And once a record follows it in the next block.
				writeSyncedBatches(t, w, &seqNum, 1, 100)
				got, err = readSeqNums(lastLog(m))
				require.NoError(t, err)
				require.Equal(t, seqNumRange(1, 2), got)
			})
		}
	}
}

// reuseWrappingFS is an errorfs.FS whose ReuseForWrite also injects into the
// writes to the file it returns, which errorfs.FS.ReuseForWrite does not.
type reuseWrappingFS struct {
	*errorfs.FS
	inj errorfs.Injector
}

func (fs *reuseWrappingFS) ReuseForWrite(
	oldname, newname string, category vfs.DiskWriteCategory,
) (vfs.File, error) {
	f, err := fs.FS.ReuseForWrite(oldname, newname, category)
	if err != nil {
		return nil, err
	}
	return &injectingWriteFile{File: f, path: newname, inj: fs.inj}, nil
}

// injectingWriteFile consults inj before each write to the file at path.
type injectingWriteFile struct {
	vfs.File
	path string
	inj  errorfs.Injector
}

func (f *injectingWriteFile) Write(p []byte) (int, error) {
	if err := f.inj.MaybeError(errorfs.Op{Kind: errorfs.OpFileWrite, Path: f.path}); err != nil {
		return 0, err
	}
	return f.File.Write(p)
}

// TestReadFailoverWALWithStalledSegment verifies that a failover WAL returned by
// Manager.List reads every record exactly once while the writer of a segment
// that the WAL switched away from, because a write to it stalled, still has
// that write to finish.
//
// That segment is a recycled file, so past what has been written to it, it
// holds the chunks of an earlier WAL. It is not the last segment, so the reader
// takes those for its end and moves on to the next segment, which has the
// record that the stalled write had not written, written again.
func TestReadFailoverWALWithStalledSegment(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var stall atomic.Pointer[string]
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseWrite := func() { releaseOnce.Do(func() { close(release) }) }
	inj := errorfs.InjectorFunc(func(op errorfs.Op) error {
		if p := stall.Load(); p != nil && op.Path == *p && op.Kind == errorfs.OpFileWrite {
			<-release
		}
		return nil
	})
	fs := &reuseWrappingFS{FS: errorfs.Wrap(vfs.NewMem(), inj), inj: inj}
	m := newAppendingTestManager(t, fs, true /* failover */).(*failoverManager)
	defer func() { require.NoError(t, m.Close()) }()
	// The stalled write has to finish for the writer and the manager to close.
	defer releaseWrite()
	var seqNum base.SeqNum
	w := createOnRecycledFile(t, m, &seqNum)
	defer func() {
		_, err := w.Close()
		require.NoError(t, err)
	}()
	first := seqNum
	writeSyncedBatches(t, w, &seqNum, 5, 100)

	// Stall the writes to the segment, and write a batch, which can only be
	// synced once the WAL switches to a new segment.
	seg0 := fs.PathJoin("primary", makeLogFilename(2, 0))
	stall.Store(&seg0)
	seqNum++
	b := make([]byte, batchrepr.HeaderLen+100)
	batchrepr.SetSeqNum(b, seqNum)
	batchrepr.SetCount(b, 1)
	var wg sync.WaitGroup
	var syncErr error
	wg.Add(1)
	_, err := w.WriteRecord(b, SyncOptions{Done: &wg, Err: &syncErr}, nil /* ref */)
	require.NoError(t, err)
	m.mu.Lock()
	ww := m.mu.ww
	m.mu.Unlock()
	require.NoError(t, ww.switchToNewDir(
		dirAndFileHandle{Dir: m.opts.Secondary, File: m.dirHandles[secondaryDirIndex]}))
	wg.Wait()
	require.NoError(t, syncErr)
	writeSyncedBatches(t, w, &seqNum, 5, 100)

	ll := lastLog(m)
	require.Equal(t, "000002: {(primary,000), (secondary,001)}", ll.String())
	got, err := readSeqNums(ll)
	require.NoError(t, err)
	require.Equal(t, seqNumRange(first, seqNum), got)

	// Once the stalled write finishes, the first segment holds the batch too.
	ww.mu.Lock()
	lw := ww.mu.writers[0].w
	ww.mu.Unlock()
	flushed := lw.FlushedOffset()
	stall.Store(nil)
	releaseWrite()
	require.Eventually(t, func() bool { return lw.FlushedOffset() > flushed },
		10*time.Second, time.Millisecond)
	got, err = readSeqNums(ll)
	require.NoError(t, err)
	require.Equal(t, seqNumRange(first, seqNum), got)
}
