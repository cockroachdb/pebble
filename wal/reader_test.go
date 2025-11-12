// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package wal

import (
	"bytes"
	"cmp"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"unicode"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/batchrepr"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadrivenutil"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/vfs"
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
			for _, l := range strings.Split(strings.TrimSpace(td.Input), "\n") {
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
					panic(fmt.Sprintf("unrecognized command %q", fields[0]))
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
