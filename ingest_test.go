// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/blobtest"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/rangekey"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/colblk"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

func TestSSTableKeyCompare(t *testing.T) {
	var buf bytes.Buffer
	datadriven.RunTest(t, "testdata/sstable_key_compare", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "cmp":
			buf.Reset()
			for line := range crstrings.LinesSeq(td.Input) {
				fields := strings.Fields(line)
				a := base.ParseInternalKey(fields[0])
				b := base.ParseInternalKey(fields[1])
				got := sstableKeyCompare(testkeys.Comparer.Compare, a, b)
				fmt.Fprintf(&buf, "%38s", fmt.Sprint(a.Pretty(base.DefaultFormatter)))
				switch got {
				case -1:
					fmt.Fprint(&buf, " < ")
				case +1:
					fmt.Fprint(&buf, " > ")
				case 0:
					fmt.Fprint(&buf, " = ")
				}
				fmt.Fprintf(&buf, "%s\n", fmt.Sprint(b.Pretty(base.DefaultFormatter)))
			}
			return buf.String()
		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}

func TestIngestLoad(t *testing.T) {
	mem := vfs.NewMem()
	keySchema := colblk.DefaultKeySchema(testkeys.Comparer, 16)

	datadriven.RunTest(t, "testdata/ingest_load", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "load":
			writerOpts := sstable.WriterOptions{
				Comparer:  testkeys.Comparer,
				KeySchema: &keySchema,
			}
			var dbVersion FormatMajorVersion
			for _, cmdArgs := range td.CmdArgs {
				v, err := strconv.Atoi(cmdArgs.Vals[0])
				if err != nil {
					return err.Error()
				}
				switch k := cmdArgs.Key; k {
				case "writer-version":
					fmv := FormatMajorVersion(v)
					writerOpts.TableFormat = fmv.MaxTableFormat()
				case "db-version":
					dbVersion = FormatMajorVersion(v)
				default:
					return fmt.Sprintf("unknown cmd %s\n", k)
				}
			}
			f, err := mem.Create("ext", vfs.WriteCategoryUnspecified)
			if err != nil {
				return err.Error()
			}
			var bv blobtest.Values
			w := sstable.NewRawWriter(objstorageprovider.NewFileWritable(f), writerOpts)
			for data := range crstrings.LinesSeq(td.Input) {
				if strings.HasPrefix(data, "Span: ") {
					data = strings.TrimPrefix(data, "Span: ")
					err := w.EncodeSpan(keyspan.ParseSpan(data))
					if err != nil {
						return err.Error()
					}
					continue
				}

				j := strings.Index(data, ":")
				if j < 0 {
					return fmt.Sprintf("malformed input: %s\n", data)
				}
				key := base.ParseInternalKey(data[:j])
				if blobtest.IsBlobHandle(data[j+1:]) {
					ih, _, err := bv.ParseInlineHandle(data[j+1:])
					require.NoError(t, err)
					if err := w.AddWithBlobHandle(key, ih, base.ShortAttribute(0), false /* forceObsolete */); err != nil {
						return err.Error()
					}
				} else {
					value := []byte(data[j+1:])
					if err := w.Add(key, value, false /* forceObsolete */); err != nil {
						return err.Error()
					}
				}
			}
			if err := w.Close(); err != nil {
				return err.Error()
			}

			opts := &Options{
				Comparer:   testkeys.Comparer,
				KeySchema:  keySchema.Name,
				KeySchemas: sstable.MakeKeySchemas(&keySchema),
				FS:         mem,
			}
			opts.WithFSDefaults()
			lr, err := ingestLoad(context.Background(), opts, dbVersion, []string{"ext"}, nil, nil, nil, nil, []base.TableNum{1})
			if err != nil {
				return err.Error()
			}
			var buf bytes.Buffer
			for _, m := range lr.local {
				fmt.Fprintf(&buf, "%d: %s-%s\n", m.TableNum, m.Smallest(), m.Largest())
				fmt.Fprintf(&buf, "  points: %s-%s\n", m.PointKeyBounds.Smallest(), m.PointKeyBounds.Largest())
				if m.HasRangeKeys {
					fmt.Fprintf(&buf, "  ranges: %s-%s\n", m.RangeKeyBounds.Smallest(), m.RangeKeyBounds.Largest())
				}
			}
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestIngestLoadRand(t *testing.T) {
	mem := vfs.NewMem()
	rng := rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))
	cmp := DefaultComparer.Compare
	version := internalFormatNewest

	randBytes := func(size int) []byte {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(rng.Int() & 0xff)
		}
		return data
	}

	paths := make([]string, 1+rng.IntN(10))
	pending := make([]base.TableNum, len(paths))
	expected := make([]ingestLocalMeta, len(paths))
	for i := range paths {
		paths[i] = fmt.Sprint(i)
		pending[i] = base.TableNum(rng.Uint64())
		expected[i] = ingestLocalMeta{
			TableMetadata: &manifest.TableMetadata{
				TableNum: pending[i],
			},
			path: paths[i],
		}

		func() {
			f, err := mem.Create(paths[i], vfs.WriteCategoryUnspecified)
			require.NoError(t, err)

			keys := make([]InternalKey, 1+rng.IntN(100))
			for i := range keys {
				keys[i] = base.MakeInternalKey(
					randBytes(1+rng.IntN(10)),
					0,
					InternalKeyKindSet)
			}
			slices.SortFunc(keys, func(a, b base.InternalKey) int {
				return base.InternalCompare(cmp, a, b)
			})

			expected[i].ExtendPointKeyBounds(cmp, keys[0], keys[len(keys)-1])

			w := sstable.NewRawWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{
				TableFormat: version.MaxTableFormat(),
			})
			var count, rawKeySize uint64
			for i := range keys {
				if i > 0 && base.InternalCompare(cmp, keys[i-1], keys[i]) == 0 {
					// Duplicate key, ignore.
					continue
				}
				require.NoError(t, w.Add(keys[i], nil, false /* forceObsolete */))
				count++
				rawKeySize += uint64(keys[i].Size())
			}
			expected[i].TableMetadata.PopulateStats(&manifest.TableStats{})
			require.NoError(t, w.Close())

			meta, err := w.Metadata()
			require.NoError(t, err)

			expected[i].Size = meta.Size
			expected[i].InitPhysicalBacking()
			expected[i].TableBacking.PopulateProperties(&sstable.Properties{
				RawKeySize:    rawKeySize,
				NumEntries:    count,
				NumDataBlocks: 1,
			})
		}()
	}

	opts := &Options{
		Comparer: base.DefaultComparer,
		FS:       mem,
	}
	opts.WithFSDefaults()
	opts.EnsureDefaults()
	lr, err := ingestLoad(context.Background(), opts, version, paths, nil, nil, nil, nil, pending)
	require.NoError(t, err)

	// Reset flaky stats.
	for _, m := range lr.local {
		m.CreationTime = 0
		if stats, ok := m.TableBacking.Properties(); ok {
			stats.CompressionStats = block.CompressionStats{}
		}
	}
	t.Log(strings.Join(pretty.Diff(expected, lr.local), "\n"))
	require.Equal(t, expected, lr.local)
}

func TestIngestLoadInvalid(t *testing.T) {
	mem := vfs.NewMem()
	f, err := mem.Create("invalid", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	opts := &Options{
		Comparer: DefaultComparer,
		FS:       mem,
	}
	opts.WithFSDefaults()
	if _, err := ingestLoad(context.Background(), opts, internalFormatNewest, []string{"invalid"}, nil, nil, nil, nil, []base.TableNum{1}); err == nil {
		t.Fatalf("expected error, but found success")
	}
}

func TestIngestSortAndVerify(t *testing.T) {
	comparers := map[string]Compare{
		"default": DefaultComparer.Compare,
		"reverse": func(a, b []byte) int {
			return DefaultComparer.Compare(b, a)
		},
	}

	t.Run("", func(t *testing.T) {
		datadriven.RunTest(t, "testdata/ingest_sort_and_verify", func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "ingest":
				var buf bytes.Buffer
				var meta []ingestLocalMeta
				var cmpName string
				d.ScanArgs(t, "cmp", &cmpName)
				cmp := comparers[cmpName]
				if cmp == nil {
					return fmt.Sprintf("%s unknown comparer: %s", d.Cmd, cmpName)
				}
				for i, data := range crstrings.Lines(d.Input) {
					parts := strings.Split(data, "-")
					if len(parts) != 2 {
						return fmt.Sprintf("malformed test case: %s", d.Input)
					}
					smallest := base.ParseInternalKey(parts[0])
					largest := base.ParseInternalKey(parts[1])
					if cmp(smallest.UserKey, largest.UserKey) > 0 {
						return fmt.Sprintf("range %v-%v is not valid", smallest, largest)
					}
					m := (&manifest.TableMetadata{}).ExtendPointKeyBounds(cmp, smallest, largest)
					m.InitPhysicalBacking()
					meta = append(meta, ingestLocalMeta{
						TableMetadata: m,
						path:          strconv.Itoa(i),
					})
				}
				lr := ingestLoadResult{local: meta}
				err := ingestSortAndVerify(cmp, lr, KeyRange{})
				if err != nil {
					return fmt.Sprintf("%v\n", err)
				}
				for i := range meta {
					fmt.Fprintf(&buf, "%s: %v-%v\n", meta[i].path, meta[i].Smallest(), meta[i].Largest())
				}
				return buf.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	})
}

func TestIngestLink(t *testing.T) {
	// Test linking of tables into the DB directory. Test cleanup when one of the
	// tables cannot be linked.

	const dir = "db"
	const count = 10
	for i := 0; i <= count; i++ {
		t.Run("", func(t *testing.T) {
			opts := &Options{FS: vfs.NewMem()}
			opts.EnsureDefaults()
			opts.WithFSDefaults()
			require.NoError(t, opts.FS.MkdirAll(dir, 0755))
			objProvider, err := objstorageprovider.Open(objstorageprovider.DefaultSettings(opts.FS, dir))
			require.NoError(t, err)
			defer objProvider.Close()

			meta := make([]ingestLocalMeta, 10)
			contents := make([][]byte, len(meta))
			for j := range meta {
				meta[j].path = fmt.Sprintf("external%d", j)
				meta[j].TableMetadata = &manifest.TableMetadata{}
				meta[j].TableNum = base.TableNum(j)
				meta[j].InitPhysicalBacking()
				f, err := opts.FS.Create(meta[j].path, vfs.WriteCategoryUnspecified)
				require.NoError(t, err)

				contents[j] = []byte(fmt.Sprintf("data%d", j))
				// memFile.Write will modify the supplied buffer when invariants are
				// enabled, so provide a throw-away copy.
				_, err = f.Write(append([]byte(nil), contents[j]...))
				require.NoError(t, err)
				require.NoError(t, f.Close())
			}

			if i < count {
				opts.FS.Remove(meta[i].path)
			}

			err = ingestLinkLocal(context.Background(), 0 /* jobID */, opts, objProvider, meta)
			if i < count {
				if err == nil {
					t.Fatalf("expected error, but found success")
				}
			} else {
				require.NoError(t, err)
			}

			files, err := opts.FS.List(dir)
			require.NoError(t, err)

			sort.Strings(files)

			if i < count {
				if len(files) > 0 {
					t.Fatalf("expected all of the files to be cleaned up, but found:\n%s",
						strings.Join(files, "\n"))
				}
			} else {
				if len(files) != count {
					t.Fatalf("expected %d files, but found:\n%s", count, strings.Join(files, "\n"))
				}
				for j := range files {
					ftype, fileNum, ok := base.ParseFilename(opts.FS, files[j])
					if !ok {
						t.Fatalf("unable to parse filename: %s", files[j])
					}
					if base.FileTypeTable != ftype {
						t.Fatalf("expected table, but found %d", ftype)
					}
					if j != int(fileNum) {
						t.Fatalf("expected table %d, but found %d", j, fileNum)
					}
					f, err := opts.FS.Open(opts.FS.PathJoin(dir, files[j]))
					require.NoError(t, err)

					data, err := io.ReadAll(f)
					require.NoError(t, err)
					require.NoError(t, f.Close())
					if !bytes.Equal(contents[j], data) {
						t.Fatalf("expected %s, but found %s", contents[j], data)
					}
				}
			}
		})
	}
}

func TestIngestLinkFallback(t *testing.T) {
	// Verify that ingestLink succeeds if linking fails by falling back to
	// copying.
	mem := vfs.NewMem()
	src, err := mem.Create("source", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)

	opts := &Options{FS: errorfs.Wrap(mem, errorfs.ErrInjected.If(errorfs.OnIndex(1)))}
	opts.EnsureDefaults()
	opts.WithFSDefaults()
	objSettings := objstorageprovider.DefaultSettings(opts.FS, "")
	// Prevent the provider from listing the dir (where we may get an injected error).
	objSettings.Local.FSDirInitialListing = []string{}
	objProvider, err := objstorageprovider.Open(objSettings)
	require.NoError(t, err)
	defer objProvider.Close()

	meta := &manifest.TableMetadata{TableNum: 1}
	meta.InitPhysicalBacking()
	err = ingestLinkLocal(context.Background(), 0, opts, objProvider, []ingestLocalMeta{{TableMetadata: meta, path: "source"}})
	require.NoError(t, err)

	dest, err := mem.Open("000001.sst")
	require.NoError(t, err)

	// We should be able to write bytes to src, and not have them show up in
	// dest.
	_, _ = src.Write([]byte("test"))
	data, err := io.ReadAll(dest)
	require.NoError(t, err)
	if len(data) != 0 {
		t.Fatalf("expected copy, but files appear to be hard linked: [%s] unexpectedly found", data)
	}
}

func TestOverlappingIngestedSSTs(t *testing.T) {
	dir := ""
	var (
		mem        *vfs.MemFS
		crashClone *vfs.MemFS
		d          *DB
		opts       *Options
		closed     = false
		blockFlush = false
	)
	cache := NewCache(0)
	defer func() {
		if d != nil && !closed {
			require.NoError(t, d.Close())
		}
		cache.Unref()
	}()
	var fsLog struct {
		sync.Mutex
		buf bytes.Buffer
	}
	reset := func(strictMem bool) {
		if d != nil && !closed {
			require.NoError(t, d.Close())
			d = nil
		}
		blockFlush = false

		if strictMem {
			mem = vfs.NewCrashableMem()
		} else {
			mem = vfs.NewMem()
		}

		require.NoError(t, mem.MkdirAll("ext", 0755))
		opts = &Options{
			FS: vfs.WithLogging(mem, func(format string, args ...interface{}) {
				fsLog.Lock()
				defer fsLog.Unlock()
				fmt.Fprintf(&fsLog.buf, format+"\n", args...)
			}),
			Cache:                       cache,
			MemTableStopWritesThreshold: 4,
			L0CompactionThreshold:       100,
			L0StopWritesThreshold:       100,
			DebugCheck:                  DebugCheckLevels,
			FormatMajorVersion:          internalFormatNewest,
			Logger:                      testutils.Logger{T: t},
		}
		opts.WithFSDefaults()
		if testing.Verbose() {
			lel := MakeLoggingEventListener(DefaultLogger)
			opts.EventListener = &lel
		}
		opts.EnsureDefaults()
		// Some of the tests require bloom filters.
		opts.Levels[0].FilterPolicy = bloom.FilterPolicy(10)

		// Disable automatic compactions because otherwise we'll race with
		// delete-only compactions triggered by ingesting range tombstones.
		opts.DisableAutomaticCompactions = true

		var err error
		d, err = Open(dir, opts)
		require.NoError(t, err)
		closed = false
		d.TestOnlyWaitForCleaning()
	}
	waitForFlush := func() {
		if d == nil {
			return
		}
		d.mu.Lock()
		for d.mu.compact.flushing {
			d.mu.compact.cond.Wait()
		}
		d.mu.Unlock()
	}
	reset(false)

	datadriven.RunTest(t, "testdata/flushable_ingest", func(t *testing.T, td *datadriven.TestData) (result string) {
		if td.HasArg("with-fs-logging") {
			fsLog.Lock()
			fsLog.buf.Reset()
			fsLog.Unlock()
			defer func() {
				fsLog.Lock()
				defer fsLog.Unlock()
				result = fsLog.buf.String() + result
			}()
		}

		switch td.Cmd {
		case "reset":
			reset(td.HasArg("strictMem"))
			return ""

		case "crash-clone":
			crashClone = mem.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 0})
			return ""

		case "reset-to-crash-clone":
			mem = crashClone
			crashClone = nil
			files, err := mem.List(dir)
			sort.Strings(files)
			require.NoError(t, err)
			return strings.Join(files, "\n")

		case "batch":
			b := d.NewIndexedBatch()
			if err := runBatchDefineCmd(td, b); err != nil {
				return err.Error()
			}
			if err := b.Commit(nil); err != nil {
				return err.Error()
			}
			return ""

		case "build":
			if err := runBuildCmd(td, d, mem); err != nil {
				return err.Error()
			}
			return ""

		case "ingest":
			if err := runIngestCmd(td, d); err != nil {
				return err.Error()
			}
			if !blockFlush {
				waitForFlush()
			}
			return ""

		case "iter":
			iter, _ := d.NewIter(nil)
			return runIterCmd(td, iter, true)

		case "lsm":
			return runLSMCmd(td, d)

		case "close":
			if closed {
				return "already closed"
			}
			require.NoError(t, d.Close())
			closed = true
			return ""

		case "ls":
			files, err := mem.List(dir)
			sort.Strings(files)
			require.NoError(t, err)
			return strings.Join(files, "\n")

		case "open":
			opts.ReadOnly = td.HasArg("readOnly")
			var err error
			d, err = Open(dir, opts)
			closed = false
			require.NoError(t, err)
			waitForFlush()
			d.TestOnlyWaitForCleaning()
			return ""

		case "blockFlush":
			blockFlush = true
			d.mu.Lock()
			d.mu.compact.flushing = true
			d.mu.Unlock()
			return ""

		case "allowFlush":
			blockFlush = false
			d.mu.Lock()
			d.mu.compact.flushing = false
			d.mu.Unlock()
			return ""

		case "flush":
			d.maybeScheduleFlush()
			waitForFlush()
			d.TestOnlyWaitForCleaning()
			return ""

		case "get":
			return runGetCmd(t, td, d)

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func testIngestSharedImpl(
	t *testing.T, createOnShared remote.CreateOnSharedStrategy, fileName string,
) {
	var d, d1, d2 *DB
	var efos map[string]*EventuallyFileOnlySnapshot
	defer func() {
		for _, e := range efos {
			require.NoError(t, e.Close())
		}
		if d1 != nil {
			require.NoError(t, d1.Close())
		}
		if d2 != nil {
			require.NoError(t, d2.Close())
		}
	}()
	creatorIDCounter := uint64(1)
	replicateCounter := 1
	var opts1, opts2 *Options

	reset := func(t *testing.T) {
		for _, e := range efos {
			require.NoError(t, e.Close())
		}
		if d1 != nil {
			require.NoError(t, d1.Close())
		}
		if d2 != nil {
			require.NoError(t, d2.Close())
		}
		efos = make(map[string]*EventuallyFileOnlySnapshot)

		sstorage := remote.NewInMem()
		mem1 := vfs.NewMem()
		mem2 := vfs.NewMem()
		require.NoError(t, mem1.MkdirAll("ext", 0755))
		require.NoError(t, mem2.MkdirAll("ext", 0755))
		opts1 = &Options{
			Comparer:              testkeys.Comparer,
			FS:                    mem1,
			LBaseMaxBytes:         1,
			L0CompactionThreshold: 100,
			L0StopWritesThreshold: 100,
			DebugCheck:            DebugCheckLevels,
			FormatMajorVersion:    FormatVirtualSSTables,
			Logger:                testutils.Logger{T: t},
		}
		lel := MakeLoggingEventListener(testutils.Logger{T: t})
		opts1.EventListener = &lel
		opts1.Experimental.RemoteStorage = remote.MakeSimpleFactory(map[remote.Locator]remote.Storage{
			"": sstorage,
		})
		opts1.Experimental.CreateOnShared = createOnShared
		opts1.Experimental.CreateOnSharedLocator = ""
		// Disable automatic compactions because otherwise we'll race with
		// delete-only compactions triggered by ingesting range tombstones.
		opts1.DisableAutomaticCompactions = true

		opts2 = &Options{}
		*opts2 = *opts1
		opts2.Experimental.RemoteStorage = remote.MakeSimpleFactory(map[remote.Locator]remote.Storage{
			"": sstorage,
		})
		opts2.Experimental.CreateOnShared = createOnShared
		opts2.Experimental.CreateOnSharedLocator = ""
		opts2.FS = mem2

		var err error
		d1, err = Open("", opts1)
		require.NoError(t, err)
		require.NoError(t, d1.SetCreatorID(creatorIDCounter))
		creatorIDCounter++
		d2, err = Open("", opts2)
		require.NoError(t, err)
		require.NoError(t, d2.SetCreatorID(creatorIDCounter))
		creatorIDCounter++
		d = d1
	}
	reset(t)

	datadriven.RunTest(t, fmt.Sprintf("testdata/%s", fileName), func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "restart":
			for _, e := range efos {
				require.NoError(t, e.Close())
			}
			if d1 != nil {
				require.NoError(t, d1.Close())
			}
			if d2 != nil {
				require.NoError(t, d2.Close())
			}

			var err error
			d1, err = Open("", opts1)
			if err != nil {
				return err.Error()
			}
			d2, err = Open("", opts2)
			if err != nil {
				return err.Error()
			}
			d = d1
			return "ok, note that the active db has been set to 1 (use 'switch' to change)"
		case "reset":
			reset(t)
			return ""
		case "switch":
			if len(td.CmdArgs) != 1 {
				return "usage: switch <1 or 2>"
			}
			switch td.CmdArgs[0].Key {
			case "1":
				d = d1
			case "2":
				d = d2
			default:
				return "usage: switch <1 or 2>"
			}
			return "ok"
		case "batch":
			b := d.NewIndexedBatch()
			if err := runBatchDefineCmd(td, b); err != nil {
				return err.Error()
			}
			if err := b.Commit(nil); err != nil {
				return err.Error()
			}
			return ""
		case "build":
			if err := runBuildCmd(td, d, d.opts.FS); err != nil {
				return err.Error()
			}
			return ""

		case "flush":
			if err := d.Flush(); err != nil {
				return err.Error()
			}
			return ""

		case "ingest":
			if err := runIngestCmd(td, d); err != nil {
				return err.Error()
			}
			// Wait for a possible flush.
			d.mu.Lock()
			for d.mu.compact.flushing {
				d.mu.compact.cond.Wait()
			}
			d.mu.Unlock()
			return ""

		case "ingest-and-excise":
			if err := runIngestAndExciseCmd(td, d); err != nil {
				return err.Error()
			}
			// Wait for a possible flush.
			d.mu.Lock()
			for d.mu.compact.flushing {
				d.mu.compact.cond.Wait()
			}
			d.mu.Unlock()
			return ""

		case "replicate":
			if len(td.CmdArgs) != 4 {
				return "usage: replicate <from-db-num> <to-db-num> <start-key> <end-key>"
			}
			var from, to *DB
			switch td.CmdArgs[0].Key {
			case "1":
				from = d1
			case "2":
				from = d2
			default:
				return "usage: replicate <from-db-num> <to-db-num> <start-key> <end-key>"
			}
			switch td.CmdArgs[1].Key {
			case "1":
				to = d1
			case "2":
				to = d2
			default:
				return "usage: replicate <from-db-num> <to-db-num> <start-key> <end-key>"
			}
			startKey := []byte(td.CmdArgs[2].Key)
			endKey := []byte(td.CmdArgs[3].Key)

			writeOpts := d.opts.MakeWriterOptions(0 /* level */, to.TableFormat())
			sstPath := fmt.Sprintf("ext/replicate%d.sst", replicateCounter)
			f, err := to.opts.FS.Create(sstPath, vfs.WriteCategoryUnspecified)
			require.NoError(t, err)
			replicateCounter++
			w := sstable.NewRawWriter(objstorageprovider.NewFileWritable(f), writeOpts)

			var sharedSSTs []SharedSSTMeta
			err = from.ScanInternal(context.TODO(), ScanInternalOptions{
				IterOptions: IterOptions{
					KeyTypes:   IterKeyTypePointsAndRanges,
					LowerBound: startKey,
					UpperBound: endKey,
				},
				VisitPointKey: func(key *InternalKey, value LazyValue, _ IteratorLevel) error {
					val, _, err := value.Value(nil)
					require.NoError(t, err)
					require.NoError(t, w.Add(base.MakeInternalKey(key.UserKey, 0, key.Kind()), val, false /* forceObsolete */))
					return nil
				},
				VisitRangeDel: func(start, end []byte, seqNum base.SeqNum) error {
					require.NoError(t, w.EncodeSpan(keyspan.Span{
						Start: start,
						End:   end,
						Keys:  []keyspan.Key{{Trailer: base.MakeTrailer(0, base.InternalKeyKindRangeDelete)}},
					}))
					return nil
				},
				VisitRangeKey: func(start, end []byte, keys []keyspan.Key) error {
					require.NoError(t, w.EncodeSpan(keyspan.Span{
						Start:     start,
						End:       end,
						Keys:      keys,
						KeysOrder: 0,
					}))
					return nil
				},
				VisitSharedFile: func(sst *SharedSSTMeta) error {
					sharedSSTs = append(sharedSSTs, *sst)
					return nil
				},
			})
			require.NoError(t, err)
			require.NoError(t, w.Close())

			_, err = to.IngestAndExcise(context.Background(), []string{sstPath}, sharedSSTs, nil /* external */, KeyRange{Start: startKey, End: endKey})
			require.NoError(t, err)
			return fmt.Sprintf("replicated %d shared SSTs", len(sharedSSTs))

		case "get":
			return runGetCmd(t, td, d)

		case "iter":
			o := &IterOptions{KeyTypes: IterKeyTypePointsAndRanges}
			var reader Reader
			reader = d
			for _, arg := range td.CmdArgs {
				switch arg.Key {
				case "mask-suffix":
					o.RangeKeyMasking.Suffix = []byte(arg.Vals[0])
				case "mask-filter":
					o.RangeKeyMasking.Filter = func() BlockPropertyFilterMask {
						return sstable.NewTestKeysMaskingFilter()
					}
				case "snapshot":
					reader = efos[arg.Vals[0]]
				}
			}
			iter, err := reader.NewIter(o)
			if err != nil {
				return err.Error()
			}
			return runIterCmd(td, iter, true)

		case "lsm":
			return runLSMCmd(td, d)

		case "metrics":
			// The asynchronous loading of table stats can change metrics, so
			// wait for all the tables' stats to be loaded.
			d.waitTableStats()

			return d.Metrics().StringForTests()

		case "wait-pending-table-stats":
			return runWaitForTableStatsCmd(td, d)

		case "excise":
			ve := &manifest.VersionEdit{
				DeletedTables: map[manifest.DeletedTableEntry]*manifest.TableMetadata{},
			}
			var exciseSpan KeyRange
			if len(td.CmdArgs) != 2 {
				panic("insufficient args for excise command")
			}
			exciseSpan.Start = []byte(td.CmdArgs[0].Key)
			exciseSpan.End = []byte(td.CmdArgs[1].Key)

			d.mu.Lock()
			d.mu.versions.logLock()
			d.mu.Unlock()
			current := d.mu.versions.currentVersion()
			exciseBounds := exciseSpan.UserKeyBounds()
			for level := range current.Levels {
				iter := current.Levels[level].Iter()
				for m := iter.SeekGE(d.cmp, exciseSpan.Start); m != nil && d.cmp(m.Smallest().UserKey, exciseSpan.End) < 0; m = iter.Next() {
					leftTable, rightTable, err := d.exciseTable(context.Background(), exciseBounds, m, level, tightExciseBounds)
					if err != nil {
						d.mu.Lock()
						d.mu.versions.logUnlock()
						d.mu.Unlock()
						return fmt.Sprintf("error when excising %s: %s", m.TableNum, err.Error())
					}
					applyExciseToVersionEdit(ve, m, leftTable, rightTable, level)
				}
			}
			d.mu.Lock()
			d.mu.versions.logUnlock()
			d.mu.Unlock()
			return fmt.Sprintf("would excise %d files, use ingest-and-excise to excise.\n%s", len(ve.DeletedTables), ve.String())

		case "file-only-snapshot":
			if len(td.CmdArgs) != 1 {
				panic("insufficient args for file-only-snapshot command")
			}
			name := td.CmdArgs[0].Key
			var keyRanges []KeyRange
			for line := range crstrings.LinesSeq(td.Input) {
				fields := strings.Fields(line)
				if len(fields) != 2 {
					return "expected two fields for file-only snapshot KeyRanges"
				}
				kr := KeyRange{Start: []byte(fields[0]), End: []byte(fields[1])}
				keyRanges = append(keyRanges, kr)
			}

			s := d.NewEventuallyFileOnlySnapshot(keyRanges)
			efos[name] = s
			return "ok"

		case "wait-for-file-only-snapshot":
			if len(td.CmdArgs) != 1 {
				panic("insufficient args for file-only-snapshot command")
			}
			name := td.CmdArgs[0].Key
			err := efos[name].WaitForFileOnlySnapshot(context.TODO(), 1*time.Millisecond)
			if err != nil {
				return err.Error()
			}
			return "ok"

		case "compact":
			err := runCompactCmd(t, td, d)
			if err != nil {
				return err.Error()
			}
			return "ok"
		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestIngestShared(t *testing.T) {
	for _, strategy := range []remote.CreateOnSharedStrategy{remote.CreateOnSharedAll, remote.CreateOnSharedLower} {
		strategyStr := "all"
		if strategy == remote.CreateOnSharedLower {
			strategyStr = "lower"
		}
		t.Run(fmt.Sprintf("createOnShared=%s", strategyStr), func(t *testing.T) {
			fileName := "ingest_shared"
			if strategy == remote.CreateOnSharedLower {
				fileName = "ingest_shared_lower"
			}
			testIngestSharedImpl(t, strategy, fileName)
		})
	}
}

func TestSimpleIngestShared(t *testing.T) {
	mem := vfs.NewMem()
	var d *DB
	var provider2 objstorage.Provider
	opts2 := Options{FS: vfs.NewMem(), FormatMajorVersion: FormatVirtualSSTables, Logger: testutils.Logger{T: t}}
	opts2.EnsureDefaults()

	// Create an objProvider where we will fake-create some sstables that can
	// then be shared back to the db instance.
	providerSettings := objstorageprovider.Settings{
		Logger: opts2.Logger,
	}
	providerSettings.Local.FS = opts2.FS
	providerSettings.Local.FSDirName = ""
	providerSettings.Local.FSDirInitialListing = nil
	providerSettings.Local.FSCleaner = opts2.Cleaner
	providerSettings.Local.NoSyncOnClose = opts2.NoSyncOnClose
	providerSettings.Local.BytesPerSync = opts2.BytesPerSync
	providerSettings.Remote.StorageFactory = remote.MakeSimpleFactory(map[remote.Locator]remote.Storage{
		"": remote.NewInMem(),
	})
	providerSettings.Remote.CreateOnShared = remote.CreateOnSharedAll
	providerSettings.Remote.CreateOnSharedLocator = ""

	provider2, err := objstorageprovider.Open(providerSettings)
	require.NoError(t, err)
	creatorIDCounter := uint64(1)
	provider2.SetCreatorID(objstorage.CreatorID(creatorIDCounter))
	creatorIDCounter++

	defer func() {
		require.NoError(t, d.Close())
	}()

	reset := func() {
		if d != nil {
			require.NoError(t, d.Close())
		}

		mem = vfs.NewMem()
		require.NoError(t, mem.MkdirAll("ext", 0755))
		opts := &Options{
			FormatMajorVersion:    FormatVirtualSSTables,
			FS:                    mem,
			L0CompactionThreshold: 100,
			L0StopWritesThreshold: 100,
			Logger:                testutils.Logger{T: t},
		}
		opts.Experimental.RemoteStorage = providerSettings.Remote.StorageFactory
		opts.Experimental.CreateOnShared = providerSettings.Remote.CreateOnShared
		opts.Experimental.CreateOnSharedLocator = providerSettings.Remote.CreateOnSharedLocator

		var err error
		d, err = Open("", opts)
		require.NoError(t, err)
		require.NoError(t, d.SetCreatorID(creatorIDCounter))
		creatorIDCounter++
	}
	reset()

	metaMap := map[base.DiskFileNum]objstorage.ObjectMetadata{}

	require.NoError(t, d.Set([]byte("d"), []byte("unexpected"), nil))
	require.NoError(t, d.Set([]byte("e"), []byte("unexpected"), nil))
	require.NoError(t, d.Set([]byte("a"), []byte("unexpected"), nil))
	require.NoError(t, d.Set([]byte("f"), []byte("unexpected"), nil))
	d.Flush()

	{
		// Create a shared file.
		fn := base.TableNum(2)
		f, meta, err := provider2.Create(context.TODO(), base.FileTypeTable, base.PhysicalTableDiskFileNum(fn),
			objstorage.CreateOptions{PreferSharedStorage: true})
		require.NoError(t, err)
		w := sstable.NewWriter(f, d.opts.MakeWriterOptions(0, d.TableFormat()))
		w.Set([]byte("d"), []byte("shared"))
		w.Set([]byte("e"), []byte("shared"))
		w.Close()
		metaMap[base.PhysicalTableDiskFileNum(fn)] = meta
	}

	m := metaMap[base.DiskFileNum(2)]
	handle, err := provider2.RemoteObjectBacking(&m)
	require.NoError(t, err)
	size, err := provider2.Size(m)
	require.NoError(t, err)

	sharedSSTMeta := SharedSSTMeta{
		Backing:          handle,
		Smallest:         base.MakeInternalKey([]byte("d"), 0, InternalKeyKindSet),
		Largest:          base.MakeInternalKey([]byte("e"), 0, InternalKeyKindSet),
		SmallestPointKey: base.MakeInternalKey([]byte("d"), 0, InternalKeyKindSet),
		LargestPointKey:  base.MakeInternalKey([]byte("e"), 0, InternalKeyKindSet),
		Level:            6,
		Size:             uint64(size + 5),
	}
	_, err = d.IngestAndExcise(
		context.Background(), []string{}, []SharedSSTMeta{sharedSSTMeta}, nil, /* external */
		KeyRange{Start: []byte("d"), End: []byte("ee")})
	require.NoError(t, err)

	// TODO(bilal): Once reading of shared sstables is in, verify that the values
	// of d and e have been updated.
}

type blockedCompaction struct {
	startBlock, unblock chan struct{}
}

func TestIngestExternal(t *testing.T) {
	var mem vfs.FS
	var d, d1, d2 *DB
	var opts *Options
	var flushed bool
	defer func() {
		if d1 != nil {
			require.NoError(t, d1.Close())
		}
		if d2 != nil {
			require.NoError(t, d2.Close())
		}
	}()

	var remoteStorage remote.Storage
	replicateCounter := 1
	reopen := func(t *testing.T) {
		if d != nil {
			if d1 != nil {
				require.NoError(t, d1.Close())
			}
			if d2 != nil {
				require.NoError(t, d2.Close())
			}
		}

		var err error
		d1, err = Open("d1", opts)
		require.NoError(t, err)
		require.NoError(t, d1.SetCreatorID(1))
		d2, err = Open("d2", opts)
		require.NoError(t, err)
		require.NoError(t, d2.SetCreatorID(2))
		d = d1
	}
	reset := func(t *testing.T, majorVersion FormatMajorVersion) {

		mem = vfs.NewMem()
		require.NoError(t, mem.MkdirAll("ext", 0755))
		remoteStorage = remote.NewInMem()
		opts = &Options{
			Comparer:              testkeys.Comparer,
			FS:                    mem,
			L0CompactionThreshold: 100,
			L0StopWritesThreshold: 100,
			DebugCheck:            DebugCheckLevels,
			EventListener: &EventListener{FlushEnd: func(info FlushInfo) {
				flushed = true
			}},
			FormatMajorVersion: majorVersion,
			Logger:             testutils.Logger{T: t},
		}
		opts.Experimental.RemoteStorage = remote.MakeSimpleFactory(map[remote.Locator]remote.Storage{
			"external-locator": remoteStorage,
		})
		opts.Experimental.CreateOnShared = remote.CreateOnSharedNone
		opts.Experimental.IngestSplit = func() bool {
			return true
		}
		// Disable automatic compactions because otherwise we'll race with
		// delete-only compactions triggered by ingesting range tombstones.
		opts.DisableAutomaticCompactions = true
		lel := MakeLoggingEventListener(testutils.Logger{T: t})
		opts.EventListener = &lel
		reopen(t)
	}
	reset(t, FormatNewest)

	datadriven.RunTest(t, "testdata/ingest_external", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "reopen":
			reopen(t)
			return ""
		case "reset":
			majorVersion := int64(FormatNewest)
			if td.HasArg("format-major-version") {
				td.ScanArgs(t, "format-major-version", &majorVersion)
			}
			reset(t, FormatMajorVersion(majorVersion))
			return ""
		case "batch":
			b := d.NewIndexedBatch()
			if err := runBatchDefineCmd(td, b); err != nil {
				return err.Error()
			}
			if err := b.Commit(nil); err != nil {
				return err.Error()
			}
			return ""
		case "build-remote":
			if err := runBuildRemoteCmd(td, d, remoteStorage); err != nil {
				return err.Error()
			}
			return ""
		case "switch":
			if len(td.CmdArgs) != 1 {
				return "usage: switch <1 or 2>"
			}
			switch td.CmdArgs[0].Key {
			case "1":
				d = d1
			case "2":
				d = d2
			default:
				return "usage: switch <1 or 2>"
			}
			return "ok"
		case "flush":
			if err := d.Flush(); err != nil {
				return err.Error()
			}
			return ""

		case "build":
			if err := runBuildCmd(td, d, mem); err != nil {
				return err.Error()
			}
			return ""

		case "ingest":
			flushed = false
			if err := runIngestCmd(td, d); err != nil {
				return err.Error()
			}
			// Wait for a possible flush.
			d.mu.Lock()
			for d.mu.compact.flushing {
				d.mu.compact.cond.Wait()
			}
			d.mu.Unlock()
			if flushed {
				return "memtable flushed"
			}
			return ""

		case "ingest-external":
			flushed = false
			if err := runIngestExternalCmd(t, td, d, remoteStorage, "external-locator"); err != nil {
				return err.Error()
			}
			// Wait for a possible flush.
			d.mu.Lock()
			for d.mu.compact.flushing {
				d.mu.compact.cond.Wait()
			}
			d.mu.Unlock()
			if flushed {
				return "memtable flushed"
			}
			return ""

		case "download":
			if len(td.CmdArgs) < 2 {
				panic("insufficient args for download command")
			}
			viaBackingFileDownload := false
			for _, arg := range td.CmdArgs[2:] {
				switch arg.Key {
				case "via-backing-file-download":
					viaBackingFileDownload = true
				default:
					return fmt.Sprintf("unexpected key: %s", arg.Key)
				}
			}
			l := []byte(td.CmdArgs[0].Key)
			r := []byte(td.CmdArgs[1].Key)
			spans := []DownloadSpan{{StartKey: l, EndKey: r, ViaBackingFileDownload: viaBackingFileDownload}}
			ctx, cancel := context.WithTimeout(context.TODO(), 1*time.Minute)
			defer cancel()
			err := d.Download(ctx, spans)
			if err != nil {
				return err.Error()
			}
			return "ok"

		case "get":
			return runGetCmd(t, td, d)

		case "iter":
			iter, _ := d.NewIter(&IterOptions{
				KeyTypes: IterKeyTypePointsAndRanges,
			})
			return runIterCmd(td, iter, true)

		case "lsm":
			return runLSMCmd(td, d)

		case "metrics":
			// The asynchronous loading of table stats can change metrics, so
			// wait for all the tables' stats to be loaded.
			d.waitTableStats()

			return d.Metrics().StringForTests()

		case "wait-pending-table-stats":
			return runWaitForTableStatsCmd(td, d)

		case "compact":
			if len(td.CmdArgs) != 2 {
				panic("insufficient args for compact command")
			}
			l := td.CmdArgs[0].Key
			r := td.CmdArgs[1].Key
			err := d.Compact(context.Background(), []byte(l), []byte(r), false)
			if err != nil {
				return err.Error()
			}
			return ""
		case "replicate":
			if len(td.CmdArgs) != 4 {
				return "usage: replicate <from-db-num> <to-db-num> <start-key> <end-key>"
			}
			var from, to *DB
			switch td.CmdArgs[0].Key {
			case "1":
				from = d1
			case "2":
				from = d2
			default:
				return "usage: replicate <from-db-num> <to-db-num> <start-key> <end-key>"
			}
			switch td.CmdArgs[1].Key {
			case "1":
				to = d1
			case "2":
				to = d2
			default:
				return "usage: replicate <from-db-num> <to-db-num> <start-key> <end-key>"
			}
			startKey := []byte(td.CmdArgs[2].Key)
			endKey := []byte(td.CmdArgs[3].Key)

			writeOpts := d.opts.MakeWriterOptions(0 /* level */, to.TableFormat())
			sstPath := fmt.Sprintf("ext/replicate%d.sst", replicateCounter)
			f, err := to.opts.FS.Create(sstPath, vfs.WriteCategoryUnspecified)
			require.NoError(t, err)
			replicateCounter++
			w := sstable.NewRawWriter(objstorageprovider.NewFileWritable(f), writeOpts)

			var externalFiles []ExternalFile
			err = from.ScanInternal(context.TODO(), ScanInternalOptions{
				IterOptions: IterOptions{
					KeyTypes:   IterKeyTypePointsAndRanges,
					LowerBound: startKey,
					UpperBound: endKey,
				},
				VisitPointKey: func(key *InternalKey, value LazyValue, _ IteratorLevel) error {
					val, _, err := value.Value(nil)
					require.NoError(t, err)
					require.NoError(t, w.Add(base.MakeInternalKey(key.UserKey, 0, key.Kind()), val, false /* forceObsolete */))
					return nil
				},
				VisitRangeDel: func(start, end []byte, seqNum base.SeqNum) error {
					require.NoError(t, w.EncodeSpan(keyspan.Span{
						Start: start,
						End:   end,
						Keys:  []keyspan.Key{{Trailer: base.MakeTrailer(0, base.InternalKeyKindRangeDelete)}},
					}))
					return nil
				},
				VisitRangeKey: func(start, end []byte, keys []keyspan.Key) error {
					require.NoError(t, w.EncodeSpan(keyspan.Span{
						Start:     start,
						End:       end,
						Keys:      keys,
						KeysOrder: 0,
					}))
					return nil
				},
				VisitExternalFile: func(sst *ExternalFile) error {
					externalFiles = append(externalFiles, *sst)
					return nil
				},
			})
			require.NoError(t, err)
			require.NoError(t, w.Close())
			_, err = to.IngestAndExcise(context.Background(), []string{sstPath}, nil /* shared */, externalFiles, KeyRange{Start: startKey, End: endKey})
			require.NoError(t, err)
			return fmt.Sprintf("replicated %d external SSTs", len(externalFiles))

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestIngestMemtableOverlaps(t *testing.T) {
	comparers := []Comparer{
		{
			Name:    "default",
			Compare: DefaultComparer.Compare,
		},
		{
			Name:    "reverse",
			Compare: func(a, b []byte) int { return DefaultComparer.Compare(b, a) },
		},
	}
	m := make(map[string]*Comparer)
	for i := range comparers {
		c := &comparers[i]
		c.AbbreviatedKey = func(key []byte) uint64 { panic("unimplemented") }
		c.Successor = func(dst, a []byte) []byte { panic("unimplemented") }
		c.Separator = func(dst, a, b []byte) []byte { panic("unimplemented") }
		m[c.Name] = c
	}

	for _, comparer := range comparers {
		t.Run(comparer.Name, func(t *testing.T) {
			var mem *memTable

			parseMeta := func(s string) *manifest.TableMetadata {
				parts := strings.Split(s, "-")
				meta := &manifest.TableMetadata{}
				if len(parts) != 2 {
					t.Fatalf("malformed table spec: %s", s)
				}
				var smallest, largest base.InternalKey
				if strings.Contains(parts[0], ".") {
					if !strings.Contains(parts[1], ".") {
						t.Fatalf("malformed table spec: %s", s)
					}
					smallest = base.ParseInternalKey(parts[0])
					largest = base.ParseInternalKey(parts[1])
				} else {
					smallest = InternalKey{UserKey: []byte(parts[0])}
					largest = InternalKey{UserKey: []byte(parts[1])}
				}
				// If we're using a reverse comparer, flip the file bounds.
				if mem.cmp(smallest.UserKey, largest.UserKey) > 0 {
					smallest, largest = largest, smallest
				}
				meta.ExtendPointKeyBounds(comparer.Compare, smallest, largest)
				meta.InitPhysicalBacking()
				return meta
			}

			datadriven.RunTest(t, "testdata/ingest_memtable_overlaps", func(t *testing.T, d *datadriven.TestData) string {
				switch d.Cmd {
				case "define":
					b := newBatch(nil)
					if err := runBatchDefineCmd(d, b); err != nil {
						return err.Error()
					}

					opts := &Options{
						Comparer: &comparer,
					}
					opts.EnsureDefaults()
					opts.WithFSDefaults()
					if len(d.CmdArgs) > 1 {
						return fmt.Sprintf("%s expects at most 1 argument", d.Cmd)
					}
					if len(d.CmdArgs) == 1 {
						opts.Comparer = m[d.CmdArgs[0].String()]
						if opts.Comparer == nil {
							return fmt.Sprintf("%s unknown comparer: %s", d.Cmd, d.CmdArgs[0].String())
						}
					}

					mem = newMemTable(memTableOptions{Options: opts})
					if err := mem.apply(b, 0); err != nil {
						return err.Error()
					}
					return ""

				case "overlaps":
					var buf bytes.Buffer
					for data := range crstrings.LinesSeq(d.Input) {
						var keyRanges []bounded
						for part := range strings.FieldsSeq(data) {
							keyRanges = append(keyRanges, parseMeta(part))
						}
						var overlaps bool
						mem.computePossibleOverlaps(func(bounded) shouldContinue {
							overlaps = true
							return stopIteration
						}, keyRanges...)
						fmt.Fprintf(&buf, "%t\n", overlaps)
					}
					return buf.String()

				default:
					return fmt.Sprintf("unknown command: %s", d.Cmd)
				}
			})
		})
	}
}

func TestKeyRangeBasic(t *testing.T) {
	cmp := base.DefaultComparer.Compare
	k1 := KeyRange{Start: []byte("b"), End: []byte("c")}
	k1UserKeyBounds := k1.UserKeyBounds()

	// Tests for Contains()
	require.True(t, k1.Contains(cmp, base.MakeInternalKey([]byte("b"), 1, InternalKeyKindSet)))
	require.False(t, k1.Contains(cmp, base.MakeInternalKey([]byte("c"), 1, InternalKeyKindSet)))
	require.True(t, k1.Contains(cmp, base.MakeInternalKey([]byte("bb"), 1, InternalKeyKindSet)))
	require.True(t, k1.Contains(cmp, base.MakeExclusiveSentinelKey(InternalKeyKindRangeDelete, []byte("c"))))

	m1 := &manifest.TableMetadata{}
	m1.ExtendPointKeyBounds(cmp, base.MakeInternalKey([]byte("b"), 1, InternalKeyKindSet),
		base.MakeInternalKey([]byte("c"), 1, InternalKeyKindSet))
	require.True(t, m1.Overlaps(cmp, &k1UserKeyBounds))
	m2 := &manifest.TableMetadata{}
	m2.ExtendPointKeyBounds(cmp, base.MakeInternalKey([]byte("c"), 1, InternalKeyKindSet),
		base.MakeInternalKey([]byte("d"), 1, InternalKeyKindSet))
	require.False(t, m2.Overlaps(cmp, &k1UserKeyBounds))
	m3 := &manifest.TableMetadata{}
	m3.ExtendPointKeyBounds(cmp, base.MakeInternalKey([]byte("a"), 1, InternalKeyKindSet),
		base.MakeExclusiveSentinelKey(InternalKeyKindRangeDelete, []byte("b")))
	require.False(t, m3.Overlaps(cmp, &k1UserKeyBounds))
	m4 := &manifest.TableMetadata{}
	m4.ExtendPointKeyBounds(cmp, base.MakeInternalKey([]byte("a"), 1, InternalKeyKindSet),
		base.MakeInternalKey([]byte("b"), 1, InternalKeyKindSet))
	require.True(t, m4.Overlaps(cmp, &k1UserKeyBounds))
}

func BenchmarkIngestOverlappingMemtable(b *testing.B) {
	assertNoError := func(err error) {
		b.Helper()
		if err != nil {
			b.Fatal(err)
		}
	}

	for count := 1; count < 6; count++ {
		b.Run(fmt.Sprintf("memtables=%d", count), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				mem := vfs.NewMem()
				d, err := Open("", &Options{
					FS: mem,
				})
				assertNoError(err)

				// Create memtables.
				for {
					assertNoError(d.Set([]byte("a"), nil, nil))
					d.mu.Lock()
					done := len(d.mu.mem.queue) == count
					d.mu.Unlock()
					if done {
						break
					}
				}

				// Create the overlapping sstable that will force a flush when ingested.
				f, err := mem.Create("ext", vfs.WriteCategoryUnspecified)
				assertNoError(err)
				w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{})
				assertNoError(w.Set([]byte("a"), nil))
				assertNoError(w.Close())

				b.StartTimer()
				assertNoError(d.Ingest(context.Background(), []string{"ext"}))
			}
		})
	}
}

func TestIngestTargetLevel(t *testing.T) {
	var d *DB
	defer func() {
		if d != nil {
			// Ignore errors because this test defines fake in-progress transactions
			// that prohibit clean shutdown.
			_ = d.Close()
		}
	}()

	datadriven.RunTest(t, "testdata/ingest_target_level", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			if d != nil {
				// Ignore errors because this test defines fake in-progress
				// transactions that prohibit clean shutdown.
				_ = d.Close()
			}

			var err error
			opts := Options{
				FormatMajorVersion: internalFormatNewest,
			}
			if d, err = runDBDefineCmd(td, &opts); err != nil {
				return err.Error()
			}

			readState := d.loadReadState()
			c := &checkConfig{
				logger:    d.opts.Logger,
				comparer:  d.opts.Comparer,
				readState: readState,
				newIters:  d.newIters,
				// TODO: runDBDefineCmd doesn't properly update the visible
				// sequence number. So we have to explicitly configure level checker with a very large
				// sequence number, otherwise the DB appears empty.
				seqNum: base.SeqNumMax,
			}
			if err := checkLevelsInternal(c); err != nil {
				return err.Error()
			}
			readState.unref()

			d.mu.Lock()
			s := d.mu.versions.currentVersion().String()
			d.mu.Unlock()
			return s

		case "target":
			var buf bytes.Buffer
			suggestSplit := false
			for _, cmd := range td.CmdArgs {
				switch cmd.Key {
				case "suggest-split":
					suggestSplit = true
				}
			}
			for target := range crstrings.LinesSeq(td.Input) {
				meta, err := manifest.ParseTableMetadataDebug(target)
				require.NoError(t, err)
				overlapChecker := &overlapChecker{
					comparer: d.opts.Comparer,
					newIters: d.newIters,
					opts: IterOptions{
						logger:   d.opts.Logger,
						Category: categoryIngest,
					},
					v: d.mu.versions.currentVersion(),
				}
				lsmOverlap, err := overlapChecker.DetermineLSMOverlap(context.Background(), meta.UserKeyBounds())
				if err != nil {
					return err.Error()
				}
				level, overlapFile, err := ingestTargetLevel(
					context.Background(), d.cmp, lsmOverlap, 1, d.mu.compact.inProgress, meta, suggestSplit)
				if err != nil {
					return err.Error()
				}
				if overlapFile != nil {
					fmt.Fprintf(&buf, "%d (split file: %s)\n", level, overlapFile.TableNum)
				} else {
					fmt.Fprintf(&buf, "%d\n", level)
				}
			}
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestIngest(t *testing.T) {
	var mem vfs.FS
	var d *DB
	var flushed bool
	if runtime.GOARCH == "386" {
		t.Skip("skipped on 32-bit due to slightly varied output")
	}
	defer func() {
		require.NoError(t, d.Close())
	}()

	reset := func(split bool) {
		if d != nil {
			require.NoError(t, d.Close())
		}

		mem = vfs.NewMem()
		require.NoError(t, mem.MkdirAll("ext", 0755))
		opts := &Options{
			FS:                    mem,
			L0CompactionThreshold: 100,
			L0StopWritesThreshold: 100,
			DebugCheck:            DebugCheckLevels,
			EventListener: &EventListener{FlushEnd: func(info FlushInfo) {
				flushed = true
			}},
			FormatMajorVersion: internalFormatNewest,
		}
		opts.Experimental.IngestSplit = func() bool {
			return split
		}
		// Disable automatic compactions because otherwise we'll race with
		// delete-only compactions triggered by ingesting range tombstones.
		opts.DisableAutomaticCompactions = true

		var err error
		d, err = Open("", opts)
		require.NoError(t, err)
	}
	reset(false /* split */)

	datadriven.RunTest(t, "testdata/ingest", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "reset":
			split := false
			for _, cmd := range td.CmdArgs {
				switch cmd.Key {
				case "enable-split":
					split = true
				default:
					return fmt.Sprintf("unexpected key: %s", cmd.Key)
				}
			}
			reset(split)
			return ""
		case "batch":
			b := d.NewIndexedBatch()
			if err := runBatchDefineCmd(td, b); err != nil {
				return err.Error()
			}
			if err := b.Commit(nil); err != nil {
				return err.Error()
			}
			return ""

		case "build":
			if err := runBuildCmd(td, d, mem); err != nil {
				return err.Error()
			}
			return ""

		case "ingest":
			flushed = false
			if err := runIngestCmd(td, d); err != nil {
				return err.Error()
			}
			// Wait for a possible flush.
			d.mu.Lock()
			for d.mu.compact.flushing {
				d.mu.compact.cond.Wait()
			}
			d.mu.Unlock()
			if flushed {
				return "memtable flushed"
			}
			return ""

		case "get":
			return runGetCmd(t, td, d)

		case "iter":
			iter, _ := d.NewIter(&IterOptions{
				KeyTypes: IterKeyTypePointsAndRanges,
			})
			return runIterCmd(td, iter, true)

		case "lsm":
			return runLSMCmd(td, d)

		case "metrics":
			// The asynchronous loading of table stats can change metrics, so
			// wait for all the tables' stats to be loaded.
			d.waitTableStats()

			return d.Metrics().StringForTests()

		case "wait-pending-table-stats":
			return runWaitForTableStatsCmd(td, d)

		case "compact":
			if len(td.CmdArgs) != 2 {
				panic("insufficient args for compact command")
			}
			l := td.CmdArgs[0].Key
			r := td.CmdArgs[1].Key
			err := d.Compact(context.Background(), []byte(l), []byte(r), false)
			if err != nil {
				return err.Error()
			}
			return ""
		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

// linkAndRemovePredicate is like errorfs.InjectIndex, and injects only on the
// set of Link and Remove operations, or the complement of that set. Link and
// Remove operation errors are hidden by Ingest.
type linkAndRemovePredicate struct {
	isComplement bool
	*errorfs.InjectIndex
}

func (p linkAndRemovePredicate) Evaluate(op errorfs.Op) bool {
	isLinkOrRemove := op.Kind == errorfs.OpLink || op.Kind == errorfs.OpRemove
	if (isLinkOrRemove && p.isComplement) || (!isLinkOrRemove && !p.isComplement) {
		return false
	}
	return p.InjectIndex.Evaluate(op)
}

var _ errorfs.Predicate = &linkAndRemovePredicate{}

func TestIngestError(t *testing.T) {
	for _, linkAndRemove := range []bool{false, true} {
		for i := int32(0); ; i++ {
			mem := vfs.NewMem()

			f0, err := mem.Create("ext0", vfs.WriteCategoryUnspecified)
			require.NoError(t, err)
			w := sstable.NewWriter(objstorageprovider.NewFileWritable(f0), sstable.WriterOptions{})
			require.NoError(t, w.Set([]byte("d"), []byte("d1")))
			require.NoError(t, w.Close())
			f1, err := mem.Create("ext1", vfs.WriteCategoryUnspecified)
			require.NoError(t, err)
			w = sstable.NewWriter(objstorageprovider.NewFileWritable(f1), sstable.WriterOptions{})
			require.NoError(t, w.Set([]byte("d"), []byte("d2")))
			require.NoError(t, w.Close())

			ii := errorfs.OnIndex(-1)
			d, err := Open("", &Options{
				FS: errorfs.Wrap(
					mem, errorfs.ErrInjected.If(linkAndRemovePredicate{
						isComplement: !linkAndRemove, InjectIndex: ii})),
				Logger:                panicLogger{},
				L0CompactionThreshold: 8,
			})
			require.NoError(t, err)
			// Force the creation of an L0 sstable that overlaps with the tables
			// we'll attempt to ingest. This ensures that we exercise filesystem
			// codepaths when determining the ingest target level.
			require.NoError(t, d.Set([]byte("a"), nil, nil))
			require.NoError(t, d.Set([]byte("d"), []byte("d0"), nil))
			require.NoError(t, d.Flush())

			checkVal := func(t *testing.T, expected string) {
				ii.Store(math.MaxInt32)
				val, closer, err := d.Get([]byte("d"))
				require.NoError(t, err)
				defer closer.Close()
				require.Equal(t, expected, string(val))
			}
			observedPanic := false
			var errWasInjected bool
			t.Run(fmt.Sprintf("link-and-remove=%t,index=%d", linkAndRemove, i), func(t *testing.T) {
				defer func() {
					if r := recover(); r != nil {
						// Error injected on link or remove should be completely hidden,
						// let alone cause a panic.
						require.False(t, linkAndRemove)
						observedPanic = true
						errWasInjected = ii.Load() < 0
						t.Logf("error with panic")
						if e, ok := r.(error); ok && errors.Is(e, errorfs.ErrInjected) {
							return
						}
						// d.opts.Logger.Fatalf won't propagate ErrInjected
						// itself, but should contain the error message.
						if strings.HasSuffix(fmt.Sprint(r), errorfs.ErrInjected.Error()) {
							return
						}
						t.Fatal(r)
					}
				}()

				ii.Store(i)
				err1 := d.Ingest(context.Background(), []string{"ext0"})
				err2 := d.Ingest(context.Background(), []string{"ext1"})
				errWasInjected = ii.Load() < 0
				// Check that the value of "d" is as expected, given the observed
				// errors, if any. We only do this if the error did not cause a panic,
				// since the DB should be usable.
				expectedVal := "d2"
				if err2 != nil {
					if err1 != nil {
						expectedVal = "d0"
					} else {
						expectedVal = "d1"
					}
				}

				checkVal(t, expectedVal)
				err := firstError(err1, err2)
				if err != nil {
					// Error injected on link or remove should be completely hidden.
					require.False(t, linkAndRemove)
					if !errors.Is(err, errorfs.ErrInjected) {
						t.Fatal(err)
					}
					if !errWasInjected {
						t.Fatalf("error observed but not injected")
					}
					t.Logf("error without panic")
					// Exercise some more operations on the DB, which must succeed. NB: we
					// have disabled error injection by calling checkVal above.
					require.NoError(t, d.Set([]byte("d"), []byte("d3"), nil))
					checkVal(t, "d3")
					require.NoError(t, d.Flush())
				} else {
					if errWasInjected && !linkAndRemove {
						t.Fatalf("error injected but not observed")
					}
				}
			})

			// d.Close may error if we failed to flush the manifest and panicked.
			ii.Store(math.MaxInt32)
			err = d.Close()
			if !observedPanic {
				require.NoError(t, err)
			}

			// If !errWasInjected, the i-th filesystem operation was never executed.
			if !errWasInjected {
				break
			}
		}
	}
}

func TestIngestIdempotence(t *testing.T) {
	// Use an on-disk filesystem, because Ingest with a MemFS will copy, not
	// link the ingested file.
	dir, err := os.MkdirTemp("", "ingest-idempotence")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	fs := vfs.Default

	path := fs.PathJoin(dir, "ext")
	f, err := fs.Create(fs.PathJoin(dir, "ext"), vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{})
	require.NoError(t, w.Set([]byte("d"), nil))
	require.NoError(t, w.Close())

	d, err := Open(dir, &Options{
		FS: fs,
	})
	require.NoError(t, err)
	const count = 4
	for i := 0; i < count; i++ {
		ingestPath := fs.PathJoin(dir, fmt.Sprintf("ext%d", i))
		require.NoError(t, fs.Link(path, ingestPath))
		require.NoError(t, d.Ingest(context.Background(), []string{ingestPath}))
	}
	require.NoError(t, d.Close())
}

func TestIngestCompact(t *testing.T) {
	mem := vfs.NewMem()
	lel := MakeLoggingEventListener(&base.InMemLogger{})
	d, err := Open("", &Options{
		EventListener:         &lel,
		FS:                    mem,
		L0CompactionThreshold: 1,
		L0StopWritesThreshold: 1,
	})
	require.NoError(t, err)

	src := func(i int) string {
		return fmt.Sprintf("ext%d", i)
	}
	f, err := mem.Create(src(0), vfs.WriteCategoryUnspecified)
	require.NoError(t, err)

	w := sstable.NewRawWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{})
	key := []byte("a")
	require.NoError(t, w.Add(base.MakeInternalKey(key, 0, InternalKeyKindSet), nil, false /* forceObsolete */))
	require.NoError(t, w.Close())

	// Make N copies of the sstable.
	const count = 20
	for i := 1; i < count; i++ {
		require.NoError(t, vfs.Copy(d.opts.FS, src(0), src(i)))
	}

	// Ingest the same sstable multiple times. Compaction should take place as
	// ingestion happens, preventing an indefinite write stall from occurring.
	for i := 0; i < count; i++ {
		if i == 10 {
			// Half-way through the ingestions, set a key in the memtable to force
			// overlap with the memtable which will require the memtable to be
			// flushed.
			require.NoError(t, d.Set(key, nil, nil))
		}
		require.NoError(t, d.Ingest(context.Background(), []string{src(i)}))
	}

	require.NoError(t, d.Close())
}

func TestConcurrentIngest(t *testing.T) {
	mem := vfs.NewMem()
	d, err := Open("", &Options{
		FS: mem,
	})
	require.NoError(t, err)

	// Create an sstable with 2 keys. This is necessary to trigger the overlap
	// bug because an sstable with a single key will not have overlap in internal
	// key space and the sequence number assignment had already guaranteed
	// correct ordering.
	src := func(i int) string {
		return fmt.Sprintf("ext%d", i)
	}
	f, err := mem.Create(src(0), vfs.WriteCategoryUnspecified)
	require.NoError(t, err)

	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{})
	require.NoError(t, w.Set([]byte("a"), nil))
	require.NoError(t, w.Set([]byte("b"), nil))
	require.NoError(t, w.Close())

	// Make N copies of the sstable.
	errCh := make(chan error, 5)
	for i := 1; i < cap(errCh); i++ {
		require.NoError(t, vfs.Copy(d.opts.FS, src(0), src(i)))
	}

	// Perform N ingestions concurrently.
	for i := 0; i < cap(errCh); i++ {
		go func(i int) {
			err := d.Ingest(context.Background(), []string{src(i)})
			if err == nil {
				if _, err = d.opts.FS.Stat(src(i)); oserror.IsNotExist(err) {
					err = nil
				}
			}
			errCh <- err
		}(i)
	}
	for i := 0; i < cap(errCh); i++ {
		require.NoError(t, <-errCh)
	}

	require.NoError(t, d.Close())
}

func TestConcurrentIngestCompact(t *testing.T) {
	for i := 0; i < 2; i++ {
		t.Run("", func(t *testing.T) {
			mem := vfs.NewMem()
			compactionReady := make(chan struct{})
			compactionBegin := make(chan struct{})
			d, err := Open("", &Options{
				FS: mem,
				EventListener: &EventListener{
					TableCreated: func(info TableCreateInfo) {
						if info.Reason == "compacting" {
							close(compactionReady)
							<-compactionBegin
						}
					},
				},
			})
			require.NoError(t, err)

			ingest := func(keys ...string) {
				t.Helper()
				f, err := mem.Create("ext", vfs.WriteCategoryUnspecified)
				require.NoError(t, err)

				w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{})
				for _, k := range keys {
					require.NoError(t, w.Set([]byte(k), nil))
				}
				require.NoError(t, w.Close())
				require.NoError(t, d.Ingest(context.Background(), []string{"ext"}))
			}

			compact := func(start, end string) {
				t.Helper()
				require.NoError(t, d.Compact(context.Background(), []byte(start), []byte(end), false))
			}

			lsm := func() string {
				d.mu.Lock()
				s := d.mu.versions.currentVersion().String()
				d.mu.Unlock()
				return s
			}

			expectLSM := func(expected string) {
				t.Helper()
				expected = strings.TrimSpace(expected)
				actual := strings.TrimSpace(lsm())
				if expected != actual {
					t.Fatalf("expected\n%s\nbut found\n%s", expected, actual)
				}
			}

			ingest("a")
			ingest("a")
			ingest("c")
			ingest("c")

			expectLSM(`
L0.0:
  000005:[a#11,SET-a#11,SET]
  000007:[c#13,SET-c#13,SET]
L6:
  000004:[a#10,SET-a#10,SET]
  000006:[c#12,SET-c#12,SET]
`)

			// At this point ingestion of an sstable containing only key "b" will be
			// targeted at L6. Yet a concurrent compaction of sstables 5 and 7 will
			// create a new sstable in L6 spanning ["a"-"c"]. So the ingestion must
			// actually target L5.

			switch i {
			case 0:
				// Compact, then ingest.
				go func() {
					<-compactionReady

					ingest("b")

					close(compactionBegin)
				}()

				compact("a", "z")

				expectLSM(`
L0.0:
  000009:[b#14,SET-b#14,SET]
L6:
  000008:[a#0,SET-c#0,SET]
`)

			case 1:
				// Ingest, then compact
				var wg sync.WaitGroup
				wg.Go(func() {
					close(compactionBegin)
					compact("a", "z")
				})

				ingest("b")
				wg.Wait()

				// Because we're performing the ingestion and compaction concurrently,
				// we can't guarantee any particular LSM structure at this point. The
				// test will fail with an assertion error due to overlapping sstables
				// if there is insufficient synchronization between ingestion and
				// compaction.
			}

			require.NoError(t, d.Close())
		})
	}
}

func TestIngestFlushQueuedMemTable(t *testing.T) {
	// Verify that ingestion forces a flush of a queued memtable.

	mem := vfs.NewMem()
	o := &Options{FS: mem}
	o.randomizeForTesting(t)
	d, err := Open("", o)
	require.NoError(t, err)

	// Add the key "a" to the memtable, then fill up the memtable with the key
	// "b". The ingested sstable will only overlap with the queued memtable.
	require.NoError(t, d.Set([]byte("a"), nil, nil))
	for {
		require.NoError(t, d.Set([]byte("b"), nil, nil))
		d.mu.Lock()
		done := len(d.mu.mem.queue) == 2
		d.mu.Unlock()
		if done {
			break
		}
	}

	ingest := func(keys ...string) {
		f, err := mem.Create("ext", vfs.WriteCategoryUnspecified)
		require.NoError(t, err)

		w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{
			TableFormat: o.FormatMajorVersion.MinTableFormat(),
		})
		for _, k := range keys {
			require.NoError(t, w.Set([]byte(k), nil))
		}
		require.NoError(t, w.Close())
		stats, err := d.IngestWithStats(context.Background(), []string{"ext"})
		require.NoError(t, err)
		require.Equal(t, stats.ApproxIngestedIntoL0Bytes, stats.Bytes)
		require.Equal(t, 1, stats.MemtableOverlappingFiles)
		require.Less(t, uint64(0), stats.Bytes)
	}

	ingest("a")

	require.NoError(t, d.Close())
}

func TestIngestStats(t *testing.T) {
	mem := vfs.NewMem()
	d, err := Open("", &Options{
		FS: mem,
	})
	require.NoError(t, err)

	ingest := func(expectedLevel int, keys ...string) {
		t.Helper()
		f, err := mem.Create("ext", vfs.WriteCategoryUnspecified)
		require.NoError(t, err)

		w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{})
		for _, k := range keys {
			require.NoError(t, w.Set([]byte(k), nil))
		}
		require.NoError(t, w.Close())
		stats, err := d.IngestWithStats(context.Background(), []string{"ext"})
		require.NoError(t, err)
		if expectedLevel == 0 {
			require.Equal(t, stats.ApproxIngestedIntoL0Bytes, stats.Bytes)
		} else {
			require.EqualValues(t, 0, stats.ApproxIngestedIntoL0Bytes)
		}
		require.Less(t, uint64(0), stats.Bytes)
	}
	ingest(6, "a")
	ingest(0, "a")
	ingest(6, "b", "g")
	ingest(0, "c")
	require.NoError(t, d.Close())
}

func TestIngestFlushQueuedLargeBatch(t *testing.T) {
	// Verify that ingestion forces a flush of a queued large batch.

	mem := vfs.NewMem()
	d, err := Open("", &Options{
		FS: mem,
	})
	require.NoError(t, err)

	// The default large batch threshold is slightly less than 1/2 of the
	// memtable size which makes triggering a problem with flushing queued large
	// batches irritating. Manually adjust the threshold to 1/8 of the memtable
	// size in order to more easily create a situation where a large batch is
	// queued but not automatically flushed.
	d.mu.Lock()
	d.largeBatchThreshold = d.opts.MemTableSize / 8
	d.mu.Unlock()

	// Set a record with a large value. This will be transformed into a large
	// batch and placed in the flushable queue.
	require.NoError(t, d.Set([]byte("a"), bytes.Repeat([]byte("v"), int(d.largeBatchThreshold)), nil))

	ingest := func(keys ...string) {
		t.Helper()
		f, err := mem.Create("ext", vfs.WriteCategoryUnspecified)
		require.NoError(t, err)

		w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{})
		for _, k := range keys {
			require.NoError(t, w.Set([]byte(k), nil))
		}
		require.NoError(t, w.Close())
		require.NoError(t, d.Ingest(context.Background(), []string{"ext"}))
	}

	ingest("a")

	require.NoError(t, d.Close())
}

func TestIngestMemtablePendingOverlap(t *testing.T) {
	mem := vfs.NewMem()
	d, err := Open("", &Options{
		FS: mem,
	})
	require.NoError(t, err)

	d.mu.Lock()
	// Use a custom commit pipeline apply function to give us control over
	// timing of events.
	assignedBatch := make(chan struct{})
	applyBatch := make(chan struct{})
	originalApply := d.commit.env.apply
	d.commit.env.apply = func(b *Batch, mem *memTable) error {
		assignedBatch <- struct{}{}
		applyBatch <- struct{}{}
		return originalApply(b, mem)
	}
	d.mu.Unlock()

	ingest := func(keys ...string) {
		t.Helper()
		f, err := mem.Create("ext", vfs.WriteCategoryUnspecified)
		require.NoError(t, err)

		w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{})
		for _, k := range keys {
			require.NoError(t, w.Set([]byte(k), nil))
		}
		require.NoError(t, w.Close())
		require.NoError(t, d.Ingest(context.Background(), []string{"ext"}))
	}

	var wg sync.WaitGroup

	// First, Set('c') begins. This call will:
	//
	// * enqueue the batch to the pending queue.
	// * allocate a sequence number `x`.
	// * write the batch to the WAL.
	//
	// and then block until we read from the `applyBatch` channel down below.
	wg.Go(func() {
		err := d.Set([]byte("c"), nil, nil)
		if err != nil {
			t.Error(err)
		}
	})

	// When the above Set('c') is ready to apply, it sends on the
	// `assignedBatch` channel. Once that happens, we start Ingest('a', 'c').
	// The Ingest('a', 'c') allocates sequence number `x + 1`.
	wg.Go(func() {
		// Wait until the Set has grabbed a sequence number before ingesting.
		<-assignedBatch
		ingest("a", "c")
	})

	// The Set('c')#1 and Ingest('a', 'c')#2 are both pending. To maintain
	// sequence number invariants, the Set needs to be applied and flushed
	// before the Ingest determines its target level.
	//
	// Sleep a bit to ensure that the Ingest has time to call into
	// AllocateSeqNum. Once it allocates its sequence number, it should see
	// that there are unpublished sequence numbers below it and spin until the
	// Set's sequence number is published. After sleeping, read from
	// `applyBatch` to actually allow the Set to apply and publish its
	// sequence number.
	time.Sleep(100 * time.Millisecond)
	<-applyBatch

	// Wait for both calls to complete.
	wg.Wait()
	require.NoError(t, d.Flush())
	require.NoError(t, d.CheckLevels(nil))
	require.NoError(t, d.Close())
}

// TestIngestMemtableOverlapRace is a regression test for the race described in
// #2196. If an ingest that checks for overlap with the mutable memtable and
// finds no overlap, it must not allow overlapping keys with later sequence
// numbers to be applied to the memtable and the memtable to be flushed before
// the ingest completes.
//
// This test operates by committing the same key concurrently:
//   - 1 goroutine repeatedly ingests the same sstable writing the key `foo`
//   - n goroutines repeatedly apply batches writing the key `foo` and trigger
//     flushes.
//
// After a while, the database is closed and the manifest is verified. Version
// edits should contain new files with monotonically increasing sequence
// numbers, since every flush and every ingest conflicts with one another.
func TestIngestMemtableOverlapRace(t *testing.T) {
	mem := vfs.NewMem()
	el := MakeLoggingEventListener(testutils.Logger{T: t})
	d, err := Open("", &Options{
		FS: mem,
		// Disable automatic compactions to keep the manifest clean; only
		// flushes and ingests.
		DisableAutomaticCompactions: true,
		// Disable the WAL to speed up batch commits.
		DisableWAL:    true,
		EventListener: &el,
		// We're endlessly appending to L0 without clearing it, so set a maximal
		// stop writes threshold.
		L0StopWritesThreshold: math.MaxInt,
		// Accumulating more than 1 immutable memtable doesn't help us exercise
		// the bug, since the committed keys need to be flushed promptly.
		MemTableStopWritesThreshold: 2,
	})
	require.NoError(t, err)

	// Prepare a sstable `ext` deleting foo.
	f, err := mem.Create("ext", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{})
	require.NoError(t, w.Delete([]byte("foo")))
	require.NoError(t, w.Close())

	var done atomic.Bool
	const numSetters = 2
	var wg sync.WaitGroup

	goUntilDone := func(fn func()) {
		wg.Go(func() {
			for !done.Load() {
				fn()
			}
		})
	}

	// Ingest in the background.
	totalIngests := 0
	goUntilDone(func() {
		filename := fmt.Sprintf("ext%d", totalIngests)
		require.NoError(t, mem.Link("ext", filename))
		require.NoError(t, d.Ingest(t.Context(), []string{filename}))
		totalIngests++
	})

	// Apply batches and trigger flushes in the background.
	wo := &WriteOptions{Sync: false}
	var localCommits [numSetters]int
	for i := 0; i < numSetters; i++ {
		v := []byte(fmt.Sprintf("v%d", i+1))
		goUntilDone(func() {
			// Commit a batch setting foo=vN.
			b := d.NewBatch()
			require.NoError(t, b.Set([]byte("foo"), v, nil))
			require.NoError(t, b.Commit(wo))
			localCommits[i]++
			d.AsyncFlush()
		})
	}
	time.Sleep(100 * time.Millisecond)
	done.Store(true)
	wg.Wait()

	var totalCommits int
	for i := 0; i < numSetters; i++ {
		totalCommits += localCommits[i]
	}
	m := d.Metrics()
	tot := m.Total()
	t.Logf("Committed %d batches.", totalCommits)
	t.Logf("Flushed %d times.", m.Flush.Count)
	t.Logf("Ingested %d sstables.", tot.TablesIngested)
	require.NoError(t, d.CheckLevels(nil))
	require.NoError(t, d.Close())

	// Replay the manifest. Every flush and ingest is a separate version edit.
	// Since they all write the same key and compactions are disabled, sequence
	// numbers of new files should be monotonically increasing.
	//
	// This check is necessary because most of these sstables are ingested into
	// L0. The L0 sublevels construction will order them by LargestSeqNum, even
	// if they're added to L0 out-of-order. The CheckLevels call at the end of
	// the test may find that the sublevels are all appropriately ordered, but
	// the manifest may reveal they were added to the LSM out-of-order.
	dbDesc, err := Peek("", mem)
	require.NoError(t, err)
	require.True(t, dbDesc.Exists)
	require.Greater(t, len(dbDesc.OptionsFilename), 0)
	f, err = mem.Open(dbDesc.ManifestFilename)
	require.NoError(t, err)
	defer f.Close()
	rr := record.NewReader(f, 0 /* logNum */)
	var largest *manifest.TableMetadata
	for {
		r, err := rr.Next()
		if err == io.EOF || err == record.ErrInvalidChunk {
			break
		}
		require.NoError(t, err)
		var ve manifest.VersionEdit
		require.NoError(t, ve.Decode(r))
		t.Log(ve.String())
		for _, f := range ve.NewTables {
			if largest != nil {
				require.Equal(t, 0, f.Level)
				if largest.LargestSeqNum > f.Meta.LargestSeqNum {
					t.Fatalf("previous largest file %s has sequence number > next file %s", largest, f.Meta)
				}
			}
			largest = f.Meta
		}
	}
}

type ingestCrashFS struct {
	vfs.FS
}

func (fs ingestCrashFS) Link(oldname, newname string) error {
	if err := fs.FS.Link(oldname, newname); err != nil {
		return err
	}
	panic(errorfs.ErrInjected)
}

type noRemoveFS struct {
	vfs.FS
}

func (fs noRemoveFS) Remove(string) error {
	return errorfs.ErrInjected
}

func TestIngestFileNumReuseCrash(t *testing.T) {
	const count = 10
	// Use an on-disk filesystem, because Ingest with a MemFS will copy, not
	// link the ingested file.
	dir, err := os.MkdirTemp("", "ingest-filenum-reuse")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	fs := vfs.Default

	readFile := func(s string) []byte {
		f, err := fs.Open(fs.PathJoin(dir, s))
		require.NoError(t, err)
		b, err := io.ReadAll(f)
		require.NoError(t, err)
		require.NoError(t, f.Close())
		return b
	}

	// Create sstables to ingest.
	var files []string
	var fileBytes [][]byte
	for i := 0; i < count; i++ {
		name := fmt.Sprintf("ext%d", i)
		f, err := fs.Create(fs.PathJoin(dir, name), vfs.WriteCategoryUnspecified)
		require.NoError(t, err)
		w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{})
		require.NoError(t, w.Set([]byte(fmt.Sprintf("foo%d", i)), nil))
		require.NoError(t, w.Close())
		files = append(files, name)
		fileBytes = append(fileBytes, readFile(name))
	}

	// Open a database with a filesystem that will successfully link the
	// ingested files but then panic. This is an approximation of what a crash
	// after linking but before updating the manifest would look like.
	d, err := Open(dir, &Options{
		FS: ingestCrashFS{FS: fs},
	})
	// A flush here ensures the file num bumps from creating OPTIONS files,
	// etc get recorded in the manifest. We want the nextFileNum after the
	// restart to be the same as one of our ingested sstables.
	require.NoError(t, err)
	require.NoError(t, d.Set([]byte("boop"), nil, nil))
	require.NoError(t, d.Flush())
	for _, f := range files {
		func() {
			defer func() { err = recover().(error) }()
			err = d.Ingest(t.Context(), []string{fs.PathJoin(dir, f)})
		}()
		if err == nil || !errors.Is(err, errorfs.ErrInjected) {
			t.Fatalf("expected injected error, got %v", err)
		}
	}
	// Leave something in the WAL so that Open will flush while replaying the
	// WAL.
	require.NoError(t, d.Set([]byte("wal"), nil, nil))
	require.NoError(t, d.Close())

	// There are now two links to each external file: the original extX link
	// and a numbered sstable link. The sstable files are still not a part of
	// the manifest and so they may be overwritten. Open will detect the
	// obsolete number sstables and try to remove them. The FS here is wrapped
	// to induce errors on Remove calls. Even if we're unsuccessful in
	// removing the obsolete files, the external files should not be
	// overwritten.
	d, err = Open(dir, &Options{FS: noRemoveFS{FS: fs}, Logger: testutils.Logger{T: t}})
	require.NoError(t, err)
	require.NoError(t, d.Set([]byte("bar"), nil, nil))
	require.NoError(t, d.Flush())
	require.NoError(t, d.Close())

	// None of the external files should change despite modifying the linked
	// versions.
	for i, f := range files {
		afterBytes := readFile(f)
		require.Equal(t, fileBytes[i], afterBytes)
	}
}

func TestIngest_UpdateSequenceNumber(t *testing.T) {
	mem := vfs.NewMem()
	cmp := base.DefaultComparer.Compare
	parse := func(input string) (sstable.RawWriter, error) {
		f, err := mem.Create("ext", vfs.WriteCategoryUnspecified)
		if err != nil {
			return nil, err
		}
		w := sstable.NewRawWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{
			TableFormat: sstable.TableFormatMax,
		})
		for data := range crstrings.LinesSeq(input) {
			if strings.HasPrefix(data, "Span: ") {
				data = strings.TrimPrefix(data, "Span: ")
				err := w.EncodeSpan(keyspan.ParseSpan(data))
				if err != nil {
					return nil, err
				}
				continue
			}
			j := strings.Index(data, ":")
			if j < 0 {
				return nil, errors.Newf("malformed input: %s\n", data)
			}
			key := base.ParseInternalKey(data[:j])
			value := []byte(data[j+1:])
			if err := w.Add(key, value, false /* forceObsolete */); err != nil {
				return nil, err
			}
		}
		return w, nil
	}

	var (
		seqNum base.SeqNum
		metas  []*manifest.TableMetadata
	)
	datadriven.RunTest(t, "testdata/ingest_update_seqnums", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "starting-seqnum":
			seqNum = base.ParseSeqNum(td.Input)
			return ""

		case "reset":
			metas = metas[:0]
			return ""

		case "load":
			w, err := parse(td.Input)
			if err != nil {
				return err.Error()
			}
			if err = w.Close(); err != nil {
				return err.Error()
			}
			defer w.Close()

			// Format the bounds of the table.
			wm, err := w.Metadata()
			if err != nil {
				return err.Error()
			}

			// Upper bounds for range dels and range keys are expected to be sentinel
			// keys.
			maybeUpdateUpperBound := func(key base.InternalKey) base.InternalKey {
				switch k := key.Kind(); {
				case k == base.InternalKeyKindRangeDelete:
					key.Trailer = base.InternalKeyRangeDeleteSentinel
				case rangekey.IsRangeKey(k):
					return base.MakeExclusiveSentinelKey(k, key.UserKey)
				}
				return key
			}

			// Construct the table metadata from the writer metadata.
			m := &manifest.TableMetadata{
				SmallestSeqNum: 0, // Simulate an ingestion.
				LargestSeqNum:  0,
			}
			if wm.HasPointKeys {
				m.ExtendPointKeyBounds(cmp, wm.SmallestPoint, wm.LargestPoint)
			}
			if wm.HasRangeDelKeys {
				m.ExtendPointKeyBounds(
					cmp,
					wm.SmallestRangeDel,
					maybeUpdateUpperBound(wm.LargestRangeDel),
				)
			}
			if wm.HasRangeKeys {
				m.ExtendRangeKeyBounds(
					cmp,
					wm.SmallestRangeKey,
					maybeUpdateUpperBound(wm.LargestRangeKey),
				)
			}
			m.InitPhysicalBacking()
			if err := m.Validate(cmp, base.DefaultFormatter); err != nil {
				return err.Error()
			}

			// Collect this file.
			metas = append(metas, m)

			// Return an index number for the file.
			return fmt.Sprintf("file %d\n", len(metas)-1)

		case "update-files":
			// Update the bounds across all files.
			for i, m := range metas {
				if err := setSeqNumInMetadata(m, seqNum+base.SeqNum(i), cmp, base.DefaultFormatter); err != nil {
					return err.Error()
				}
			}

			var buf bytes.Buffer
			for i, m := range metas {
				fmt.Fprintf(&buf, "file %d:\n", i)
				fmt.Fprintf(&buf, "  combined: %s-%s\n", m.Smallest(), m.Largest())
				fmt.Fprintf(&buf, "    points: %s-%s\n", m.PointKeyBounds.Smallest(), m.PointKeyBounds.Largest())
				if m.HasRangeKeys {
					fmt.Fprintf(&buf, "    ranges: %s-%s\n", m.RangeKeyBounds.Smallest(), m.RangeKeyBounds.Largest())
				}
			}

			return buf.String()

		default:
			return fmt.Sprintf("unknown command %s\n", td.Cmd)
		}
	})
}

func TestIngestCleanup(t *testing.T) {
	fns := []base.TableNum{0, 1, 2}

	testCases := []struct {
		closeFiles   []base.TableNum
		cleanupFiles []base.TableNum
		wantErr      string
	}{
		// Close and remove all files.
		{
			closeFiles:   fns,
			cleanupFiles: fns,
		},
		// Remove a non-existent file.
		{
			closeFiles:   fns,
			cleanupFiles: []base.TableNum{3},
			wantErr:      "unknown to the objstorage provider",
		},
		// Remove a file that has not been closed.
		{
			closeFiles:   []base.TableNum{0, 2},
			cleanupFiles: fns,
			wantErr:      oserror.ErrInvalid.Error(),
		},
		// Remove all files, one of which is still open, plus a file that does not exist.
		{
			closeFiles:   []base.TableNum{0, 2},
			cleanupFiles: []base.TableNum{0, 1, 2, 3},
			wantErr:      oserror.ErrInvalid.Error(), // The first error encountered is due to the open file.
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			mem := vfs.NewMem()
			mem.UseWindowsSemantics(true)
			objProvider, err := objstorageprovider.Open(objstorageprovider.DefaultSettings(mem, ""))
			require.NoError(t, err)
			defer objProvider.Close()

			// Create the files in the VFS.
			metaMap := make(map[base.TableNum]objstorage.Writable)
			for _, fn := range fns {
				w, _, err := objProvider.Create(t.Context(), base.FileTypeTable, base.PhysicalTableDiskFileNum(fn), objstorage.CreateOptions{})
				require.NoError(t, err)

				metaMap[fn] = w
			}

			// Close a select number of files.
			for _, m := range tc.closeFiles {
				w, ok := metaMap[m]
				if !ok {
					continue
				}
				require.NoError(t, w.Finish())
			}

			// Cleanup the set of files in the FS.
			var toRemove []ingestLocalMeta
			for _, fn := range tc.cleanupFiles {
				m := &manifest.TableMetadata{TableNum: fn}
				m.InitPhysicalBacking()
				toRemove = append(toRemove, ingestLocalMeta{TableMetadata: m})
			}

			err = ingestCleanup(objProvider, toRemove)
			if tc.wantErr != "" {
				require.Error(t, err, "got no error, expected %s", tc.wantErr)
				require.Contains(t, err.Error(), tc.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// fatalCapturingLogger captures a fatal error instead of panicking.
type fatalCapturingLogger struct {
	t   testing.TB
	err error
}

// Infof implements the Logger interface.
func (l *fatalCapturingLogger) Infof(fmt string, args ...interface{}) {
	l.t.Logf(fmt, args...)
}

// Errorf implements the Logger interface.
func (l *fatalCapturingLogger) Errorf(fmt string, args ...interface{}) {
	l.t.Logf(fmt, args...)
}

// Fatalf implements the Logger interface.
func (l *fatalCapturingLogger) Fatalf(_ string, args ...interface{}) {
	l.err = args[0].(error)
}

func TestIngestValidation(t *testing.T) {
	type keyVal struct {
		key, val []byte
	}
	// The corruptionLocation enum defines where to corrupt an sstable if
	// anywhere. corruptionLocation{Start,End} describe the start and end
	// data blocks. corruptionLocationInternal describes a random data block
	// that's neither the start or end blocks. The Ingest operation does not
	// read the entire sstable, only the start and end blocks, so corruption
	// introduced using corruptionLocationInternal will not be discovered until
	// the asynchronous validation job runs.
	type corruptionLocation int
	const (
		corruptionLocationNone corruptionLocation = iota
		corruptionLocationStart
		corruptionLocationEnd
		corruptionLocationInternal
	)
	// The errReportLocation type defines an enum to allow tests to enforce
	// expectations about how an error surfaced during ingestion or validation
	// is reported. Asynchronous validation that uncovers corruption should call
	// Fatalf on the Logger. Asychronous validation that encounters
	// non-corruption errors should surface it through the
	// EventListener.BackgroundError func.
	type errReportLocation int
	const (
		errReportLocationNone errReportLocation = iota
		errReportLocationIngest
		errReportLocationFatal
		errReportLocationBackgroundError
	)
	const (
		nKeys     = 1_000
		keySize   = 16
		valSize   = 100
		blockSize = 100

		ingestTableName = "ext"
	)

	seed := uint64(time.Now().UnixNano())
	rng := rand.New(rand.NewPCG(0, seed))
	t.Logf("rng seed = %d", seed)

	// errfsCounter is used by test cases that make use of an errorfs.Injector
	// to inject errors into the ingest validation code path.
	var errfsCounter atomic.Int32
	testCases := []struct {
		description     string
		cLoc            corruptionLocation
		wantErrType     errReportLocation
		wantErr         error
		errorfsInjector errorfs.Injector
	}{
		{
			description: "no corruption",
			cLoc:        corruptionLocationNone,
			wantErrType: errReportLocationNone,
		},
		{
			description: "start block",
			cLoc:        corruptionLocationStart,
			wantErr:     ErrCorruption,
			wantErrType: errReportLocationIngest,
		},
		{
			description: "end block",
			cLoc:        corruptionLocationEnd,
			wantErr:     ErrCorruption,
			wantErrType: errReportLocationIngest,
		},
		{
			description: "non-end block",
			cLoc:        corruptionLocationInternal,
			wantErr:     ErrCorruption,
			wantErrType: errReportLocationFatal,
		},
		{
			description: "non-corruption error",
			cLoc:        corruptionLocationNone,
			wantErr:     errorfs.ErrInjected,
			wantErrType: errReportLocationBackgroundError,
			errorfsInjector: errorfs.InjectorFunc(func(op errorfs.Op) error {
				// Inject an error on the first read-at operation on an sstable
				// (excluding the read on the sstable before ingestion has
				// linked it in).
				if op.Path != "ext" && op.Kind != errorfs.OpFileReadAt || filepath.Ext(op.Path) != ".sst" {
					return nil
				}
				if errfsCounter.Add(1) == 1 {
					return errorfs.ErrInjected
				}
				return nil
			}),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			errfsCounter.Store(0)
			var wg sync.WaitGroup
			wg.Add(1)

			fs := vfs.NewMem()
			var testFS vfs.FS = fs
			if tc.errorfsInjector != nil {
				testFS = errorfs.Wrap(fs, tc.errorfsInjector)
			}

			// backgroundErr is populated by EventListener.BackgroundError.
			var backgroundErr error
			logger := &fatalCapturingLogger{t: t}
			opts := &Options{
				// Disable table stats so that injected errors can't be accidentally
				// injected into the table stats collector read, and so the table
				// stats collector won't prime the table+block cache such that the
				// error injection won't trigger at all during ingest validation.
				DisableTableStats: true,
				FS:                testFS,
				Logger:            logger,
				EventListener: &EventListener{
					TableValidated: func(i TableValidatedInfo) {
						wg.Done()
					},
					BackgroundError: func(err error) {
						backgroundErr = err
					},
				},
			}
			opts.Experimental.ValidateOnIngest = true
			d, err := Open("", opts)
			require.NoError(t, err)
			defer func() { require.NoError(t, d.Close()) }()

			corrupt := func(f vfs.File) {
				readable, err := sstable.NewSimpleReadable(f)
				require.NoError(t, err)
				// Compute the layout of the sstable in order to find the
				// appropriate block locations to corrupt.
				r, err := sstable.NewReader(t.Context(), readable, opts.MakeReaderOptions())
				require.NoError(t, err)
				l, err := r.Layout()
				require.NoError(t, err)

				// Select an appropriate data block to corrupt.
				var blockIdx int
				switch tc.cLoc {
				case corruptionLocationStart:
					blockIdx = 0
				case corruptionLocationEnd:
					blockIdx = len(l.Data) - 1
				case corruptionLocationInternal:
					blockIdx = 1 + rng.IntN(len(l.Data)-2)
				default:
					t.Fatalf("unknown corruptionLocation: %T", tc.cLoc)
				}
				bh := l.Data[blockIdx]

				// Corrupting a key will cause the ingestion to fail due to a
				// malformed key, rather than a block checksum mismatch.
				// Instead, we corrupt the last byte in the selected block,
				// before the trailer, which corresponds to a value.
				offset := bh.Offset + bh.Length - 1
				_, err = f.WriteAt([]byte("\xff"), int64(offset))
				require.NoError(t, err)
				require.NoError(t, r.Close())
			}

			type errT struct {
				errLoc errReportLocation
				err    error
			}
			runIngest := func(keyVals []keyVal) (et errT) {
				f, err := fs.Create(ingestTableName, vfs.WriteCategoryUnspecified)
				require.NoError(t, err)
				defer func() { _ = fs.Remove(ingestTableName) }()

				w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{
					BlockSize:   blockSize, // Create many smaller blocks.
					Comparer:    opts.Comparer,
					Compression: sstable.NoCompression, // For simpler debugging.
					KeySchema:   opts.KeySchemas[opts.KeySchema],
				})
				for _, kv := range keyVals {
					require.NoError(t, w.Set(kv.key, kv.val))
				}
				require.NoError(t, w.Close())

				// Possibly corrupt the file.
				if tc.cLoc != corruptionLocationNone {
					f, err = fs.OpenReadWrite(ingestTableName, vfs.WriteCategoryUnspecified)
					require.NoError(t, err)
					corrupt(f)
				}

				// Ingest the external table.
				err = d.Ingest(t.Context(), []string{ingestTableName})
				if err != nil {
					et.errLoc = errReportLocationIngest
					et.err = err
					return
				}

				// Wait for the validation on the sstable to complete.
				wg.Wait()

				// Return any error encountered during validation.
				if logger.err != nil {
					et.errLoc = errReportLocationFatal
					et.err = logger.err
				} else if backgroundErr != nil {
					et.errLoc = errReportLocationBackgroundError
					et.err = backgroundErr
				}
				return
			}

			// Construct a set of keys to ingest.
			var keyVals []keyVal
			for i := 0; i < nKeys; i++ {
				key := make([]byte, keySize)
				for j := range key {
					key[j] = byte(rng.Uint32())
				}

				val := make([]byte, valSize)
				for j := range val {
					val[j] = byte(rng.Uint32())
				}

				keyVals = append(keyVals, keyVal{key, val})
			}

			// Keys must be sorted.
			slices.SortFunc(keyVals, func(a, b keyVal) int { return d.cmp(a.key, b.key) })

			// Run the ingestion.
			et := runIngest(keyVals)

			// Assert we saw the errors we expect.
			switch tc.wantErrType {
			case errReportLocationNone:
				require.Equal(t, errReportLocationNone, et.errLoc)
				require.NoError(t, et.err)
			case errReportLocationIngest:
				require.Equal(t, errReportLocationIngest, et.errLoc)
				require.Error(t, et.err)
				require.True(t, errors.Is(et.err, tc.wantErr))
			case errReportLocationFatal:
				require.Equal(t, errReportLocationFatal, et.errLoc)
				require.Error(t, et.err)
				require.True(t, errors.Is(et.err, tc.wantErr))
			case errReportLocationBackgroundError:
				require.Equal(t, errReportLocationBackgroundError, et.errLoc)
				require.Error(t, et.err)
				require.True(t, errors.Is(et.err, tc.wantErr))
			default:
				t.Fatalf("unknown wantErrType %T", tc.wantErrType)
			}
		})
	}
}

// BenchmarkManySSTables measures the cost of various operations with various
// counts of SSTables within the database.
func BenchmarkManySSTables(b *testing.B) {
	counts := []int{10, 1_000, 10_000, 100_000, 1_000_000}
	ops := []string{"ingest", "calculateInuseKeyRanges"}
	for _, op := range ops {
		b.Run(op, func(b *testing.B) {
			for _, count := range counts {
				b.Run(fmt.Sprintf("sstables=%d", count), func(b *testing.B) {
					mem := vfs.NewMem()
					d, err := Open("", &Options{
						FS: mem,
					})
					require.NoError(b, err)

					var paths []string
					for i := 0; i < count; i++ {
						n := fmt.Sprintf("%07d", i)
						f, err := mem.Create(n, vfs.WriteCategoryUnspecified)
						require.NoError(b, err)
						w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{})
						require.NoError(b, w.Set([]byte(n), nil))
						require.NoError(b, w.Close())
						paths = append(paths, n)
					}
					require.NoError(b, d.Ingest(b.Context(), paths))

					{
						const broadIngest = "broad.sst"
						f, err := mem.Create(broadIngest, vfs.WriteCategoryUnspecified)
						require.NoError(b, err)
						w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{})
						require.NoError(b, w.Set([]byte("0"), nil))
						require.NoError(b, w.Set([]byte("Z"), nil))
						require.NoError(b, w.Close())
						require.NoError(b, d.Ingest(b.Context(), []string{broadIngest}))
					}

					switch op {
					case "ingest":
						runBenchmarkManySSTablesIngest(b, d, mem, count)
					case "calculateInuseKeyRanges":
						runBenchmarkManySSTablesInUseKeyRanges(b, d, count)
					}
					require.NoError(b, d.Close())
				})
			}
		})
	}
}

func runBenchmarkManySSTablesIngest(b *testing.B, d *DB, fs vfs.FS, count int) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := fmt.Sprintf("%07d", count+i)
		f, err := fs.Create(n, vfs.WriteCategoryUnspecified)
		require.NoError(b, err)
		w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{})
		require.NoError(b, w.Set([]byte(n), nil))
		require.NoError(b, w.Close())
		require.NoError(b, d.Ingest(b.Context(), []string{n}))
	}
}

func runBenchmarkManySSTablesInUseKeyRanges(b *testing.B, d *DB, count int) {
	// This benchmark is pretty contrived, but it's not easy to write a
	// microbenchmark for this in a more natural way. L6 has many files, and
	// L5 has 1 file spanning the entire breadth of L5.
	d.mu.Lock()
	defer d.mu.Unlock()
	v := d.mu.versions.currentVersion()
	b.ResetTimer()

	smallest := []byte("0")
	largest := []byte("z")
	for i := 0; i < b.N; i++ {
		_ = v.CalculateInuseKeyRanges(d.mu.versions.latest.l0Organizer, 0, numLevels-1, smallest, largest)
	}
}
