// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func testCheckpointImpl(t *testing.T, ddFile string, createOnShared bool) {
	dbs := make(map[string]*DB)
	defer func() {
		for _, db := range dbs {
			if db.closed.Load() == nil {
				require.NoError(t, db.Close())
			}
		}
	}()

	mem := vfs.NewMem()
	var memLog base.InMemLogger
	remoteMem := remote.NewInMem()
	makeOptions := func() *Options {
		opts := &Options{
			FS:                          vfs.WithLogging(mem, memLog.Infof),
			FormatMajorVersion:          internalFormatNewest,
			L0CompactionThreshold:       10,
			DisableAutomaticCompactions: true,
			Logger:                      testutils.Logger{T: t},
		}
		opts.RemoteStorage = remote.MakeSimpleFactory(map[remote.Locator]remote.Storage{
			remote.MakeLocator(""): remoteMem,
		})
		if createOnShared {
			opts.CreateOnShared = remote.CreateOnSharedAll
		}
		opts.DisableTableStats = true
		opts.private.testingAlwaysWaitForCleanup = true
		// The testdata captures file open patterns that match the V1 iterator
		// stack. TODO(radu): port to V2.
		opts.IteratorStack = IteratorStackV1
		return opts
	}

	datadriven.RunTest(t, ddFile, func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "batch":
			if len(td.CmdArgs) != 1 {
				return "batch <db>"
			}
			memLog.Reset()
			d := dbs[td.CmdArgs[0].String()]
			b := d.NewBatch()
			if err := runBatchDefineCmd(td, b); err != nil {
				return err.Error()
			}
			if err := b.Commit(Sync); err != nil {
				return err.Error()
			}
			return memLog.String()

		case "checkpoint":
			if len(td.CmdArgs) < 2 {
				return "checkpoint <db> <dir> [restrict=(start-end, ...)] [minimal]"
			}
			var opts []CheckpointOption
			for _, arg := range td.CmdArgs[2:] {
				switch arg.Key {
				case "restrict":
					var spans []CheckpointSpan
					for _, v := range arg.Vals {
						splits := strings.SplitN(v, "-", 2)
						if len(splits) != 2 {
							return fmt.Sprintf("invalid restrict range %q", v)
						}
						spans = append(spans, CheckpointSpan{
							Start: []byte(splits[0]),
							End:   []byte(splits[1]),
						})
					}
					opts = append(opts, WithRestrictToSpans(spans))
				case "minimal":
					opts = append(opts, WithMinimalManifest())
				}
			}
			memLog.Reset()
			d := dbs[td.CmdArgs[0].String()]
			if err := d.Checkpoint(td.CmdArgs[1].String(), opts...); err != nil {
				return err.Error()
			}
			if td.HasArg("nondeterministic") {
				memLog.Reset()
				return ""
			}
			return memLog.String()

		case "ingest-and-excise":
			d := dbs[td.CmdArgs[0].String()]

			// Hacky but the command doesn't expect a db string. Get rid of it.
			td.CmdArgs = td.CmdArgs[1:]
			if err := runIngestAndExciseCmd(td, d); err != nil {
				return err.Error()
			}
			return ""

		case "build":
			d := dbs[td.CmdArgs[0].String()]

			// Hacky but the command doesn't expect a db string. Get rid of it.
			td.CmdArgs = td.CmdArgs[1:]
			if err := runBuildCmd(td, d, mem); err != nil {
				return err.Error()
			}
			return ""

		case "lsm":
			d := dbs[td.CmdArgs[0].String()]

			// Hacky but the command doesn't expect a db string. Get rid of it.
			td.CmdArgs = td.CmdArgs[1:]
			return runLSMCmd(td, d)

		case "compact":
			if len(td.CmdArgs) != 1 {
				return "compact <db>"
			}
			memLog.Reset()
			d := dbs[td.CmdArgs[0].String()]
			if err := d.Compact(context.Background(), nil, []byte("\xff"), false); err != nil {
				return err.Error()
			}
			d.TestOnlyWaitForCleaning()
			return memLog.String()

		case "print-backing":
			// Print the virtual backings in the version. Used to test whether the
			// checkpoint removed the backings correctly.
			if len(td.CmdArgs) != 1 {
				return "print-backing <db>"
			}
			d := dbs[td.CmdArgs[0].String()]
			d.mu.Lock()
			d.mu.versions.logLock()
			fileNums := d.mu.versions.latest.virtualBackings.DiskFileNums()
			d.mu.versions.logUnlock()
			d.mu.Unlock()

			var buf bytes.Buffer
			for _, f := range fileNums {
				buf.WriteString(fmt.Sprintf("%s\n", f.String()))
			}
			return buf.String()

		case "close":
			if len(td.CmdArgs) != 1 {
				return "close <db>"
			}
			d := dbs[td.CmdArgs[0].String()]
			require.NoError(t, d.Close())
			return ""

		case "flush":
			if len(td.CmdArgs) != 1 {
				return "flush <db>"
			}
			memLog.Reset()
			d := dbs[td.CmdArgs[0].String()]
			if err := d.Flush(); err != nil {
				return err.Error()
			}
			return memLog.String()

		case "list":
			if len(td.CmdArgs) != 1 {
				return "list <dir>"
			}
			paths, err := mem.List(td.CmdArgs[0].String())
			if err != nil {
				return err.Error()
			}
			sort.Strings(paths)
			return fmt.Sprintf("%s\n", strings.Join(paths, "\n"))

		case "open":
			if len(td.CmdArgs) < 1 {
				return "open <dir> [readonly]"
			}
			opts := makeOptions()
			require.NoError(t, parseDBOptionsArgs(opts, td.CmdArgs[1:]))

			memLog.Reset()
			dir := td.CmdArgs[0].String()
			if _, ok := dbs[dir]; ok {
				require.NoError(t, dbs[dir].Close())
				dbs[dir] = nil
			}

			d, err := Open(dir, opts)
			if err != nil {
				return err.Error()
			}
			dbs[dir] = d
			if len(dbs) == 1 && createOnShared {
				// This is the first db. Set a creator ID.
				if err := d.SetCreatorID(1); err != nil {
					return err.Error()
				}
			}
			waitForCompactionsAndTableStats(d)

			if td.HasArg("nondeterministic") {
				memLog.Reset()
				return ""
			}
			return memLog.String()

		case "scan":
			if len(td.CmdArgs) != 1 {
				return "scan <db>"
			}
			memLog.Reset()
			d := dbs[td.CmdArgs[0].String()]
			iter, _ := d.NewIter(nil)
			for valid := iter.First(); valid; valid = iter.Next() {
				memLog.Infof("%s %s", iter.Key(), iter.Value())
			}
			memLog.Infof(".")
			if err := iter.Close(); err != nil {
				memLog.Infof("%v\n", err)
			}
			return memLog.String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestCopyCheckpointOptions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.NewMem()
	datadriven.RunTest(t, "testdata/copy_checkpoint_options", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "copy":
			f, err := fs.Create("old", vfs.WriteCategoryUnspecified)
			require.NoError(t, err)
			_, err = io.WriteString(f, td.Input)
			require.NoError(t, err)
			require.NoError(t, f.Close())

			if err := copyCheckpointOptions(fs, "old", "new"); err != nil {
				return err.Error()
			}

			f, err = fs.Open("new")
			require.NoError(t, err)
			newFile, err := io.ReadAll(f)
			require.NoError(t, err)
			require.NoError(t, f.Close())
			return string(newFile)
		default:
			t.Fatalf("unrecognized command %q", td.Cmd)
			return ""
		}
	})
}

func TestCheckpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Run("shared=false", func(t *testing.T) {
		testCheckpointImpl(t, "testdata/checkpoint", false /* createOnShared */)
	})
	t.Run("shared=true", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skipf("skipped on windows")
		}
		testCheckpointImpl(t, "testdata/checkpoint_shared", true /* createOnShared */)
	})
}

func TestCheckpointCompaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.NewMem()
	d, err := Open("", &Options{FS: fs, Logger: testutils.Logger{T: t}})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())

	var wg sync.WaitGroup
	wg.Go(func() {
		defer cancel()
		for i := 0; ctx.Err() == nil; i++ {
			if err := d.Set([]byte(fmt.Sprintf("key%06d", i)), nil, nil); err != nil {
				t.Error(err)
				return
			}
		}
	})
	wg.Go(func() {
		defer cancel()
		for ctx.Err() == nil {
			if err := d.Compact(t.Context(), []byte("key"), []byte("key999999"), false); err != nil {
				t.Error(err)
				return
			}
		}
	})
	check := make(chan string, 100)
	wg.Go(func() {
		defer cancel()
		defer close(check)
		for i := 0; ctx.Err() == nil && i < 50; i++ {
			dir := fmt.Sprintf("checkpoint%06d", i)
			if err := d.Checkpoint(dir); err != nil {
				t.Error(err)
				return
			}
			select {
			case <-ctx.Done():
				return
			case check <- dir:
			}
		}
	})
	wg.Go(func() {
		opts := &Options{FS: fs, Logger: testutils.Logger{T: t}}
		defer cancel()
		for dir := range check {
			d2, err := Open(dir, opts)
			if err != nil {
				t.Error(err)
				return
			}
			// Check the checkpoint has all the sstables that the manifest
			// claims it has.
			tableInfos, _ := d2.SSTables()
			for _, tables := range tableInfos {
				for _, tbl := range tables {
					if tbl.Virtual {
						continue
					}
					if _, err := fs.Stat(base.MakeFilepath(fs, dir, base.FileTypeTable, base.PhysicalTableDiskFileNum(tbl.FileNum))); err != nil {
						t.Error(err)
						return
					}
				}
			}
			if err := d2.Close(); err != nil {
				t.Error(err)
				return
			}
		}
	})
	<-ctx.Done()
	wg.Wait()
	require.NoError(t, d.Close())
}

func TestCheckpointFlushWAL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const checkpointPath = "checkpoints/checkpoint"
	fs := vfs.NewCrashableMem()
	opts := &Options{FS: fs, Logger: testutils.Logger{T: t}}
	key, value := []byte("key"), []byte("value")

	// Create a checkpoint from an unsynced DB.
	{
		d, err := Open("", opts)
		require.NoError(t, err)
		{
			wb := d.NewBatch()
			err = wb.Set(key, value, nil)
			require.NoError(t, err)
			err = d.Apply(wb, NoSync)
			require.NoError(t, err)
		}
		err = d.Checkpoint(checkpointPath, WithFlushedWAL())
		require.NoError(t, err)
		require.NoError(t, d.Close())
		fs = fs.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 0})
	}

	// Check that the WAL has been flushed in the checkpoint.
	{
		files, err := fs.List(checkpointPath)
		require.NoError(t, err)
		hasLogFile := false
		for _, f := range files {
			info, err := fs.Stat(fs.PathJoin(checkpointPath, f))
			require.NoError(t, err)
			if strings.HasSuffix(f, ".log") {
				hasLogFile = true
				require.NotZero(t, info.Size())
			}
		}
		require.True(t, hasLogFile)
	}

	// Check that the checkpoint contains the expected data.
	{
		d, err := Open(checkpointPath, opts)
		require.NoError(t, err)
		iter, _ := d.NewIter(nil)
		require.True(t, iter.First())
		require.Equal(t, key, iter.Key())
		require.Equal(t, value, iter.Value())
		require.False(t, iter.Next())
		require.NoError(t, iter.Close())
		require.NoError(t, d.Close())
	}
}

func TestCheckpointManyFiles(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("skipping because of short flag")
	}
	const checkpointPath = "checkpoint"
	opts := &Options{
		FS:                          vfs.NewMem(),
		FormatMajorVersion:          internalFormatNewest,
		DisableAutomaticCompactions: true,
		Logger:                      testutils.Logger{T: t},
	}
	// Disable compression to speed up the test.
	opts.EnsureDefaults()
	for i := range opts.Levels {
		opts.Levels[i].Compression = func() *sstable.CompressionProfile { return sstable.NoCompression }
	}

	d, err := Open("", opts)
	require.NoError(t, err)
	defer d.Close()

	mkKey := func(x int) []byte {
		return []byte(fmt.Sprintf("key%06d", x))
	}
	// We want to test the case where the appended record with the excluded files
	// makes the manifest cross 32KB. This will happen for a range of values
	// around 450.
	n := 400 + rand.IntN(100)
	for i := 0; i < n; i++ {
		err := d.Set(mkKey(i), nil, nil)
		require.NoError(t, err)
		err = d.Flush()
		require.NoError(t, err)
	}
	err = d.Checkpoint(checkpointPath, WithRestrictToSpans([]CheckpointSpan{
		{
			Start: mkKey(0),
			End:   mkKey(10),
		},
	}))
	require.NoError(t, err)

	// Open the checkpoint and iterate through all the keys.
	{
		d, err := Open(checkpointPath, opts)
		require.NoError(t, err)
		iter, _ := d.NewIter(nil)
		require.True(t, iter.First())
		require.NoError(t, iter.Error())
		n := 1
		for iter.Next() {
			n++
		}
		require.NoError(t, iter.Error())
		require.NoError(t, iter.Close())
		require.NoError(t, d.Close())
		require.Equal(t, 10, n)
	}
}

func TestCheckpointMinimalManifest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("skipping because of short flag")
	}

	opts := &Options{
		FS:                          vfs.NewMem(),
		FormatMajorVersion:          internalFormatNewest,
		DisableAutomaticCompactions: true,
		Logger:                      testutils.Logger{T: t},
	}
	opts.ValueSeparationPolicy = func() ValueSeparationPolicy {
		return ValueSeparationPolicy{
			Enabled:                true,
			MinimumSize:            8,
			MinimumMVCCGarbageSize: 8,
			MaxBlobReferenceDepth:  5,
		}
	}
	opts.EnsureDefaults()
	for i := range opts.Levels {
		opts.Levels[i].Compression = func() *sstable.CompressionProfile { return sstable.NoCompression }
	}

	d, err := Open("", opts)
	require.NoError(t, err)
	defer d.Close()

	// Write some data to the DB, including values that qualify for value
	// separation.
	mkKey := func(x int) []byte {
		return []byte(fmt.Sprintf("key%06d", x))
	}
	for i := 0; i < 100; i++ {
		val := bytes.Repeat([]byte("v"), i%32)
		require.NoError(t, d.Set(mkKey(i), val, nil))
		require.NoError(t, d.Flush())
	}
	// Write unflushed data to the DB to exercise WAL replay.
	for i := 0; i < 100; i++ {
		val := bytes.Repeat([]byte("v"), i%32)
		require.NoError(t, d.Set(mkKey(i), val, nil))
	}

	// Create two checkpoints, one with the default manifest and one with a
	// minimal manifest.
	require.NoError(t, d.Checkpoint("default"))
	require.NoError(t, d.Checkpoint("minimal", WithMinimalManifest()))

	expected, err := getCheckpointContents("default", opts)
	require.NoError(t, err)
	actual, err := getCheckpointContents("minimal", opts)
	require.NoError(t, err)
	require.Equal(t, expected, actual)

	// Create two checkpoints with restricted spans, one with the default
	// manifest and one with a minimal manifest.
	span := []CheckpointSpan{{Start: mkKey(0), End: mkKey(10)}}
	require.NoError(t, d.Checkpoint("with-spans", WithRestrictToSpans(span)))
	require.NoError(t, d.Checkpoint("minimal-with-spans", WithRestrictToSpans(span), WithMinimalManifest()))

	expected, err = getCheckpointContents("with-spans", opts)
	require.NoError(t, err)
	actual, err = getCheckpointContents("minimal-with-spans", opts)
	require.NoError(t, err)
	require.Equal(t, expected, actual)

	// Ensure the MANIFESTs contain a single record.
	rr, err := getManifestRecords(d.opts.FS, "minimal")
	require.NoError(t, err)
	require.Len(t, rr, 1)
	rr, err = getManifestRecords(d.opts.FS, "minimal-with-spans")
	require.NoError(t, err)
	require.Len(t, rr, 1)
}

func TestCheckpointMinimalManifest_MarkedForCompaction(t *testing.T) {
	defer leaktest.AfterTest(t)()

	fs := vfs.NewMem()
	require.NoError(t, fs.MkdirAll("ck", 0755))

	v := manifest.NewInitialVersion(base.DefaultComparer)
	included := &manifest.TableMetadata{TableNum: 1}
	excluded := &manifest.TableMetadata{TableNum: 2}
	v.MarkedForCompaction.Insert(included, 6)
	v.MarkedForCompaction.Insert(excluded, 6)

	excludedTables := map[manifest.DeletedTableEntry]*manifest.TableMetadata{
		{Level: 6, FileNum: excluded.TableNum}: excluded,
	}
	snapshot := &manifest.VersionEdit{ComparerName: base.DefaultComparer.Name, NextFileNum: 2}

	d := &DB{opts: &Options{Comparer: base.DefaultComparer}}
	require.NoError(t, d.writeMinimalCheckpointManifest(
		fs, internalFormatNewest, "ck", snapshot, 1, v, excludedTables, nil, nil))

	// Read back the single record and verify the filter kept only the included table.
	rr, err := getManifestRecords(fs, "ck")
	require.NoError(t, err)

	require.Len(t, rr, 1)
	require.Len(t, rr[0].TablesMarkedForCompaction, 1)
	require.Equal(t, included.TableNum, rr[0].TablesMarkedForCompaction[0].TableNum)
}

// TestCheckpointFlushableIngest is a regression test: a Checkpoint taken while
// there are pending flushable ingest entries in the memtable queue must copy
// the corresponding SSTable files to the checkpoint directory. Without the fix,
// opening the checkpoint would fail with:
//
//	pebble: error when opening flushable ingest files: file does not exist
func TestCheckpointFlushableIngest(t *testing.T) {
	mem := vfs.NewMem()
	require.NoError(t, mem.MkdirAll("ext", 0755))

	opts := &Options{
		FS:                          mem,
		FormatMajorVersion:          internalFormatNewest,
		DisableAutomaticCompactions: true,
		Logger:                      testutils.Logger{T: t},
	}
	d, err := Open("db", opts)
	require.NoError(t, err)

	// Write a key to the memtable. A subsequent ingest whose key range overlaps
	// with the memtable is taken along the flushable-ingest path instead of
	// forcing a synchronous flush, which is the scenario under test.
	require.NoError(t, d.Set([]byte("b"), []byte("memtable"), Sync))

	// Build a small SSTable in the external directory containing the same key.
	sstPath := "ext/foo.sst"
	f, err := mem.Create(sstPath, vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), d.opts.MakeWriterOptions(0, d.TableFormat()))
	require.NoError(t, w.Set([]byte("b"), []byte("ingested")))
	require.NoError(t, w.Close())

	// Prevent automatic flushes from draining the flushable queue before we
	// can verify the ingestedFlushable and take a checkpoint.
	// DisableAutomaticCompactions does not disable flushes.
	d.mu.Lock()
	d.mu.compact.flushing = true
	d.mu.Unlock()

	// Ingest the SSTable. Because it overlaps with the memtable key "b", it is
	// added to the flushable queue as an ingestedFlushable rather than being
	// placed directly into L0.
	require.NoError(t, d.Ingest(context.Background(), []string{sstPath}))

	// Confirm that the ingest went through the flushable path.
	d.mu.Lock()
	var hasFlushableIngest bool
	for _, entry := range d.mu.mem.queue {
		if _, ok := entry.flushable.(*ingestedFlushable); ok {
			hasFlushableIngest = true
			break
		}
	}
	d.mu.Unlock()
	require.True(t, hasFlushableIngest, "expected ingest to be enqueued as a flushable ingest")

	// Checkpoint without flushing first. The checkpoint must copy the
	// ingestedFlushable SSTable files so that WAL replay on open succeeds.
	require.NoError(t, d.Checkpoint("checkpoint"))

	// Re-enable flushing so Close does not deadlock.
	d.mu.Lock()
	d.mu.compact.flushing = false
	d.mu.Unlock()

	require.NoError(t, d.Close())

	// Opening the checkpoint previously failed with:
	//   pebble: error when opening flushable ingest files: file does not exist
	d2, err := Open("checkpoint", &Options{
		FS:                 mem,
		FormatMajorVersion: internalFormatNewest,
		Logger:             testutils.Logger{T: t},
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, d2.Close()) }()

	// The ingested value (higher sequence number) should shadow the memtable
	// value for key "b".
	val, closer, err := d2.Get([]byte("b"))
	require.NoError(t, err)
	require.Equal(t, []byte("ingested"), val)
	closer.Close()
}

type checkpointContents struct {
	kvs                []string
	comparerName       string
	minUnflushedLogNum base.DiskFileNum
	nextFileNum        uint64
	logSeqNum          base.SeqNum
	tables             []string
	virtualBackings    []string
	blobFiles          []string
}

// getCheckpointContents extracts data and file metadata from a checkpoint
// to support checkpoint equality comparison.
func getCheckpointContents(dir string, opts *Options) (result checkpointContents, err error) {
	// Open in read-only mode to avoid mutating checkpoint on-disk state.
	roOpts := *opts
	roOpts.ReadOnly = true
	ckDB, err := Open(dir, &roOpts)
	if err != nil {
		return result, err
	}
	defer ckDB.Close()

	iter, err := ckDB.NewIter(nil)
	if err != nil {
		return result, err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		result.kvs = append(result.kvs, fmt.Sprintf("%s=%s", iter.Key(), iter.Value()))
	}
	if err := iter.Error(); err != nil {
		return result, err
	}

	ckDB.mu.Lock()
	defer ckDB.mu.Unlock()
	v := ckDB.mu.versions.currentVersion()
	result.comparerName = ckDB.opts.Comparer.Name
	result.minUnflushedLogNum = ckDB.mu.versions.minUnflushedLogNum
	result.nextFileNum = ckDB.mu.versions.nextFileNum.Load()
	result.logSeqNum = ckDB.mu.versions.logSeqNum.Load()

	for level, lm := range v.Levels {
		for meta := range lm.All() {
			id := fmt.Sprintf("L%d.%s", level, meta.TableNum)
			result.tables = append(result.tables, id)
		}
	}
	for backing := range ckDB.mu.versions.latest.virtualBackings.All() {
		result.virtualBackings = append(result.virtualBackings, backing.DiskFileNum.String())
	}
	sort.Strings(result.virtualBackings)
	for meta := range v.BlobFiles.All() {
		result.blobFiles = append(result.blobFiles, meta.FileID.String())
	}
	sort.Strings(result.blobFiles)

	return result, nil
}

// getManifestRecords reads all MANIFEST records from a checkpoint dir.
func getManifestRecords(fs vfs.FS, dir string) ([]manifest.VersionEdit, error) {
	files, err := fs.List(dir)
	if err != nil {
		return nil, err
	}

	result := make([]manifest.VersionEdit, 0)
	for _, filename := range files {
		fileType, _, ok := base.ParseFilename(fs, filename)
		if !ok || fileType != base.FileTypeManifest {
			continue
		}

		f, err := fs.Open(fs.PathJoin(dir, filename))
		if err != nil {
			return nil, err
		}
		defer f.Close()

		rr := record.NewReader(f, 0 /* logNum */)

		for {
			r, err := rr.Next()
			if err == io.EOF || record.IsInvalidRecord(err) {
				break
			}
			if err != nil {
				return nil, err
			}

			var ve manifest.VersionEdit
			err = ve.Decode(r)
			if err != nil {
				return nil, err
			}
			result = append(result, ve)
		}
	}

	return result, nil
}
