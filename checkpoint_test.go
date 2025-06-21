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

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage/remote"
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
			Logger:                      testLogger{t},
		}
		opts.Experimental.EnableColumnarBlocks = func() bool { return true }
		opts.Experimental.RemoteStorage = remote.MakeSimpleFactory(map[remote.Locator]remote.Storage{
			"": remoteMem,
		})
		if createOnShared {
			opts.Experimental.CreateOnShared = remote.CreateOnSharedAll
		}
		opts.DisableTableStats = true
		opts.private.testingAlwaysWaitForCleanup = true
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
				return "checkpoint <db> <dir> [restrict=(start-end, ...)]"
			}
			var opts []CheckpointOption
			if len(td.CmdArgs) == 3 {
				var spans []CheckpointSpan
				for _, v := range td.CmdArgs[2].Vals {
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
			fileNums := d.mu.versions.virtualBackings.DiskFileNums()
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
			panic(fmt.Sprintf("unrecognized command %q", td.Cmd))
		}
	})
}

func TestCheckpoint(t *testing.T) {
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
	fs := vfs.NewMem()
	d, err := Open("", &Options{FS: fs, Logger: testLogger{t: t}})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(4)
	go func() {
		defer cancel()
		defer wg.Done()
		for i := 0; ctx.Err() == nil; i++ {
			if err := d.Set([]byte(fmt.Sprintf("key%06d", i)), nil, nil); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	go func() {
		defer cancel()
		defer wg.Done()
		for ctx.Err() == nil {
			if err := d.Compact(context.Background(), []byte("key"), []byte("key999999"), false); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	check := make(chan string, 100)
	go func() {
		defer cancel()
		defer close(check)
		defer wg.Done()
		for i := 0; ctx.Err() == nil && i < 200; i++ {
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
	}()
	go func() {
		opts := &Options{FS: fs, Logger: testLogger{t: t}}
		defer cancel()
		defer wg.Done()
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
	}()
	<-ctx.Done()
	wg.Wait()
	require.NoError(t, d.Close())
}

func TestCheckpointFlushWAL(t *testing.T) {
	const checkpointPath = "checkpoints/checkpoint"
	fs := vfs.NewCrashableMem()
	opts := &Options{FS: fs, Logger: testLogger{t: t}}
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
	if testing.Short() {
		t.Skip("skipping because of short flag")
	}
	const checkpointPath = "checkpoint"
	opts := &Options{
		FS:                          vfs.NewMem(),
		FormatMajorVersion:          internalFormatNewest,
		DisableAutomaticCompactions: true,
		Logger:                      testLogger{t},
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
