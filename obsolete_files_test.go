// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestCleaner(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dbs := make(map[string]*DB)
	defer func() {
		for _, db := range dbs {
			require.NoError(t, db.Close())
		}
	}()

	mem := vfs.NewMem()
	var memLog base.InMemLogger
	fs := vfs.WithLogging(mem, memLog.Infof)
	datadriven.RunTest(t, "testdata/cleaner", func(t *testing.T, td *datadriven.TestData) string {
		memLog.Reset()
		switch td.Cmd {
		case "batch":
			if len(td.CmdArgs) != 1 {
				return "batch <db>"
			}
			d := dbs[td.CmdArgs[0].String()]
			b := d.NewBatch()
			if err := runBatchDefineCmd(td, b); err != nil {
				return err.Error()
			}
			if err := b.Commit(Sync); err != nil {
				return err.Error()
			}
			return memLog.String()

		case "compact":
			if len(td.CmdArgs) != 1 {
				return "compact <db>"
			}
			d := dbs[td.CmdArgs[0].String()]
			if err := d.Compact(context.Background(), nil, []byte("\xff"), false); err != nil {
				return err.Error()
			}
			return memLog.String()

		case "flush":
			if len(td.CmdArgs) != 1 {
				return "flush <db>"
			}
			d := dbs[td.CmdArgs[0].String()]
			if err := d.Flush(); err != nil {
				return err.Error()
			}
			return memLog.String()

		case "close":
			if len(td.CmdArgs) != 1 {
				return "close <db>"
			}
			dbDir := td.CmdArgs[0].String()
			d := dbs[dbDir]
			if err := d.Close(); err != nil {
				return err.Error()
			}
			delete(dbs, dbDir)
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
			if len(td.CmdArgs) < 1 || len(td.CmdArgs) > 3 {
				return "open <dir> [archive] [readonly]"
			}
			dir := td.CmdArgs[0].String()
			opts := &Options{
				FS:     fs,
				WALDir: dir + "_wal",
				Logger: testutils.Logger{T: t},
			}
			opts.WithFSDefaults()

			for i := 1; i < len(td.CmdArgs); i++ {
				switch td.CmdArgs[i].String() {
				case "readonly":
					opts.ReadOnly = true
				case "archive":
					opts.Cleaner = ArchiveCleaner{}
				default:
					return "open <dir> [archive] [readonly]"
				}
			}
			// Asynchronous table stats retrieval makes the output flaky.
			opts.DisableTableStats = true
			opts.private.testingAlwaysWaitForCleanup = true
			d, err := Open(dir, opts)
			if err != nil {
				return err.Error()
			}
			d.TestOnlyWaitForCleaning()
			dbs[dir] = d
			return memLog.String()

		case "create-bogus-file":
			if len(td.CmdArgs) != 1 {
				return "create-bogus-file <db/file>"
			}
			dst, err := fs.Create(td.CmdArgs[0].String(), vfs.WriteCategoryUnspecified)
			require.NoError(t, err)
			_, err = dst.Write([]byte("bogus data"))
			require.NoError(t, err)
			require.NoError(t, dst.Sync())
			require.NoError(t, dst.Close())
			return memLog.String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestCleanupManagerCloseWithPacing(t *testing.T) {
	mem := vfs.NewMem()
	opts := &Options{
		FS:                     mem,
		TargetByteDeletionRate: func() int { return 1024 }, // 1 KB/s - slow pacing
	}
	opts.EnsureDefaults()

	objProvider, err := objstorageprovider.Open(objstorageprovider.Settings{
		FS:        mem,
		FSDirName: "/",
	})
	require.NoError(t, err)
	defer objProvider.Close()

	getDeletePacerInfo := func() deletionPacerInfo {
		return deletionPacerInfo{
			freeBytes: 10 << 30,
		}
	}

	cm := openCleanupManager(opts, objProvider, getDeletePacerInfo)

	// Create obsolete files that would normally take a long time to delete.
	// At 1 KB/s, 100 files of 10 KB each would take 1000 seconds.
	largeFiles := make([]obsoleteFile, 100)
	for i := range largeFiles {
		largeFiles[i] = obsoleteFile{
			fileType: base.FileTypeTable,
			fs:       mem,
			path:     fmt.Sprintf("test%02d.sst", i+1),
			fileNum:  base.DiskFileNum(i + 1),
			fileSize: 10 << 10,
			isLocal:  true,
		}
	}

	cm.EnqueueJob(1, largeFiles, obsoleteObjectStats{})

	done := make(chan struct{})
	go func() {
		defer close(done)
		cm.Close()
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatalf("timed out waiting for cleanupManager.Close() to return")
	}
}

// TestCleanupManagerFallingBehind verifies that we disable pacing when the jobs
// channel reaches the high threshold.
func TestCleanupManagerFallingBehind(t *testing.T) {
	mem := vfs.NewMem()
	var rate atomic.Int32
	rate.Store(10 * MB) // 10MB/s
	opts := &Options{
		FS:                      mem,
		FreeSpaceThresholdBytes: 1,
		TargetByteDeletionRate:  func() int { return int(rate.Load()) }, // 10 MB/s
	}
	opts.EnsureDefaults()

	objProvider, err := objstorageprovider.Open(objstorageprovider.Settings{
		FS:        mem,
		FSDirName: "/",
	})
	require.NoError(t, err)
	defer objProvider.Close()

	getDeletePacerInfo := func() deletionPacerInfo {
		return deletionPacerInfo{
			freeBytes: 10 * GB,
			liveBytes: 10 * GB,
		}
	}

	cm := openCleanupManager(opts, objProvider, getDeletePacerInfo)

	x := 0
	addJob := func(fileSize int) {
		x++
		cm.EnqueueJob(1, []obsoleteFile{{
			fileType: base.FileTypeTable,
			fs:       mem,
			path:     fmt.Sprintf("test%02d.sst", x),
			fileNum:  base.DiskFileNum(x),
			fileSize: uint64(fileSize),
			isLocal:  true,
		}}, obsoleteObjectStats{})
	}

	for range jobsQueueLowThreshold {
		addJob(1 * MB)
	}
	// At 1MB, each job will take 100ms each. Note that the rate increase based on
	// history won't make much difference, since the enqueued size is averaged
	// over 5 minutes.
	time.Sleep(50 * time.Millisecond)
	require.Greater(t, len(cm.jobsCh), jobsQueueLowThreshold/2)
	t.Logf("%d", len(cm.jobsCh))

	// Add enough jobs to exceed the high threshold. We add small jobs so that the
	// historic rate doesn't grow significantly.
	require.Greater(t, jobsQueueDepth, jobsQueueHighThreshold+jobsQueueLowThreshold)
	t.Logf("B")
	for range jobsQueueHighThreshold {
		addJob(1)
	}

	for i := 0; ; i++ {
		time.Sleep(10 * time.Millisecond)
		if len(cm.jobsCh) <= jobsQueueHighThreshold {
			break
		}
		if i == 1000 {
			t.Fatalf("jobs channel length never dropped below high threshold (%d vs %d)", len(cm.jobsCh), jobsQueueHighThreshold)
		}
	}
	// Set a high rate so the rest of the jobs finish quickly.
	rate.Store(1 * GB)
	cm.Close()
}

// TestUnpacedFilesJumpQueue verifies that files which are not subject to pacing
// (WALs, manifests, remote objects) are deleted right away, even when paced
// files that were enqueued earlier are still waiting out the pacing delay.
func TestUnpacedFilesJumpQueue(t *testing.T) {
	mem := vfs.NewMem()
	var mu struct {
		sync.Mutex
		deleted []base.FileType
	}
	record := func(fileType base.FileType) {
		mu.Lock()
		defer mu.Unlock()
		mu.deleted = append(mu.deleted, fileType)
	}
	numDeleted := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(mu.deleted)
	}
	opts := &Options{
		FS:                      mem,
		FreeSpaceThresholdBytes: 1,
		// 1 KB/s; a 10MB table takes ~3 hours to get through the pacer.
		TargetByteDeletionRate: func() int { return 1024 },
		EventListener: &EventListener{
			TableDeleted:    func(TableDeleteInfo) { record(base.FileTypeTable) },
			WALDeleted:      func(WALDeleteInfo) { record(base.FileTypeLog) },
			ManifestDeleted: func(ManifestDeleteInfo) { record(base.FileTypeManifest) },
		},
	}
	opts.EnsureDefaults()

	objProvider, err := objstorageprovider.Open(objstorageprovider.Settings{
		FS:        mem,
		FSDirName: "/",
		FSCleaner: base.DeleteCleaner{},
	})
	require.NoError(t, err)
	defer objProvider.Close()

	getDeletePacerInfo := func() deletionPacerInfo {
		return deletionPacerInfo{
			freeBytes: 10 * GB,
			liveBytes: 10 * GB,
		}
	}
	cm := openCleanupManager(opts, objProvider, getDeletePacerInfo)

	// The files must exist, otherwise their deletion is a no-op which doesn't
	// notify the event listener.
	table := func(fileNum int, isLocal bool) obsoleteFile {
		w, _, err := objProvider.Create(
			context.Background(), base.FileTypeTable, base.DiskFileNum(fileNum), objstorage.CreateOptions{},
		)
		require.NoError(t, err)
		require.NoError(t, w.Finish())
		return obsoleteFile{
			fileType: base.FileTypeTable,
			fs:       mem,
			path:     fmt.Sprintf("%06d.sst", fileNum),
			fileNum:  base.DiskFileNum(fileNum),
			fileSize: 10 * MB,
			isLocal:  isLocal,
		}
	}
	manifest := func(fileNum int) obsoleteFile {
		path := fmt.Sprintf("MANIFEST-%06d", fileNum)
		f, err := mem.Create(path, vfs.WriteCategoryUnspecified)
		require.NoError(t, err)
		require.NoError(t, f.Close())
		return obsoleteFile{
			fileType: base.FileTypeManifest,
			fs:       mem,
			path:     path,
			fileNum:  base.DiskFileNum(fileNum),
			fileSize: 1 * MB,
			isLocal:  true,
		}
	}
	wal := func(fileNum int) obsoleteFile {
		path := fmt.Sprintf("%06d.log", fileNum)
		f, err := mem.Create(path, vfs.WriteCategoryUnspecified)
		require.NoError(t, err)
		require.NoError(t, f.Close())
		return obsoleteFile{
			fileType: base.FileTypeLog,
			fs:       mem,
			path:     path,
			fileNum:  base.DiskFileNum(fileNum),
			fileSize: 64 * MB,
			isLocal:  true,
		}
	}
	deleted := func() []base.FileType {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(mu.deleted)
	}

	// The first table is deleted right away and puts the pacer in debt; at
	// 1 KB/s, 10MB take hours to pay off.
	cm.EnqueueJob(1, []obsoleteFile{table(1, true /* isLocal */)}, obsoleteObjectStats{})
	require.Eventually(t, func() bool { return numDeleted() >= 1 }, 30*time.Second, time.Millisecond)

	// A second table has to wait for the debt, but a WAL, a manifest and a remote
	// table enqueued in the same job after it must not.
	remoteTableStats := obsoleteObjectStats{
		tablesAll: countAndSize{count: 1, size: 10 * MB},
	}
	cm.EnqueueJob(2, []obsoleteFile{
		table(2, true /* isLocal */), wal(3), manifest(4), table(5, false /* isLocal */),
	}, remoteTableStats)
	require.Eventually(t, func() bool { return numDeleted() >= 4 }, 30*time.Second, time.Millisecond)

	// A WAL enqueued while the second table is still waiting also goes first.
	cm.EnqueueJob(3, []obsoleteFile{wal(6)}, obsoleteObjectStats{})
	require.Eventually(t, func() bool { return numDeleted() >= 5 }, 30*time.Second, time.Millisecond)

	require.Equal(t, []base.FileType{
		base.FileTypeTable,    // job 1
		base.FileTypeLog,      // job 2, unpaced
		base.FileTypeManifest, // job 2, unpaced
		base.FileTypeTable,    // job 2, unpaced (remote)
		base.FileTypeLog,      // job 3
	}, deleted())

	// The stats of a job that was split are only accounted for once the paced
	// part completes.
	require.Equal(t, obsoleteObjectStats{}, cm.CompletedStats())

	// Close disables pacing, so the second table is deleted as well.
	cm.Close()
	require.Equal(t, []base.FileType{
		base.FileTypeTable,
		base.FileTypeLog,
		base.FileTypeManifest,
		base.FileTypeTable,
		base.FileTypeLog,
		base.FileTypeTable, // job 2, paced
	}, deleted())
	require.Equal(t, remoteTableStats, cm.CompletedStats())
}
