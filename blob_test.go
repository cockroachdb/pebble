// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestBlob(t *testing.T) {
	var mem vfs.FS
	var d *DB
	var flushed bool
	defer func() {
		require.NoError(t, d.Close())
	}()

	unclosedIters := map[string]*Iterator{}
	reset := func(createMem bool) {
		if d != nil {
			for k, iter := range unclosedIters {
				require.NoError(t, iter.Close())
				delete(unclosedIters, k)
			}
			require.NoError(t, d.Close())
		}

		if createMem {
			mem = vfs.NewMem()
		}
		require.NoError(t, mem.MkdirAll("ext", 0755))
		opts := &Options{
			FS:                    mem,
			L0CompactionThreshold: 100,
			L0StopWritesThreshold: 100,
			DebugCheck:            DebugCheckLevels,
			EventListener: &EventListener{FlushEnd: func(info FlushInfo) {
				flushed = true
			}},
			FormatMajorVersion: FormatNewest,
		}
		opts.Levels = make([]LevelOptions, numLevels)
		opts.Levels[0] = LevelOptions{
			TargetFileSize:                         90,
			TargetFileSizeIncludingBlobValueSize:   90,
			TargetBlobFileSizeBasedOnBlobValueSize: 5,
		}
		for i := 1; i < numLevels; i++ {
			opts.Levels[i] = opts.Levels[0]
		}
		opts.Experimental.BlobValueSizeThreshold = 1
		opts.Experimental.EnableValueBlocks = func() bool { return true }
		// Disable automatic compactions because otherwise we'll race with
		// delete-only compactions triggered by ingesting range tombstones.
		opts.DisableAutomaticCompactions = true

		var err error
		d, err = Open("", opts)
		require.NoError(t, err)
	}
	reset(true)

	lsmString := func() string {
		d.mu.Lock()
		s := d.mu.versions.currentVersion().String()
		blobString := d.mu.versions.BlobLevels.String()
		d.mu.Unlock()
		if len(blobString) > 0 {
			s = fmt.Sprintf("%s%s", s, blobString)
		}
		return s
	}

	datadriven.RunTest(t, "testdata/blob", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "reset":
			reset(true)
			return ""

		case "reopen":
			reset(false)
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

		case "flush":
			if err := d.Flush(); err != nil {
				return err.Error()
			}
			return lsmString()

		case "build":
			if err := runBuildCmd(td, d, mem); err != nil {
				return err.Error()
			}
			return ""

		case "ingest":
			flushed = false
			if err := runIngestCmd(td, d, mem); err != nil {
				return err.Error()
			}
			// Wait for a possible flush.
			d.mu.Lock()
			for d.mu.compact.flushing {
				d.mu.compact.cond.Wait()
			}
			d.mu.Unlock()
			var s string
			if flushed {
				s = "memtable flushed\n"
			}
			return s + lsmString()

		case "get":
			return runGetCmd(td, d)

		case "iter":
			var name string
			if td.HasArg("name") {
				td.ScanArgs(t, "name", &name)
			}
			_, ok := unclosedIters[name]
			if ok {
				return fmt.Sprintf("iter %s already open", name)
			}
			iter := d.NewIter(&IterOptions{
				KeyTypes: IterKeyTypePointsAndRanges,
			})
			closeIter := name == ""
			rv := runIterCmd(td, iter, closeIter)
			if !closeIter {
				unclosedIters[name] = iter
			}
			return rv

		case "close-iter":
			var name string
			td.ScanArgs(t, "name", &name)
			iter := unclosedIters[name]
			if iter == nil {
				return fmt.Sprintf("iter %s not found", name)
			}
			err := iter.Close()
			var rv string
			if err != nil {
				rv = err.Error()
			}
			delete(unclosedIters, name)
			// The deletion of obsolete files happens asynchronously when an iterator
			// is closed. Wait for the obsolete tables to be deleted. Note that
			// waiting on cleaner.cond isn't precisely correct.
			d.mu.Lock()
			for d.mu.cleaner.cleaning || len(d.mu.versions.obsoleteTables) > 0 ||
				len(d.mu.versions.obsoleteBlobFiles) > 0 {
				d.mu.cleaner.cond.Wait()
			}
			d.mu.Unlock()

			return rv

		case "list-files":
			paths, err := mem.List("")
			if err != nil {
				return err.Error()
			}
			i := 0
			for j, path := range paths {
				if len(path) < 5 {
					continue
				}
				include := false
				if path[len(path)-4:] == ".sst" {
					include = true
				} else if path[len(path)-5:] == ".blob" {
					include = true
				}
				if include {
					paths[i] = paths[j]
					i++
				}
			}
			paths = paths[:i]
			sort.Strings(paths)
			return fmt.Sprintf("%s\n", strings.Join(paths, "\n"))

		case "lsm":
			return lsmString()

		case "metrics":
			// The asynchronous loading of table stats can change metrics, so
			// wait for all the tables' stats to be loaded.
			d.mu.Lock()
			d.waitTableStats()
			d.mu.Unlock()

			return d.Metrics().String()

		case "wait-pending-table-stats":
			return runTableStatsCmd(td, d)

		case "compact":
			if len(td.CmdArgs) != 2 {
				panic("insufficient args for compact command")
			}
			l := td.CmdArgs[0].Key
			r := td.CmdArgs[1].Key
			err := d.Compact([]byte(l), []byte(r), false)
			if err != nil {
				return err.Error()
			}
			return lsmString()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}
