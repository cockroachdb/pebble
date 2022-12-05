// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestCheckpoint(t *testing.T) {
	dbs := make(map[string]*DB)
	defer func() {
		for _, db := range dbs {
			require.NoError(t, db.Close())
		}
	}()

	var buf syncedBuffer
	mem := vfs.NewMem()
	opts := &Options{
		FS:                    loggingFS{mem, &buf},
		FormatMajorVersion:    FormatNewest,
		L0CompactionThreshold: 10,
	}

	datadriven.RunTest(t, "testdata/checkpoint", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "batch":
			if len(td.CmdArgs) != 1 {
				return "batch <db>"
			}
			buf.Reset()
			d := dbs[td.CmdArgs[0].String()]
			b := d.NewBatch()
			if err := runBatchDefineCmd(td, b); err != nil {
				return err.Error()
			}
			if err := b.Commit(Sync); err != nil {
				return err.Error()
			}
			return buf.String()

		case "checkpoint":
			if len(td.CmdArgs) != 2 {
				return "checkpoint <db> <dir>"
			}
			buf.Reset()
			d := dbs[td.CmdArgs[0].String()]
			if err := d.Checkpoint(td.CmdArgs[1].String()); err != nil {
				return err.Error()
			}
			return buf.String()

		case "compact":
			if len(td.CmdArgs) != 1 {
				return "compact <db>"
			}
			buf.Reset()
			d := dbs[td.CmdArgs[0].String()]
			if err := d.Compact(nil, []byte("\xff"), false); err != nil {
				return err.Error()
			}
			return buf.String()

		case "flush":
			if len(td.CmdArgs) != 1 {
				return "flush <db>"
			}
			buf.Reset()
			d := dbs[td.CmdArgs[0].String()]
			if err := d.Flush(); err != nil {
				return err.Error()
			}
			return buf.String()

		case "list":
			if len(td.CmdArgs) != 1 {
				return "list <dir>"
			}
			paths, err := mem.List(td.CmdArgs[0].String())
			if err != nil {
				return err.Error()
			}
			sort.Strings(paths)
			buf.Reset()
			fmt.Fprintf(&buf, "%s\n", strings.Join(paths, "\n"))
			return buf.String()

		case "open":
			if len(td.CmdArgs) != 1 && len(td.CmdArgs) != 2 {
				return "open <dir> [readonly]"
			}
			opts.ReadOnly = false
			if len(td.CmdArgs) == 2 {
				if td.CmdArgs[1].String() != "readonly" {
					return "open <dir> [readonly]"
				}
				opts.ReadOnly = true
			}

			buf.Reset()
			dir := td.CmdArgs[0].String()
			d, err := Open(dir, opts)
			if err != nil {
				return err.Error()
			}
			dbs[dir] = d
			return buf.String()

		case "scan":
			if len(td.CmdArgs) != 1 {
				return "scan <db>"
			}
			buf.Reset()
			d := dbs[td.CmdArgs[0].String()]
			iter := d.NewIter(nil)
			for valid := iter.First(); valid; valid = iter.Next() {
				fmt.Fprintf(&buf, "%s %s\n", iter.Key(), iter.Value())
			}
			fmt.Fprintf(&buf, ".\n")
			if err := iter.Close(); err != nil {
				fmt.Fprintf(&buf, "%v\n", err)
			}
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestCheckpointCompaction(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open("", &Options{FS: fs})
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
			if err := d.Compact([]byte("key"), []byte("key999999"), false); err != nil {
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
			dir := fmt.Sprintf("checkpoint%6d", i)
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
		opts := &Options{FS: fs}
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
					if _, err := fs.Stat(base.MakeFilepath(fs, dir, base.FileTypeTable, tbl.FileNum)); err != nil {
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
	fs := vfs.NewStrictMem()
	opts := &Options{FS: fs}
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
		fs.ResetToSyncedState()
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
		iter := d.NewIter(nil)
		require.True(t, iter.First())
		require.Equal(t, key, iter.Key())
		require.Equal(t, value, iter.Value())
		require.False(t, iter.Next())
		require.NoError(t, iter.Close())
		require.NoError(t, d.Close())
	}
}
