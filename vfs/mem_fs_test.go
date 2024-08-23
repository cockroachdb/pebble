// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func runMemFSDataDriven(t *testing.T, path string, fs *MemFS) {
	var f File
	datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
		var err error
		switch td.Cmd {
		case "create":
			f, err = fs.Create(td.CmdArgs[0].String(), WriteCategoryUnspecified)
		case "link":
			err = fs.Link(td.CmdArgs[0].String(), td.CmdArgs[1].String())
		case "open":
			f, err = fs.Open(td.CmdArgs[0].String())
		case "open-dir":
			f, err = fs.OpenDir(td.CmdArgs[0].String())
		case "open-read-write":
			f, err = fs.OpenReadWrite(td.CmdArgs[0].String(), WriteCategoryUnspecified)
		case "mkdirall":
			err = fs.MkdirAll(td.CmdArgs[0].String(), 0755)
		case "remove":
			err = fs.Remove(td.CmdArgs[0].String())
		case "rename":
			err = fs.Rename(td.CmdArgs[0].String(), td.CmdArgs[1].String())
		case "reuse-for-write":
			f, err = fs.ReuseForWrite(td.CmdArgs[0].String(), td.CmdArgs[1].String(), WriteCategoryUnspecified)
		case "reset-to-synced":
			fs.ResetToSyncedState()
		case "ignore-syncs":
			fs.SetIgnoreSyncs(true)
		case "stop-ignoring-syncs":
			fs.SetIgnoreSyncs(false)
		case "f.write":
			_, err = f.Write([]byte(strings.TrimSpace(td.Input)))
		case "f.sync":
			err = f.Sync()
		case "f.read":
			n, _ := strconv.Atoi(td.CmdArgs[0].String())
			buf := make([]byte, n)
			_, err = io.ReadFull(f, buf)
			if err != nil {
				break
			}
			return string(buf)
		case "f.readat":
			n, _ := strconv.Atoi(td.CmdArgs[0].String())
			off, _ := strconv.Atoi(td.CmdArgs[0].String())
			buf := make([]byte, n)
			_, err = f.ReadAt(buf, int64(off))
			if err != nil {
				break
			}
			return string(buf)
		case "f.close":
			f, err = nil, f.Close()
		case "f.stat.name":
			var fi FileInfo
			fi, err = f.Stat()
			if err != nil {
				break
			}
			return fi.Name()
		case "list":
			list, err := fs.List(td.CmdArgs[0].String())
			if err != nil {
				break
			}
			sort.Strings(list)
			return strings.Join(list, "\n")
		case "fs-string":
			return fs.String()
		default:
			t.Fatalf("unknown command %q", td.Cmd)
		}
		if err != nil {
			return fmt.Sprintf("error: %v", err)
		}
		return ""
	})
	// Both "" and "/" are allowed to be used to refer to the root of the FS
	// for the purposes of cloning.
	checkClonedIsEquivalent(t, fs, "")
	checkClonedIsEquivalent(t, fs, "/")
}

// Test that the FS can be cloned and that the clone serializes identically.
func checkClonedIsEquivalent(t *testing.T, fs *MemFS, path string) {
	t.Helper()
	clone := NewMem()
	cloned, err := Clone(fs, clone, path, path)
	require.NoError(t, err)
	require.True(t, cloned)
	require.Equal(t, fs.String(), clone.String())
}

func TestMemFSBasics(t *testing.T) {
	runMemFSDataDriven(t, "testdata/memfs_basics", NewMem())
}

func TestMemFSList(t *testing.T) {
	runMemFSDataDriven(t, "testdata/memfs_list", NewMem())
}

func TestMemFSStrict(t *testing.T) {
	runMemFSDataDriven(t, "testdata/memfs_strict", NewStrictMem())
}

func TestMemFile(t *testing.T) {
	want := "foo"
	f := NewMemFile([]byte(want))
	buf, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if got := string(buf); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestMemFSLock(t *testing.T) {
	filesystems := map[string]FS{}
	fileLocks := map[string]io.Closer{}

	datadriven.RunTest(t, "testdata/memfs_lock", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "mkfs":
			for _, arg := range td.CmdArgs {
				filesystems[arg.String()] = NewMem()
			}
			return "OK"

		// lock fs=<filesystem-name> handle=<handle> path=<path>
		case "lock":
			var filesystemName string
			var path string
			var handle string
			td.ScanArgs(t, "fs", &filesystemName)
			td.ScanArgs(t, "path", &path)
			td.ScanArgs(t, "handle", &handle)
			fs := filesystems[filesystemName]
			if fs == nil {
				return fmt.Sprintf("filesystem %q doesn't exist", filesystemName)
			}
			l, err := fs.Lock(path)
			if err != nil {
				return err.Error()
			}
			fileLocks[handle] = l
			return "OK"

		// mkdirall fs=<filesystem-name> path=<path>
		case "mkdirall":
			var filesystemName string
			var path string
			td.ScanArgs(t, "fs", &filesystemName)
			td.ScanArgs(t, "path", &path)
			fs := filesystems[filesystemName]
			if fs == nil {
				return fmt.Sprintf("filesystem %q doesn't exist", filesystemName)
			}
			err := fs.MkdirAll(path, 0755)
			if err != nil {
				return err.Error()
			}
			return "OK"

		// close handle=<handle>
		case "close":
			var handle string
			td.ScanArgs(t, "handle", &handle)
			err := fileLocks[handle].Close()
			delete(fileLocks, handle)
			if err != nil {
				return err.Error()
			}
			return "OK"
		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}
