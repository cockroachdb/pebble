// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

import (
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFileWrappersHaveFd(t *testing.T) {
	// Use the real filesystem so that we can test vfs.Default.
	dir, err := ioutil.TempDir("", "wal-replay")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// Skip test if vfs.Default does not return files with Fd() for some reason.
	filePath := path.Join(dir, "test-1")
	f, err := os.Create(filePath)
	require.NoError(t, err)
	if _, err := f.WriteString("test"); err != nil {
		t.Fatal(err)
	}
	require.NoError(t, f.Close())

	// File wrapper case 1: Check if diskHealthCheckingFile has Fd().
	fs2 := WithDiskHealthChecks(Default, 10 * time.Second, func(s string, duration time.Duration) {})
	f2, err := fs2.Open(filePath)
	require.NoError(t, err)
	if _, ok := f2.(fileWithFd); !ok {
		t.Fatal("expected diskHealthCheckingFile to export Fd() method")
	}
	// File wrapper case 2: Check if syncingFile has Fd().
	f3 := NewSyncingFile(f2, SyncingFileOptions{BytesPerSync: 8 << 10 /* 8 KB */})
	if _, ok := f3.(fileWithFd); !ok {
		t.Fatal("expected syncingFile to export Fd() method")
	}
	require.NoError(t, f3.Close())
}
