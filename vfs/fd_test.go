// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFileWrappersHaveFd(t *testing.T) {
	// Use the real filesystem so that we can test vfs.Default, which returns
	// files with Fd().
	tmpf, err := os.CreateTemp("", "pebble-db-fd-file")
	require.NoError(t, err)
	filename := tmpf.Name()
	defer os.Remove(filename)

	// File wrapper case 1: Check if diskHealthCheckingFile has Fd().
	fs2, closer := WithDiskHealthChecks(Default, 10*time.Second,
		func(s string, duration time.Duration) {})
	defer closer.Close()
	f2, err := fs2.Open(filename)
	require.NoError(t, err)
	if _, ok := f2.(fdGetter); !ok {
		t.Fatal("expected diskHealthCheckingFile to export Fd() method")
	}
	// File wrapper case 2: Check if syncingFile has Fd().
	f3 := NewSyncingFile(f2, SyncingFileOptions{BytesPerSync: 8 << 10 /* 8 KB */})
	if _, ok := f3.(fdGetter); !ok {
		t.Fatal("expected syncingFile to export Fd() method")
	}
	require.NoError(t, f2.Close())
}
