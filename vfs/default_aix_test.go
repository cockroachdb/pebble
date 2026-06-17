// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build aix

package vfs

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAIXDirSync verifies that syncing a directory handle succeeds on AIX.
// fsync(2) of a directory descriptor fails with EINVAL on AIX (see
// default_aix.go); aixDir must absorb that error so that Pebble's directory
// syncs, which it issues after creating or renaming files, do not fail.
//
// This test only runs on AIX: on other platforms fsync of a directory succeeds
// and the EINVAL branch is unreachable. It is also not exercised by CI, which
// has no AIX runner; it is intended to be run against an AIX test binary built
// with `go test -c`.
func TestAIXDirSync(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble-aix-dirsync")
	require.NoError(t, err)
	defer func() { _ = os.RemoveAll(dir) }()

	d, err := Default.OpenDir(dir)
	require.NoError(t, err)
	defer func() { _ = d.Close() }()

	require.NoError(t, d.Sync())
	require.NoError(t, d.SyncData())

	// SyncTo is a no-op for directories and always reports fullSync == false.
	fullSync, err := d.SyncTo(0)
	require.NoError(t, err)
	require.False(t, fullSync)
}
