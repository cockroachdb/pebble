// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build windows

package vfs

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestWindowsDirSyncFlushes exercises OpenDir → Sync → Close on a real
// directory that has just received a new file, the case the directory flush
// exists for: the directory entry of a newly created file only becomes
// durable when the parent directory handle is flushed. Before the change,
// Sync was an unconditional no-op on Windows; after it, Sync performs a real
// FlushFileBuffers on a dedicated GENERIC_WRITE|FILE_FLAG_BACKUP_SEMANTICS
// handle.
func TestWindowsDirSyncFlushes(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "created-just-before-sync"))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	d, err := Default.OpenDir(dir)
	require.NoError(t, err)
	defer d.Close()
	require.NoError(t, d.Sync())
}

// TestWindowsDirSyncPropagatesErrors pins the contract that Sync actually
// attempts the flush and reports failures. icacls denies write-data and
// append-data to Everyone on the directory (note: the coarse "(W)" mask also
// blocks read-only opens, so the narrower specific rights are used); the
// flush handle then fails to open with ERROR_ACCESS_DENIED and Sync must
// return that error. The pre-change no-op returned nil here unconditionally,
// so this test is red against master and green with the change.
func TestWindowsDirSyncPropagatesErrors(t *testing.T) {
	dir := t.TempDir()
	out, err := exec.Command("icacls", dir, "/deny", "*S-1-1-0:(WD,AD)").CombinedOutput()
	require.NoError(t, err, "icacls: %s", out)
	t.Cleanup(func() {
		_ = exec.Command("icacls", dir, "/reset").Run()
	})
	d, err := Default.OpenDir(dir)
	require.NoError(t, err)
	defer d.Close()
	err = d.Sync()
	require.Error(t, err, "Sync must report the flush failure, not silently succeed")
	t.Logf("Sync on a write-denied directory: %v", err)
}