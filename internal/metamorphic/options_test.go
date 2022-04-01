// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestSetupInitialState(t *testing.T) {
	// Construct a small database in the test's TempDir.
	initialStatePath := t.TempDir()
	{
		d, err := pebble.Open(initialStatePath, &pebble.Options{})
		require.NoError(t, err)
		const maxKeyLen = 2
		ks := testkeys.Alpha(maxKeyLen)
		var key [maxKeyLen]byte
		for i := 0; i < ks.Count(); i++ {
			n := testkeys.WriteKey(key[:], ks, i)
			require.NoError(t, d.Set(key[:n], key[:n], pebble.NoSync))
			if i%100 == 0 {
				require.NoError(t, d.Flush())
			}
		}
		require.NoError(t, d.Close())
	}
	require.NoError(t, vfs.Default.MkdirAll(filepath.Join(initialStatePath, "wal"), os.ModePerm))
	ls, err := vfs.Default.List(initialStatePath)
	require.NoError(t, err)

	// setupInitialState with an initial state path set to the test's TempDir
	// should populate opts.opts.FS with the directory's contents.
	opts := &testOptions{
		opts:             defaultOptions(),
		initialStatePath: initialStatePath,
		initialStateDesc: "test",
	}
	require.NoError(t, setupInitialState("", opts))
	copied, err := opts.opts.FS.List("")
	require.NoError(t, err)
	require.ElementsMatch(t, ls, copied)
}
