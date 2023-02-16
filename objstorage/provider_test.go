// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorage

import (
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestNotExistError(t *testing.T) {
	// TODO(radu): test with shared objects.
	var log base.InMemLogger
	fs := vfs.WithLogging(vfs.NewMem(), log.Infof)
	provider, err := Open(DefaultSettings(fs, ""))
	require.NoError(t, err)

	require.True(t, IsNotExistError(provider.Remove(base.FileTypeTable, 1)))
	_, err = provider.OpenForReading(base.FileTypeTable, 1)
	require.True(t, IsNotExistError(err))

	w, _, err := provider.Create(base.FileTypeTable, 1)
	require.NoError(t, err)
	_, err = w.Write([]byte("foo"))
	require.NoError(t, err)
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())

	// Remove the underlying file.
	require.NoError(t, fs.Remove(base.MakeFilename(base.FileTypeTable, 1)))
	require.True(t, IsNotExistError(provider.Remove(base.FileTypeTable, 1)))
}
