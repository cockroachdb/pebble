// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorageprovider

import (
	"context"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

type pathJoinCountingFS struct {
	vfs.FS
	pathJoins int
}

func (f *pathJoinCountingFS) PathJoin(elem ...string) string {
	f.pathJoins++
	return f.FS.PathJoin(elem...)
}

func TestLocalObjectPath(t *testing.T) {
	ctx := context.Background()

	t.Run("created", func(t *testing.T) {
		mem := vfs.NewMem()
		fs := &pathJoinCountingFS{FS: mem}
		require.NoError(t, fs.MkdirAll("hot", 0755))
		require.NoError(t, fs.MkdirAll("cold", 0755))

		settings := DefaultSettings(fs, "hot")
		settings.Local.ColdTier.FS = fs
		settings.Local.ColdTier.FSDirName = "cold"
		p, err := Open(settings)
		require.NoError(t, err)
		defer func() { require.NoError(t, p.Close()) }()

		for _, tc := range []struct {
			fileType base.FileType
			fileNum  base.DiskFileNum
			tier     base.StorageTier
			dir      string
		}{
			{fileType: base.FileTypeTable, fileNum: 1, tier: base.HotTier, dir: "hot"},
			{fileType: base.FileTypeBlob, fileNum: 2, tier: base.ColdTier, dir: "cold"},
		} {
			w, meta, err := p.Create(ctx, tc.fileType, tc.fileNum, objstorage.CreateOptions{Tier: tc.tier})
			require.NoError(t, err)
			require.NoError(t, w.Finish())
			require.Equal(t, base.MakeFilepath(mem, tc.dir, tc.fileType, tc.fileNum), meta.Local.Path)

			fs.pathJoins = 0
			require.Equal(t, meta.Local.Path, p.Path(meta))
			_, err = p.Size(meta)
			require.NoError(t, err)
			r, err := p.OpenForReading(ctx, tc.fileType, tc.fileNum, objstorage.OpenOptions{})
			require.NoError(t, err)
			require.NoError(t, r.Close())
			require.NoError(t, p.Remove(tc.fileType, tc.fileNum))
			require.Zero(t, fs.pathJoins)
		}
	})

	t.Run("discovered", func(t *testing.T) {
		mem := vfs.NewMem()
		require.NoError(t, mem.MkdirAll("db", 0755))
		path := base.MakeFilepath(mem, "db", base.FileTypeTable, 1)
		file, err := mem.Create(path, vfs.WriteCategoryUnspecified)
		require.NoError(t, err)
		require.NoError(t, file.Close())

		fs := &pathJoinCountingFS{FS: mem}
		p, err := Open(DefaultSettings(fs, "db"))
		require.NoError(t, err)
		defer func() { require.NoError(t, p.Close()) }()

		meta, err := p.Lookup(base.FileTypeTable, 1)
		require.NoError(t, err)
		require.Equal(t, path, meta.Local.Path)

		fs.pathJoins = 0
		require.Equal(t, path, p.Path(meta))
		_, err = p.Size(meta)
		require.NoError(t, err)
		r, err := p.OpenForReading(ctx, base.FileTypeTable, 1, objstorage.OpenOptions{})
		require.NoError(t, err)
		require.NoError(t, r.Close())
		require.NoError(t, p.Remove(base.FileTypeTable, 1))
		require.Zero(t, fs.pathJoins)
	})

	t.Run("fallback", func(t *testing.T) {
		mem := vfs.NewMem()
		fs := &pathJoinCountingFS{FS: mem}
		require.NoError(t, fs.MkdirAll("db", 0755))
		p, err := Open(DefaultSettings(fs, "db"))
		require.NoError(t, err)
		defer func() { require.NoError(t, p.Close()) }()

		// reportCorruption passes bare metadata when a local object is missing.
		meta := objstorage.ObjectMetadata{
			DiskFileNum: 1,
			FileType:    base.FileTypeTable,
		}
		fs.pathJoins = 0
		require.Equal(t, base.MakeFilepath(mem, "db", base.FileTypeTable, 1), p.Path(meta))
		require.NotZero(t, fs.pathJoins)
	})
}
