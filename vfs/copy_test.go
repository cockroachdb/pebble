// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs_test

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
	"github.com/stretchr/testify/require"
)

var errCloseInjected = errors.New("injected close error")

// errCloseFS wraps an FS so that files created on it return an error from
// Close. errorfs deliberately does not inject on close.
type errCloseFS struct {
	vfs.FS
}

func (fs errCloseFS) Create(
	name string, category vfs.DiskWriteCategory,
) (vfs.File, error) {
	f, err := fs.FS.Create(name, category)
	if err != nil {
		return nil, err
	}
	return errCloseFile{File: f}, nil
}

type errCloseFile struct {
	vfs.File
}

func (f errCloseFile) Close() error {
	_ = f.File.Close()
	return errCloseInjected
}

// TestCopyDstCloseError verifies that CopyAcrossFS and LimitedCopy propagate
// an error returned by the destination file's Close.
func TestCopyDstCloseError(t *testing.T) {
	srcFS := vfs.NewMem()
	f, err := srcFS.Create("src", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	_, err = f.Write([]byte("hello"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	err = vfs.CopyAcrossFS(srcFS, "src", errCloseFS{vfs.NewMem()}, "dst")
	require.ErrorIs(t, err, errCloseInjected)

	mem := vfs.NewMem()
	f, err = mem.Create("src", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	_, err = f.Write([]byte("hello"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	err = vfs.LimitedCopy(errCloseFS{mem}, "src", "dst", 3)
	require.ErrorIs(t, err, errCloseInjected)
}

// TestCopyDstCloseOnError verifies that a failed copy still closes the
// destination file. MemFS' Windows semantics make Remove fail while a file
// is open, exposing a leaked descriptor.
func TestCopyDstCloseOnError(t *testing.T) {
	for _, tc := range []struct {
		name string
		copy func(fs vfs.FS) error
	}{
		{
			name: "CopyAcrossFS",
			copy: func(fs vfs.FS) error {
				return vfs.CopyAcrossFS(fs, "src", fs, "dst")
			},
		},
		{
			name: "LimitedCopy",
			copy: func(fs vfs.FS) error {
				return vfs.LimitedCopy(fs, "src", "dst", 3)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := vfs.NewMem()
			mem.UseWindowsSemantics(true)
			fs := errorfs.Wrap(mem, errorfs.InjectorFunc(func(op errorfs.Op) error {
				if op.Kind == errorfs.OpFileWrite && op.Path == "dst" {
					return errorfs.ErrInjected
				}
				return nil
			}))

			f, err := fs.Create("src", vfs.WriteCategoryUnspecified)
			require.NoError(t, err)
			_, err = f.Write([]byte("hello"))
			require.NoError(t, err)
			require.NoError(t, f.Close())

			err = tc.copy(fs)
			require.ErrorIs(t, err, errorfs.ErrInjected)

			// Fails if the copy error path left the destination file open.
			require.NoError(t, fs.Remove("dst"))
		})
	}
}
