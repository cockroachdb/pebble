// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// TestFileSetSampling is a smoke test of the fileSet sampling implementation.
// It verifies that a large file is usually chosen when sampling from a set of
// files with one large file and multiple small files.
func TestFileSetSampling(t *testing.T) {
	const (
		largeFileSize = 10000
		smallFileSize = 1
		numSmallFiles = 10
		iterations    = 10000
	)
	// The probability of choosing a small file should be ~0.001. Verify that it doesn't happen

	// Create an in-memory filesystem.
	memFS := vfs.NewMem()
	createWithSize := func(name string, size int) {
		f, err := memFS.Create(name, vfs.WriteCategoryUnspecified)
		require.NoError(t, err)
		n, err := f.Write(bytes.Repeat([]byte("a"), size))
		require.NoError(t, err)
		require.Equal(t, size, n)
		require.NoError(t, f.Close())
	}

	// Create a large file.
	largeFileName := fmt.Sprintf("%s.sst", base.DiskFileNum(123456))
	createWithSize(largeFileName, largeFileSize)
	for i := 0; i < numSmallFiles; i++ {
		createWithSize(fmt.Sprintf("%s.sst", base.DiskFileNum(i)), smallFileSize)
	}
	rng := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	smallFileChosen := 0
	for i := 0; i < iterations; i++ {
		dbStorage := newVFSStorage(fsWrapper{memFS}, "")
		fs, err := makeFileSet(dbStorage, rng)
		require.NoError(t, err)
		file := fs.Sample()
		if file != largeFileName {
			smallFileChosen++
		}
	}
	// We expect a small file to be chosen ~0.001 of the time. The probability of
	// observing this 10x more often than expected in 10,000 trials is negligible,
	// less than (e^90 / 10^10)^10 < 10^-60 by Chernoff bound.
	expected := float64(smallFileSize*numSmallFiles) / float64(smallFileSize*numSmallFiles+largeFileSize)
	actual := float64(smallFileChosen) / float64(iterations)
	require.Less(t, actual, expected*10)
}

// fsWrapper stubs out Stat and changes the ModTime so files appear older.
type fsWrapper struct {
	*vfs.MemFS
}

func (f fsWrapper) Stat(name string) (vfs.FileInfo, error) {
	info, err := f.MemFS.Stat(name)
	return fileInfoWrapper{info}, err
}

// fileInfoWrapper stubs out ModTime so the file appears older.
type fileInfoWrapper struct {
	vfs.FileInfo
}

func (f fileInfoWrapper) ModTime() time.Time {
	return time.Now().Add(-time.Hour)
}
