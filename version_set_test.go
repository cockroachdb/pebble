// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func writeAndIngest(t *testing.T, mem vfs.FS, d *DB, k InternalKey, v []byte, filename string) {
	path := mem.PathJoin("ext", filename)
	f, err := mem.Create(path)
	require.NoError(t, err)
	w := sstable.NewWriter(f, sstable.WriterOptions{})
	require.NoError(t, w.Add(k, v))
	require.NoError(t, w.Close())
	require.NoError(t, d.Ingest([]string{path}))
}

func TestVersionSetCheckpoint(t *testing.T) {
	mem := vfs.NewMem()
	require.NoError(t, mem.MkdirAll("ext", 0755))

	opts := &Options{
		FS:                  mem,
		MaxManifestFileSize: 1,
	}
	d, err := Open("", opts)
	require.NoError(t, err)

	// Multiple manifest files are created such that the latest one must have a correct snapshot
	// of the preceding state for the DB to be opened correctly and see the written data.
	writeAndIngest(t, mem, d, base.MakeInternalKey([]byte("a"), 0, InternalKeyKindSet), []byte("b"), "a")
	writeAndIngest(t, mem, d, base.MakeInternalKey([]byte("c"), 0, InternalKeyKindSet), []byte("d"), "c")
	require.NoError(t, d.Close())
	d, err = Open("", opts)
	require.NoError(t, err)
	checkValue := func(k string, expected string) {
		v, closer, err := d.Get([]byte(k))
		require.NoError(t, err)
		require.Equal(t, expected, string(v))
		closer.Close()
	}
	checkValue("a", "b")
	checkValue("c", "d")
	require.NoError(t, d.Close())
}

func TestVersionSetSeqNums(t *testing.T) {
	mem := vfs.NewMem()
	require.NoError(t, mem.MkdirAll("ext", 0755))

	opts := &Options{
		FS:                  mem,
		MaxManifestFileSize: 1,
	}
	d, err := Open("", opts)
	require.NoError(t, err)

	writeAndIngest(t, mem, d, base.MakeInternalKey([]byte("a"), 0, InternalKeyKindSet), []byte("b"), "a")
	writeAndIngest(t, mem, d, base.MakeInternalKey([]byte("c"), 0, InternalKeyKindSet), []byte("d"), "c")
	require.NoError(t, d.Close())
	d, err = Open("", opts)
	require.NoError(t, err)
	defer d.Close()

	// Check that the manifest has the correct LastSeqNum, equalling the highest
	// observed SeqNum.
	filenames, err := mem.List("")
	require.NoError(t, err)
	var manifest vfs.File
	for _, filename := range filenames {
		fileType, _, ok := base.ParseFilename(mem, filename)
		if ok && fileType == fileTypeManifest {
			manifest, err = mem.Open(filename)
			require.NoError(t, err)
		}
	}
	require.NotNil(t, manifest)
	defer manifest.Close()
	rr := record.NewReader(manifest, 0 /* logNum */)
	lastSeqNum := uint64(0)
	for {
		r, err := rr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		var ve versionEdit
		err = ve.Decode(r)
		require.NoError(t, err)
		if ve.LastSeqNum != 0 {
			lastSeqNum = ve.LastSeqNum
		}
	}
	// 2 ingestions happened, so LastSeqNum should equal 2.
	require.Equal(t, uint64(2), lastSeqNum)
	// logSeqNum is always one greater than the last assigned sequence number.
	require.Equal(t, d.mu.versions.atomic.logSeqNum, lastSeqNum+1)
}

// TestCurrentV2_NewestManifest tests recovery when one of the `CURRENT`
// or `CURRENT-v2` files are out-of-date. In practice either file may be
// out-of-date. `CURRENT-v2` may be out of date if a previous version of
// Pebble last opened the database. `CURRENT` may be out of date if the
// previous process crashed after updating `CURRENT-v2` but before
// updating `CURRENT`.
func TestCurrentV2_NewestManifest(t *testing.T) {
	for _, currentFileToRevert := range []string{"CURRENT", "CURRENT-v2"} {
		t.Run(currentFileToRevert, func(t *testing.T) {
			mem := vfs.NewMem()
			require.NoError(t, mem.MkdirAll("ext", 0755))
			opts := &Options{
				FS:                  mem,
				MaxManifestFileSize: 1,
			}
			d, err := Open("", opts)
			require.NoError(t, err)
			writeAndIngest(t, mem, d, base.MakeInternalKey([]byte("a"), 0, InternalKeyKindSet), []byte("a"), "a")
			writeAndIngest(t, mem, d, base.MakeInternalKey([]byte("b"), 0, InternalKeyKindSet), []byte("b"), "b")

			// Because of the low MaxManifestFileSize, there should've been a
			// few manifest rotations.
			oldCurrent := readFile(t, mem, currentFileToRevert)

			// Another ingest should produce another rotation.
			writeAndIngest(t, mem, d, base.MakeInternalKey([]byte("c"), 0, InternalKeyKindSet), []byte("c"), "c")

			if newCurrent := readFile(t, mem, currentFileToRevert); bytes.Equal(oldCurrent, newCurrent) {
				t.Fatalf("%q file unexpectedly unchanged", currentFileToRevert)
			}
			require.NoError(t, d.Close())

			// Revert one of the current files to the previous state.
			writeFile(t, mem, currentFileToRevert, oldCurrent)

			// As long as one of the CURRENT files is in the updated
			// state, Open should restore the correct manifest.
			d, err = Open("", opts)
			require.NoError(t, err)
			v, closer, err := d.Get([]byte("c"))
			require.NoError(t, err)
			require.Equal(t, []byte("c"), v)
			require.NoError(t, closer.Close())
			require.NoError(t, d.Close())
		})
	}
}

func writeFile(t testing.TB, fs vfs.FS, path string, data []byte) {
	f, err := fs.Create(path)
	require.NoError(t, err)
	_, err = f.Write(data)
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

func readFile(t testing.TB, fs vfs.FS, path string) []byte {
	f, err := fs.Open(path)
	require.NoError(t, err)
	b, err := ioutil.ReadAll(f)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return b
}

type fauxEncryptedFS struct {
	vfs.FS
}

func (fs fauxEncryptedFS) Create(path string) (vfs.File, error) {
	f, err := fs.FS.Create(path)
	if err != nil {
		return nil, err
	}
	return fauxEncryptedFile{f}, nil
}

func (fs fauxEncryptedFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.FS.Open(name, opts...)
	if err != nil {
		return nil, err
	}
	return fauxEncryptedFile{f}, nil
}

func (fs fauxEncryptedFS) Attributes() vfs.Attributes {
	attrs := fs.FS.Attributes()
	attrs.Description = fmt.Sprintf("faux-encrypted( %s )", attrs.Description)
	attrs.RenameIsAtomic = false
	return attrs
}

func (fs fauxEncryptedFS) Unwrap() vfs.FS {
	return fs.FS
}

type fauxEncryptedFile struct {
	vfs.File
}

func (f fauxEncryptedFile) Write(b []byte) (int, error) {
	for i := range b {
		b[i] = b[i] + 1
	}
	return f.File.Write(b)
}

func (f fauxEncryptedFile) Read(b []byte) (int, error) {
	n, err := f.File.Read(b)
	for i := 0; i < n; i++ {
		b[i] = b[i] - 1
	}
	return n, err
}

func (f fauxEncryptedFile) ReadAt(p []byte, off int64) (int, error) {
	n, err := f.File.ReadAt(p, off)
	for i := 0; i < n; i++ {
		p[i] = p[i] - 1
	}
	return n, err
}

// TestManifestRotation_NonAtomicEncrypted tests a VFS similar to
// CockroachDB's encryption-at-rest VFS. The faux encryption-at-rest VFS
// used here increments each byte once and does not provide atomic
// renames. Pebble should unwrap the encryption-at-rest VFS and use the
// underlying MemFS for storing the `CURRENT-v2` file.
func TestManifestRotation_NonAtomicEncrypted(t *testing.T) {
	mem := vfs.NewMem()
	fs := fauxEncryptedFS{mem}
	require.NoError(t, fs.MkdirAll("ext", 0755))
	opts := &Options{
		FS:                  fs,
		MaxManifestFileSize: 1,
	}
	d, err := Open("", opts)
	require.NoError(t, err)
	writeAndIngest(t, fs, d, base.MakeInternalKey([]byte("a"), 0, InternalKeyKindSet), []byte("a"), "a")
	writeAndIngest(t, fs, d, base.MakeInternalKey([]byte("b"), 0, InternalKeyKindSet), []byte("b"), "b")
	writeAndIngest(t, fs, d, base.MakeInternalKey([]byte("c"), 0, InternalKeyKindSet), []byte("c"), "c")
	current := string(readFile(t, mem, "CURRENT"))
	currentV2 := string(readFile(t, mem, "CURRENT-v2"))
	require.NotEqual(t, current, currentV2)
	require.True(t, strings.HasPrefix(currentV2, "MANIFEST-"))
	require.False(t, strings.HasPrefix(current, "MANIFEST-"))
	require.NoError(t, d.Close())
}
