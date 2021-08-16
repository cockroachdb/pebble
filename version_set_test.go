// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
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

func readFile(t testing.TB, fs vfs.FS, path string) []byte {
	f, err := fs.Open(path)
	require.NoError(t, err)
	b, err := ioutil.ReadAll(f)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return b
}

func writeFile(t testing.TB, fs vfs.FS, path string, data []byte) {
	f, err := fs.Create(path)
	require.NoError(t, err)
	_, err = f.Write(data)
	require.NoError(t, err)
	require.NoError(t, f.Close())
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
// underlying MemFS for storing the `CURRENT-XXXXXX` file.
func TestManifestRotation_NonAtomicEncrypted(t *testing.T) {
	mem := vfs.NewMem()
	fs := fauxEncryptedFS{mem}
	require.NoError(t, fs.MkdirAll("ext", 0755))
	opts := &Options{
		FS:                  fs,
		MaxManifestFileSize: 1,
		FormatMajorVersion:  FormatMostCompatible,
	}
	d, err := Open("", opts)
	require.NoError(t, err)
	writeAndIngest(t, fs, d, base.MakeInternalKey([]byte("a"), 0, InternalKeyKindSet), []byte("a"), "a")

	require.True(t, strings.HasPrefix(string(readFile(t, fs, "CURRENT")), "MANIFEST-"))
	require.True(t, strings.HasPrefix(string(readFile(t, mem, "CURRENT")), "NBOJGFTU."))

	// Perform the upgrade to the next format version.
	require.NoError(t, d.RatchetFormatMajorVersion(FormatCurrentVersioned))

	// The previous CURRENT file should now point a dummy nonexistent
	// manifest.
	require.Equal(t, string(readFile(t, fs, "CURRENT")), "MANIFEST-000000\n")

	// There should be a CURRENT-000002 file containing a sensicial
	// MANIFEST value. It should be legible directly through the mem FS,
	// without any decryption.
	require.True(t, strings.HasPrefix(string(readFile(t, mem, "CURRENT-000002")), "MANIFEST-"))

	writeAndIngest(t, fs, d, base.MakeInternalKey([]byte("b"), 0, InternalKeyKindSet), []byte("b"), "b")
	writeAndIngest(t, fs, d, base.MakeInternalKey([]byte("c"), 0, InternalKeyKindSet), []byte("c"), "c")
	require.True(t, strings.HasPrefix(string(readFile(t, mem, "CURRENT-000002")), "MANIFEST-"))

	// Reading the old-style `CURRENT` file through the encrypted FS
	// should still return the version tombstone.
	require.Equal(t, "MANIFEST-000000\n", string(readFile(t, fs, "CURRENT")))
	require.NoError(t, d.Close())
}
