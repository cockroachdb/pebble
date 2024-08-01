// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/atomicfs"
	"github.com/stretchr/testify/require"
)

func writeAndIngest(t *testing.T, mem vfs.FS, d *DB, k InternalKey, v []byte, filename string) {
	path := mem.PathJoin("ext", filename)
	f, err := mem.Create(path, vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	w := sstable.NewRawWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{})
	require.NoError(t, w.Add(k, v))
	require.NoError(t, w.Close())
	require.NoError(t, d.Ingest(context.Background(), []string{path}))
}

func TestVersionSet(t *testing.T) {
	opts := &Options{
		FS:       vfs.NewMem(),
		Comparer: base.DefaultComparer,
		Logger:   testLogger{t},
	}
	opts.EnsureDefaults()
	mu := &sync.Mutex{}
	marker, _, err := atomicfs.LocateMarker(opts.FS, "", manifestMarkerName)
	require.NoError(t, err)
	provider, err := objstorageprovider.Open(objstorageprovider.DefaultSettings(opts.FS, "" /* dirName */))
	require.NoError(t, err)
	var vs versionSet
	require.NoError(t, vs.create(
		0 /* jobID */, "" /* dirname */, provider, opts, marker,
		func() FormatMajorVersion { return FormatVirtualSSTables },
		mu,
	))
	vs.logSeqNum.Store(100)

	metas := make(map[base.FileNum]*manifest.FileMetadata)
	backings := make(map[base.DiskFileNum]*manifest.FileBacking)
	// When we parse VersionEdits, we get a new FileBacking each time. We need to
	// deduplicate them, since they hold a ref count.
	dedupBacking := func(b *manifest.FileBacking) *manifest.FileBacking {
		if existing, ok := backings[b.DiskFileNum]; ok {
			return existing
		}
		// The first time we see a backing, we also set a size.
		b.Size = uint64(b.DiskFileNum) * 1000
		backings[b.DiskFileNum] = b
		return b
	}

	refs := make(map[string]*version)
	datadriven.RunTest(t, "testdata/version_set", func(t *testing.T, td *datadriven.TestData) (retVal string) {
		// createFile only exists to prevent versionSet from complaining that a
		// file that is becoming a zombie does not exist.
		createFile := func(fileNum base.DiskFileNum) {
			w, _, err := provider.Create(context.Background(), fileTypeTable, fileNum, objstorage.CreateOptions{})
			require.NoError(t, err)
			require.NoError(t, w.Finish())
		}
		var buf strings.Builder

		switch td.Cmd {
		case "apply":
			ve, err := manifest.ParseVersionEditDebug(td.Input)
			if err != nil {
				td.Fatalf(t, "%v", err)
			}
			for _, nf := range ve.NewFiles {
				// Set a size that depends on FileNum.
				nf.Meta.Size = uint64(nf.Meta.FileNum) * 100
				nf.Meta.FileBacking = dedupBacking(nf.Meta.FileBacking)
				metas[nf.Meta.FileNum] = nf.Meta
				if !nf.Meta.Virtual {
					createFile(nf.Meta.FileBacking.DiskFileNum)
				}
			}
			for de := range ve.DeletedFiles {
				m := metas[de.FileNum]
				if m == nil {
					td.Fatalf(t, "unknown FileNum %s", de.FileNum)
				}
				ve.DeletedFiles[de] = m
			}
			for i := range ve.CreatedBackingTables {
				ve.CreatedBackingTables[i] = dedupBacking(ve.CreatedBackingTables[i])
				createFile(ve.CreatedBackingTables[i].DiskFileNum)
			}

			fileMetrics := newFileMetrics(ve.NewFiles)
			for de, f := range ve.DeletedFiles {
				lm := fileMetrics[de.Level]
				if lm == nil {
					lm = &LevelMetrics{}
					fileMetrics[de.Level] = lm
				}
				lm.NumFiles--
				lm.Size -= int64(f.Size)
			}

			mu.Lock()
			vs.logLock()

			forceRotation := rand.Intn(3) == 0
			err = vs.logAndApply(
				0 /* jobID */, ve, fileMetrics, forceRotation,
				func() []compactionInfo { return nil },
			)
			mu.Unlock()
			if err != nil {
				td.Fatalf(t, "%v", err)
			}
			// Show the edit, so that we can see the fields populated by Apply. We
			// zero out the next file number because it is not deterministic (because
			// of the randomized forceRotation).
			ve.NextFileNum = 0
			fmt.Fprintf(&buf, "applied:\n%s", ve.String())

		case "protect-backing":
			n, _ := strconv.Atoi(td.CmdArgs[0].String())
			vs.virtualBackings.Protect(base.DiskFileNum(n))

		case "unprotect-backing":
			n, _ := strconv.Atoi(td.CmdArgs[0].String())
			vs.virtualBackings.Unprotect(base.DiskFileNum(n))

		case "ref-version":
			name := td.CmdArgs[0].String()
			refs[name] = vs.currentVersion()
			refs[name].Ref()

		case "unref-version":
			name := td.CmdArgs[0].String()
			refs[name].Unref()

		case "reopen":
			var err error
			var filename string
			marker, filename, err = atomicfs.LocateMarker(opts.FS, "", manifestMarkerName)
			if err != nil {
				td.Fatalf(t, "error locating marker: %v", err)
			}
			_, manifestNum, ok := base.ParseFilename(opts.FS, filename)
			if !ok {
				td.Fatalf(t, "invalid manifest file name %q", filename)
			}
			vs = versionSet{}
			err = vs.load(
				"", provider, opts, manifestNum, marker,
				func() FormatMajorVersion { return FormatVirtualSSTables }, mu,
			)
			if err != nil {
				td.Fatalf(t, "error loading manifest: %v", err)
			}

			// Repopulate the maps.
			metas = make(map[base.FileNum]*manifest.FileMetadata)
			backings = make(map[base.DiskFileNum]*manifest.FileBacking)
			v := vs.currentVersion()
			for _, l := range v.Levels {
				it := l.Iter()
				for f := it.First(); f != nil; f = it.Next() {
					metas[f.FileNum] = f
					dedupBacking(f.FileBacking)
				}
			}

		default:
			td.Fatalf(t, "unknown command: %s", td.Cmd)
		}

		fmt.Fprintf(&buf, "current version:\n")
		for _, l := range strings.Split(vs.currentVersion().DebugString(), "\n") {
			if l != "" {
				fmt.Fprintf(&buf, "  %s\n", l)
			}
		}
		buf.WriteString(vs.virtualBackings.String())
		if len(vs.zombieTables) == 0 {
			buf.WriteString("no zombie tables\n")
		} else {
			var nums []base.DiskFileNum
			for k := range vs.zombieTables {
				nums = append(nums, k)
			}
			buf.WriteString("zombie tables:")
			slices.Sort(nums)
			for _, n := range nums {
				fmt.Fprintf(&buf, " %s", n)
			}
			buf.WriteString("\n")
		}

		if len(vs.obsoleteTables) == 0 {
			buf.WriteString("no obsolete tables\n")
		} else {
			buf.WriteString("obsolete tables:")
			for _, fi := range vs.obsoleteTables {
				fmt.Fprintf(&buf, " %s", fi.FileNum)
			}
			buf.WriteString("\n")
		}

		return buf.String()
	})
}

func TestVersionSetCheckpoint(t *testing.T) {
	mem := vfs.NewMem()
	require.NoError(t, mem.MkdirAll("ext", 0755))

	opts := &Options{
		FS:                  mem,
		MaxManifestFileSize: 1,
		Logger:              testLogger{t: t},
	}
	d, err := Open("", opts)
	require.NoError(t, err)

	// Multiple manifest files are created such that the latest one must have a correct snapshot
	// of the preceding state for the DB to be opened correctly and see the written data.
	// Snapshot has no files, so first edit will cause manifest rotation.
	writeAndIngest(t, mem, d, base.MakeInternalKey([]byte("a"), 0, InternalKeyKindSet), []byte("b"), "a")
	// Snapshot has no files, and manifest has an edit from the previous ingest,
	// so this second ingest will cause manifest rotation.
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
		Logger:              testLogger{t: t},
	}
	d, err := Open("", opts)
	require.NoError(t, err)

	// Snapshot has no files, so first edit will cause manifest rotation.
	writeAndIngest(t, mem, d, base.MakeInternalKey([]byte("a"), 0, InternalKeyKindSet), []byte("b"), "a")
	// Snapshot has no files, and manifest has an edit from the previous ingest,
	// so this second ingest will cause manifest rotation.
	writeAndIngest(t, mem, d, base.MakeInternalKey([]byte("c"), 0, InternalKeyKindSet), []byte("d"), "c")
	require.NoError(t, d.Close())
	d, err = Open("", opts)
	require.NoError(t, err)
	defer d.Close()
	d.TestOnlyWaitForCleaning()

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
	var lastSeqNum base.SeqNum
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
	// 2 ingestions happened, so LastSeqNum should equal base.SeqNumStart + 1.
	require.Equal(t, base.SeqNum(11), lastSeqNum)
	// logSeqNum is always one greater than the last assigned sequence number.
	require.Equal(t, d.mu.versions.logSeqNum.Load(), lastSeqNum+1)
}
