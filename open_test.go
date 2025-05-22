// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	randv1 "math/rand"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/metamorphic"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/atomicfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
	"github.com/cockroachdb/pebble/wal"
	"github.com/cockroachdb/redact"
	"github.com/ghemawat/stream"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

func TestOpenSharedTableCache(t *testing.T) {
	c := cache.New(cacheDefaultSize)
	tc := NewTableCache(c, 16, 100)
	defer tc.Unref()
	defer c.Unref()

	d0, err := Open("", testingRandomized(t, &Options{
		FS:         vfs.NewMem(),
		Cache:      c,
		TableCache: tc,
	}))
	if err != nil {
		t.Errorf("d0 Open: %s", err.Error())
	}
	defer d0.Close()

	d1, err := Open("", testingRandomized(t, &Options{
		FS:         vfs.NewMem(),
		Cache:      c,
		TableCache: tc,
	}))
	if err != nil {
		t.Errorf("d1 Open: %s", err.Error())
	}
	defer d1.Close()

	// Make sure that the Open function is using the passed in table cache
	// when the TableCache option is set.
	require.Equalf(
		t, d0.tableCache.tableCache, d1.tableCache.tableCache,
		"expected tableCache for both d0 and d1 to be the same",
	)
}

func TestErrorIfExists(t *testing.T) {
	opts := testingRandomized(t, &Options{
		FS:            vfs.NewMem(),
		ErrorIfExists: true,
	})
	defer ensureFilesClosed(t, opts)()

	d0, err := Open("", opts)
	require.NoError(t, err)
	require.NoError(t, d0.Close())

	if _, err := Open("", opts); !errors.Is(err, ErrDBAlreadyExists) {
		t.Fatalf("expected db-already-exists error, got %v", err)
	}

	opts.ErrorIfExists = false
	d1, err := Open("", opts)
	require.NoError(t, err)
	require.NoError(t, d1.Close())
}

func TestErrorIfNotExists(t *testing.T) {
	opts := testingRandomized(t, &Options{
		FS:               vfs.NewMem(),
		ErrorIfNotExists: true,
	})
	defer ensureFilesClosed(t, opts)()

	_, err := Open("", opts)
	if !errors.Is(err, ErrDBDoesNotExist) {
		t.Fatalf("expected db-does-not-exist error, got %v", err)
	}

	// Create the DB and try again.
	opts.ErrorIfNotExists = false
	d0, err := Open("", opts)
	require.NoError(t, err)
	require.NoError(t, d0.Close())

	opts.ErrorIfNotExists = true
	d1, err := Open("", opts)
	require.NoError(t, err)
	require.NoError(t, d1.Close())
}

func TestErrorIfNotPristine(t *testing.T) {
	opts := testingRandomized(t, &Options{
		FS:                 vfs.NewMem(),
		ErrorIfNotPristine: true,
	})
	defer ensureFilesClosed(t, opts)()

	d0, err := Open("", opts)
	require.NoError(t, err)
	require.NoError(t, d0.Close())

	// Store is pristine; ok to open.
	d1, err := Open("", opts)
	require.NoError(t, err)
	require.NoError(t, d1.Set([]byte("foo"), []byte("bar"), Sync))
	require.NoError(t, d1.Close())

	if _, err := Open("", opts); !errors.Is(err, ErrDBNotPristine) {
		t.Fatalf("expected db-not-pristine error, got %v", err)
	}

	// Run compaction and make sure we're still not allowed to open.
	opts.ErrorIfNotPristine = false
	d2, err := Open("", opts)
	require.NoError(t, err)
	require.NoError(t, d2.Compact([]byte("a"), []byte("z"), false /* parallelize */))
	require.NoError(t, d2.Close())

	opts.ErrorIfNotPristine = true
	if _, err := Open("", opts); !errors.Is(err, ErrDBNotPristine) {
		t.Fatalf("expected db-already-exists error, got %v", err)
	}
}

func TestOpen_WALFailover(t *testing.T) {
	filesystems := map[string]vfs.FS{}

	extractFSAndPath := func(cmdArg datadriven.CmdArg) (fs vfs.FS, dir string) {
		var ok bool
		if fs, ok = filesystems[cmdArg.Vals[0]]; !ok {
			fs = vfs.NewMem()
			filesystems[cmdArg.Vals[0]] = fs
		}
		dir = cmdArg.Vals[1]
		return fs, dir
	}

	getFSAndPath := func(td *datadriven.TestData, key string) (fs vfs.FS, dir string, ok bool) {
		cmdArg, ok := td.Arg(key)
		if !ok {
			return nil, "", false
		}
		fs, dir = extractFSAndPath(cmdArg)
		return fs, dir, ok
	}

	datadriven.RunTest(t, "testdata/open_wal_failover", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "grep-between":
			fs, path, ok := getFSAndPath(td, "path")
			if !ok {
				return "no `dir` provided"
			}
			var start, end string
			td.ScanArgs(t, "start", &start)
			td.ScanArgs(t, "end", &end)
			// Read the entirety of the file into memory (rather than passing
			// f into stream.ReadLines) to avoid a data race between closing the
			// file and stream.ReadLines asynchronously reading the file.
			f, err := fs.Open(path)
			if err != nil {
				return err.Error()
			}
			data, err := io.ReadAll(f)
			if err != nil {
				return err.Error()
			}
			require.NoError(t, f.Close())

			var buf bytes.Buffer
			if err := stream.Run(stream.Sequence(
				stream.ReadLines(bytes.NewReader(data)),
				streamFilterBetweenGrep(start, end),
				stream.WriteLines(&buf),
			)); err != nil {
				return err.Error()
			}
			return buf.String()
		case "list":
			fs, dir, ok := getFSAndPath(td, "path")
			if !ok {
				return "no `path` provided"
			}
			ls, err := fs.List(dir)
			if err != nil {
				return err.Error()
			}
			slices.Sort(ls)
			var buf bytes.Buffer
			for _, f := range ls {
				fmt.Fprintf(&buf, "  %s\n", f)
			}
			return buf.String()
		case "open":
			var dataDir string
			o := &Options{Logger: testLogger{t}}
			for _, cmdArg := range td.CmdArgs {
				switch cmdArg.Key {
				case "path":
					o.FS, dataDir = extractFSAndPath(cmdArg)
				case "secondary":
					fs, dir := extractFSAndPath(cmdArg)
					o.WALFailover = &WALFailoverOptions{
						Secondary: wal.Dir{FS: fs, Dirname: dir},
					}
				case "wal-recovery-dir":
					fs, dir := extractFSAndPath(cmdArg)
					o.WALRecoveryDirs = append(o.WALRecoveryDirs, wal.Dir{FS: fs, Dirname: dir})
				default:
					return fmt.Sprintf("unrecognized cmdArg %q", cmdArg.Key)
				}
			}
			if o.FS == nil {
				return "no path"
			}
			d, err := Open(dataDir, o)
			if err != nil {
				return err.Error()
			}
			require.NoError(t, d.Close())
			return "ok"
		case "stat":
			fs, path, ok := getFSAndPath(td, "path")
			if !ok {
				return "no `path` provided"
			}
			finfo, err := fs.Stat(path)
			if err != nil {
				return err.Error()
			}
			return fmt.Sprintf("IsDir: %t", finfo.IsDir())
		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}

func TestOpenAlreadyLocked(t *testing.T) {
	runTest := func(t *testing.T, lockPath, dirname string, fs vfs.FS) {
		opts := testingRandomized(t, &Options{FS: fs})
		var err error
		opts.Lock, err = LockDirectory(lockPath, fs)
		require.NoError(t, err)

		d, err := Open(dirname, opts)
		require.NoError(t, err)
		require.NoError(t, d.Set([]byte("foo"), []byte("bar"), Sync))

		// Try to open the same database reusing the Options containing the same
		// Lock. It should error when it observes that it's already referenced.
		_, err = Open(dirname, opts)
		require.Error(t, err)

		// Close the database.
		require.NoError(t, d.Close())

		// Now Opening should succeed again.
		d, err = Open(dirname, opts)
		require.NoError(t, err)
		require.NoError(t, d.Close())

		require.NoError(t, opts.Lock.Close())
		// There should be no more remaining references.
		require.Equal(t, int32(0), opts.Lock.refs.Load())
	}
	t.Run("memfs", func(t *testing.T) {
		runTest(t, "", "", vfs.NewMem())
	})
	t.Run("disk", func(t *testing.T) {
		t.Run("absolute", func(t *testing.T) {
			dir := t.TempDir()
			runTest(t, dir, dir, vfs.Default)
		})
		t.Run("relative", func(t *testing.T) {
			dir := t.TempDir()
			original, err := os.Getwd()
			require.NoError(t, err)
			defer func() { require.NoError(t, os.Chdir(original)) }()

			wd := filepath.Dir(dir)
			require.NoError(t, os.Chdir(wd))
			lockPath, err := filepath.Rel(wd, dir)
			require.NoError(t, err)
			runTest(t, lockPath, dir, vfs.Default)
		})
	})
}

func TestNewDBFilenames(t *testing.T) {
	versions := map[FormatMajorVersion][]string{
		internalFormatNewest: {
			"000002.log",
			"LOCK",
			"MANIFEST-000001",
			"OPTIONS-000003",
			"marker.format-version.000006.019",
			"marker.manifest.000001.MANIFEST-000001",
		},
	}

	for formatVers, want := range versions {
		t.Run(fmt.Sprintf("vers=%s", formatVers), func(t *testing.T) {
			mem := vfs.NewMem()
			fooBar := mem.PathJoin("foo", "bar")
			d, err := Open(fooBar, &Options{
				FS:                 mem,
				FormatMajorVersion: formatVers,
			})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			if err := d.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			got, err := mem.List(fooBar)
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			sort.Strings(got)
			if !reflect.DeepEqual(got, want) {
				t.Errorf("\ngot  %v\nwant %v", got, want)
			}
		})
	}
}

func testOpenCloseOpenClose(t *testing.T, fs vfs.FS, root string) {
	opts := testingRandomized(t, &Options{FS: fs})

	useStoreRelativeWALPath := rand.IntN(2) == 0
	for _, startFromEmpty := range []bool{false, true} {
		for _, walDirname := range []string{"", "wal"} {
			for _, length := range []int{-1, 0, 1, 1000, 10000, 100000} {
				dirname := "sharedDatabase" + walDirname
				if startFromEmpty {
					dirname = "startFromEmpty" + walDirname + strconv.Itoa(length)
				}
				dirname = fs.PathJoin(root, dirname)
				if walDirname == "" {
					opts.WALDir = ""
				} else {
					if useStoreRelativeWALPath {
						opts.WALDir = MakeStoreRelativePath(fs, walDirname)
					} else {
						opts.WALDir = fs.PathJoin(dirname, walDirname)
					}
				}

				got, xxx := []byte(nil), ""
				if length >= 0 {
					xxx = strings.Repeat("x", length)
				}

				d0, err := Open(dirname, opts)
				if err != nil {
					t.Fatalf("sfe=%t, length=%d: Open #0: %v",
						startFromEmpty, length, err)
					continue
				}
				if length >= 0 {
					err = d0.Set([]byte("key"), []byte(xxx), nil)
					if err != nil {
						t.Errorf("sfe=%t, length=%d: Set: %v",
							startFromEmpty, length, err)
						continue
					}
				}
				err = d0.Close()
				if err != nil {
					t.Errorf("sfe=%t, length=%d: Close #0: %v",
						startFromEmpty, length, err)
					continue
				}

				d1, err := Open(dirname, opts)
				if err != nil {
					t.Errorf("sfe=%t, length=%d: Open #1: %v",
						startFromEmpty, length, err)
					continue
				}
				if length >= 0 {
					var closer io.Closer
					got, closer, err = d1.Get([]byte("key"))
					if err != nil {
						t.Errorf("sfe=%t, length=%d: Get: %v",
							startFromEmpty, length, err)
						continue
					}
					got = append([]byte(nil), got...)
					closer.Close()
				}
				err = d1.Close()
				if err != nil {
					t.Errorf("sfe=%t, length=%d: Close #1: %v",
						startFromEmpty, length, err)
					continue
				}

				if length >= 0 && string(got) != xxx {
					t.Errorf("sfe=%t, length=%d: got value differs from set value",
						startFromEmpty, length)
					continue
				}

				{
					got, err := opts.FS.List(dirname)
					if err != nil {
						t.Fatalf("List: %v", err)
					}
					var optionsCount int
					for _, s := range got {
						if t, _, ok := base.ParseFilename(opts.FS, s); ok && t == fileTypeOptions {
							optionsCount++
						}
					}
					if optionsCount != 1 {
						t.Fatalf("expected 1 OPTIONS file, but found %d", optionsCount)
					}
				}
			}
		}
	}
}

func TestOpenCloseOpenClose(t *testing.T) {
	for _, fstype := range []string{"disk", "mem"} {
		t.Run(fstype, func(t *testing.T) {
			var fs vfs.FS
			var dir string
			switch fstype {
			case "disk":
				var err error
				dir, err = os.MkdirTemp("", "open-close")
				require.NoError(t, err)
				defer func() {
					_ = os.RemoveAll(dir)
				}()
				fs = vfs.Default
			case "mem":
				dir = ""
				fs = vfs.NewMem()
			}
			testOpenCloseOpenClose(t, fs, dir)
		})
	}
}

func TestOpenOptionsCheck(t *testing.T) {
	mem := vfs.NewMem()
	opts := &Options{FS: mem}

	d, err := Open("", opts)
	require.NoError(t, err)
	require.NoError(t, d.Close())

	fooCmp := *base.DefaultComparer
	fooCmp.Name = "foo"

	opts = &Options{
		Comparer: &fooCmp,
		FS:       mem,
	}
	_, err = Open("", opts)
	require.Regexp(t, `comparer name from file.*!=.*`, err)

	opts = &Options{
		Merger: &Merger{Name: "bar"},
		FS:     mem,
	}
	_, err = Open("", opts)
	require.Regexp(t, `merger name from file.*!=.*`, err)
}

func TestOpenCrashWritingOptions(t *testing.T) {
	memFS := vfs.NewMem()

	d, err := Open("", &Options{FS: memFS})
	require.NoError(t, err)
	require.NoError(t, d.Close())

	// Open the database again, this time with a mocked filesystem that
	// will only succeed in partially writing the OPTIONS file.
	fs := optionsTornWriteFS{FS: memFS}
	_, err = Open("", &Options{FS: fs, Logger: testLogger{t}})
	require.Error(t, err)

	// Re-opening the database must succeed.
	d, err = Open("", &Options{FS: memFS, Logger: testLogger{t}})
	require.NoError(t, err)
	require.NoError(t, d.Close())
}

type optionsTornWriteFS struct {
	vfs.FS
}

func (fs optionsTornWriteFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	file, err := fs.FS.Create(name, category)
	if file != nil {
		file = optionsTornWriteFile{File: file}
	}
	return file, err
}

type optionsTornWriteFile struct {
	vfs.File
}

func (f optionsTornWriteFile) Write(b []byte) (int, error) {
	// Look for the OPTIONS-XXXXXX file's `comparer=` field.
	comparerKey := []byte("comparer=")
	i := bytes.Index(b, comparerKey)
	if i == -1 {
		return f.File.Write(b)
	}
	// Write only the contents through `comparer=` and return an error.
	n, _ := f.File.Write(b[:i+len(comparerKey)])
	return n, syscall.EIO
}

func TestOpenReadOnly(t *testing.T) {
	mem := vfs.NewMem()

	{
		// Opening a non-existent DB in read-only mode should result in no mutable
		// filesystem operations.
		var memLog base.InMemLogger
		_, err := Open("non-existent", testingRandomized(t, &Options{
			FS:       vfs.WithLogging(mem, memLog.Infof),
			ReadOnly: true,
			WALDir:   "non-existent-waldir",
		}))
		if err == nil {
			t.Fatalf("expected error, but found success")
		}
		const expected = `open-dir: non-existent`
		if trimmed := strings.TrimSpace(memLog.String()); expected != trimmed {
			t.Fatalf("expected %q, but found %q", expected, trimmed)
		}
	}

	{
		// Opening a DB with a non-existent WAL dir in read-only mode should result
		// in no mutable filesystem operations other than the LOCK.
		var memLog base.InMemLogger
		_, err := Open("", testingRandomized(t, &Options{
			FS:       vfs.WithLogging(mem, memLog.Infof),
			ReadOnly: true,
			WALDir:   "non-existent-waldir",
		}))
		if err == nil {
			t.Fatalf("expected error, but found success")
		}
		const expected = "open-dir: \nopen-dir: non-existent-waldir\nclose:"
		if trimmed := strings.TrimSpace(memLog.String()); expected != trimmed {
			t.Fatalf("expected %q, but found %q", expected, trimmed)
		}
	}

	var contents []string
	{
		// Create a new DB and populate it with a small amount of data.
		d, err := Open("", testingRandomized(t, &Options{
			FS: mem,
		}))
		require.NoError(t, err)
		require.NoError(t, d.Set([]byte("test"), nil, nil))
		require.NoError(t, d.Close())
		contents, err = mem.List("")
		require.NoError(t, err)
		sort.Strings(contents)
	}

	{
		// Re-open the DB read-only. The directory contents should be unchanged.
		d, err := Open("", testingRandomized(t, &Options{
			FS:       mem,
			ReadOnly: true,
		}))
		require.NoError(t, err)

		// Verify various write operations fail in read-only mode.
		require.EqualValues(t, ErrReadOnly, d.Compact(nil, []byte("\xff"), false))
		require.EqualValues(t, ErrReadOnly, d.Flush())
		require.EqualValues(t, ErrReadOnly, func() error { _, err := d.AsyncFlush(); return err }())

		require.EqualValues(t, ErrReadOnly, d.Delete(nil, nil))
		require.EqualValues(t, ErrReadOnly, d.DeleteRange(nil, nil, nil))
		require.EqualValues(t, ErrReadOnly, d.Ingest(context.Background(), nil))
		require.EqualValues(t, ErrReadOnly, d.LogData(nil, nil))
		require.EqualValues(t, ErrReadOnly, d.Merge(nil, nil, nil))
		require.EqualValues(t, ErrReadOnly, d.Set(nil, nil, nil))

		// Verify we can still read in read-only mode.
		require.NoError(t, func() error {
			_, closer, err := d.Get([]byte("test"))
			if closer != nil {
				closer.Close()
			}
			return err
		}())

		checkIter := func(iter *Iterator, err error) {
			t.Helper()

			var keys []string
			for valid := iter.First(); valid; valid = iter.Next() {
				keys = append(keys, string(iter.Key()))
			}
			require.NoError(t, iter.Close())
			expectedKeys := []string{"test"}
			if diff := pretty.Diff(keys, expectedKeys); diff != nil {
				t.Fatalf("%s\n%s", strings.Join(diff, "\n"), keys)
			}
		}

		checkIter(d.NewIter(nil))

		b := d.NewIndexedBatch()
		checkIter(b.NewIter(nil))
		require.EqualValues(t, ErrReadOnly, b.Commit(nil))
		require.EqualValues(t, ErrReadOnly, d.Apply(b, nil))

		s := d.NewSnapshot()
		checkIter(s.NewIter(nil))
		require.NoError(t, s.Close())

		require.NoError(t, d.Close())

		newContents, err := mem.List("")
		require.NoError(t, err)

		sort.Strings(newContents)
		if diff := pretty.Diff(contents, newContents); diff != nil {
			t.Fatalf("%s", strings.Join(diff, "\n"))
		}
	}
}

func TestOpenWALReplay(t *testing.T) {
	largeValue := []byte(strings.Repeat("a", 100<<10))
	hugeValue := []byte(strings.Repeat("b", 10<<20))
	checkIter := func(iter *Iterator, err error) {
		t.Helper()

		var keys []string
		for valid := iter.First(); valid; valid = iter.Next() {
			keys = append(keys, string(iter.Key()))
		}
		require.NoError(t, iter.Close())
		expectedKeys := []string{"1", "2", "3", "4", "5"}
		if diff := pretty.Diff(keys, expectedKeys); diff != nil {
			t.Fatalf("%s\n%s", strings.Join(diff, "\n"), keys)
		}
	}

	for _, readOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("read-only=%t", readOnly), func(t *testing.T) {
			// Create a new DB and populate it with some data.
			const dir = ""
			mem := vfs.NewMem()
			d, err := Open(dir, testingRandomized(t, &Options{
				FS:           mem,
				MemTableSize: 32 << 20,
			}))
			require.NoError(t, err)
			// All these values will fit in a single memtable, so on closing the db there
			// will be no sst and all the data is in a single WAL.
			require.NoError(t, d.Set([]byte("1"), largeValue, nil))
			require.NoError(t, d.Set([]byte("2"), largeValue, nil))
			require.NoError(t, d.Set([]byte("3"), largeValue, nil))
			require.NoError(t, d.Set([]byte("4"), hugeValue, nil))
			require.NoError(t, d.Set([]byte("5"), largeValue, nil))
			checkIter(d.NewIter(nil))
			require.NoError(t, d.Close())
			files, err := mem.List(dir)
			require.NoError(t, err)
			sort.Strings(files)
			logCount, sstCount := 0, 0
			for _, fname := range files {
				if strings.HasSuffix(fname, ".sst") {
					sstCount++
				}
				if strings.HasSuffix(fname, ".log") {
					logCount++
				}
			}
			require.Equal(t, 0, sstCount)
			// The memtable size starts at 256KB and doubles up to 32MB. But,
			// we'll jump to a power of two large enough if a batch motivating a
			// rotation is larger than the next power of two. We expect 3 WALs
			// here.
			require.Equal(t, 3, logCount)

			// Re-open the DB with a smaller memtable. Values for 1, 2 will fit in the first memtable;
			// value for 3 will go in the next memtable; value for 4 will be in a flushable batch
			// which will cause the previous memtable to be flushed; value for 5 will go in the next
			// memtable
			d, err = Open(dir, testingRandomized(t, &Options{
				FS:           mem,
				MemTableSize: 300 << 10,
				ReadOnly:     readOnly,
			}))
			require.NoError(t, err)

			if readOnly {
				m := d.Metrics()
				require.Equal(t, int64(logCount), m.WAL.ObsoleteFiles)
				d.mu.Lock()
				require.NotNil(t, d.mu.mem.mutable)
				d.mu.Unlock()
			}
			checkIter(d.NewIter(nil))
			require.NoError(t, d.Close())
		})
	}
}

// Reproduction for https://github.com/cockroachdb/pebble/issues/2234.
func TestWALReplaySequenceNumBug(t *testing.T) {
	mem := vfs.NewMem()
	d, err := Open("", testingRandomized(t, &Options{
		FS: mem,
	}))
	require.NoError(t, err)

	d.mu.Lock()
	// Disable any flushes.
	d.mu.compact.flushing = true
	d.mu.Unlock()

	require.NoError(t, d.Set([]byte("1"), nil, nil))
	require.NoError(t, d.Set([]byte("2"), nil, nil))

	// Write a large batch. This should go to a separate memtable.
	largeValue := []byte(strings.Repeat("a", int(d.largeBatchThreshold)))
	require.NoError(t, d.Set([]byte("1"), largeValue, nil))

	// This write should go the mutable memtable after the large batch in the
	// memtable queue.
	d.Set([]byte("1"), nil, nil)

	d.mu.Lock()
	d.mu.compact.flushing = false
	d.mu.Unlock()

	// Make sure none of the flushables have been flushed.
	require.Equal(t, 3, len(d.mu.mem.queue))

	// Close the db. This doesn't cause a flush of the memtables, so they'll
	// have to be replayed when the db is reopened.
	require.NoError(t, d.Close())

	files, err := mem.List("")
	require.NoError(t, err)
	sort.Strings(files)
	sstCount := 0
	for _, fname := range files {
		if strings.HasSuffix(fname, ".sst") {
			sstCount++
		}
	}
	require.Equal(t, 0, sstCount)

	// Reopen db in read only mode to force read only wal replay.
	d, err = Open("", &Options{
		FS:       mem,
		ReadOnly: true,
		Logger:   testLogger{t},
	})
	require.NoError(t, err)
	val, c, _ := d.Get([]byte("1"))
	require.Equal(t, []byte{}, val)
	c.Close()
	require.NoError(t, d.Close())
}

// Similar to TestOpenWALReplay, except we test replay behavior after a
// memtable has been flushed. We test all 3 reasons for flushing: forced, size,
// and large-batch.
func TestOpenWALReplay2(t *testing.T) {
	for _, readOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("read-only=%t", readOnly), func(t *testing.T) {
			for _, reason := range []string{"forced", "size", "large-batch"} {
				t.Run(reason, func(t *testing.T) {
					mem := vfs.NewMem()
					d, err := Open("", testingRandomized(t, &Options{
						FS:           mem,
						MemTableSize: 256 << 10,
					}))
					require.NoError(t, err)

					switch reason {
					case "forced":
						require.NoError(t, d.Set([]byte("1"), nil, nil))
						require.NoError(t, d.Flush())
						require.NoError(t, d.Set([]byte("2"), nil, nil))
					case "size":
						largeValue := []byte(strings.Repeat("a", 100<<10))
						require.NoError(t, d.Set([]byte("1"), largeValue, nil))
						require.NoError(t, d.Set([]byte("2"), largeValue, nil))
						require.NoError(t, d.Set([]byte("3"), largeValue, nil))
					case "large-batch":
						largeValue := []byte(strings.Repeat("a", int(d.largeBatchThreshold)))
						require.NoError(t, d.Set([]byte("1"), nil, nil))
						require.NoError(t, d.Set([]byte("2"), largeValue, nil))
						require.NoError(t, d.Set([]byte("3"), nil, nil))
					}
					require.NoError(t, d.Close())

					files, err := mem.List("")
					require.NoError(t, err)
					sort.Strings(files)
					sstCount := 0
					for _, fname := range files {
						if strings.HasSuffix(fname, ".sst") {
							sstCount++
						}
					}
					require.Equal(t, 1, sstCount)

					// Re-open the DB with a smaller memtable. Values for 1, 2 will fit in the first memtable;
					// value for 3 will go in the next memtable; value for 4 will be in a flushable batch
					// which will cause the previous memtable to be flushed; value for 5 will go in the next
					// memtable
					d, err = Open("", testingRandomized(t, &Options{
						FS:           mem,
						MemTableSize: 300 << 10,
						ReadOnly:     readOnly,
					}))
					require.NoError(t, err)
					require.NoError(t, d.Close())
				})
			}
		})
	}
}

// TestTwoWALReplayCorrupt tests WAL-replay behavior when the first of the two
// WALs is corrupted with an sstable checksum error. Replay must stop at the
// first WAL because otherwise we may violate point-in-time recovery
// semantics. See #864.
func TestTwoWALReplayCorrupt(t *testing.T) {
	// Use the real filesystem so that we can seek and overwrite WAL data
	// easily.
	dir, err := os.MkdirTemp("", "wal-replay")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	d, err := Open(dir, testingRandomized(t, &Options{
		MemTableStopWritesThreshold: 4,
		MemTableSize:                2048,
	}))
	require.NoError(t, err)
	d.mu.Lock()
	d.mu.compact.flushing = true
	d.mu.Unlock()
	require.NoError(t, d.Set([]byte("1"), []byte(strings.Repeat("a", 1024)), nil))
	require.NoError(t, d.Set([]byte("2"), nil, nil))
	d.mu.Lock()
	d.mu.compact.flushing = false
	d.mu.Unlock()
	require.NoError(t, d.Close())

	// We should have two WALs.
	var logs []string
	ls, err := vfs.Default.List(dir)
	require.NoError(t, err)
	for _, name := range ls {
		if filepath.Ext(name) == ".log" {
			logs = append(logs, name)
		}
	}
	sort.Strings(logs)
	if len(logs) < 2 {
		t.Fatalf("expected at least two log files, found %d", len(logs))
	}

	// Corrupt the (n-1)th WAL by zeroing four bytes, 100 bytes from the end
	// of the file.
	f, err := os.OpenFile(filepath.Join(dir, logs[len(logs)-2]), os.O_RDWR, os.ModePerm)
	require.NoError(t, err)
	off, err := f.Seek(-100, 2)
	require.NoError(t, err)
	_, err = f.Write([]byte{0, 0, 0, 0})
	require.NoError(t, err)
	require.NoError(t, f.Close())
	t.Logf("zeored four bytes in %s at offset %d\n", logs[len(logs)-2], off)

	// Re-opening the database should detect and report the corruption.
	_, err = Open(dir, nil)
	require.Error(t, err, "pebble: corruption")
}

// TestCrashOpenCrashAfterWALCreation tests a database that exits
// ungracefully, begins recovery, creates the new WAL but promptly exits
// ungracefully again.
//
// This sequence has the potential to be problematic with the strict_wal_tail
// behavior because the first crash's WAL has an unclean tail. By the time the
// new WAL is created, the current manifest's MinUnflushedLogNum must be
// higher than the previous WAL.
func TestCrashOpenCrashAfterWALCreation(t *testing.T) {
	fs := vfs.NewCrashableMem()

	getLogs := func() (logs []string) {
		ls, err := fs.List("")
		require.NoError(t, err)
		for _, name := range ls {
			if filepath.Ext(name) == ".log" {
				logs = append(logs, name)
			}
		}
		return logs
	}

	d, err := Open("", testingRandomized(t, &Options{FS: fs}))
	require.NoError(t, err)
	require.NoError(t, d.Set([]byte("abc"), nil, Sync))

	// simulate a crash. This will leave the WAL
	// without an EOF trailer. It won't be an 'unclean tail' yet since the
	// log file was not recycled, but we'll fix that down below.
	crashFS := fs.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 0})
	require.NoError(t, d.Close())

	fs = crashFS

	// There should be one WAL.
	logs := getLogs()
	if len(logs) != 1 {
		t.Fatalf("expected one log file, found %d", len(logs))
	}

	// The one WAL file doesn't have an EOF trailer, but since it wasn't
	// recycled it won't have garbage at the end. Rewrite it so that it has
	// the same contents it currently has, followed by garbage.
	{
		f, err := fs.Open(logs[0])
		require.NoError(t, err)
		b, err := io.ReadAll(f)
		require.NoError(t, err)
		require.NoError(t, f.Close())
		f, err = fs.Create(logs[0], vfs.WriteCategoryUnspecified)
		require.NoError(t, err)
		_, err = f.Write(b)
		require.NoError(t, err)
		_, err = f.Write([]byte{0xde, 0xad, 0xbe, 0xef})
		require.NoError(t, err)
		require.NoError(t, f.Sync())
		require.NoError(t, f.Close())
		dir, err := fs.OpenDir("")
		require.NoError(t, err)
		require.NoError(t, dir.Sync())
		require.NoError(t, dir.Close())
	}

	// Open the database again (with syncs respected again). Wrap the
	// filesystem with an errorfs that will turn off syncs after a new .log
	// file is created and after a subsequent directory sync occurs. This
	// simulates a crash after the new log file is created and synced.
	crashFS = nil
	var crashFSAtomic atomic.Pointer[vfs.MemFS]
	{
		var walCreated, dirSynced atomic.Bool
		d, err := Open("", &Options{
			FS: errorfs.Wrap(fs, errorfs.InjectorFunc(func(op errorfs.Op) error {
				if dirSynced.Load() {
					crashFSAtomic.CompareAndSwap(nil, fs.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 0}))
				}
				if op.Kind == errorfs.OpCreate && filepath.Ext(op.Path) == ".log" {
					walCreated.Store(true)
				}
				// Record when there's a sync of the data directory after the
				// WAL was created. The data directory will have an empty
				// path because that's what we passed into Open.
				if op.Kind == errorfs.OpFileSync && op.Path == "" && walCreated.Load() {
					dirSynced.Store(true)
				}
				return nil
			})),
			Logger: testLogger{t},
		})
		require.NoError(t, err)
		require.NoError(t, d.Close())
	}

	require.NotNil(t, crashFSAtomic.Load())
	fs = crashFSAtomic.Load()

	newLogs := getLogs()
	if n := len(newLogs); n > 2 || n < 1 {
		t.Fatalf("expected one or two logs, found %d\n", n)
	} else if n == 1 {
		// On rare occasions, we can race between the cleaner cleaning away the old log
		// and d.Close(). If we only see one log, confirm that it has a higher
		// lognum than the previous log.
		origLogNum, err := strconv.Atoi(strings.Split(logs[0], ".")[0])
		require.NoError(t, err)
		curLogNum, err := strconv.Atoi(strings.Split(newLogs[0], ".")[0])
		require.NoError(t, err)
		require.Greater(t, curLogNum, origLogNum)
	}

	// Finally, open the database with syncs enabled.
	d, err = Open("", testingRandomized(t, &Options{FS: fs}))
	require.NoError(t, err)
	require.NoError(t, d.Close())
}

// TestOpenWALReplayReadOnlySeqNums tests opening a database:
//   - in read-only mode
//   - with multiple unflushed log files that must replayed
//   - a MANIFEST that sets the last sequence number to a number greater than
//     the unflushed log files
//
// See cockroachdb/cockroach#48660.
func TestOpenWALReplayReadOnlySeqNums(t *testing.T) {
	const root = ""
	mem := vfs.NewMem()

	copyFiles := func(srcDir, dstDir string) {
		files, err := mem.List(srcDir)
		require.NoError(t, err)
		for _, f := range files {
			require.NoError(t, vfs.Copy(mem, mem.PathJoin(srcDir, f), mem.PathJoin(dstDir, f)))
		}
	}

	// Create a new database under `/original` with a couple sstables.
	dir := mem.PathJoin(root, "original")
	d, err := Open(dir, testingRandomized(t, &Options{FS: mem}))
	require.NoError(t, err)
	require.NoError(t, d.Set([]byte("a"), nil, nil))
	require.NoError(t, d.Flush())
	require.NoError(t, d.Set([]byte("a"), nil, nil))
	require.NoError(t, d.Flush())

	// Prevent flushes so that multiple unflushed log files build up.
	d.mu.Lock()
	d.mu.compact.flushing = true
	d.mu.Unlock()

	require.NoError(t, d.Set([]byte("b"), nil, nil))
	d.AsyncFlush()
	require.NoError(t, d.Set([]byte("c"), nil, nil))
	d.AsyncFlush()
	require.NoError(t, d.Set([]byte("e"), nil, nil))

	// Manually compact some of the key space so that the latest `logSeqNum` is
	// written to the MANIFEST. This produces a MANIFEST where the `logSeqNum`
	// is greater than the sequence numbers contained in the
	// `minUnflushedLogNum` log file
	require.NoError(t, d.Compact([]byte("a"), []byte("a\x00"), false))
	d.mu.Lock()
	for d.mu.compact.compactingCount > 0 {
		d.mu.compact.cond.Wait()
	}
	d.mu.Unlock()

	d.TestOnlyWaitForCleaning()
	// While the MANIFEST is still in this state, copy all the files in the
	// database to a new directory.
	replayDir := mem.PathJoin(root, "replay")
	require.NoError(t, mem.MkdirAll(replayDir, os.ModePerm))
	copyFiles(dir, replayDir)

	d.mu.Lock()
	d.mu.compact.flushing = false
	d.mu.Unlock()
	require.NoError(t, d.Close())

	// Open the copy of the database in read-only mode. Since we copied all
	// the files before the flushes were allowed to complete, there should be
	// multiple unflushed log files that need to replay. Since the manual
	// compaction completed, the `logSeqNum` read from the manifest should be
	// greater than the unflushed log files' sequence numbers.
	d, err = Open(replayDir, testingRandomized(t, &Options{
		FS:       mem,
		ReadOnly: true,
	}))
	require.NoError(t, err)
	require.NoError(t, d.Close())
}

func TestOpenWALReplayMemtableGrowth(t *testing.T) {
	mem := vfs.NewMem()
	const memTableSize = 64 * 1024 * 1024
	opts := &Options{
		MemTableSize: memTableSize,
		FS:           mem,
	}
	opts.testingRandomized(t)
	func() {
		db, err := Open("", opts)
		require.NoError(t, err)
		defer db.Close()
		b := db.NewBatch()
		defer b.Close()
		key := make([]byte, 8)
		val := make([]byte, 16*1024*1024)
		b.Set(key, val, nil)
		require.NoError(t, db.Apply(b, Sync))
	}()
	db, err := Open("", opts)
	require.NoError(t, err)
	db.Close()
}

func TestPeek(t *testing.T) {
	// The file paths are UNIX-oriented. To avoid duplicating the test fixtures
	// just for Windows, just skip the tests on Windows.
	if runtime.GOOS == "windows" {
		t.Skip()
	}

	datadriven.RunTest(t, "testdata/peek", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "peek":
			desc, err := Peek(td.CmdArgs[0].String(), vfs.Default)
			if err != nil {
				return fmt.Sprintf("err=%v", err)
			}
			return desc.String()
		default:
			return fmt.Sprintf("unrecognized command %q\n", td.Cmd)
		}
	})
}

func TestGetVersion(t *testing.T) {
	mem := vfs.NewMem()
	opts := &Options{
		FS: mem,
	}
	opts.testingRandomized(t)

	// Case 1: No options file.
	version, err := GetVersion("", mem)
	require.NoError(t, err)
	require.Empty(t, version)

	// Case 2: Pebble created file.
	db, err := Open("", opts)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	version, err = GetVersion("", mem)
	require.NoError(t, err)
	require.Equal(t, "0.1", version)

	// Case 3: Manually created OPTIONS file with a higher number.
	highestOptionsNum := base.DiskFileNum(0)
	ls, err := mem.List("")
	require.NoError(t, err)
	for _, filename := range ls {
		ft, fn, ok := base.ParseFilename(mem, filename)
		if !ok {
			continue
		}
		switch ft {
		case fileTypeOptions:
			if fn > highestOptionsNum {
				highestOptionsNum = fn
			}
		}
	}
	f, _ := mem.Create(fmt.Sprintf("OPTIONS-%d", highestOptionsNum+1), vfs.WriteCategoryUnspecified)
	_, err = f.Write([]byte("[Version]\n  pebble_version=0.2\n"))
	require.NoError(t, err)
	err = f.Close()
	require.NoError(t, err)
	version, err = GetVersion("", mem)
	require.NoError(t, err)
	require.Equal(t, "0.2", version)

	// Case 4: Manually created OPTIONS file with a RocksDB number.
	f, _ = mem.Create(fmt.Sprintf("OPTIONS-%d", highestOptionsNum+2), vfs.WriteCategoryUnspecified)
	_, err = f.Write([]byte("[Version]\n  rocksdb_version=6.2.1\n"))
	require.NoError(t, err)
	err = f.Close()
	require.NoError(t, err)
	version, err = GetVersion("", mem)
	require.NoError(t, err)
	require.Equal(t, "rocksdb v6.2.1", version)
}

// TestOpenNeverFlushed verifies that we can open a database that had an
// ingestion but no other operations.
func TestOpenNeverFlushed(t *testing.T) {
	mem := vfs.NewMem()

	sstFile, err := mem.Create("to-ingest.sst", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)

	writerOpts := sstable.WriterOptions{}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(sstFile), writerOpts)
	for _, key := range []string{"a", "b", "c", "d"} {
		require.NoError(t, w.Set([]byte(key), []byte("val-"+key)))
	}
	require.NoError(t, w.Close())

	opts := &Options{
		FS:     mem,
		Logger: testLogger{t},
	}
	db, err := Open("", opts)
	require.NoError(t, err)
	require.NoError(t, db.Ingest(context.Background(), []string{"to-ingest.sst"}))
	require.NoError(t, db.Close())

	db, err = Open("", opts)
	require.NoError(t, err)

	val, closer, err := db.Get([]byte("b"))
	require.NoError(t, err)
	require.Equal(t, "val-b", string(val))
	require.NoError(t, closer.Close())

	require.NoError(t, db.Close())
}

func TestOpen_ErrorIfUnknownFormatVersion(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open("", &Options{
		FS:                 fs,
		FormatMajorVersion: FormatMinSupported,
	})
	require.NoError(t, err)
	require.NoError(t, d.Close())

	// Move the marker to a version that does not exist.
	m, _, err := atomicfs.LocateMarker(fs, "", formatVersionMarkerName)
	require.NoError(t, err)
	require.NoError(t, m.Move("999999"))
	require.NoError(t, m.Close())

	_, err = Open("", &Options{
		FS:                 fs,
		FormatMajorVersion: FormatMinSupported,
	})
	require.Error(t, err)
	require.EqualError(t, err, `pebble: database "" written in unknown format major version 999999`)
}

// ensureFilesClosed updates the provided Options to wrap the filesystem. It
// returns a closure that when invoked fails the test if any files opened by the
// filesystem are not closed.
//
// This function is intended to be used in tests with defer.
//
//	opts := &Options{FS: vfs.NewMem()}
//	defer ensureFilesClosed(t, opts)()
//	/* test code */
func ensureFilesClosed(t *testing.T, o *Options) func() {
	fs := &closeTrackingFS{
		FS:    o.FS,
		files: map[*closeTrackingFile]struct{}{},
	}
	o.FS = fs
	return func() {
		// fs.files should be empty if all the files were closed.
		for f := range fs.files {
			t.Errorf("An open file was never closed. Opened at:\n%s", f.stack)
		}
	}
}

type closeTrackingFS struct {
	vfs.FS
	files map[*closeTrackingFile]struct{}
}

func (fs *closeTrackingFS) wrap(file vfs.File, err error) (vfs.File, error) {
	if err != nil {
		return nil, err
	}
	f := &closeTrackingFile{
		File:  file,
		fs:    fs,
		stack: debug.Stack(),
	}
	fs.files[f] = struct{}{}
	return f, err
}

func (fs *closeTrackingFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	return fs.wrap(fs.FS.Create(name, category))
}

func (fs *closeTrackingFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	return fs.wrap(fs.FS.Open(name))
}

func (fs *closeTrackingFS) OpenDir(name string) (vfs.File, error) {
	return fs.wrap(fs.FS.OpenDir(name))
}

func (fs *closeTrackingFS) ReuseForWrite(
	oldname, newname string, category vfs.DiskWriteCategory,
) (vfs.File, error) {
	return fs.wrap(fs.FS.ReuseForWrite(oldname, newname, category))
}

type closeTrackingFile struct {
	vfs.File
	fs    *closeTrackingFS
	stack []byte
}

func (f *closeTrackingFile) Close() error {
	delete(f.fs.files, f)
	return f.File.Close()
}

func TestCheckConsistency(t *testing.T) {
	const dir = "./test"
	mem := vfs.NewMem()
	mem.MkdirAll(dir, 0755)

	provider, err := objstorageprovider.Open(objstorageprovider.DefaultSettings(mem, dir))
	require.NoError(t, err)
	defer provider.Close()

	parseMeta := func(s string) (*manifest.FileMetadata, error) {
		if len(s) == 0 {
			return nil, nil
		}
		parts := strings.Split(s, ":")
		if len(parts) != 2 {
			return nil, errors.Errorf("malformed table spec: %q", s)
		}
		fileNum, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			return nil, err
		}
		size, err := strconv.Atoi(strings.TrimSpace(parts[1]))
		if err != nil {
			return nil, err
		}
		m := &manifest.FileMetadata{
			FileNum: base.FileNum(fileNum),
			Size:    uint64(size),
		}
		m.InitPhysicalBacking()
		return m, nil
	}

	datadriven.RunTest(t, "testdata/version_check_consistency",
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "check-consistency":
				var filesByLevel [manifest.NumLevels][]*manifest.FileMetadata
				var files *[]*manifest.FileMetadata

				for _, data := range strings.Split(d.Input, "\n") {
					switch data {
					case "L0", "L1", "L2", "L3", "L4", "L5", "L6":
						level, err := strconv.Atoi(data[1:])
						if err != nil {
							return err.Error()
						}
						files = &filesByLevel[level]

					default:
						m, err := parseMeta(data)
						if err != nil {
							return err.Error()
						}
						if m != nil {
							*files = append(*files, m)
						}
					}
				}

				redactErr := false
				for _, arg := range d.CmdArgs {
					switch v := arg.String(); v {
					case "redact":
						redactErr = true
					default:
						return fmt.Sprintf("unknown argument: %q", v)
					}
				}

				v := manifest.NewVersion(base.DefaultComparer, 0, filesByLevel)
				err := checkConsistency(v, dir, provider)
				if err != nil {
					if redactErr {
						redacted := redact.Sprint(err).Redact()
						return string(redacted)
					}
					return err.Error()
				}
				return "OK"

			case "build":
				for _, data := range strings.Split(d.Input, "\n") {
					m, err := parseMeta(data)
					if err != nil {
						return err.Error()
					}
					path := base.MakeFilepath(mem, dir, base.FileTypeTable, m.FileBacking.DiskFileNum)
					_ = mem.Remove(path)
					f, err := mem.Create(path, vfs.WriteCategoryUnspecified)
					if err != nil {
						return err.Error()
					}
					_, err = f.Write(make([]byte, m.Size))
					if err != nil {
						return err.Error()
					}
					f.Close()
				}
				return ""

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func TestOpenRatchetsNextFileNum(t *testing.T) {
	mem := vfs.NewMem()
	memShared := remote.NewInMem()

	opts := &Options{FS: mem, Logger: testLogger{t}}
	opts.Experimental.CreateOnShared = remote.CreateOnSharedAll
	opts.Experimental.RemoteStorage = remote.MakeSimpleFactory(map[remote.Locator]remote.Storage{
		"": memShared,
	})
	d, err := Open("", opts)
	require.NoError(t, err)
	d.SetCreatorID(1)

	require.NoError(t, d.Set([]byte("foo"), []byte("value"), nil))
	require.NoError(t, d.Set([]byte("bar"), []byte("value"), nil))
	require.NoError(t, d.Flush())
	require.NoError(t, d.Compact([]byte("a"), []byte("z"), false))

	// Create a shared file with the newest file num and then close the db.
	d.mu.Lock()
	nextFileNum := d.mu.versions.getNextDiskFileNum()
	w, _, err := d.objProvider.Create(context.TODO(), fileTypeTable, nextFileNum, objstorage.CreateOptions{PreferSharedStorage: true})
	require.NoError(t, err)
	require.NoError(t, w.Write([]byte("foobar")))
	require.NoError(t, w.Finish())
	require.NoError(t, d.objProvider.Sync())
	d.mu.Unlock()

	// Write one key and then close the db. This write will stay in the memtable,
	// forcing the reopen to do a compaction on open.
	require.NoError(t, d.Set([]byte("foo1"), []byte("value"), nil))
	require.NoError(t, d.Close())

	// Reopen db. Compactions should happen without error.
	d, err = Open("", opts)
	require.NoError(t, err)
	require.NoError(t, d.Set([]byte("foo2"), []byte("value"), nil))
	require.NoError(t, d.Set([]byte("bar2"), []byte("value"), nil))
	require.NoError(t, d.Flush())
	require.NoError(t, d.Compact([]byte("a"), []byte("z"), false))

}

func TestMkdirAllAndSyncParents(t *testing.T) {
	if filepath.Separator != '/' {
		t.Skip("skipping due to path separator")
	}
	pwd, err := os.Getwd()
	require.NoError(t, err)
	defer func() { os.Chdir(pwd) }()

	filesystems := map[string]vfs.FS{}
	rootPaths := map[string]string{}
	var buf bytes.Buffer
	datadriven.RunTest(t, "testdata/mkdir_all_and_sync_parents", func(t *testing.T, td *datadriven.TestData) string {
		buf.Reset()
		switch td.Cmd {
		case "mkfs":
			var fsName string
			td.ScanArgs(t, "fs", &fsName)
			if td.HasArg("memfs") {
				filesystems[fsName] = vfs.NewMem()
				return "new memfs"
			}
			filesystems[fsName] = vfs.Default
			rootPaths[fsName] = t.TempDir()
			return "new default fs"
		case "mkdir-all-and-sync-parents":
			var fsName, path string
			td.ScanArgs(t, "fs", &fsName)
			td.ScanArgs(t, "path", &path)
			if p, ok := rootPaths[fsName]; ok {
				require.NoError(t, os.Chdir(p))
			}
			fs := vfs.WithLogging(filesystems[fsName], func(format string, args ...interface{}) {
				fmt.Fprintf(&buf, format+"\n", args...)
			})
			f, err := mkdirAllAndSyncParents(fs, path)
			if err != nil {
				return err.Error()
			}
			require.NoError(t, f.Close())
			return buf.String()
		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}

// TestWALFailoverRandomized is a randomzied test exercising recovery in the
// presence of WAL failover. It repeatedly opens a database, writes a number of
// batches concurrently and simulates a hard crash using vfs.NewCrashableMem. It
// ensures that the resulting DB state opens successfully, and the contents of
// the DB match the expectations based on the keys written.
//
// This test is partially a regression test for #3865.
func TestWALFailoverRandomized(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed %d", seed)
	mem := vfs.NewCrashableMem()
	makeOptions := func(mem *vfs.MemFS) *Options {
		failoverOpts := WALFailoverOptions{
			Secondary: wal.Dir{FS: mem, Dirname: "secondary"},
			FailoverOptions: wal.FailoverOptions{
				PrimaryDirProbeInterval:      time.Microsecond,
				HealthyProbeLatencyThreshold: 20 * time.Microsecond,
				HealthyInterval:              10 * time.Microsecond,
				UnhealthySamplingInterval:    time.Microsecond,
				UnhealthyOperationLatencyThreshold: func() (time.Duration, bool) {
					return 10 * time.Microsecond, true
				},
				ElevatedWriteStallThresholdLag: 50 * time.Microsecond,
			},
		}

		mean := time.Duration(rand.ExpFloat64() * float64(time.Microsecond))
		p := rand.Float64()
		t.Logf("Injecting mean %s of latency with p=%.3f", mean, p)
		fs := errorfs.Wrap(mem, errorfs.RandomLatency(errorfs.Randomly(p, seed), mean, seed, time.Second))
		return &Options{
			FS:                          fs,
			FormatMajorVersion:          internalFormatNewest,
			Logger:                      testLogger{t},
			MemTableSize:                128 << 10, // 128 KiB
			MemTableStopWritesThreshold: 4,
			WALFailover:                 &failoverOpts,
		}
	}

	// KV state tracking.
	//
	// This test uses all uint16 big-endian integers as a keyspace. Values are
	// randomly sized but always contain the key in the first two bytes. We
	// track the state of all KVs throughout the test (whether they're
	// definitely set, maybe set or definitely unset).
	//
	// Note that the test may wrap around to the beginning of the keyspace. This
	// may cause KVs left at kvMaybeSet to be written and be definitively set
	// the second time around.
	type kvState int8
	const (
		kvUnset    kvState = 0
		kvMaybeSet kvState = 1
		kvSet      kvState = 2
	)
	const keyspaceSize = math.MaxUint16 + 1
	var kvs struct {
		sync.Mutex
		states   [keyspaceSize]kvState
		count    uint64 // [0, math.MaxUint16]; INVARIANT: states[count:] all zeroes
		crashing bool
	}
	setIsCrashing := func(crashing bool) {
		kvs.Lock()
		defer kvs.Unlock()
		kvs.crashing = crashing
	}
	// transitionState is called by goroutines responsible for committing
	// batches to the engine. Note that 'i' is the index of the KV before
	// wrapping around and needs to be modded by math.MaxUint16.
	transitionState := func(i, count uint64, state kvState) {
		kvs.Lock()
		defer kvs.Unlock()
		if kvs.crashing && state == kvSet {
			// We're racing with a CrashClone call and it's indeterminate
			// whether what we think we synced actually made the cut. Leave the
			// kvs at the kvMaybeSet.
			state = kvMaybeSet
		}
		for j := uint64(0); j < count; j++ {
			idx := (i + j) % keyspaceSize
			kvs.states[idx] = max(kvs.states[idx], state)
		}
		kvs.count = max(kvs.count, i+count, math.MaxUint16)
	}
	// validateState is called on recovery to ensure that engine state agrees
	// with the tracked KV state.
	validateState := func(d *DB) {
		it, err := d.NewIter(nil)
		require.NoError(t, err)
		valid := it.First()
		for i := 0; i < int(kvs.count); i++ {
			var kvIsSet bool
			if valid {
				require.Len(t, it.Key(), 2)
				require.Equal(t, it.Key(), it.Value()[:2])
				kvIsSet = binary.BigEndian.Uint16(it.Key()) == uint16(i)
			}
			if kvIsSet && kvs.states[i] == kvUnset {
				t.Fatalf("key %04x is set; state says it should be unset", i)
			} else if !kvIsSet && kvs.states[i] == kvSet {
				t.Fatalf("key %04x is unset; state says it should be set", i)
			}
			if kvIsSet {
				valid = it.Next()
			}
		}
		require.NoError(t, it.Close())
	}

	d, err := Open("primary", makeOptions(mem))
	require.NoError(t, err)
	rng := rand.New(rand.NewPCG(0, uint64(seed)))
	var wg sync.WaitGroup
	var n uint64
	randomOps := metamorphic.Weighted[func()]{
		{Weight: 1, Item: func() {
			time.Sleep(time.Microsecond * time.Duration(rand.IntN(30)))
			t.Log("initiating hard crash")
			setIsCrashing(true)
			// Take a crash-consistent clone of the filesystem and use that going forward.
			mem = mem.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 50, RNG: rng})
			wg.Wait() // Wait for outstanding batch commits to finish.
			_ = d.Close()
			d, err = Open("primary", makeOptions(mem))
			require.NoError(t, err)
			validateState(d)
			setIsCrashing(false)
		}},
		{Weight: 20, Item: func() {
			count := rng.IntN(14) + 1
			var k [2]byte
			var v [4096]byte
			b := d.NewBatch()
			for i := 0; i < count; i++ {
				j := uint16((n + uint64(i)) % keyspaceSize)
				binary.BigEndian.PutUint16(k[:], j)
				vn := max(rng.IntN(cap(v)), 2)
				binary.BigEndian.PutUint16(v[:], j)
				require.NoError(t, b.Set(k[:], v[:vn], nil))
			}
			maybeSync := NoSync
			if rng.IntN(2) == 1 {
				maybeSync = Sync
			}
			wg.Add(1)
			go func(n, count uint64) {
				defer wg.Done()
				transitionState(n, count, kvMaybeSet)
				require.NoError(t, b.Commit(maybeSync))
				if maybeSync == Sync {
					transitionState(n, count, kvSet)
				}
			}(n, uint64(count))
			n += uint64(count)
		}},
	}
	nextRandomOp := randomOps.RandomDeck(randv1.New(randv1.NewSource(rng.Int64())))
	for o := 0; o < 1000; o++ {
		nextRandomOp()()
	}
}
