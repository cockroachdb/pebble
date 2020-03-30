// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

func TestErrorIfExists(t *testing.T) {
	for _, b := range [...]bool{false, true} {
		mem := vfs.NewMem()
		d0, err := Open("", &Options{
			FS: mem,
		})
		if err != nil {
			t.Errorf("b=%v: d0 Open: %v", b, err)
			continue
		}
		if err := d0.Close(); err != nil {
			t.Errorf("b=%v: d0 Close: %v", b, err)
			continue
		}

		d1, err := Open("", &Options{
			FS:            mem,
			ErrorIfExists: b,
		})
		if d1 != nil {
			defer d1.Close()
		}
		if got := err != nil; got != b {
			t.Errorf("b=%v: d1 Open: err is %v, got (err != nil) is %v, want %v", b, err, got, b)
			continue
		}
	}
}

func TestErrorIfNotExists(t *testing.T) {
	mem := vfs.NewMem()
	_, err := Open("", &Options{
		FS:               mem,
		ErrorIfNotExists: true,
	})
	if err == nil {
		t.Fatalf("expected error, but found success")
	} else if !strings.HasSuffix(err.Error(), os.ErrNotExist.Error()) {
		t.Fatalf("expected not exists, but found %q", err)
	}

	// Create the DB and try again.
	d, err := Open("", &Options{
		FS:               mem,
		ErrorIfNotExists: false,
	})
	require.NoError(t, err)
	require.NoError(t, d.Close())

	// The DB exists, so the setting of ErrorIfNotExists is a no-op.
	d, err = Open("", &Options{
		FS:               mem,
		ErrorIfNotExists: true,
	})
	require.NoError(t, err)
	require.NoError(t, d.Close())
}

func TestNewDBFilenames(t *testing.T) {
	mem := vfs.NewMem()
	fooBar := mem.PathJoin("foo", "bar")
	d, err := Open(fooBar, &Options{
		FS: mem,
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
	want := []string{
		"000002.log",
		"CURRENT",
		"LOCK",
		"MANIFEST-000001",
		"OPTIONS-000003",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("\ngot  %v\nwant %v", got, want)
	}
}

func testOpenCloseOpenClose(t *testing.T, fs vfs.FS, root string) {
	opts := &Options{
		FS: fs,
	}

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
					opts.WALDir = fs.PathJoin(dirname, walDirname)
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
				dir, err = ioutil.TempDir("", "open-close")
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

	opts = &Options{
		Comparer: &Comparer{Name: "foo"},
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

func TestOpenReadOnly(t *testing.T) {
	mem := vfs.NewMem()

	{
		// Opening a non-existent DB in read-only mode should result in no mutable
		// filesystem operations.
		var buf syncedBuffer
		_, err := Open("non-existent", &Options{
			FS:       loggingFS{mem, &buf},
			ReadOnly: true,
			WALDir:   "non-existent-waldir",
		})
		if err == nil {
			t.Fatalf("expected error, but found success")
		}
		const expected = `open-dir: non-existent`
		if trimmed := strings.TrimSpace(buf.String()); expected != trimmed {
			t.Fatalf("expected %q, but found %q", expected, trimmed)
		}
	}

	{
		// Opening a DB with a non-existent WAL dir in read-only mode should result
		// in no mutable filesystem operations other than the LOCK.
		var buf syncedBuffer
		_, err := Open("", &Options{
			FS:       loggingFS{mem, &buf},
			ReadOnly: true,
			WALDir:   "non-existent-waldir",
		})
		if err == nil {
			t.Fatalf("expected error, but found success")
		}
		const expected = "open-dir: \nopen-dir: non-existent-waldir"
		if trimmed := strings.TrimSpace(buf.String()); expected != trimmed {
			t.Fatalf("expected %q, but found %q", expected, trimmed)
		}
	}

	var contents []string
	{
		// Create a new DB and populate it with a small amount of data.
		d, err := Open("", &Options{
			FS: mem,
		})
		require.NoError(t, err)
		require.NoError(t, d.Set([]byte("test"), nil, nil))
		require.NoError(t, d.Close())
		contents, err = mem.List("")
		require.NoError(t, err)
		sort.Strings(contents)
	}

	{
		// Re-open the DB read-only. The directory contents should be unchanged.
		d, err := Open("", &Options{
			FS:       mem,
			ReadOnly: true,
		})
		require.NoError(t, err)

		// Verify various write operations fail in read-only mode.
		require.EqualValues(t, ErrReadOnly, d.Compact(nil, nil))
		require.EqualValues(t, ErrReadOnly, d.Flush())
		require.EqualValues(t, ErrReadOnly, func() error { _, err := d.AsyncFlush(); return err }())

		require.EqualValues(t, ErrReadOnly, d.Delete(nil, nil))
		require.EqualValues(t, ErrReadOnly, d.DeleteRange(nil, nil, nil))
		require.EqualValues(t, ErrReadOnly, d.Ingest(nil))
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

		checkIter := func(iter *Iterator) {
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
	checkIter := func(iter *Iterator) {
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
			d, err := Open(dir, &Options{
				FS:           mem,
				MemTableSize: 32 << 20,
			})
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
			// The memtable size starts at 256KB and doubles up to 32MB so we expect 5
			// logs (one for each doubling).
			require.Equal(t, 7, logCount)

			// Re-open the DB with a smaller memtable. Values for 1, 2 will fit in the first memtable;
			// value for 3 will go in the next memtable; value for 4 will be in a flushable batch
			// which will cause the previous memtable to be flushed; value for 5 will go in the next
			// memtable
			d, err = Open(dir, &Options{
				FS:           mem,
				MemTableSize: 300 << 10,
				ReadOnly:     readOnly,
			})
			require.NoError(t, err)

			if readOnly {
				d.mu.Lock()
				require.Equal(t, 10, len(d.mu.mem.queue))
				require.NotNil(t, d.mu.mem.mutable)
				d.mu.Unlock()
			}
			checkIter(d.NewIter(nil))
			require.NoError(t, d.Close())
		})
	}
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
					d, err := Open("", &Options{
						FS:           mem,
						MemTableSize: 256 << 10,
					})
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
						largeValue := []byte(strings.Repeat("a", d.largeBatchThreshold))
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
					d, err = Open("", &Options{
						FS:           mem,
						MemTableSize: 300 << 10,
						ReadOnly:     readOnly,
					})
					require.NoError(t, err)
					require.NoError(t, d.Close())
				})
			}
		})
	}
}

func TestOpenWALReplayMemtableGrowth(t *testing.T) {
	mem := vfs.NewMem()
	const memTableSize = 64 * 1024 * 1024
	opts := &Options{
		MemTableSize: memTableSize,
		FS:           mem,
	}
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

func TestGetVersion(t *testing.T) {
	mem := vfs.NewMem()
	opts := &Options{
		FS: mem,
	}

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
	highestOptionsNum := uint64(0)
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
	f, _ := mem.Create(fmt.Sprintf("OPTIONS-%d", highestOptionsNum+1))
	_, err = f.Write([]byte("[Version]\n  pebble_version=0.2\n"))
	require.NoError(t, err)
	err = f.Close()
	require.NoError(t, err)
	version, err = GetVersion("", mem)
	require.NoError(t, err)
	require.Equal(t, "0.2", version)

	// Case 4: Manually created OPTIONS file with a RocksDB number.
	f, _ = mem.Create(fmt.Sprintf("OPTIONS-%d", highestOptionsNum+2))
	_, err = f.Write([]byte("[Version]\n  rocksdb_version=6.2.1\n"))
	require.NoError(t, err)
	err = f.Close()
	require.NoError(t, err)
	version, err = GetVersion("", mem)
	require.NoError(t, err)
	require.Equal(t, "rocksdb v6.2.1", version)
}
