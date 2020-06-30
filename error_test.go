// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/errorfs"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

type panicLogger struct{}

func (l panicLogger) Infof(format string, args ...interface{}) {
}

func (l panicLogger) Fatalf(format string, args ...interface{}) {
	panic(errors.Errorf("fatal: "+format, args...))
}

// corruptFS injects a corruption in the `index`th byte read.
type corruptFS struct {
	vfs.FS
	index     int32
	bytesRead int32
}

func newCorruptFS(index int32) *corruptFS {
	return &corruptFS{
		FS:        vfs.NewMem(),
		index:     index,
		bytesRead: 0,
	}
}

func (fs corruptFS) maybeCorrupt(n int32, p []byte) {
	newBytesRead := atomic.AddInt32(&fs.bytesRead, n)
	pIdx := newBytesRead - 1 - fs.index
	if pIdx >= 0 && pIdx < n {
		p[pIdx]++
	}
}

func (fs *corruptFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.FS.Open(name)
	if err != nil {
		return nil, err
	}
	cf := corruptFile{f, fs}
	for _, opt := range opts {
		opt.Apply(cf)
	}
	return cf, nil
}

type corruptFile struct {
	vfs.File
	fs *corruptFS
}

func (f corruptFile) Read(p []byte) (int, error) {
	n, err := f.File.Read(p)
	f.fs.maybeCorrupt(int32(n), p)
	return n, err
}

func (f corruptFile) ReadAt(p []byte, off int64) (int, error) {
	n, err := f.File.ReadAt(p, off)
	f.fs.maybeCorrupt(int32(n), p)
	return n, err
}

func expectLSM(expected string, d *DB, t *testing.T) {
	t.Helper()
	expected = strings.TrimSpace(expected)
	d.mu.Lock()
	actual := d.mu.versions.currentVersion().DebugString(base.DefaultFormatter)
	d.mu.Unlock()
	actual = strings.TrimSpace(actual)
	if expected != actual {
		t.Fatalf("expected\n%s\nbut found\n%s", expected, actual)
	}
}

// TestErrors repeatedly runs a short sequence of operations, injecting FS
// errors at different points, until success is achieved.
func TestErrors(t *testing.T) {
	run := func(fs *errorfs.FS) (err error) {
		defer func() {
			if r := recover(); r != nil {
				if e, ok := r.(error); ok {
					err = e
				} else {
					t.Fatal(r)
				}
			}
		}()

		d, err := Open("", &Options{
			FS:     fs,
			Logger: panicLogger{},
		})
		if err != nil {
			return err
		}

		key := []byte("a")
		value := []byte("b")
		if err := d.Set(key, value, nil); err != nil {
			return err
		}
		if err := d.Flush(); err != nil {
			return err
		}
		if err := d.Compact(nil, nil); err != nil {
			return err
		}

		iter := d.NewIter(nil)
		for valid := iter.First(); valid; valid = iter.Next() {
		}
		if err := iter.Close(); err != nil {
			return err
		}
		return d.Close()
	}

	errorCounts := make(map[string]int)
	for i := int32(0); ; i++ {
		fs := errorfs.Wrap(vfs.NewMem(), errorfs.OnIndex(i))
		err := run(fs)
		if err == nil {
			t.Logf("success %d\n", i)
			break
		}
		errorCounts[err.Error()]++
	}

	expectedErrors := []string{
		"MANIFEST flush failed: injected error",
		"MANIFEST sync failed: injected error",
		"MANIFEST set current failed: injected error",
		"MANIFEST dirsync failed: injected error",
	}
	for _, expected := range expectedErrors {
		if errorCounts[expected] == 0 {
			t.Errorf("expected error %q did not occur", expected)
		}
	}
}

// TestRequireReadError injects FS errors into read operations at successively later
// points until all operations can complete. It requires an operation fails any time
// an error was injected. This differs from the TestErrors case above as that one
// cannot require operations fail since it involves flush/compaction, which retry
// internally and succeed following an injected error.
func TestRequireReadError(t *testing.T) {
	run := func(index int32) (err error) {
		// Perform setup with error injection disabled as it involves writes/background ops.
		inj := errorfs.OnIndex(-1)
		fs := errorfs.Wrap(vfs.NewMem(), inj)
		opts := &Options{
			FS:     fs,
			Logger: panicLogger{},
		}
		opts.private.disableTableStats = true
		d, err := Open("", opts)
		require.NoError(t, err)

		defer func() {
			if d != nil {
				require.NoError(t, d.Close())
			}
		}()

		key1 := []byte("a1")
		key2 := []byte("a2")
		value := []byte("b")
		require.NoError(t, d.Set(key1, value, nil))
		require.NoError(t, d.Set(key2, value, nil))
		require.NoError(t, d.Flush())
		require.NoError(t, d.Compact(key1, key2))
		require.NoError(t, d.DeleteRange(key1, key2, nil))
		require.NoError(t, d.Set(key1, value, nil))
		require.NoError(t, d.Flush())
		expectLSM(`
0.0:
  000007:[a1#4,SET-a2#72057594037927935,RANGEDEL]
6:
  000005:[a1#1,SET-a2#2,SET]
`, d, t)

		// Now perform foreground ops with error injection enabled.
		inj.SetIndex(index)
		iter := d.NewIter(nil)
		if err := iter.Error(); err != nil {
			return err
		}
		numFound := 0
		expectedKeys := [][]byte{key1, key2}
		for valid := iter.First(); valid; valid = iter.Next() {
			if !bytes.Equal(iter.Key(), expectedKeys[numFound]) {
				t.Fatalf("expected key %v; found %v", expectedKeys[numFound], iter.Key())
			}
			if !bytes.Equal(iter.Value(), value) {
				t.Fatalf("expected value %v; found %v", value, iter.Value())
			}
			numFound++
		}
		if err := iter.Close(); err != nil {
			return err
		}
		if err := d.Close(); err != nil {
			return err
		}
		d = nil
		// Reaching here implies all read operations succeeded. This
		// should only happen when we reached a large enough index at
		// which `errorfs.FS` did not return any error.
		if i := inj.Index(); i < 0 {
			t.Errorf("FS error injected %d ops ago went unreported", -i)
		}
		if numFound != 2 {
			t.Fatalf("expected 2 values; found %d", numFound)
		}
		return nil
	}

	for i := int32(0); ; i++ {
		err := run(i)
		if err == nil {
			t.Logf("no failures reported at index %d", i)
			break
		}
	}
}

// TestCorruptReadError verifies that reads to a corrupted file detect the
// corruption and return an error. In this case the filesystem reads return
// successful status but the data they return is corrupt.
func TestCorruptReadError(t *testing.T) {
	run := func(index int32) (err error) {
		// Perform setup with corruption injection disabled as it involves writes/background ops.
		fs := &corruptFS{
			FS:    vfs.NewMem(),
			index: -1,
		}
		opts := &Options{
			FS:     fs,
			Logger: panicLogger{},
		}
		opts.private.disableTableStats = true
		d, err := Open("", opts)
		if err != nil {
			t.Fatalf("%v", err)
		}
		defer func() {
			if d != nil {
				require.NoError(t, d.Close())
			}
		}()

		key1 := []byte("a1")
		key2 := []byte("a2")
		value := []byte("b")
		require.NoError(t, d.Set(key1, value, nil))
		require.NoError(t, d.Set(key2, value, nil))
		require.NoError(t, d.Flush())
		require.NoError(t, d.Compact(key1, key2))
		require.NoError(t, d.DeleteRange(key1, key2, nil))
		require.NoError(t, d.Set(key1, value, nil))
		require.NoError(t, d.Flush())
		expectLSM(`
0.0:
  000007:[a1#4,SET-a2#72057594037927935,RANGEDEL]
6:
  000005:[a1#1,SET-a2#2,SET]
`, d, t)

		// Now perform foreground ops with corruption injection enabled.
		atomic.StoreInt32(&fs.index, index)
		iter := d.NewIter(nil)
		if err := iter.Error(); err != nil {
			return err
		}

		numFound := 0
		expectedKeys := [][]byte{key1, key2}
		for valid := iter.First(); valid; valid = iter.Next() {
			if !bytes.Equal(iter.Key(), expectedKeys[numFound]) {
				t.Fatalf("expected key %v; found %v", expectedKeys[numFound], iter.Key())
			}
			if !bytes.Equal(iter.Value(), value) {
				t.Fatalf("expected value %v; found %v", value, iter.Value())
			}
			numFound++
		}
		if err := iter.Close(); err != nil {
			return err
		}
		if err := d.Close(); err != nil {
			return err
		}
		d = nil
		// Reaching here implies all read operations succeeded. This
		// should only happen when we reached a large enough index at
		// which `corruptFS` did not inject any corruption.
		if atomic.LoadInt32(&fs.bytesRead) > fs.index {
			t.Errorf("corruption error injected at index %d went unreported", fs.index)
		}
		if numFound != 2 {
			t.Fatalf("expected 2 values; found %d", numFound)
		}
		return nil
	}
	for i := int32(0); ; i++ {
		err := run(i)
		if err == nil {
			t.Logf("no failures reported at index %d", i)
			break
		}
	}
}
