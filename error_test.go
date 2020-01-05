// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
)

type panicLogger struct{}

func (l panicLogger) Infof(format string, args ...interface{}) {
}

func (l panicLogger) Fatalf(format string, args ...interface{}) {
	panic(fmt.Errorf("fatal: "+format, args...))
}

type errorFS struct {
	vfs.FS
	index int32
}

func newErrorFS(index int32) *errorFS {
	return &errorFS{
		FS:    vfs.NewMem(),
		index: index,
	}
}

func (fs *errorFS) maybeError() error {
	if atomic.AddInt32(&fs.index, -1) == -1 {
		return fmt.Errorf("injected error")
	}
	return nil
}

func (fs *errorFS) Create(name string) (vfs.File, error) {
	if err := fs.maybeError(); err != nil {
		return nil, err
	}
	f, err := fs.FS.Create(name)
	if err != nil {
		return nil, err
	}
	return errorFile{f, fs}, nil
}

func (fs *errorFS) Link(oldname, newname string) error {
	if err := fs.maybeError(); err != nil {
		return err
	}
	return fs.FS.Link(oldname, newname)
}

func (fs *errorFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	if err := fs.maybeError(); err != nil {
		return nil, err
	}
	f, err := fs.FS.Open(name)
	if err != nil {
		return nil, err
	}
	ef := errorFile{f, fs}
	for _, opt := range opts {
		opt.Apply(ef)
	}
	return ef, nil
}

func (fs *errorFS) OpenDir(name string) (vfs.File, error) {
	if err := fs.maybeError(); err != nil {
		return nil, err
	}
	f, err := fs.FS.OpenDir(name)
	if err != nil {
		return nil, err
	}
	return errorFile{f, fs}, nil
}

func (fs *errorFS) Remove(name string) error {
	if _, err := fs.FS.Stat(name); os.IsNotExist(err) {
		return nil
	}

	if err := fs.maybeError(); err != nil {
		return err
	}
	return fs.FS.Remove(name)
}

func (fs *errorFS) Rename(oldname, newname string) error {
	if err := fs.maybeError(); err != nil {
		return err
	}
	return fs.FS.Rename(oldname, newname)
}

func (fs *errorFS) ReuseForWrite(oldname, newname string) (vfs.File, error) {
	if err := fs.maybeError(); err != nil {
		return nil, err
	}
	return fs.FS.ReuseForWrite(oldname, newname)
}

func (fs *errorFS) MkdirAll(dir string, perm os.FileMode) error {
	if err := fs.maybeError(); err != nil {
		return err
	}
	return fs.FS.MkdirAll(dir, perm)
}

func (fs *errorFS) Lock(name string) (io.Closer, error) {
	if err := fs.maybeError(); err != nil {
		return nil, err
	}
	return fs.FS.Lock(name)
}

func (fs *errorFS) List(dir string) ([]string, error) {
	if err := fs.maybeError(); err != nil {
		return nil, err
	}
	return fs.FS.List(dir)
}

func (fs *errorFS) Stat(name string) (os.FileInfo, error) {
	if err := fs.maybeError(); err != nil {
		return nil, err
	}
	return fs.FS.Stat(name)
}

type errorFile struct {
	file vfs.File
	fs   *errorFS
}

func (f errorFile) Close() error {
	// We don't inject errors during close as those calls should never fail in
	// practice.
	return f.file.Close()
}

func (f errorFile) Read(p []byte) (int, error) {
	if err := f.fs.maybeError(); err != nil {
		return 0, err
	}
	return f.file.Read(p)
}

func (f errorFile) ReadAt(p []byte, off int64) (int, error) {
	if err := f.fs.maybeError(); err != nil {
		return 0, err
	}
	return f.file.ReadAt(p, off)
}

func (f errorFile) Write(p []byte) (int, error) {
	if err := f.fs.maybeError(); err != nil {
		return 0, err
	}
	return f.file.Write(p)
}

func (f errorFile) Stat() (os.FileInfo, error) {
	if err := f.fs.maybeError(); err != nil {
		return nil, err
	}
	return f.file.Stat()
}

func (f errorFile) Sync() error {
	if err := f.fs.maybeError(); err != nil {
		return err
	}
	return f.file.Sync()
}

// TestErrors repeatedly runs a short sequence of operations, injecting FS
// errors at different points, until success is achieved.
func TestErrors(t *testing.T) {
	run := func(fs *errorFS) (err error) {
		defer func() {
			if r := recover(); r != nil {
				if e, ok := r.(error); ok {
					err = e
				} else {
					err = fmt.Errorf("%v", r)
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
		fs := newErrorFS(i)
		err := run(fs)
		if err == nil {
			t.Logf("success %d\n", i)
			break
		}
		errorCounts[err.Error()]++
	}

	expectedErrors := []string{
		"fatal: MANIFEST flush failed: injected error",
		"fatal: MANIFEST sync failed: injected error",
		"fatal: MANIFEST set current failed: injected error",
		"fatal: MANIFEST dirsync failed: injected error",
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
		fs := &errorFS{
			FS:    vfs.NewMem(),
			index: -1,
		}
		d, err := Open("", &Options{
			FS:     fs,
			Logger: panicLogger{},
		})
		if err != nil {
			t.Fatalf("%v", err)
		}

		key1 := []byte("a1")
		key2 := []byte("a2")
		value := []byte("b")
		if err := d.Set(key1, value, nil); err != nil {
			t.Fatalf("%v", err)
		}
		if err := d.Set(key2, value, nil); err != nil {
			t.Fatalf("%v", err)
		}
		if err := d.Flush(); err != nil {
			t.Fatalf("%v", err)
		}
		if err := d.Compact(key1, key2); err != nil {
			t.Fatalf("%v", err)
		}
		if err := d.DeleteRange(key1, key2, nil); err != nil {
			t.Fatalf("%v", err)
		}
		if err := d.Set(key1, value, nil); err != nil {
			t.Fatalf("%v", err)
		}
		if err := d.Flush(); err != nil {
			t.Fatalf("%v", err)
		}

		// Now perform foreground ops with error injection enabled.
		fs.index = index
		iter := d.NewIter(nil)
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
		// Reaching here implies all read operations succeeded. This
		// should only happen when we reached a large enough index at
		// which `errorFS` did not return any error.
		if fs.index < 0 {
			t.Errorf("FS error injected %d ops ago went unreported", -fs.index)
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
