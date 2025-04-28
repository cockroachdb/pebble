// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"math"
	"math/rand/v2"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
	"github.com/stretchr/testify/require"
)

type panicLogger struct{}

func (l panicLogger) Infof(format string, args ...interface{})  {}
func (l panicLogger) Errorf(format string, args ...interface{}) {}

func (l panicLogger) Fatalf(format string, args ...interface{}) {
	panic(errors.Errorf("fatal: "+format, args...))
}

// corruptFS injects a corruption in the `index`th byte read.
type corruptFS struct {
	vfs.FS
	// index is the index of the byte which we will corrupt.
	index     atomic.Int32
	bytesRead atomic.Int32
}

func (fs *corruptFS) maybeCorrupt(n int32, p []byte) {
	newBytesRead := fs.bytesRead.Add(n)
	pIdx := newBytesRead - 1 - fs.index.Load()
	if pIdx >= 0 && pIdx < n {
		p[pIdx]++
	}
}

func (fs *corruptFS) maybeCorruptAt(n int32, p []byte, offset int64) {
	pIdx := fs.index.Load() - int32(offset)
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
	f.fs.maybeCorruptAt(int32(n), p, off)
	return n, err
}

func expectLSM(expected string, d *DB, t *testing.T) {
	t.Helper()
	expected = strings.TrimSpace(expected)
	d.mu.Lock()
	actual := d.mu.versions.currentVersion().String()
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
		if err := d.Compact(nil, []byte("\xff"), false); err != nil {
			return err
		}

		iter, _ := d.NewIter(nil)
		for valid := iter.First(); valid; valid = iter.Next() {
		}
		if err := iter.Close(); err != nil {
			return err
		}
		return d.Close()
	}

	errorCounts := make(map[string]int)
	for i := int32(0); ; i++ {
		fs := errorfs.Wrap(vfs.NewMem(), errorfs.ErrInjected.If(errorfs.OnIndex(i)))
		err := run(fs)
		if err == nil {
			t.Logf("success %d\n", i)
			break
		}
		errMsg := err.Error()
		if !strings.Contains(errMsg, "injected error") {
			t.Fatalf("unexpected errors: %v", err)
		}
		errorCounts[errMsg]++
	}

	expectedErrors := []string{
		"fatal: MANIFEST flush failed: injected error",
		"fatal: MANIFEST sync failed: injected error",
		"fatal: MANIFEST set current failed: injected error",
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
	run := func(formatVersion FormatMajorVersion, index int32) (err error) {
		// Perform setup with error injection disabled as it involves writes/background ops.
		ii := errorfs.OnIndex(-1)
		fs := errorfs.Wrap(vfs.NewMem(), errorfs.ErrInjected.If(ii))
		opts := &Options{
			DisableTableStats:  true,
			FS:                 fs,
			Logger:             panicLogger{},
			FormatMajorVersion: formatVersion,
		}
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
		require.NoError(t, d.Compact(key1, key2, false))
		require.NoError(t, d.DeleteRange(key1, key2, nil))
		require.NoError(t, d.Set(key1, value, nil))
		require.NoError(t, d.Flush())
		expectLSM(`
L0.0:
  000007:[a1#13,SET-a2#inf,RANGEDEL]
L6:
  000005:[a1#10,SET-a2#11,SET]
`, d, t)

		// Now perform foreground ops with error injection enabled.
		ii.Store(index)
		iter, _ := d.NewIter(nil)
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
			d = nil
			return err
		}
		d = nil
		// Reaching here implies all read operations succeeded. This
		// should only happen when we reached a large enough index at
		// which `errorfs.FS` did not return any error.
		if i := ii.Load(); i < 0 {
			t.Errorf("FS error injected %d ops ago went unreported", -i)
		}
		if numFound != 2 {
			t.Fatalf("expected 2 values; found %d", numFound)
		}
		return nil
	}

	versions := []FormatMajorVersion{FormatMinSupported, internalFormatNewest}
	for _, version := range versions {
		t.Run(fmt.Sprintf("version-%s", version), func(t *testing.T) {
			for i := int32(0); ; i++ {
				err := run(version, i)
				if err == nil {
					t.Logf("no failures reported at index %d", i)
					break
				}
			}
		})
	}
}

// TestCorruptReadError verifies that reads to a corrupted file detect the
// corruption and return an error. In this case the filesystem reads return
// successful status but the data they return is corrupt.
func TestCorruptReadError(t *testing.T) {
	run := func(formatVersion FormatMajorVersion, index int32) (err error) {
		// Perform setup with corruption injection disabled as it involves writes/background ops.
		fs := &corruptFS{
			FS: vfs.NewMem(),
		}
		fs.index.Store(-1)
		opts := &Options{
			DisableTableStats:  true,
			FS:                 fs,
			Logger:             base.NoopLoggerAndTracer{},
			FormatMajorVersion: formatVersion,
		}
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
		require.NoError(t, d.Compact(key1, key2, false))
		require.NoError(t, d.DeleteRange(key1, key2, nil))
		require.NoError(t, d.Set(key1, value, nil))
		require.NoError(t, d.Flush())
		expectLSM(`
L0.0:
  000007:[a1#13,SET-a2#inf,RANGEDEL]
L6:
  000005:[a1#10,SET-a2#11,SET]
`, d, t)

		// Now perform foreground ops with corruption injection enabled.
		fs.index.Store(index)
		iter, _ := d.NewIter(nil)
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
		if bytesRead := fs.bytesRead.Load(); bytesRead > index {
			t.Errorf("corruption error injected at index %d went unreported", index)
		}
		if numFound != 2 {
			t.Fatalf("expected 2 values; found %d", numFound)
		}
		return nil
	}
	versions := []FormatMajorVersion{FormatMinSupported, internalFormatNewest}
	for _, version := range versions {
		t.Run(fmt.Sprintf("version-%s", version), func(t *testing.T) {
			for i := int32(0); ; i++ {
				err := run(version, i)
				if err == nil {
					t.Logf("no failures reported at index %d", i)
					break
				}
			}
		})
	}
}

func TestDBWALRotationCrash(t *testing.T) {
	memfs := vfs.NewCrashableMem()

	var crashFS *vfs.MemFS
	var index atomic.Int32
	inj := errorfs.InjectorFunc(func(op errorfs.Op) error {
		if op.Kind.ReadOrWrite() == errorfs.OpIsWrite && index.Add(-1) == -1 {
			crashFS = memfs.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 0})
		}
		return nil
	})
	triggered := func() bool { return index.Load() < 0 }

	run := func(fs *errorfs.FS, k int32) (err error) {
		opts := &Options{
			DisableTableStats: true,
			FS:                fs,
			Logger:            panicLogger{},
			MemTableSize:      2048,
		}
		d, err := Open("", opts)
		if err != nil || triggered() {
			return err
		}

		// Write keys with the FS set up to simulate a crash by ignoring
		// syncs on the k-th write operation.
		index.Store(k)
		key := []byte("test")
		for i := 0; i < 10; i++ {
			v := []byte(strings.Repeat("b", i))
			err = d.Set(key, v, nil)
			if err != nil || triggered() {
				break
			}
		}
		err = firstError(err, d.Close())
		return err
	}

	fs := errorfs.Wrap(memfs, inj)
	for k := int32(0); ; k++ {
		// Run, simulating a crash after the k-th write operation after Open.
		index.Store(math.MaxInt32)
		err := run(fs, k)
		if !triggered() {
			// Stop when we reach a value of k greater than the number of
			// write operations performed during `run`.
			t.Logf("No crash at write operation %d\n", k)
			if err != nil {
				t.Fatalf("Filesystem did not 'crash', but error returned: %s", err)
			}
			break
		}
		t.Logf("Crashed at write operation % 2d, error: %v\n", k, err)

		// Reset the filesystem to its state right before the simulated
		// "crash", restore syncs, and run again without crashing.
		memfs = crashFS
		index.Store(math.MaxInt32)
		require.NoError(t, run(fs, k))
	}
}

func TestDBCompactionCrash(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Log("seed", seed)

	// This test uses the strict MemFS with the error injector to simulate
	// crashes. Each subtest runs with a crash induced at the k-th write
	// operation. Each subsequent run increases k by +1-5 until a subtest runs
	// to completion without performing a k-th write operation.
	//
	// crashIndex holds the value of k at which the crash is induced and is
	// decremented by the errorfs on each write operation.
	var crashIndex atomic.Int32
	var crashFS *vfs.MemFS
	crashRNG := rand.New(rand.NewPCG(0, uint64(seed)))
	mkFS := func() vfs.FS {
		memfs := vfs.NewCrashableMem()
		inj := errorfs.InjectorFunc(func(op errorfs.Op) error {
			if op.Kind.ReadOrWrite() == errorfs.OpIsWrite && crashIndex.Add(-1) == -1 {
				// Allow an arbitrary subset of non-synced state to survive beyond the
				// crash point.
				crashFS = memfs.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 10, RNG: crashRNG})
			}
			return nil
		})
		return errorfs.Wrap(memfs, inj)
	}
	triggered := func() bool { return crashIndex.Load() < 0 }

	// run opens the store and performs some random writes, simulating a crash
	// at the k-th write operation.
	run := func(t *testing.T, fs vfs.FS, k int32, seed int64) (i int64, err error) {
		// On windows, time.Sleep usually takes at least 15ms, which makes this test
		// really slow. See https://github.com/golang/go/issues/61042,
		// https://github.com/golang/go/issues/44343.
		if runtime.GOOS != "windows" {
			fs = errorfs.Wrap(fs, errorfs.RandomLatency(nil, 20*time.Microsecond, seed, 0 /* no limit */))
		}
		rng := rand.New(rand.NewPCG(0, uint64(seed)))
		maxConcurrentCompactions := rng.IntN(3) + 2
		opts := &Options{
			DisableTableStats:           true,
			FS:                          fs,
			Logger:                      testLogger{t: t},
			MemTableSize:                128 << 10,
			CompactionConcurrencyRange:  func() (int, int) { return 1, maxConcurrentCompactions },
			LBaseMaxBytes:               64 << 10,
			L0CompactionThreshold:       2,
			L0CompactionFileThreshold:   2,
			MemTableStopWritesThreshold: 10,
			L0StopWritesThreshold:       10,
		}
		if testing.Verbose() {
			lel := MakeLoggingEventListener(opts.Logger)
			opts.EventListener = &lel
		}
		d, err := Open("", opts)
		if err != nil || triggered() {
			return 0, err
		}

		// Set index to k so that the k-th write operation decrements it to zero
		// and simulates a crash.
		crashIndex.Store(k)

		// Write keys in random order in batches of random sizes.
		const maxKeyLength = 2
		const valLength = 4 << 10
		timestamps := []int{10, 5}
		ks := testkeys.Alpha(maxKeyLength)
		ks = ks.EveryN(10)
		buf := make([]byte, ks.MaxLen()+testkeys.MaxSuffixLen)
		vbuf := make([]byte, valLength)
		b := d.NewBatch()
		perm := rng.Perm(int(ks.Count()))
	done:
		for _, ts := range timestamps {
			for _, i := range perm {
				n := testkeys.WriteKeyAt(buf, ks, int64(i), int64(ts))
				for j := range vbuf {
					vbuf[j] = byte(rng.Uint32())
				}
				require.NoError(t, err)
				require.NoError(t, b.Set(buf[:n], vbuf, nil))
				if rng.IntN(10) == 1 {
					if err = d.Apply(b, nil); err != nil || triggered() {
						b = nil
						break done
					}
					b.Reset()
				}
				if rng.IntN(100) == 1 {
					if err = d.Flush(); err != nil || triggered() {
						break done
					}
				}
			}
		}
		if b != nil && b.Count() > 0 {
			err = firstError(err, d.Apply(b, nil))
		}
		err = firstError(err, d.Close())
		return i, err
	}

	// Run the test with increasing values of k until a subtest runs to
	// completion without performing a k-th write operation.
	done := false
	rng := rand.New(rand.NewPCG(0, uint64(seed)))
	for k := int32(0); !done; k += rng.Int32N(5) + 1 {
		t.Run(fmt.Sprintf("k=%d", k), func(t *testing.T) {
			// Run, simulating a crash by ignoring syncs after the k-th write
			// operation after Open.
			crashIndex.Store(math.MaxInt32)
			i, err := run(t, mkFS(), k, seed)
			if !triggered() {
				// Stop when we reach a value of k greater than the number of
				// write operations performed during `run`.
				t.Logf("No crash at write operation %d\n", k)
				if err != nil {
					t.Fatalf("Filesystem did not 'crash', but error returned: %s", err)
				}
				done = true
				return
			}
			t.Logf("Simulated crash at write operation % 2d after writing %d keys, error: %v\n", k, i, err)

			// Reset the filesystem to its state right before the simulated
			// "crash", restore syncs and run again without crashing. No errors
			// should be encountered.
			crashIndex.Store(math.MaxInt32)
			_, err = run(t, crashFS, math.MaxInt32, seed)
			require.False(t, triggered())
			// TODO(jackson): Add assertions on the database keys.
			require.NoError(t, err)
		})
	}
}
