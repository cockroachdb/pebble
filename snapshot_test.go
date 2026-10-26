// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestSnapshotListToSlice(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		vals []base.SeqNum
	}{
		{nil},
		{[]base.SeqNum{1}},
		{[]base.SeqNum{1, 2, 3}},
		{[]base.SeqNum{3, 2, 1}},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			var l snapshotList
			l.init()
			for _, v := range c.vals {
				l.pushBack(&Snapshot{seqNum: v})
			}
			slice := l.toSlice()
			if !reflect.DeepEqual(c.vals, slice) {
				t.Fatalf("expected %d, but got %d", c.vals, slice)
			}
		})
	}
}

func testSnapshotImpl(t *testing.T, newSnapshot func(d *DB) Reader) {
	var d *DB
	var snapshots map[string]Reader

	close := func() {
		for _, s := range snapshots {
			require.NoError(t, s.Close())
		}
		snapshots = nil
		if d != nil {
			require.NoError(t, d.Close())
			d = nil
		}
	}
	defer close()

	randVersion := func() FormatMajorVersion {
		return FormatMinSupported + FormatMajorVersion(rand.IntN(int(internalFormatNewest-FormatMinSupported)+1))
	}
	datadriven.RunTest(t, "testdata/snapshot", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			close()

			var err error
			options := &Options{
				FS:                 vfs.NewMem(),
				FormatMajorVersion: randVersion(),
				Logger:             testutils.Logger{T: t},
			}
			if td.HasArg("block-size") {
				var blockSize int
				td.ScanArgs(t, "block-size", &blockSize)
				options.Levels[0].BlockSize = blockSize
				options.Levels[0].IndexBlockSize = blockSize
			}
			d, err = Open("", options)
			if err != nil {
				return err.Error()
			}
			snapshots = make(map[string]Reader)

			for line := range crstrings.LinesSeq(td.Input) {
				parts := strings.Fields(line)
				var err error
				switch parts[0] {
				case "set":
					if len(parts) != 3 {
						return fmt.Sprintf("%s expects 2 arguments", parts[0])
					}
					err = d.Set([]byte(parts[1]), []byte(parts[2]), nil)
				case "del":
					if len(parts) != 2 {
						return fmt.Sprintf("%s expects 1 argument", parts[0])
					}
					err = d.Delete([]byte(parts[1]), nil)
				case "merge":
					if len(parts) != 3 {
						return fmt.Sprintf("%s expects 2 arguments", parts[0])
					}
					err = d.Merge([]byte(parts[1]), []byte(parts[2]), nil)
				case "snapshot":
					if len(parts) != 2 {
						return fmt.Sprintf("%s expects 1 argument", parts[0])
					}
					snapshots[parts[1]] = newSnapshot(d)
				case "compact":
					if len(parts) != 2 {
						return fmt.Sprintf("%s expects 1 argument", parts[0])
					}
					keys := strings.Split(parts[1], "-")
					if len(keys) != 2 {
						return fmt.Sprintf("malformed key range: %s", parts[1])
					}
					err = d.Compact(context.Background(), []byte(keys[0]), []byte(keys[1]), false)
				default:
					return fmt.Sprintf("unknown op: %s", parts[0])
				}
				if err != nil {
					return err.Error()
				}
			}
			return ""

		case "db-state":
			d.mu.Lock()
			s := d.mu.versions.currentVersion().String()
			d.mu.Unlock()
			return s

		case "iter":
			var iter *Iterator
			if len(td.CmdArgs) == 1 {
				if td.CmdArgs[0].Key != "snapshot" {
					return fmt.Sprintf("unknown argument: %s", td.CmdArgs[0])
				}
				if len(td.CmdArgs[0].Vals) != 1 {
					return fmt.Sprintf("%s expects 1 value: %s", td.CmdArgs[0].Key, td.CmdArgs[0])
				}
				name := td.CmdArgs[0].Vals[0]
				snapshot := snapshots[name]
				if snapshot == nil {
					return fmt.Sprintf("unable to find snapshot \"%s\"", name)
				}
				iter, _ = snapshot.NewIter(nil)
			} else {
				iter, _ = d.NewIter(nil)
			}
			defer iter.Close()

			var b bytes.Buffer
			for line := range crstrings.LinesSeq(td.Input) {
				parts := strings.Fields(line)
				switch parts[0] {
				case "first":
					iter.First()
				case "last":
					iter.Last()
				case "seek-ge":
					if len(parts) != 2 {
						return "seek-ge <key>\n"
					}
					iter.SeekGE([]byte(strings.TrimSpace(parts[1])))
				case "seek-lt":
					if len(parts) != 2 {
						return "seek-lt <key>\n"
					}
					iter.SeekLT([]byte(strings.TrimSpace(parts[1])))
				case "next":
					iter.Next()
				case "prev":
					iter.Prev()
				default:
					return fmt.Sprintf("unknown op: %s", parts[0])
				}
				if iter.Valid() {
					fmt.Fprintf(&b, "%s:%s\n", iter.Key(), iter.Value())
				} else if err := iter.Error(); err != nil {
					fmt.Fprintf(&b, "err=%v\n", err)
				} else {
					fmt.Fprintf(&b, ".\n")
				}
			}
			return b.String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testSnapshotImpl(t, func(d *DB) Reader {
		return d.NewSnapshot()
	})
}

func TestEventuallyFileOnlySnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testSnapshotImpl(t, func(d *DB) Reader {
		// NB: all keys in testdata/snapshot fall within the ASCII keyrange a-z.
		return d.NewEventuallyFileOnlySnapshot([]KeyRange{{Start: []byte("a"), End: []byte("z")}})
	})
}

func TestSnapshotClosed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	d, err := Open("", &Options{
		FS:     vfs.NewMem(),
		Logger: testutils.Logger{T: t},
	})
	require.NoError(t, err)

	catch := func(f func()) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = r.(error)
			}
		}()
		f()
		return nil
	}

	snap := d.NewSnapshot()
	require.NoError(t, snap.Close())
	require.True(t, errors.Is(catch(func() { _ = snap.Close() }), ErrClosed))
	require.True(t, errors.Is(catch(func() { _, _, _ = snap.Get(nil) }), ErrClosed))
	require.True(t, errors.Is(catch(func() { snap.NewIter(nil) }), ErrClosed))

	for _, fileOnly := range []bool{true, false} {
		if !fileOnly {
			// The EFOS overlaps the memtable, so it is not file-only.
			require.NoError(t, d.Set([]byte("a"), nil, nil))
		}
		efos := d.NewEventuallyFileOnlySnapshot([]KeyRange{{Start: []byte("a"), End: []byte("z")}})
		require.Equal(t, fileOnly, efos.hasTransitioned())
		require.NoError(t, efos.Close())
		require.True(t, errors.Is(catch(func() { _, _, _ = efos.Get([]byte("a")) }), ErrClosed))
		require.True(t, errors.Is(catch(func() { _, _ = efos.NewIter(nil) }), ErrClosed))
		require.True(t, errors.Is(catch(func() {
			_ = efos.ScanInternal(context.Background(), ScanInternalOptions{})
		}), ErrClosed))
	}

	require.NoError(t, d.Close())
}

func TestSnapshotRangeDeletionStress(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const runs = 200
	const middleKey = runs * runs

	d, err := Open("", &Options{
		FS:     vfs.NewMem(),
		Logger: testutils.Logger{T: t},
	})
	require.NoError(t, err)

	mkkey := func(k int) []byte {
		return []byte(fmt.Sprintf("%08d", k))
	}
	v := []byte("hello world")

	snapshots := make([]*Snapshot, 0, runs)
	for r := 0; r < runs; r++ {
		// We use a keyspace that is 2*runs*runs wide. In other words there are
		// 2*runs sections of the keyspace, each with runs elements. On every
		// run, we write to the r-th element of each section of the keyspace.
		for i := 0; i < 2*runs; i++ {
			err := d.Set(mkkey(runs*i+r), v, nil)
			require.NoError(t, err)
		}

		// Now we delete some of the keyspace through a DeleteRange. We delete from
		// the middle of the keyspace outwards. The keyspace is made of 2*runs
		// sections, and we delete an additional two of these sections per run.
		err := d.DeleteRange(mkkey(middleKey-runs*r), mkkey(middleKey+runs*r), nil)
		require.NoError(t, err)

		snapshots = append(snapshots, d.NewSnapshot())
	}

	// Check that all the snapshots contain the expected number of keys.
	// Iterating over so many keys is slow, so do it in parallel.
	var wg sync.WaitGroup
	sem := make(chan struct{}, runtime.GOMAXPROCS(0))
	for r := range snapshots {
		wg.Add(1)
		sem <- struct{}{}
		go func(r int) {
			defer func() {
				<-sem
				wg.Done()
			}()

			// Count the keys at this snapshot.
			iter, _ := snapshots[r].NewIter(nil)
			var keysFound int
			for iter.First(); iter.Valid(); iter.Next() {
				keysFound++
			}
			err := firstError(iter.Error(), iter.Close())
			if err != nil {
				t.Error(err)
				return
			}

			// At the time that this snapshot was taken, (r+1)*2*runs unique keys
			// were Set (one in each of the 2*runs sections per run).  But this
			// run also deleted the 2*r middlemost sections.  When this snapshot
			// was taken, a Set to each of those sections had been made (r+1)
			// times, so 2*r*(r+1) previously-set keys are now deleted.

			keysExpected := (r+1)*2*runs - 2*r*(r+1)
			if keysFound != keysExpected {
				t.Errorf("%d: found %d keys, want %d", r, keysFound, keysExpected)
			}
			if err := snapshots[r].Close(); err != nil {
				t.Error(err)
			}
		}(r)
	}
	wg.Wait()
	require.NoError(t, d.Close())
}

// TestNewSnapshotRace tests atomicity of NewSnapshot.
//
// It tests for a regression of a previous race condition in which NewSnapshot
// would retrieve the visible sequence number for a new snapshot before
// locking the database mutex to add the snapshot. A write and flush that
// that occurred between the reading of the sequence number and appending the
// snapshot could drop keys required by the snapshot.
func TestNewSnapshotRace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const runs = 10
	d, err := Open("", &Options{
		FS:     vfs.NewMem(),
		Logger: testutils.Logger{T: t},
	})
	require.NoError(t, err)

	v := []byte(`foo`)
	ch := make(chan string)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for k := range ch {
			if err := d.Set([]byte(k), v, nil); err != nil {
				t.Error(err)
				return
			}
			if err := d.Flush(); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for i := 0; i < runs; i++ {
		// This main test goroutine sets `k` before creating a new snapshot.
		// The key `k` should always be present within the snapshot.
		k := fmt.Sprintf("key%06d", i)
		require.NoError(t, d.Set([]byte(k), v, nil))

		// Lock d.mu in another goroutine so that our call to NewSnapshot
		// will need to contend for d.mu.
		wg.Add(1)
		locked := make(chan struct{})
		go func() {
			defer wg.Done()
			d.mu.Lock()
			close(locked)
			time.Sleep(20 * time.Millisecond)
			d.mu.Unlock()
		}()
		<-locked

		// Tell the other goroutine to overwrite `k` with a later sequence
		// number. It's indeterminate which key we'll read, but we should
		// always read one of them.
		ch <- k
		s := d.NewSnapshot()
		_, c, err := s.Get([]byte(k))
		require.NoError(t, err)
		require.NoError(t, c.Close())
		require.NoError(t, s.Close())
	}
	close(ch)
	wg.Wait()
	require.NoError(t, d.Close())
}

// Test for fix to https://github.com/cockroachdb/pebble/issues/5390.
func TestEFOSAndExciseRace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	o := &Options{
		FS:                          vfs.NewMem(),
		L0CompactionThreshold:       10,
		DisableAutomaticCompactions: true,
		FormatMajorVersion:          FormatNewest,
	}
	beforeIngestApply := make(chan struct{}, 1)
	waitForCh := make(chan struct{}, 1)
	o.private.testingBeforeIngestApplyFunc = func() {
		beforeIngestApply <- struct{}{}
		<-waitForCh
	}
	d, err := Open("", o)
	require.NoError(t, err)
	defer d.Close()
	require.NoError(t, d.Set([]byte("c"), nil, nil))
	require.NoError(t, d.Flush())
	require.NoError(t, d.Set([]byte("a"), nil, nil))
	go func() {
		_, err = d.IngestAndExcise(context.Background(), nil, nil, nil,
			KeyRange{Start: []byte("b"), End: []byte("d")})
	}()
	<-beforeIngestApply
	// IngestAndExcise is blocked, after waiting for memtable flush.
	//
	// Create a EFOS that overlaps with the excise. It will block due to the
	// ongoing excise.
	snapDone := make(chan struct{})
	go func() {
		snap := d.NewEventuallyFileOnlySnapshot([]KeyRange{{Start: []byte("a"), End: []byte("d")}})
		defer func() {
			require.NoError(t, snap.Close())
			snapDone <- struct{}{}
		}()
		checkIter := func() {
			iter, err := snap.NewIter(nil)
			// The iter only sees a, since it comes after the excise.
			require.NoError(t, err)
			require.True(t, iter.First())
			require.Equal(t, []byte("a"), iter.Key())
			require.False(t, iter.Next())
			require.NoError(t, iter.Close())
		}
		checkIter()
		// Flush and transition to FOS.
		require.NoError(t, d.Flush())
		require.NoError(t, snap.WaitForFileOnlySnapshot(context.Background(), 0))
		checkIter()
	}()
	time.Sleep(100 * time.Millisecond)
	// Unblock the excise, which eventually unblocks the snapshot creation.
	waitForCh <- struct{}{}
	// Wait for the snapshot checking to finish.
	<-snapDone
}

// TestEFOSReadDuringTransition is a regression test for a race in
// EventuallyFileOnlySnapshot.ScanInternal, which checked under es.mu that the
// EFOS had not transitioned to a file-only snapshot, but loaded the DB's
// current readState only after releasing es.mu. If the EFOS transitioned in
// that window, compactions were no longer bound by its sequence number, and
// the scan could read a version in which a compaction had rewritten keys
// visible to the EFOS: here, dropping a key deleted after the EFOS was created
// and zeroing the sequence number of a key written after it, which makes that
// key visible to the EFOS.
//
// The testing hook flushes (transitioning the EFOS) and compacts within that
// window, for each way of reading from an EFOS.
func TestEFOSReadDuringTransition(t *testing.T) {
	defer leaktest.AfterTest(t)()
	readers := []struct {
		name string
		read func(t *testing.T, es *EventuallyFileOnlySnapshot) []string
	}{
		{
			name: "ScanInternal",
			read: func(t *testing.T, es *EventuallyFileOnlySnapshot) []string {
				var kvs []string
				require.NoError(t, es.ScanInternal(context.Background(), ScanInternalOptions{
					VisitPointKey: func(key *InternalKey, value LazyValue, _ IteratorLevel) error {
						v, _, err := value.Value(nil)
						if err != nil {
							return err
						}
						kvs = append(kvs, fmt.Sprintf("%s=%s", key.UserKey, v))
						return nil
					},
				}))
				return kvs
			},
		},
		{
			name: "NewIter",
			read: func(t *testing.T, es *EventuallyFileOnlySnapshot) []string {
				iter, err := es.NewIter(nil)
				require.NoError(t, err)
				var kvs []string
				for valid := iter.First(); valid; valid = iter.Next() {
					kvs = append(kvs, fmt.Sprintf("%s=%s", iter.Key(), iter.Value()))
				}
				require.NoError(t, iter.Close())
				return kvs
			},
		},
	}
	for _, r := range readers {
		t.Run(r.name, func(t *testing.T) {
			var hook func()
			o := &Options{
				FS:                          vfs.NewMem(),
				DisableAutomaticCompactions: true,
				FormatMajorVersion:          FormatNewest,
				Logger:                      testutils.Logger{T: t},
			}
			o.private.testingAfterEFOSAcquireIterStateFunc = func() {
				if fn := hook; fn != nil {
					hook = nil
					fn()
				}
			}
			d, err := Open("", o)
			require.NoError(t, err)
			defer func() { require.NoError(t, d.Close()) }()

			require.NoError(t, d.Set([]byte("a"), []byte("a"), nil))
			require.NoError(t, d.Set([]byte("b"), []byte("b"), nil))
			require.NoError(t, d.Flush())
			// x is in the memtable, so the EFOS is not file-only yet.
			require.NoError(t, d.Set([]byte("x"), []byte("x"), nil))
			es := d.NewEventuallyFileOnlySnapshot([]KeyRange{{Start: []byte("a"), End: []byte("z")}})
			defer func() { require.NoError(t, es.Close()) }()
			require.False(t, es.hasTransitioned())
			// Writes the EFOS must not observe.
			require.NoError(t, d.Delete([]byte("b"), nil))
			require.NoError(t, d.Set([]byte("c"), []byte("c"), nil))

			expected := []string{"a=a", "b=b", "x=x"}
			hookRan := false
			hook = func() {
				hookRan = true
				// The flush transitions the EFOS to a file-only snapshot, which
				// unregisters its internal Snapshot. The compaction then merges
				// the two overlapping L0 tables into L6 as if there were no
				// snapshots, dropping b and zeroing the seqnum of c.
				require.NoError(t, d.Flush())
				require.True(t, es.hasTransitioned())
				require.NoError(t, d.Compact(context.Background(), []byte("a"), []byte("z"), false /* parallelize */))
			}
			require.Equal(t, expected, r.read(t, es))
			require.True(t, hookRan)
			// A read that starts after the transition uses the file-only version.
			require.Equal(t, expected, r.read(t, es))
		})
	}
}
