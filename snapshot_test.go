// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestSnapshotListToSlice(t *testing.T) {
	testCases := []struct {
		vals []uint64
	}{
		{nil},
		{[]uint64{1}},
		{[]uint64{1, 2, 3}},
		{[]uint64{3, 2, 1}},
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

func TestSnapshot(t *testing.T) {
	var d *DB
	var snapshots map[string]*Snapshot

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

	datadriven.RunTest(t, "testdata/snapshot", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			close()

			var err error
			d, err = Open("", &Options{
				FS: vfs.NewMem(),
			})
			if err != nil {
				return err.Error()
			}
			snapshots = make(map[string]*Snapshot)

			for _, line := range strings.Split(td.Input, "\n") {
				parts := strings.Fields(line)
				if len(parts) == 0 {
					continue
				}
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
					snapshots[parts[1]] = d.NewSnapshot()
				case "compact":
					if len(parts) != 2 {
						return fmt.Sprintf("%s expects 1 argument", parts[0])
					}
					keys := strings.Split(parts[1], "-")
					if len(keys) != 2 {
						return fmt.Sprintf("malformed key range: %s", parts[1])
					}
					err = d.Compact([]byte(keys[0]), []byte(keys[1]))
				default:
					return fmt.Sprintf("unknown op: %s", parts[0])
				}
				if err != nil {
					return err.Error()
				}
			}
			return ""

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
				iter = snapshot.NewIter(nil)
			} else {
				iter = d.NewIter(nil)
			}
			defer iter.Close()

			var b bytes.Buffer
			for _, line := range strings.Split(td.Input, "\n") {
				parts := strings.Fields(line)
				if len(parts) == 0 {
					continue
				}
				switch parts[0] {
				case "first":
					iter.First()
				case "last":
					iter.Last()
				case "seek-ge":
					if len(parts) != 2 {
						return fmt.Sprintf("seek-ge <key>\n")
					}
					iter.SeekGE([]byte(strings.TrimSpace(parts[1])))
				case "seek-lt":
					if len(parts) != 2 {
						return fmt.Sprintf("seek-lt <key>\n")
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

func TestSnapshotClosed(t *testing.T) {
	d, err := Open("", &Options{
		FS: vfs.NewMem(),
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
	require.EqualValues(t, ErrClosed, catch(func() { _ = snap.Close() }))
	require.EqualValues(t, ErrClosed, catch(func() { _, _, _ = snap.Get(nil) }))
	require.EqualValues(t, ErrClosed, catch(func() { snap.NewIter(nil) }))

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
	const runs = 10
	d, err := Open("", &Options{FS: vfs.NewMem()})
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
