// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"encoding/binary"
	"flag"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

var monotonicReadsDuration = flag.Duration(
	"monotonic-reads-duration", 2*time.Second, "duration of TestMonotonicReads",
)

// TestMonotonicReads is a hook-free stress test for read consistency under
// concurrent writes, flushes and compactions. A single writer overwrites one
// key with an increasing counter (occasionally in a large batch), while
// readers check:
//   - a new Get, iterator, snapshot or EFOS never returns a value older than
//     the last value whose commit returned before the read started;
//   - a snapshot or EFOS keeps returning the same value.
func TestMonotonicReads(t *testing.T) {
	d, err := Open("", &Options{
		FS:                          vfs.NewMem(),
		MemTableSize:                256 << 10,
		MemTableStopWritesThreshold: 8,
		L0StopWritesThreshold:       1000,
		Logger:                      base.NoopLoggerAndTracer{},
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()

	key := []byte("k")
	enc := func(v uint64) []byte { return binary.BigEndian.AppendUint64(nil, v) }
	read := func(r Reader) (uint64, error) {
		v, closer, err := r.Get(key)
		if errors.Is(err, ErrNotFound) {
			return 0, nil
		} else if err != nil {
			return 0, err
		}
		defer closer.Close()
		return binary.BigEndian.Uint64(v), nil
	}

	var acked atomic.Uint64
	var stop atomic.Bool
	errCh := make(chan error, 1)
	fail := func(format string, args ...any) {
		select {
		case errCh <- errors.Errorf(format, args...):
		default:
		}
		stop.Store(true)
	}
	var wg sync.WaitGroup
	run := func(f func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				f()
			}
		}()
	}

	// Writer. Every 1024th write is a large batch, which is queued as its own
	// flushable.
	pad := make([]byte, 200<<10)
	var i uint64
	run(func() {
		i++
		b := d.NewBatch()
		_ = b.Set(key, enc(i), nil)
		if i%1024 == 0 {
			_ = b.Set([]byte("pad"), pad, nil)
		}
		if err := b.Commit(NoSync); err != nil {
			fail("commit: %v", err)
		}
		_ = b.Close()
		acked.Store(i)
	})

	// Flusher.
	run(func() {
		ch, err := d.AsyncFlush()
		if err != nil {
			fail("flush: %v", err)
			return
		}
		<-ch
	})

	// Readers that use a fresh read each time.
	for range 2 {
		run(func() {
			lo := acked.Load()
			v, err := read(d)
			if err != nil {
				fail("get: %v", err)
			} else if v < lo {
				fail("get returned %d, but %d was already committed", v, lo)
			}
		})
		run(func() {
			lo := acked.Load()
			it, _ := d.NewIter(nil)
			var v uint64
			if it.SeekGE(key) && string(it.Key()) == string(key) {
				v = binary.BigEndian.Uint64(it.Value())
			}
			if err := it.Close(); err != nil {
				fail("iter: %v", err)
			} else if v < lo {
				fail("iterator returned %d, but %d was already committed", v, lo)
			}
		})
	}

	// Snapshot and EFOS readers. Each keeps a few open snapshots and re-reads
	// them to check that their value doesn't change.
	var closeSnapshots []func()
	type snapReader interface {
		Reader
		Close() error
	}
	snapshotReader := func(kind string, newSnap func() snapReader) {
		type entry struct {
			s snapReader
			v uint64
		}
		var ring [8]entry
		var n int
		run(func() {
			e := &ring[n%len(ring)]
			n++
			if e.s != nil {
				if v, err := read(e.s); err != nil {
					fail("%s get: %v", kind, err)
				} else if v != e.v {
					fail("%s returned %d, but earlier returned %d", kind, v, e.v)
				}
				_ = e.s.Close()
			}
			lo := acked.Load()
			e.s = newSnap()
			var err error
			if e.v, err = read(e.s); err != nil {
				fail("%s get: %v", kind, err)
			} else if e.v < lo {
				fail("new %s returned %d, but %d was already committed", kind, e.v, lo)
			}
		})
		closeSnapshots = append(closeSnapshots, func() {
			for _, e := range ring {
				if e.s != nil {
					_ = e.s.Close()
				}
			}
		})
	}
	snapshotReader("snapshot", func() snapReader { return d.NewSnapshot() })
	snapshotReader("EFOS", func() snapReader {
		return d.NewEventuallyFileOnlySnapshot([]KeyRange{{Start: key, End: []byte("l")}})
	})

	select {
	case err = <-errCh:
	case <-time.After(*monotonicReadsDuration):
		stop.Store(true)
	}
	wg.Wait()
	for _, f := range closeSnapshots {
		f()
	}
	require.NoError(t, err, "after %d writes", acked.Load())
	t.Logf("%d writes", acked.Load())
}
