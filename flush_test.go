// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestManualFlush(t *testing.T) {
	getOptions := func() *Options {
		opts := &Options{
			FS:                    vfs.NewMem(),
			L0CompactionThreshold: 10,
		}
		opts.DisableAutomaticCompactions = true
		return opts
	}
	d, err := Open("", getOptions())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, d.Close())
	}()

	datadriven.RunTest(t, "testdata/manual_flush", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "batch":
			b := d.NewBatch()
			if err := runBatchDefineCmd(td, b); err != nil {
				return err.Error()
			}
			b.Commit(nil)
			return ""

		case "flush":
			if err := d.Flush(); err != nil {
				return err.Error()
			}

			d.mu.Lock()
			s := d.mu.versions.currentVersion().String()
			d.mu.Unlock()
			return s

		case "async-flush":
			d.mu.Lock()
			cur := d.mu.versions.currentVersion()
			d.mu.Unlock()

			if _, err := d.AsyncFlush(); err != nil {
				return err.Error()
			}

			err := try(100*time.Microsecond, 20*time.Second, func() error {
				d.mu.Lock()
				defer d.mu.Unlock()
				if cur == d.mu.versions.currentVersion() {
					return errors.New("flush has not occurred")
				}
				return nil
			})
			if err != nil {
				return err.Error()
			}

			d.mu.Lock()
			s := d.mu.versions.currentVersion().String()
			d.mu.Unlock()
			return s

		case "reset":
			if err := d.Close(); err != nil {
				return err.Error()
			}
			d, err = Open("", getOptions())
			if err != nil {
				return err.Error()
			}
			return ""

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

// TestFlushDelRangeEmptyKey tests flushing a range tombstone that begins with
// an empty key. The empty key is a valid key but can be confused with nil.
func TestFlushDelRangeEmptyKey(t *testing.T) {
	d, err := Open("", &Options{FS: vfs.NewMem()})
	require.NoError(t, err)
	require.NoError(t, d.DeleteRange([]byte{}, []byte("z"), nil))
	require.NoError(t, d.Flush())
	require.NoError(t, d.Close())
}

// TestFlushEmptyKey tests that flushing an empty key does not trigger that key
// order invariant assertions.
func TestFlushEmptyKey(t *testing.T) {
	d, err := Open("", &Options{FS: vfs.NewMem()})
	require.NoError(t, err)
	require.NoError(t, d.Set(nil, []byte("hello"), nil))
	require.NoError(t, d.Flush())
	val, closer, err := d.Get(nil)
	require.NoError(t, err)
	require.Equal(t, val, []byte("hello"))
	require.NoError(t, closer.Close())
	require.NoError(t, d.Close())
}

// TestFlushWaitsForPublish verifies that a flush does not drop a key in favor
// of a newer key in the same flush whose batch has been applied but not yet
// published. A reader at a sequence number below the newer key -- a plain read,
// a snapshot or an EFOS created while the batch is unpublished -- must still
// see the older key, or the deletion it represents, both before and after the
// flush.
func TestFlushWaitsForPublish(t *testing.T) {
	defer leaktest.AfterTest(t)()

	scenarios := []struct {
		name string
		// setup writes the state of "k" that the blocked write shadows.
		setup func(t *testing.T, d *DB)
		// want is the value of "k" below the blocked write; "" means not found.
		want string
	}{
		{
			// "k" is deleted in the memtable over a value in L6. Dropping the
			// deletion resurrects the value.
			name: "delete-over-l6",
			setup: func(t *testing.T, d *DB) {
				require.NoError(t, d.Set([]byte("k"), []byte("ancient"), nil))
				require.NoError(t, d.Flush())
				require.NoError(t, d.Compact([]byte("a"), []byte("z"), false))
				require.NoError(t, d.Delete([]byte("k"), nil))
			},
			want: "",
		},
		{
			// "k" is set in the memtable. Dropping the value loses it.
			name: "set-in-memtable",
			setup: func(t *testing.T, d *DB) {
				require.NoError(t, d.Set([]byte("k"), []byte("old"), nil))
			},
			want: "old",
		},
	}
	// The blocked write of k=new is one of:
	//  - batch: a batch applied to the memtable;
	//  - large-batch: a batch queued as its own flushable, which shares its WAL
	//    with the memtable before it;
	//  - flushable-ingest: an ingested sstable that overlaps the memtable, which
	//    is queued as its own flushable.
	writers := []string{"batch", "large-batch", "flushable-ingest"}
	for _, sc := range scenarios {
		for _, writer := range writers {
			for _, reader := range []string{"get", "snapshot", "efos"} {
				t.Run(fmt.Sprintf("%s/%s/%s", sc.name, writer, reader), func(t *testing.T) {
					testFlushWaitsForPublish(t, sc.setup, sc.want, writer, reader)
				})
			}
		}
	}
}

func testFlushWaitsForPublish(
	t *testing.T, setup func(t *testing.T, d *DB), want string, writer, reader string,
) {
	blocker := newPublishBlocker()
	o := &Options{
		FS:                          vfs.NewMem(),
		DisableAutomaticCompactions: true,
		FormatMajorVersion:          FormatNewest,
		MemTableSize:                256 << 10,
		// The memtable rotation in AsyncFlush must not stall on the queued
		// large batch or flushable ingest, which cannot flush until this
		// goroutine lets it publish.
		MemTableStopWritesThreshold: 8,
	}
	o.private.testingBeforePublishFunc = blocker.hook()
	d, err := Open("", o)
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()

	get := func(r snapshotReader) string {
		t.Helper()
		v, closer, err := r.Get([]byte("k"))
		if errors.Is(err, ErrNotFound) {
			return ""
		}
		require.NoError(t, err)
		defer func() { require.NoError(t, closer.Close()) }()
		return string(v)
	}

	setup(t, d)
	if writer == "flushable-ingest" {
		f, err := o.FS.Create("ext", vfs.WriteCategoryUnspecified)
		require.NoError(t, err)
		w := sstable.NewWriter(objstorageprovider.NewFileWritable(f),
			d.opts.MakeWriterOptions(0, d.TableFormat()))
		require.NoError(t, w.Set([]byte("k"), []byte("new")))
		require.NoError(t, w.Close())
	}

	// Write k=new and block it in commitPipeline.publish: it has been applied
	// (and has dropped its memtable writer ref, if any) but visibleSeqNum is
	// below it.
	blocker.enable()
	commitDone := make(chan error, 1)
	go func() {
		switch writer {
		case "batch", "large-batch":
			b := d.NewBatch()
			require.NoError(t, b.Set([]byte("k"), []byte("new"), nil))
			if writer == "large-batch" {
				require.NoError(t, b.Set([]byte("pad"), make([]byte, o.MemTableSize), nil))
			}
			commitDone <- b.Commit(nil)
		case "flushable-ingest":
			commitDone <- d.Ingest(context.Background(), []string{"ext"})
		default:
			commitDone <- errors.Newf("unknown writer %q", writer)
		}
	}()
	<-blocker.entered
	if writer == "flushable-ingest" {
		d.mu.Lock()
		_, ok := d.mu.mem.queue[len(d.mu.mem.queue)-2].flushable.(*ingestedFlushable)
		d.mu.Unlock()
		require.True(t, ok, "ingest was not queued as a flushable")
	}
	require.Equal(t, want, get(d))

	// Request a flush and wait until no flush is running. A flush that ignored
	// the unpublished write would have completed by then.
	flushed, err := d.AsyncFlush()
	require.NoError(t, err)
	d.mu.Lock()
	for d.mu.compact.flushing {
		d.mu.compact.cond.Wait()
	}
	d.mu.Unlock()
	flushedBeforePublish := false
	select {
	case <-flushed:
		flushedBeforePublish = true
	default:
	}

	var r snapshotReader = d
	switch reader {
	case "get":
	case "snapshot":
		s := d.NewSnapshot()
		defer func() { require.NoError(t, s.Close()) }()
		r = s
	case "efos":
		s := d.NewEventuallyFileOnlySnapshot([]KeyRange{{Start: []byte("a"), End: []byte("z")}})
		defer func() { require.NoError(t, s.Close()) }()
		r = s
	default:
		t.Fatalf("unknown reader %q", reader)
	}
	require.Equal(t, want, get(r))
	require.False(t, flushedBeforePublish, "flush completed while a flushed write was unpublished")

	// Publishing the write lets the flush run.
	blocker.unblock()
	require.NoError(t, <-commitDone)
	select {
	case <-flushed:
	case <-time.After(30 * time.Second):
		t.Fatal("flush did not complete after the write was published")
	}
	require.Equal(t, "new", get(d))
	if reader != "get" {
		require.Equal(t, want, get(r))
		require.NoError(t, d.Compact([]byte("a"), []byte("z"), false))
		require.Equal(t, want, get(r))
	}
}
