// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/petermattis/pebble/internal/base"
	"github.com/petermattis/pebble/internal/datadriven"
	"github.com/petermattis/pebble/sstable"
	"github.com/petermattis/pebble/vfs"
	"github.com/stretchr/testify/require"
)

type syncedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncedBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Reset()
}

func (b *syncedBuffer) Write(p []byte) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncedBuffer) Infof(format string, args ...interface{}) {
	s := fmt.Sprintf(format, args...)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Write([]byte(s))
	if n := len(s); n == 0 || s[n-1] != '\n' {
		b.buf.Write([]byte("\n"))
	}
}

func (b *syncedBuffer) Fatalf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

func (b *syncedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type loggingFS struct {
	vfs.FS
	w io.Writer
}

func (fs loggingFS) Create(name string) (vfs.File, error) {
	fmt.Fprintf(fs.w, "create: %s\n", name)
	f, err := fs.FS.Create(name)
	if err != nil {
		return nil, err
	}
	return loggingFile{f, name, fs.w}, nil
}

func (fs loggingFS) Link(oldname, newname string) error {
	fmt.Fprintf(fs.w, "link: %s -> %s\n", oldname, newname)
	return fs.FS.Link(oldname, newname)
}

func (fs loggingFS) OpenDir(name string) (vfs.File, error) {
	fmt.Fprintf(fs.w, "open-dir: %s\n", name)
	f, err := fs.FS.OpenDir(name)
	if err != nil {
		return nil, err
	}
	return loggingFile{f, name, fs.w}, nil
}

func (fs loggingFS) Rename(oldname, newname string) error {
	fmt.Fprintf(fs.w, "rename: %s -> %s\n", oldname, newname)
	return fs.FS.Rename(oldname, newname)
}

func (fs loggingFS) MkdirAll(dir string, perm os.FileMode) error {
	fmt.Fprintf(fs.w, "mkdir-all: %s %#o\n", dir, perm)
	return fs.FS.MkdirAll(dir, perm)
}

type loggingFile struct {
	vfs.File
	name string
	w    io.Writer
}

func (f loggingFile) Sync() error {
	fmt.Fprintf(f.w, "sync: %s\n", f.name)
	return f.File.Sync()
}

// Verify event listener actions, as well as expected filesystem operations.
func TestEventListener(t *testing.T) {
	var d *DB
	var buf syncedBuffer
	mem := vfs.NewMem()
	err := mem.MkdirAll("ext", 0755)
	if err != nil {
		t.Fatal(err)
	}

	datadriven.RunTest(t, "testdata/event_listener", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "open":
			buf.Reset()
			var err error
			d, err = Open("db", &Options{
				FS:                  loggingFS{mem, &buf},
				EventListener:       MakeLoggingEventListener(&buf),
				MaxManifestFileSize: 1,
				WALDir:              "wal",
			})
			if err != nil {
				return err.Error()
			}
			return buf.String()

		case "flush":
			buf.Reset()
			if err := d.Set([]byte("a"), nil, nil); err != nil {
				return err.Error()
			}
			if err := d.Flush(); err != nil {
				return err.Error()
			}
			return buf.String()

		case "compact":
			buf.Reset()
			if err := d.Set([]byte("a"), nil, nil); err != nil {
				return err.Error()
			}
			if err := d.Compact([]byte("a"), []byte("b")); err != nil {
				return err.Error()
			}
			return buf.String()

		case "ingest":
			buf.Reset()
			f, err := mem.Create("ext/0")
			if err != nil {
				return err.Error()
			}
			w := sstable.NewWriter(f, nil, LevelOptions{})
			if err := w.Add(base.MakeInternalKey([]byte("a"), 0, InternalKeyKindSet), nil); err != nil {
				return err.Error()
			}
			if err := w.Close(); err != nil {
				return err.Error()
			}
			if err := d.Ingest([]string{"ext/0"}); err != nil {
				return err.Error()
			}
			if err := mem.Remove("ext/0"); err != nil {
				return err.Error()
			}
			return buf.String()

		case "metrics":
			return d.Metrics().String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

type createDelayingFS struct {
	vfs.FS
	index          int32         // index of sstable whose creation will be delayed
	createReleased chan struct{} // when closed, the delayed sstable creation can proceed
	createDelayed  chan struct{} // if non-nil, will be closed before delaying sstable creation
}

func (fs *createDelayingFS) Create(name string) (vfs.File, error) {
	if strings.HasSuffix(name, ".sst") && atomic.AddInt32(&fs.index, -1) == -1 {
		if fs.createDelayed != nil {
			close(fs.createDelayed)
		}
		<-fs.createReleased
	}
	return fs.FS.Create(name)
}

// Verify `WriteStallBegin()` and `WriteStallEnd()` events for stalls triggered
// for reaching memtable count limit.
func TestWriteStallForMemTableLimit(t *testing.T) {
	const memTableLimitReason = "memtable count limit reached"
	const memTableLimit = 2

	stallBegan := make(chan struct{})
	stallEnded := make(chan struct{})
	mem := vfs.NewMem()
	// `customFS` delays the sstable with index 0, which is the first flushed sstable.
	// Delay until writes stall, which we trigger by calling `AsyncFlush()` again while
	// the first flush is still delayed.
	customFS := createDelayingFS{
		FS:             mem,
		index:          0,
		createReleased: stallBegan,
		createDelayed:  nil,
	}
	listener := EventListener{
		WriteStallBegin: func(info WriteStallBeginInfo) {
			require.EqualValues(t, memTableLimitReason, info.Reason)
			close(stallBegan)
		},
		WriteStallEnd: func() {
			close(stallEnded)
		},
	}
	d, err := Open("db", &Options{
		EventListener:               listener,
		FS:                          &customFS,
		MemTableStopWritesThreshold: memTableLimit,
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < memTableLimit; i++ {
		if err := d.Set([]byte("a"), nil, NoSync); err != nil {
			t.Fatal(err)
		}
		_, err := d.AsyncFlush()
		if err != nil {
			t.Fatal(err)
		}
	}
	<-stallBegan
	<-stallEnded
}

// Verify `WriteStallBegin()` and `WriteStallEnd()` events for stalls triggered
// for reaching L0 file count limit.
func TestWriteStallForL0FileLimit(t *testing.T) {
	const l0FileLimitReason = "L0 file count limit exceeded"
	const (
		l0FileCompactionTrigger = 2
		l0FileStopTrigger       = 4
	)

	stallBegan := make(chan struct{})
	stallEnded := make(chan struct{})
	createDelayed := make(chan struct{})
	mem := vfs.NewMem()
	// The indexes of the first few sstables are as follows:
	//
	// * index 0: first flushed sstable
	// * index 1: second flushed sstable
	// * index 2: first sstable created by L0->Lbase compaction
	//
	// `customFS` delays the sstable with index 2, i.e., an Lbase file created during
	// compaction. Delay until writes stall, which we trigger by calling `Flush()`
	// repeatedly while the L0->Lbase compaction is still delayed.
	customFS := createDelayingFS{mem, l0FileCompactionTrigger /* index */, stallBegan, /* createReleased */
		createDelayed}
	listener := EventListener{
		WriteStallBegin: func(info WriteStallBeginInfo) {
			require.EqualValues(t, l0FileLimitReason, info.Reason)
			close(stallBegan)
		},
		WriteStallEnd: func() {
			close(stallEnded)
		},
	}
	d, err := Open("db", &Options{
		EventListener:         listener,
		FS:                    &customFS,
		L0CompactionThreshold: l0FileCompactionTrigger,
		L0StopWritesThreshold: l0FileStopTrigger,
	})
	if err != nil {
		t.Fatal(err)
	}

	// TODO(ajkr): Are the two files pending L0->Lbase not counted towards the
	// limit? Shouldn't they be?
	for i := 0; i < l0FileStopTrigger+l0FileCompactionTrigger; i++ {
		if err := d.Set([]byte("a"), nil, NoSync); err != nil {
			t.Fatal(err)
		}
		d.Flush()
		if i == l0FileCompactionTrigger-1 {
			// make sure compaction gets started on the Lbase file before
			// we proceed to flushing the next sstable (we wouldn't want the
			// delay to be applied on an L0 file accidentally).
			<-createDelayed
		}
	}
	<-stallBegan
	<-stallEnded
}
