// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"io"
	"sync"
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

type writeDelayingFS struct {
	vfs.FS
	writesReleased chan struct{}
}

func (fs writeDelayingFS) Create(name string) (vfs.File, error) {
	f, err := fs.FS.Create(name)
	if err != nil {
		return nil, err
	}
	if fs.writesReleased == nil {
		return f, nil
	}
	return writeDelayingFile{f, fs.writesReleased}, nil
}

type writeDelayingFile struct {
	vfs.File
	writesReleased chan struct{}
}

func (f writeDelayingFile) Write(p []byte) (n int, err error) {
	<-f.writesReleased
	return f.File.Write(p)
}

// Verify `WriteStallBegin()` and `WriteStallEnd()` events. The setup logic
// is a bit different so it is tested separately from the other events for now.
func TestWriteStallEvents(t *testing.T) {
	mem := vfs.NewMem()
	customFS := writeDelayingFS{mem, nil /* writesReleased */}

	stallBegan := make(chan struct{})
	stallEnded := make(chan struct{})
	listener := EventListener{
		WriteStallBegin: func(info WriteStallBeginInfo) {
			require.EqualValues(t, "memtable count limit reached", info.Reason)
			close(stallBegan)
		},
		WriteStallEnd: func() {
			close(stallEnded)
		},
	}
	d, err := Open("db", &Options{
		DisableWAL:                  true,
		EventListener:               listener,
		FS:                          customFS,
		MemTableStopWritesThreshold: 2,
	})
	if err != nil {
		t.Fatal(err)
	}

	// from now on delay writes until stall happens. Since WAL is disabled this results
	// in delaying manifest writes, which blocks the flush thread. That causes memtables
	// to accumulate, eventually resulting in a stall.
	customFS.writesReleased = stallBegan
	var flushesDone []chan struct{}
	for i := 0; i < 2; i++ {
		flushDone, err := d.AsyncFlush()
		if err != nil {
			t.Fatal(err)
		}
		flushesDone = append(flushesDone, flushDone)
	}
	<-stallBegan
	<-stallEnded
	for _, flushDone := range flushesDone {
		<-flushDone
	}
}
