// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
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
	b.Infof(format, args...)
	runtime.Goexit()
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

func (fs loggingFS) Lock(name string) (io.Closer, error) {
	fmt.Fprintf(fs.w, "lock: %s\n", name)
	return fs.FS.Lock(name)
}

type loggingFile struct {
	vfs.File
	name string
	w    io.Writer
}

func (f loggingFile) Close() error {
	fmt.Fprintf(f.w, "close: %s\n", f.name)
	return f.File.Close()
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

		case "close":
			buf.Reset()
			if err := d.Close(); err != nil {
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

		case "checkpoint":
			buf.Reset()
			if err := d.Checkpoint("checkpoint"); err != nil {
				return err.Error()
			}
			return buf.String()

		case "disable-file-deletions":
			buf.Reset()
			d.mu.Lock()
			d.disableFileDeletions()
			d.mu.Unlock()
			return buf.String()

		case "enable-file-deletions":
			buf.Reset()
			func() {
				defer func() {
					if r := recover(); r != nil {
						fmt.Fprint(&buf, r)
					}
				}()
				d.mu.Lock()
				defer d.mu.Unlock()
				d.enableFileDeletions()
			}()
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

func TestWriteStallEvents(t *testing.T) {
	const flushCount = 10
	const writeStallEnd = "write stall ending"

	testCases := []struct {
		delayFlush bool
		expected   string
	}{
		{true, "memtable count limit reached"},
		{false, "L0 file count limit exceeded"},
	}

	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			stallEnded := make(chan struct{}, 1)
			createReleased := make(chan struct{}, flushCount)
			var buf syncedBuffer
			var delayOnce sync.Once
			listener := EventListener{
				TableCreated: func(info TableCreateInfo) {
					if c.delayFlush == (info.Reason == "flushing") {
						delayOnce.Do(func() {
							<-createReleased
						})
					}
				},
				WriteStallBegin: func(info WriteStallBeginInfo) {
					fmt.Fprintln(&buf, info.String())
					createReleased <- struct{}{}
				},
				WriteStallEnd: func() {
					fmt.Fprintln(&buf, writeStallEnd)
					select {
					case stallEnded <- struct{}{}:
					default:
					}
				},
			}
			d, err := Open("db", &Options{
				EventListener:               listener,
				FS:                          vfs.NewMem(),
				MemTableStopWritesThreshold: 2,
				L0CompactionThreshold:       2,
				L0StopWritesThreshold:       2,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()

			for i := 0; i < flushCount; i++ {
				if err := d.Set([]byte("a"), nil, NoSync); err != nil {
					t.Fatal(err)
				}
				ch, err := d.AsyncFlush()
				if err != nil {
					t.Fatal(err)
				}
				// If we're delaying the flush (because we're testing for memtable
				// write stalls), we can't wait for the flush to finish as doing so
				// would deadlock. If we're not delaying the flush (because we're
				// testing for L0 write stals), we wait for the flush to finish so we
				// don't create too many memtables which would trigger a memtable write
				// stall.
				if !c.delayFlush {
					<-ch
				}
				if strings.Contains(buf.String(), c.expected) {
					break
				}
			}
			<-stallEnded

			events := buf.String()
			require.Contains(t, events, c.expected)
			require.Contains(t, events, writeStallEnd)
			if testing.Verbose() {
				t.Logf("\n%s", events)
			}
		})
	}
}
