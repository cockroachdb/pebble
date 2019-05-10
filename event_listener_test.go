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

	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/internal/datadriven"
	"github.com/petermattis/pebble/sstable"
	"github.com/petermattis/pebble/vfs"
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

func newLoggingEventListener(w io.Writer) *db.EventListener {
	return &db.EventListener{
		CompactionBegin: func(info db.CompactionInfo) {
			fmt.Fprintf(w, "#%d: compaction begin: L%d -> L%d\n", info.JobID,
				info.Input.Level, info.Input.Level+1)
		},
		CompactionEnd: func(info db.CompactionInfo) {
			fmt.Fprintf(w, "#%d: compaction end: L%d -> L%d\n", info.JobID,
				info.Input.Level, info.Input.Level+1)
		},
		FlushBegin: func(info db.FlushInfo) {
			fmt.Fprintf(w, "#%d: flush begin\n", info.JobID)
		},
		FlushEnd: func(info db.FlushInfo) {
			fmt.Fprintf(w, "#%d: flush end: %d\n", info.JobID, info.Output.FileNum)
		},
		TableDeleted: func(info db.TableDeleteInfo) {
			fmt.Fprintf(w, "#%d: table deleted: %d\n", info.JobID, info.FileNum)
		},
		TableIngested: func(info db.TableIngestInfo) {
			fmt.Fprintf(w, "#%d: table ingested\n", info.JobID)
		},
		WALCreated: func(info db.WALCreateInfo) {
			fmt.Fprintf(w, "#%d: WAL created: %d recycled=%d\n",
				info.JobID, info.FileNum, info.RecycledFileNum)
		},
		WALDeleted: func(info db.WALDeleteInfo) {
			fmt.Fprintf(w, "#%d: WAL deleted: %d\n", info.JobID, info.FileNum)
		},
	}
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
			d, err = Open("db", &db.Options{
				FS:                  loggingFS{mem, &buf},
				EventListener:       newLoggingEventListener(&buf),
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
			w := sstable.NewWriter(f, nil, db.LevelOptions{})
			if err := w.Add(db.MakeInternalKey([]byte("a"), 0, db.InternalKeyKindSet), nil); err != nil {
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

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}
