// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/redact"
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

func (fs loggingFS) ReuseForWrite(oldname, newname string) (vfs.File, error) {
	fmt.Fprintf(fs.w, "reuseForWrite: %s -> %s\n", oldname, newname)
	f, err := fs.FS.ReuseForWrite(oldname, newname)
	if err == nil {
		f = loggingFile{f, newname, fs.w}
	}
	return f, err
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
	require.NoError(t, mem.MkdirAll("ext", 0755))

	datadriven.RunTest(t, "testdata/event_listener", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "open":
			buf.Reset()
			lel := MakeLoggingEventListener(&buf)
			opts := &Options{
				FS:                    loggingFS{mem, &buf},
				FormatMajorVersion:    FormatNewest,
				EventListener:         &lel,
				MaxManifestFileSize:   1,
				L0CompactionThreshold: 10,
				WALDir:                "wal",
			}
			// The table stats collector runs asynchronously and its
			// timing is less predictable. It increments nextJobID, which
			// can make these tests flaky. The TableStatsLoaded event is
			// tested separately in TestTableStats.
			opts.private.disableTableStats = true
			var err error
			d, err = Open("db", opts)
			if err != nil {
				return err.Error()
			}
			t := time.Now()
			d.timeNow = func() time.Time {
				t = t.Add(time.Second)
				return t
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
			if err := d.Compact([]byte("a"), []byte("b"), false); err != nil {
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
			w := sstable.NewWriter(f, sstable.WriterOptions{
				TableFormat: d.FormatMajorVersion().MaxTableFormat(),
			})
			if err := w.Add(base.MakeInternalKey([]byte("a"), 0, InternalKeyKindSet), nil); err != nil {
				return err.Error()
			}
			if err := w.Close(); err != nil {
				return err.Error()
			}
			if err := d.Ingest([]string{"ext/0"}); err != nil {
				return err.Error()
			}
			return buf.String()

		case "metrics":
			// The asynchronous loading of table stats can change metrics, so
			// wait for all the tables' stats to be loaded.
			d.mu.Lock()
			d.waitTableStats()
			d.mu.Unlock()

			return d.Metrics().String()

		case "sstables":
			var buf bytes.Buffer
			tableInfos, _ := d.SSTables()
			for i, level := range tableInfos {
				if len(level) == 0 {
					continue
				}
				fmt.Fprintf(&buf, "%d:\n", i)
				for _, m := range level {
					fmt.Fprintf(&buf, "  %d:[%s-%s]\n",
						m.FileNum, m.Smallest.UserKey, m.Largest.UserKey)
				}
			}
			return buf.String()

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
			listener := &EventListener{
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
				MemTableSize:                initialMemTableSize,
				MemTableStopWritesThreshold: 2,
				L0CompactionThreshold:       2,
				L0StopWritesThreshold:       2,
			})
			require.NoError(t, err)
			defer d.Close()

			for i := 0; i < flushCount; i++ {
				require.NoError(t, d.Set([]byte("a"), nil, NoSync))

				ch, err := d.AsyncFlush()
				require.NoError(t, err)

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

type redactLogger struct {
	logger Logger
}

// Infof implements the Logger.Infof interface.
func (l redactLogger) Infof(format string, args ...interface{}) {
	l.logger.Infof("%s", redact.Sprintf(format, args...).Redact())
}

// Fatalf implements the Logger.Fatalf interface.
func (l redactLogger) Fatalf(format string, args ...interface{}) {
	l.logger.Fatalf("%s", redact.Sprintf(format, args...).Redact())
}

func TestEventListenerRedact(t *testing.T) {
	// The vast majority of event listener fields logged are safe and do not
	// need to be redacted. Verify that the rare, unsafe error does appear in
	// the log redacted.
	var log syncedBuffer
	l := MakeLoggingEventListener(redactLogger{logger: &log})
	l.WALDeleted(WALDeleteInfo{
		JobID:   5,
		FileNum: FileNum(20),
		Err:     errors.Errorf("unredacted error: %s", "unredacted string"),
	})
	require.Equal(t, "[JOB 5] WAL delete error: unredacted error: ‹×›\n", log.String())
}

func TestEventListenerEnsureDefaultsBackgroundError(t *testing.T) {
	e := EventListener{}
	e.EnsureDefaults(nil)
	e.BackgroundError(errors.New("an example error"))
}

func TestEventListenerEnsureDefaultsSetsAllCallbacks(t *testing.T) {
	e := EventListener{}
	e.EnsureDefaults(nil)
	testAllCallbacksSetInEventListener(t, e)
}

func TestMakeLoggingEventListenerSetsAllCallbacks(t *testing.T) {
	e := MakeLoggingEventListener(nil)
	testAllCallbacksSetInEventListener(t, e)
}

func TestTeeEventListenerSetsAllCallbacks(t *testing.T) {
	e := TeeEventListener(EventListener{}, EventListener{})
	testAllCallbacksSetInEventListener(t, e)
}

func testAllCallbacksSetInEventListener(t *testing.T, e EventListener) {
	t.Helper()
	v := reflect.ValueOf(e)
	for i := 0; i < v.NumField(); i++ {
		fType := v.Type().Field(i)
		fVal := v.Field(i)
		require.Equal(t, reflect.Func, fType.Type.Kind(), "unexpected non-func field: %s", fType.Name)
		require.False(t, fVal.IsNil(), "unexpected nil field: %s", fType.Name)
	}
}
