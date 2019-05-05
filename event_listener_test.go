// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/vfs"
)

type syncedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
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

func (b *syncedBuffer) Infof(format string, args ...interface{}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Write([]byte(fmt.Sprintf(format+"\n", args...)))
}

func (b *syncedBuffer) Fatalf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

func TestEventListener(t *testing.T) {
	testCases := []struct {
		useCustomListener bool
		expected          string
	}{
		{false, `[JOB 2] Created WAL #5
[JOB 3] Flushing
[JOB 3] Flushed sstable #6
[JOB 4] Compacting 1@0 sstables to L1
[JOB 4] Compacted to L1; 1 new sstables
[JOB 5] Created WAL #7 by recycling #3
[JOB 6] Flushing
[JOB 6] Flushed sstable #8
[JOB 7] Compacting 1@0 and 1@1 sstables to L1
[JOB 7] Compacted to L1; 0 new sstables
[JOB 7] Deleted sstable #6
[JOB 7] Deleted sstable #8
`},
		{true, `#2: WAL created: 5 recycled=0
#3: flush begin
#3: flush end: 6
#4: compaction begin: L0 -> L1
#4: compaction end: L0 -> L1
#5: WAL created: 7 recycled=3
#6: flush begin
#6: flush end: 8
#7: compaction begin: L0 -> L1
#7: compaction end: L0 -> L1
#7: table deleted: 6
#7: table deleted: 8
`},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("useCustomListener=%t", tc.useCustomListener), func(t *testing.T) {
			opts := &db.Options{VFS: vfs.NewMem()}
			var buf syncedBuffer
			if tc.useCustomListener {
				opts.EventListener = &db.EventListener{
					CompactionBegin: func(info db.CompactionInfo) {
						fmt.Fprintf(&buf, "#%d: compaction begin: L%d -> L%d\n", info.JobID,
							info.Input.Level, info.Input.Level+1)
					},
					CompactionEnd: func(info db.CompactionInfo) {
						fmt.Fprintf(&buf, "#%d: compaction end: L%d -> L%d\n", info.JobID,
							info.Input.Level, info.Input.Level+1)
					},
					FlushBegin: func(info db.FlushInfo) {
						fmt.Fprintf(&buf, "#%d: flush begin\n", info.JobID)
					},
					FlushEnd: func(info db.FlushInfo) {
						fmt.Fprintf(&buf, "#%d: flush end: %d\n", info.JobID, info.Output.FileNum)
					},
					TableDeleted: func(info db.TableDeleteInfo) {
						fmt.Fprintf(&buf, "#%d: table deleted: %d\n", info.JobID, info.FileNum)
					},
					TableIngested: func(info db.TableIngestInfo) {
						fmt.Fprintf(&buf, "#%d: table ingested\n", info.JobID)
					},
					WALCreated: func(info db.WALCreateInfo) {
						fmt.Fprintf(&buf, "#%d: WAL created: %d recycled=%d\n",
							info.JobID, info.FileNum, info.RecycledFileNum)
					},
					WALDeleted: func(info db.WALDeleteInfo) {
						fmt.Fprintf(&buf, "#%d: WAL deleted: %d\n", info.JobID, info.FileNum)
					},
				}
			} else {
				opts.Logger = &buf
			}
			d, err := Open("", opts)
			if err != nil {
				t.Fatal(err)
			}

			if err := d.Set([]byte("a"), nil, nil); err != nil {
				t.Fatal(err)
			}
			if err := d.Flush(); err != nil {
				t.Fatal(err)
			}
			if err := d.Compact([]byte("a"), []byte("b")); err != nil {
				t.Fatal(err)
			}
			if err := d.Delete([]byte("a"), nil); err != nil {
				t.Fatal(err)
			}
			if err := d.Compact([]byte("a"), []byte("b")); err != nil {
				t.Fatal(err)
			}
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}

			if v := buf.String(); tc.expected != v {
				t.Fatalf("expected\n%s\nbut found\n%s", tc.expected, v)
			}
		})
	}
}
