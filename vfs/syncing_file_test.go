// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

import (
	"bytes"
	"fmt"
	"os"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSyncingFile(t *testing.T) {
	const mb = 1 << 20

	tmpf, err := os.CreateTemp("", "pebble-db-syncing-file-")
	require.NoError(t, err)

	filename := tmpf.Name()
	require.NoError(t, tmpf.Close())
	defer os.Remove(filename)

	f, err := Default.Create(filename)
	require.NoError(t, err)

	s := NewSyncingFile(f, SyncingFileOptions{})
	if s == f {
		t.Fatalf("failed to wrap: %p != %p", f, s)
	}
	s = NewSyncingFile(f, SyncingFileOptions{BytesPerSync: 8 << 10 /* 8 KB */})
	s = s.(*fdFileWrapper).File
	s.(*syncingFile).fd = 1
	s.(*syncingFile).syncTo = func(offset int64) error {
		s.(*syncingFile).ratchetSyncOffset(offset)
		return nil
	}

	t.Logf("sync_file_range=%t", s.(*syncingFile).useSyncRange)

	testCases := []struct {
		n              int64
		expectedSyncTo int64
	}{
		{mb, -1},
		{mb, mb},
		{4 << 10, mb},
		{4 << 10, mb + 8<<10},
		{8 << 10, mb + 16<<10},
		{16 << 10, mb + 32<<10},
	}
	for i, c := range testCases {
		_, err := s.Write(make([]byte, c.n))
		require.NoError(t, err)

		syncTo := atomic.LoadInt64(&s.(*syncingFile).atomic.syncOffset)
		if c.expectedSyncTo != syncTo {
			t.Fatalf("%d: expected sync to %d, but found %d", i, c.expectedSyncTo, syncTo)
		}
	}
}

func TestSyncingFileClose(t *testing.T) {
	testCases := []struct {
		syncToEnabled bool
		expected      string
	}{
		{true, `sync-to(1048576): test [<nil>]
sync-to(2097152): test [<nil>]
sync-to(3145728): test [<nil>]
pre-close: test [offset=4194304 sync-offset=3145728]
sync: test [<nil>]
close: test [<nil>]
`},
		// When SyncFileRange is not being used, the last sync call ends up syncing
		// all of the data causing syncingFile.Close to elide the sync.
		{false, `sync: test [<nil>]
sync: test [<nil>]
pre-close: test [offset=4194304 sync-offset=4194304]
close: test [<nil>]
`},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			tmpf, err := os.CreateTemp("", "pebble-db-syncing-file-")
			require.NoError(t, err)

			filename := tmpf.Name()
			require.NoError(t, tmpf.Close())
			defer os.Remove(filename)

			f, err := Default.Create(filename)
			require.NoError(t, err)

			var buf bytes.Buffer
			lf := loggingFile{f, "test", &buf}

			s := NewSyncingFile(lf, SyncingFileOptions{BytesPerSync: 8 << 10 /* 8 KB */}).(*syncingFile)
			if c.syncToEnabled {
				s.fd = 1
				s.syncData = lf.Sync
				s.syncTo = func(offset int64) error {
					s.ratchetSyncOffset(offset)
					fmt.Fprintf(lf.w, "sync-to(%d): %s [%v]\n", offset, lf.name, err)
					return nil
				}
			} else {
				s.fd = 0
			}

			write := func(n int64) {
				t.Helper()
				_, err := s.Write(make([]byte, n))
				require.NoError(t, err)
			}

			const mb = 1 << 20
			write(2 * mb)
			write(mb)
			write(mb)

			fmt.Fprintf(lf.w, "pre-close: %s [offset=%d sync-offset=%d]\n",
				lf.name, atomic.LoadInt64(&s.atomic.offset), atomic.LoadInt64(&s.atomic.syncOffset))
			require.NoError(t, s.Close())

			if s := buf.String(); c.expected != s {
				t.Fatalf("expected\n%s\nbut found\n%s", c.expected, s)
			}
		})
	}
}

func TestSyncingFileNoSyncOnClose(t *testing.T) {
	testCases := []struct {
		useSyncRange bool
		expectBefore int64
		expectAfter  int64
	}{
		{false, 2 << 20, 2 << 20},
		{true, 2 << 20, 3<<20 + 128},
	}

	for _, c := range testCases {
		t.Run(fmt.Sprintf("useSyncRange=%v", c.useSyncRange), func(t *testing.T) {
			tmpf, err := os.CreateTemp("", "pebble-db-syncing-file-")
			require.NoError(t, err)

			filename := tmpf.Name()
			require.NoError(t, tmpf.Close())
			defer os.Remove(filename)

			f, err := Default.Create(filename)
			require.NoError(t, err)

			var buf bytes.Buffer
			lf := loggingFile{f, "test", &buf}

			s := NewSyncingFile(lf, SyncingFileOptions{NoSyncOnClose: true, BytesPerSync: 8 << 10}).(*syncingFile)
			s.useSyncRange = c.useSyncRange

			write := func(n int64) {
				t.Helper()
				_, err := s.Write(make([]byte, n))
				require.NoError(t, err)
			}

			const mb = 1 << 20
			write(2 * mb) // Sync first 2MB
			write(mb)     // No sync because syncToOffset = 3M-1M = 2M
			write(128)    // No sync for the same reason

			syncToBefore := atomic.LoadInt64(&s.atomic.syncOffset)
			require.NoError(t, s.Close())
			syncToAfter := atomic.LoadInt64(&s.atomic.syncOffset)

			if syncToBefore != c.expectBefore || syncToAfter != c.expectAfter {
				t.Fatalf("Expected syncTo before and after closing are %d %d but found %d %d",
					c.expectBefore, c.expectAfter, syncToBefore, syncToAfter)
			}
		})
	}
}

func BenchmarkSyncWrite(b *testing.B) {
	const targetSize = 16 << 20

	var wsizes []int
	if testing.Verbose() {
		wsizes = []int{64, 512, 1 << 10, 2 << 10, 4 << 10, 8 << 10, 16 << 10, 32 << 10}
	} else {
		wsizes = []int{64}
	}

	run := func(b *testing.B, wsize int, newFile func(string) File) {
		tmpf, err := os.CreateTemp("", "pebble-db-syncing-file-")
		if err != nil {
			b.Fatal(err)
		}
		filename := tmpf.Name()
		_ = tmpf.Close()
		defer os.Remove(filename)

		var f File
		var size int
		buf := make([]byte, wsize)

		b.SetBytes(int64(len(buf)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if f == nil {
				b.StopTimer()
				f = newFile(filename)
				size = 0
				b.StartTimer()
			}
			if _, err := f.Write(buf); err != nil {
				b.Fatal(err)
			}
			if err := f.Sync(); err != nil {
				b.Fatal(err)
			}
			size += len(buf)
			if size >= targetSize {
				_ = f.Close()
				f = nil
			}
		}
		b.StopTimer()
	}

	b.Run("no-prealloc", func(b *testing.B) {
		for _, wsize := range wsizes {
			b.Run(fmt.Sprintf("wsize=%d", wsize), func(b *testing.B) {
				run(b, wsize, func(filename string) File {
					_ = os.Remove(filename)
					t, err := os.Create(filename)
					if err != nil {
						b.Fatal(err)
					}
					return NewSyncingFile(t, SyncingFileOptions{PreallocateSize: 0})
				})
			})
		}
	})

	b.Run("prealloc-4MB", func(b *testing.B) {
		for _, wsize := range wsizes {
			b.Run(fmt.Sprintf("wsize=%d", wsize), func(b *testing.B) {
				run(b, wsize, func(filename string) File {
					_ = os.Remove(filename)
					t, err := os.Create(filename)
					if err != nil {
						b.Fatal(err)
					}
					return NewSyncingFile(t, SyncingFileOptions{PreallocateSize: 4 << 20})
				})
			})
		}
	})

	b.Run("reuse", func(b *testing.B) {
		for _, wsize := range wsizes {
			b.Run(fmt.Sprintf("wsize=%d", wsize), func(b *testing.B) {
				init := true
				run(b, wsize, func(filename string) File {
					if init {
						init = false

						t, err := os.OpenFile(filename, os.O_RDWR, 0755)
						if err != nil {
							b.Fatal(err)
						}
						if _, err := t.Write(make([]byte, targetSize)); err != nil {
							b.Fatal(err)
						}
						if err := t.Sync(); err != nil {
							b.Fatal(err)
						}
						t.Close()
					}

					t, err := os.OpenFile(filename, os.O_RDWR, 0755)
					if err != nil {
						b.Fatal(err)
					}
					return NewSyncingFile(t, SyncingFileOptions{PreallocateSize: 0})
				})
			})
		}
	})
}
