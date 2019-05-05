// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync/atomic"
	"testing"
)

func TestSyncingFile(t *testing.T) {
	const mb = 1 << 20

	tmpf, err := ioutil.TempFile("", "pebble-db-syncing-file-")
	if err != nil {
		t.Fatal(err)
	}
	filename := tmpf.Name()
	defer os.Remove(filename)

	f, err := Default.Create(filename)
	if err != nil {
		t.Fatal(err)
	}
	s := NewSyncingFile(f, SyncingFileOptions{})
	if s == f {
		t.Fatalf("failed to wrap: %p != %p", f, s)
	}
	s = NewSyncingFile(f, SyncingFileOptions{BytesPerSync: 8 << 10 /* 8 KB */})
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
		{mb, 0},
		{mb, mb},
		{4 << 10, mb},
		{4 << 10, mb + 8<<10},
		{8 << 10, mb + 16<<10},
		{16 << 10, mb + 32<<10},
	}
	for i, c := range testCases {
		if _, err := s.Write(make([]byte, c.n)); err != nil {
			t.Fatal(err)
		}
		syncTo := atomic.LoadInt64(&s.(*syncingFile).atomic.syncOffset)
		if c.expectedSyncTo != syncTo {
			t.Fatalf("%d: expected sync to %d, but found %d", i, c.expectedSyncTo, syncTo)
		}
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
		tmpf, err := ioutil.TempFile("", "pebble-db-syncing-file-")
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
