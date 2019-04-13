// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package storage

import (
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
	s := NewSyncingFile(f, 0)
	if s != f {
		t.Fatalf("unexpected wrapping: %p != %p", f, s)
	}
	s = NewSyncingFile(f, 8<<10 /* 8 KB */)
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
