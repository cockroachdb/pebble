// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

func TestDBFinalizerCalled(t *testing.T) {
	var mm sync.Mutex
	var dbFinalizerRun *sync.Cond = sync.NewCond(&mm)
	// Wait until the finalizer is run.
	doneCh := make(chan bool)
	go func() {
		dbFinalizerRun.L.Lock()
		dbFinalizerRun.Wait()
		dbFinalizerRun.L.Unlock()
		doneCh <- true
	}()

	{
		mem := vfs.NewMem()
		d, err := Open("", &Options{
			FS:              mem,
			DBFinalizerCond: dbFinalizerRun,
		})
		if err != nil {
			t.Errorf("DB didn't open.")
		}
		d.closed.Load()
	}

	runtime.GC()

	select {
	case <-doneCh:
	case <-time.After(time.Second * 10):
		t.Errorf("DB finalizer never ran.")
	}
}
