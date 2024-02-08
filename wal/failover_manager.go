// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package wal

import (
	"sync"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

type dirIndex int

const (
	primaryDirIndex dirIndex = iota
	secondaryDirIndex
	numDirIndices
)

type dirAndFileHandle struct {
	Dir
	vfs.File
}

// switchableWriter is a subset of failoverWriter needed by failoverMonitor.
type switchableWriter interface {
	switchToNewDir(dirAndFileHandle) error
	ongoingLatencyOrErrorForCurDir() (time.Duration, error)
}

type stopper struct {
	quiescer chan struct{} // Closed when quiescing
	wg       sync.WaitGroup
}

func newStopper() *stopper {
	return &stopper{
		quiescer: make(chan struct{}),
	}
}

func (s *stopper) runAsync(f func()) {
	s.wg.Add(1)
	go func() {
		f()
		s.wg.Done()
	}()
}

// shouldQuiesce returns a channel which will be closed when stop() has been
// invoked and outstanding goroutines should begin to quiesce.
func (s *stopper) shouldQuiesce() <-chan struct{} {
	return s.quiescer
}

func (s *stopper) stop() {
	close(s.quiescer)
	s.wg.Wait()
}

// Make lint happy.
var _ = (&stopper{}).shouldQuiesce
