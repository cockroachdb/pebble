// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package testutils

import (
	"fmt"
	"os"
	"runtime"
	"testing"
)

// Logger is a logger that writes to a testing.TB.
type Logger struct {
	T testing.TB
}

func (l Logger) Infof(format string, args ...interface{}) {
	l.T.Logf(format, args...)
}

func (l Logger) Errorf(format string, args ...interface{}) {
	l.T.Logf(format, args...)
}

func (l Logger) Fatalf(format string, args ...interface{}) {
	// Print the error and all goroutine stacks, then terminate the process.
	// This matches DefaultLogger.Fatalf (which calls os.Exit) rather than using
	// t.Fatalf (which calls runtime.Goexit). Goexit only terminates the calling
	// goroutine; when Logger.Fatalf is called from a background goroutine
	// (compaction, flush), this silently kills that goroutine without cleaning
	// up DB state, causing Close to hang indefinitely. See #5780.
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	fmt.Fprintf(os.Stderr, "testutils.Logger.Fatalf(%s): %s\n%s\n",
		l.T.Name(), fmt.Sprintf(format, args...), buf[:n])
	os.Exit(1)
}
