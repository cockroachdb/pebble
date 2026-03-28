// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package testutils

import (
	"testing"

	"github.com/cockroachdb/errors"
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
	// Use panic instead of l.T.Fatalf. t.Fatalf calls runtime.Goexit which only
	// terminates the calling goroutine. When Logger.Fatalf is called from a
	// background goroutine (compaction, flush), Goexit silently kills that
	// goroutine without decrementing DB internal counters (compactingCount,
	// flushing), causing Close to hang. panic propagates across goroutines and
	// crashes the process with a clear stack trace, matching production behavior
	// (DefaultLogger.Fatalf calls os.Exit).
	panic(errors.AssertionFailedf(format, args...))
}
