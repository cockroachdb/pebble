// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package testutils

import "testing"

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
	l.T.Helper()
	l.T.Fatalf(format, args...)
}
