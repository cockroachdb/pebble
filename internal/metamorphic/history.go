// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"io"
	"log"

	"github.com/cockroachdb/pebble"
)

// history records the results of running a series of operations.
type history struct {
	log *log.Logger
}

func newHistory(writers ...io.Writer) *history {
	h := &history{}
	h.log = log.New(io.MultiWriter(writers...), "", 0)
	return h
}

// Recordf records the results of a single operation.
func (h *history) Recordf(format string, args ...interface{}) {
	h.log.Printf(format, args...)
}

// Logger returns a pebble.Logger that will output to history.
func (h *history) Logger() pebble.Logger {
	return &historyLogger{log: h.log}
}

// historyLogger is an implementation of the pebble.Logger interface which
// outputs to a stdlib logger, prefixing the log messages with "//"-style
// comments.
type historyLogger struct {
	log *log.Logger
}

// Infof implements the pebble.Logger interface. Note that the output is
// commented.
func (h *historyLogger) Infof(format string, args ...interface{}) {
	// TODO(peter): Prefix every line with "// INFO:".
	h.log.Printf("// INFO: "+format, args...)
}

// Fatalf implements the pebble.Logger interface. Note that the output is
// commented.
func (h *historyLogger) Fatalf(format string, args ...interface{}) {
	// TODO(peter): Prefix every line with "// FATAL:".
	h.log.Printf("// FATAL: "+format, args...)
}
