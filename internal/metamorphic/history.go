// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"fmt"
	"io"
	"log"
	"regexp"
	"strings"

	"github.com/cockroachdb/pebble"
)

// history records the results of running a series of operations.
type history struct {
	failed bool
	failRE *regexp.Regexp
	log    *log.Logger
	seq    int
}

func newHistory(failRE string, writers ...io.Writer) *history {
	h := &history{}
	if len(failRE) > 0 {
		h.failRE = regexp.MustCompile(failRE)
	}
	h.log = log.New(io.MultiWriter(writers...), "", 0)
	return h
}

// Recordf records the results of a single operation.
func (h *history) Recordf(format string, args ...interface{}) {
	if strings.Contains(format, "\n") {
		// We could remove this restriction but suffixing every line with "#<seq>".
		panic(fmt.Sprintf("format string must not contain \\n: %q", format))
	}

	// We suffix every line with #<seq> in order to provide a marker to locate
	// the line using the diff output. This is necessary because the diff of two
	// histories is done after stripping comment lines (`// ...`) from the
	// history output, which ruins the line number information in the diff
	// output.
	h.seq++
	m := fmt.Sprintf(format, args...) + fmt.Sprintf(" #%d", h.seq)
	h.log.Print(m)

	if h.failRE != nil && h.failRE.MatchString(m) {
		h.failed = true
	}
}

// Failed returns true if the fail regular expression has matched the results
// of an operation.
func (h *history) Failed() bool {
	return h.failed
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

func (h *historyLogger) format(prefix, format string, args ...interface{}) string {
	var buf strings.Builder
	orig := fmt.Sprintf(format, args...)
	for _, line := range strings.Split(strings.TrimSpace(orig), "\n") {
		buf.WriteString(prefix)
		buf.WriteString(line)
		buf.WriteString("\n")
	}
	return buf.String()
}

// Infof implements the pebble.Logger interface. Note that the output is
// commented.
func (h *historyLogger) Infof(format string, args ...interface{}) {
	_ = h.log.Output(2, h.format("// INFO: ", format, args...))
}

// Fatalf implements the pebble.Logger interface. Note that the output is
// commented.
func (h *historyLogger) Fatalf(format string, args ...interface{}) {
	_ = h.log.Output(2, h.format("// FATAL: ", format, args...))
}
