// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/pmezard/go-difflib/difflib"
	"github.com/stretchr/testify/require"
)

// history records the results of running a series of operations.
//
// history also implements the pebble.Logger interface, outputting to a stdlib
// logger, prefixing the log messages with "//"-style comments.
type history struct {
	err    atomic.Value
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
		err := errors.Errorf("failure regexp %q matched output: %s", h.failRE, m)
		h.err.Store(err)
	}
}

// Error returns an error if the test has failed from log output, either a
// failure regexp match or a call to Fatalf.
func (h *history) Error() error {
	if v := h.err.Load(); v != nil {
		return v.(error)
	}
	return nil
}

func (h *history) format(prefix, format string, args ...interface{}) string {
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
func (h *history) Infof(format string, args ...interface{}) {
	_ = h.log.Output(2, h.format("// INFO: ", format, args...))
}

// Fatalf implements the pebble.Logger interface. Note that the output is
// commented.
func (h *history) Fatalf(format string, args ...interface{}) {
	_ = h.log.Output(2, h.format("// FATAL: ", format, args...))
	h.err.Store(errors.Errorf(format, args...))
}

// CompareHistories takes a slice of file paths containing history files. It
// performs a diff comparing the first path to all other paths. CompareHistories
// returns the index and diff for the first history that differs. If all the
// histories are identical, CompareHistories returns a zero index and an empty
// string.
func CompareHistories(t *testing.T, paths []string) (i int, diff string) {
	base := readHistory(t, paths[0])
	for i := 1; i < len(paths); i++ {
		lines := readHistory(t, paths[i])
		diff := difflib.UnifiedDiff{
			A:       base,
			B:       lines,
			Context: 5,
		}
		text, err := difflib.GetUnifiedDiffString(diff)
		require.NoError(t, err)
		if text != "" {
			return i, text
		}
	}
	return 0, ""
}

// Read a history file, stripping out lines that begin with a comment.
func readHistory(t *testing.T, historyPath string) []string {
	data, err := ioutil.ReadFile(historyPath)
	require.NoError(t, err)
	lines := difflib.SplitLines(string(data))
	newLines := make([]string, 0, len(lines))
	for _, line := range lines {
		if strings.HasPrefix(line, "// ") {
			continue
		}
		newLines = append(newLines, line)
	}
	return newLines
}

func readLogicalState(t *testing.T, path string) []string {
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return difflib.SplitLines(string(data))
}
