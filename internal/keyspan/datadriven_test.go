// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
)

// runIterCmd evaluates a datadriven command controlling an internal
// keyspan.FragmentIterator, returning a string with the results of the iterator
// operations.
func runIterCmd(t *testing.T, td *datadriven.TestData, iter FragmentIterator) string {
	var buf bytes.Buffer
	lines := strings.Split(strings.TrimSpace(td.Input), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		i := strings.IndexByte(line, '#')
		iterCmd := line
		if i > 0 {
			iterCmd = string(line[:i])
		}
		runIterOp(&buf, iter, iterCmd)
		fmt.Fprintln(&buf)
	}
	return strings.TrimSpace(buf.String())
}

var iterDelim = map[rune]bool{',': true, ' ': true, '(': true, ')': true, '"': true}

func runIterOp(w io.Writer, it FragmentIterator, op string) {
	fields := strings.FieldsFunc(op, func(r rune) bool { return iterDelim[r] })
	var s *Span
	switch strings.ToLower(fields[0]) {
	case "first":
		s = it.First()
	case "last":
		s = it.Last()
	case "seekge", "seek-ge":
		if len(fields) == 1 {
			panic(fmt.Sprintf("unable to parse iter op %q", op))
		}
		s = it.SeekGE([]byte(fields[1]))
	case "seeklt", "seek-lt":
		if len(fields) == 1 {
			panic(fmt.Sprintf("unable to parse iter op %q", op))
		}
		s = it.SeekLT([]byte(fields[1]))
	case "next":
		s = it.Next()
	case "prev":
		s = it.Prev()
	default:
		panic(fmt.Sprintf("unrecognized iter op %q", fields[0]))
	}
	if s == nil {
		fmt.Fprint(w, "<nil>")
		if err := it.Error(); err != nil {
			fmt.Fprintf(w, " err=<%s>", it.Error())
		}
		return
	}
	fmt.Fprint(w, s)
}
