// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
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
	"github.com/cockroachdb/pebble/internal/base"
)

func TestTruncate(t *testing.T) {
	cmp := base.DefaultComparer.Compare
	fmtKey := base.DefaultComparer.FormatKey
	var iter FragmentIterator
	var savedIter FragmentIterator
	defer func() {
		if savedIter != nil {
			savedIter.Close()
			savedIter = nil
		}
	}()

	datadriven.RunTest(t, "testdata/truncate", func(t *testing.T, d *datadriven.TestData) string {
		doTruncate := func() FragmentIterator {
			if len(d.Input) > 0 {
				t.Fatalf("unexpected input: %s", d.Input)
			}
			if len(d.CmdArgs) < 1 || len(d.CmdArgs) > 3 {
				t.Fatalf("expected 1-3 arguments: %s", d.CmdArgs)
			}
			parts := strings.Split(d.CmdArgs[0].String(), "-")
			var startKey, endKey *base.InternalKey
			if len(d.CmdArgs) > 1 {
				for _, arg := range d.CmdArgs[1:] {
					switch arg.Key {
					case "startKey":
						startKey = &base.InternalKey{}
						*startKey = base.ParseInternalKey(arg.Vals[0])
					case "endKey":
						endKey = &base.InternalKey{}
						*endKey = base.ParseInternalKey(arg.Vals[0])
					}
				}
			}
			if len(parts) != 2 {
				t.Fatalf("malformed arg: %s", d.CmdArgs[0])
			}
			lower := []byte(parts[0])
			upper := []byte(parts[1])

			tIter := Truncate(
				cmp, iter, lower, upper, startKey, endKey, false,
			)
			return tIter
		}

		switch d.Cmd {
		case "build":
			tombstones := buildSpans(t, cmp, fmtKey, d.Input, base.InternalKeyKindRangeDelete)
			iter = NewIter(cmp, tombstones)
			return formatAlphabeticSpans(tombstones)

		case "truncate":
			tIter := doTruncate()
			defer tIter.Close()
			var truncated []Span
			for s := tIter.First(); s != nil; s = tIter.Next() {
				truncated = append(truncated, s.ShallowClone())
			}
			return formatAlphabeticSpans(truncated)

		case "truncate-and-save-iter":
			if savedIter != nil {
				savedIter.Close()
			}
			savedIter = doTruncate()
			return "ok"

		case "saved-iter":
			return runIterCmd(t, d, savedIter)

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

// runIterCmd evaluates a datadriven command controlling an internal
// keyspan.FragmentIterator, returning a string with the results of the iterator
// operations.
func runIterCmd(t *testing.T, td *datadriven.TestData, iter FragmentIterator) string {
	var buf bytes.Buffer
	lines := strings.Split(strings.TrimSpace(td.Input), "\n")
	for i, line := range lines {
		if i > 0 {
			fmt.Fprintln(&buf)
		}
		line = strings.TrimSpace(line)
		i := strings.IndexByte(line, '#')
		iterCmd := line
		if i > 0 {
			iterCmd = string(line[:i])
		}
		dataDrivenRunIterOp(&buf, iter, iterCmd)
	}
	return buf.String()
}

func dataDrivenRunIterOp(w io.Writer, it FragmentIterator, op string) {
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
