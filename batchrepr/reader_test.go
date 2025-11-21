// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package batchrepr

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"unicode"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
)

func TestReader(t *testing.T) {
	datadriven.RunTest(t, "testdata/reader", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "is-empty":
			repr := readRepr(t, td.Input)
			return fmt.Sprint(IsEmpty(repr))

		case "scan":
			repr := readRepr(t, td.Input)
			h, _ := ReadHeader(repr)
			r := Read(repr)
			var out strings.Builder
			fmt.Fprintf(&out, "Header: %s\n", h)
			for {
				kind, ukey, value, ok, err := r.Next()
				if !ok {
					if err != nil {
						fmt.Fprintf(&out, "err: %s\n", err)
					} else {
						fmt.Fprint(&out, "eof")
					}
					break
				}
				fmt.Fprintf(&out, "%s: %q: %q\n", kind, ukey, value)
			}
			return out.String()

		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}

func readRepr(t testing.TB, str string) []byte {
	var reprBuf bytes.Buffer
	for l := range crstrings.LinesSeq(str) {
		// Remove any trailing comments behind #.
		if i := strings.IndexRune(l, '#'); i >= 0 {
			l = l[:i]
		}
		// Strip all whitespace from the line.
		l = strings.Map(func(r rune) rune {
			if unicode.IsSpace(r) {
				return -1
			}
			return r
		}, l)
		b, err := hex.DecodeString(l)
		if err != nil {
			t.Fatal(err)
		}
		reprBuf.Write(b)
	}
	return reprBuf.Bytes()
}
