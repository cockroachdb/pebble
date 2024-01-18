// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package batchrepr

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/datadriven"
)

func TestWriter(t *testing.T) {
	var repr []byte
	datadriven.RunTest(t, "testdata/writer", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "init":
			repr = readRepr(t, td.Input)
			return ""

		case "read-header":
			h, ok := ReadHeader(repr)
			if !ok {
				return "header too small"
			}
			return h.String()

		case "set-count":
			count, err := strconv.ParseUint(td.CmdArgs[0].Key, 10, 32)
			if err != nil {
				return err.Error()
			}
			SetCount(repr, uint32(count))
			return prettyBinaryRepr(repr)

		case "set-seqnum":
			seqNum, err := strconv.ParseUint(td.CmdArgs[0].Key, 10, 64)
			if err != nil {
				return err.Error()
			}
			SetSeqNum(repr, seqNum)
			return prettyBinaryRepr(repr)

		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}

func prettyBinaryRepr(repr []byte) string {
	if len(repr) < HeaderLen {
		return fmt.Sprintf("%x", repr)
	}

	var buf bytes.Buffer
	h, _ := ReadHeader(repr)

	fmt.Fprintf(&buf, "%x %x # %s\n", repr[:countOffset], repr[countOffset:HeaderLen], h)
	for r := Read(repr); len(r) > 0; {
		prevLen := len(r)
		kind, ukey, _, ok, err := r.Next()
		switch {
		case err != nil:
			// The remainder of the repr is invalid. Print the remainder
			// on a single line.
			fmt.Fprintf(&buf, "%x", repr[len(repr)-prevLen:])
			return buf.String()
		case !ok:
			// We're finished iterating through the repr.
			return buf.String()
		default:
			// Next() decoded a single KV. Print the bytes we iterated
			// over verbatim on a single line.
			i := len(repr) - prevLen
			j := len(repr) - len(r)
			// Print the kind byte separated by a space to make it
			// easier to read.
			fmt.Fprintf(&buf, "%x %-22x # %s %q\n", repr[i:i+1], repr[i+1:j], kind, ukey)
		}
	}
	return buf.String()
}
