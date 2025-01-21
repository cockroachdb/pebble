// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package batchrepr

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/internal/binfmt"
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
			seqNum := base.ParseSeqNum(td.CmdArgs[0].Key)
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

	f := binfmt.New(repr).LineWidth(40)
	f.HexBytesln(8, " seqnum=%d", f.PeekUint(8))
	f.HexBytesln(4, " count=%d", f.PeekUint(4))
	for r := Read(repr); len(r) > 0; {
		prevLen := len(r)
		kind, ukey, _, ok, err := r.Next()
		switch {
		case err != nil:
			// The remainder of the repr is invalid. Print the remainder
			// on a single line.
			f.HexBytesln(len(repr)-prevLen, "invalid: %v", err)
			return f.String()
		case !ok:
			// We're finished iterating through the repr.
			return f.String()
		default:
			// Next() decoded a single KV. Print the bytes we iterated
			// over verbatim on a single line.
			i := len(repr) - prevLen
			j := len(repr) - len(r)
			// Print the kind byte separated by a space to make it
			// easier to read.
			f.Line(j-i).Append("x ").HexBytes(1).Append(" ").HexBytes(j-i-1).Done("%s %q", kind, ukey)
		}
	}
	return f.String()
}
