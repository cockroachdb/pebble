// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package blob

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/stretchr/testify/require"
)

func TestIndexBlockEncoding(t *testing.T) {
	var decoder indexBlockDecoder
	var buf bytes.Buffer
	datadriven.RunTest(t, "testdata/index_block", func(t *testing.T, d *datadriven.TestData) string {
		buf.Reset()
		switch d.Cmd {
		case "build":
			var e indexBlockEncoder
			e.Init()
			for _, line := range crstrings.Lines(d.Input) {
				fields := strings.Fields(line)
				require.Len(t, fields, 2)
				var err error
				var h block.Handle
				h.Offset, err = strconv.ParseUint(fields[0], 10, 64)
				require.NoError(t, err)
				h.Length, err = strconv.ParseUint(fields[1], 10, 64)
				require.NoError(t, err)
				e.AddBlockHandle(h)
			}

			data := e.Finish()
			decoder.Init(data)
			fmt.Fprint(&buf, decoder.DebugString())
			return buf.String()
		case "get":
			for _, arg := range d.CmdArgs {
				blockNum, err := strconv.Atoi(arg.Key)
				require.NoError(t, err)
				h := decoder.BlockHandle(uint32(blockNum))
				fmt.Fprintf(&buf, "%d: %s\n", blockNum, h)
			}
			return buf.String()
		default:
			panic(fmt.Sprintf("unknown command: %s", d.Cmd))
		}
	})
}
