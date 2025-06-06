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
	var decoder IndexBlockDecoder
	var buf bytes.Buffer
	datadriven.RunTest(t, "testdata/index_block", func(t *testing.T, d *datadriven.TestData) string {
		buf.Reset()
		switch d.Cmd {
		case "build":
			var e indexBlockEncoder
			e.Init()
			lines := crstrings.Lines(d.Input)
			var i int
			for i = 0; i < len(lines); i++ {
				line := lines[i]
				if line == "virtual-block-mappings" {
					i += 1
					break
				}
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
			if i < len(lines) {
				for _, line := range lines[i:] {
					fields := strings.Fields(line)
					require.Len(t, fields, 3)
					vblockID, err := strconv.ParseInt(fields[0], 10, 64)
					require.NoError(t, err)
					physicalBlockIndex, err := strconv.ParseInt(fields[1], 10, 64)
					require.NoError(t, err)
					valueID, err := strconv.ParseInt(fields[2], 10, 64)
					require.NoError(t, err)
					e.AddVirtualBlockMapping(BlockID(vblockID), int(physicalBlockIndex), BlockValueID(valueID))
				}
			}

			data := e.Finish()
			decoder.Init(data)
			fmt.Fprint(&buf, decoder.DebugString())
			return buf.String()

		case "get":
			for _, arg := range d.CmdArgs {
				blockIndex, err := strconv.Atoi(arg.Key)
				require.NoError(t, err)
				h := decoder.BlockHandle(blockIndex)
				fmt.Fprintf(&buf, "%d: %s\n", blockIndex, h)
			}
			return buf.String()

		case "remap-virtual-blockid":
			for _, arg := range d.CmdArgs {
				blockID, err := strconv.ParseInt(arg.Key, 10, 64)
				require.NoError(t, err)
				blockIndex, valueIDOffset := decoder.RemapVirtualBlockID(BlockID(blockID))
				fmt.Fprintf(&buf, "%d -> block %d, with valueID offset %d\n", blockID, blockIndex, valueIDOffset)
			}
			return buf.String()

		default:
			panic(fmt.Sprintf("unknown command: %s", d.Cmd))
		}
	})
}
