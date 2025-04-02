// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/stretchr/testify/require"
)

func TestMetaIndexBlock(t *testing.T) {
	var decoder KeyValueBlockDecoder
	var buf bytes.Buffer
	datadriven.RunTest(t, "testdata/meta_index_block", func(t *testing.T, d *datadriven.TestData) string {
		buf.Reset()
		switch d.Cmd {
		case "build":
			var w KeyValueBlockWriter
			w.Init()
			for _, line := range strings.Split(d.Input, "\n") {
				fields := strings.Fields(line)
				key := fields[0]
				var err error
				var h block.Handle
				h.Offset, err = strconv.ParseUint(fields[1], 10, 64)
				require.NoError(t, err)
				h.Length, err = strconv.ParseUint(fields[2], 10, 64)
				require.NoError(t, err)
				var encHandle [120]byte
				n := h.EncodeVarints(encHandle[:])
				w.AddBlockHandle([]byte(key), encHandle[:n])
			}

			data := w.Finish(w.Rows())
			decoder.Init(data)
			fmt.Fprintf(&buf, decoder.DebugString())
			return buf.String()
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}
