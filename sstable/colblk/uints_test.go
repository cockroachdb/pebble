// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
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
	"github.com/cockroachdb/pebble/internal/aligned"
	"github.com/cockroachdb/pebble/internal/binfmt"
)

func TestUints(t *testing.T) {
	var b8 UintBuilder[uint8]
	var b16 UintBuilder[uint16]
	var b32 UintBuilder[uint32]
	var b64 UintBuilder[uint64]

	var out bytes.Buffer
	var widths []int
	var writers []ColumnWriter
	datadriven.RunTest(t, "testdata/uints", func(t *testing.T, td *datadriven.TestData) string {
		out.Reset()
		switch td.Cmd {
		case "init":
			widths = widths[:0]
			writers = writers[:0]
			td.ScanArgs(t, "widths", &widths)
			defaultZero := td.HasArg("default-zero")
			for _, w := range widths {
				switch w {
				case 8:
					b8.init(defaultZero, 0)
					writers = append(writers, &b8)
				case 16:
					b16.init(defaultZero, 0)
					writers = append(writers, &b16)
				case 32:
					b32.init(defaultZero, 0)
					writers = append(writers, &b32)
				case 64:
					b64.init(defaultZero, 0)
					writers = append(writers, &b64)
				default:
					panic(fmt.Sprintf("unknown width: %d", w))
				}
				fmt.Fprintf(&out, "b%d\n", w)
			}
			return out.String()
		case "write":
			for _, f := range strings.Fields(td.Input) {
				delim := strings.IndexByte(f, ':')
				i, err := strconv.Atoi(f[:delim])
				if err != nil {
					return err.Error()
				}
				for _, width := range widths {
					v, err := strconv.ParseUint(f[delim+1:], 10, width)
					if err != nil {
						return err.Error()
					}
					switch width {
					case 8:
						b8.Set(i, uint8(v))
					case 16:
						b16.Set(i, uint16(v))
					case 32:
						b32.Set(i, uint32(v))
					case 64:
						b64.Set(i, v)
					default:
						panic(fmt.Sprintf("unknown width: %d", width))
					}
				}
			}
			return out.String()
		case "size":
			var rowCounts []int
			td.ScanArgs(t, "rows", &rowCounts)
			for wIdx, w := range writers {
				fmt.Fprintf(&out, "b%d:\n", widths[wIdx])
				for _, rows := range rowCounts {
					fmt.Fprintf(&out, "  %d: %T.Size(%d, 0) = %d\n", widths[wIdx], w, rows, w.Size(rows, 0))
				}
			}
			return out.String()
		case "finish":
			var rows int
			var finishWidths []int
			td.ScanArgs(t, "rows", &rows)
			td.ScanArgs(t, "widths", &finishWidths)
			var newWriters []ColumnWriter
			var newWidths []int
			for wIdx, width := range widths {
				var shouldFinish bool
				for _, fw := range finishWidths {
					shouldFinish = shouldFinish || width == fw
				}
				if shouldFinish {
					sz := writers[wIdx].Size(rows, 0)
					buf := aligned.ByteSlice(int(sz))
					_, desc := writers[wIdx].Finish(0, rows, 0, buf)
					fmt.Fprintf(&out, "b%d: %T:\n", width, writers[wIdx])
					f := binfmt.New(buf).LineWidth(20)
					uintsToBinFormatter(f, rows, desc)
					fmt.Fprintf(&out, "%s", f.String())
				} else {
					fmt.Fprintf(&out, "Keeping b%d open\n", width)
					newWidths = append(newWidths, width)
					newWriters = append(newWriters, writers[wIdx])
				}
			}
			writers = newWriters
			widths = newWidths
			return out.String()
		default:
			panic(fmt.Sprintf("unknown command: %s", td.Cmd))
		}
	})
}
