package binfmt

import (
	"fmt"
	"os"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestHexFmt(t *testing.T) {
	var width, offset, length int
	datadriven.RunTest(t, "testdata/hexfmt", func(t *testing.T, d *datadriven.TestData) string {
		width = 16
		d.ScanArgs(t, "pos", &offset, &length)
		d.MaybeScanArgs(t, "width", &width)
		switch d.Cmd {
		case "read-file":
			path := d.CmdArgs[0].String()
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			return HexDump(data[offset:offset+length], width, d.HasArg("include-offsets"))
		case "sequential":
			var size int
			d.ScanArgs(t, "size", &size)
			data := make([]byte, size)
			for i := 0; i < size; i++ {
				data[i] = byte(i)
			}
			return HexDump(data, width, d.HasArg("include-offsets"))
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}
