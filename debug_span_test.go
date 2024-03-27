package pebble

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestDebugSpan(t *testing.T) {
	var d *DB
	defer func() {
		if d != nil {
			require.NoError(t, d.Close())
		}
	}()

	datadriven.RunTest(t, "testdata/debug_span", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			if d != nil {
				if err := d.Close(); err != nil {
					return err.Error()
				}
			}
			opts := &Options{
				FS:         vfs.NewMem(),
				DebugCheck: DebugCheckLevels,
			}
			var err error
			d, err = runDBDefineCmd(td, opts)
			if err != nil {
				return err.Error()
			}
			d.mu.Lock()
			s := d.mu.versions.currentVersion().String()
			d.mu.Unlock()
			return s

		case "debug-span":
			var start, end []byte
			for _, cmdArg := range td.CmdArgs {
				switch cmdArg.Key {
				case "start":
					start = []byte(cmdArg.Vals[0])
				case "end":
					end = []byte(cmdArg.Vals[0])
				default:
					return fmt.Sprintf("unknown arg %q", cmdArg.Key)
				}
			}
			s, err := d.DebugSpan(start, end)
			if err != nil {
				return err.Error()
			}
			return s.String()

		case "metrics":
			return d.Metrics().String()

		default:
			return fmt.Sprintf("unknown command %q", td.Cmd)
		}
	})
}
