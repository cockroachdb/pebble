// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
)

// TestLSMScanCursorExternalFiles tests the lsmScanCursor with external file
// iteration. This verifies the cursor correctly tracks position across levels
// and advances past processed files.
func TestLSMScanCursor(t *testing.T) {
	cmp := bytes.Compare
	objProvider := initDownloadTestProvider(t)

	var vers *manifest.Version
	var cursor lsmScanCursor
	datadriven.RunTest(t, "testdata/lsm_scan_cursor", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			var err error
			const flushSplitBytes = 10 * 1024 * 1024
			l0Organizer := manifest.NewL0Organizer(base.DefaultComparer, flushSplitBytes)
			vers, err = manifest.ParseVersionDebug(base.DefaultComparer, l0Organizer, td.Input)
			if err != nil {
				td.Fatalf(t, "%v", err)
			}
			return vers.DebugString()

		case "cursor":
			var lower, upper string
			td.ScanArgs(t, "lower", &lower)
			td.ScanArgs(t, "upper", &upper)
			bounds := base.UserKeyBoundsEndExclusive([]byte(lower), []byte(upper))

			var buf strings.Builder
			for line := range crstrings.LinesSeq(td.Input) {
				fields := strings.Fields(line)
				fmt.Fprintf(&buf, "%s:\n", fields[0])
				switch cmd := fields[0]; cmd {
				case "start":
					cursor = lsmScanCursor{
						level:  0,
						key:    bounds.Start,
						seqNum: 0,
					}
					fmt.Fprintf(&buf, "  %s\n", cursor)

				case "next-file":
					f, level := nextExternalFile(cursor, cmp, objProvider, bounds, vers)
					if f != nil {
						// Verify that fCursor still points to this file.
						f2, level2 := nextExternalFile(makeLSMCursorAtFile(f, level), cmp, objProvider, bounds, vers)
						if f != f2 {
							td.Fatalf(t, "nextExternalFile returned different file")
						}
						if level != level2 {
							td.Fatalf(t, "nextExternalFile returned different level")
						}
						cursor = makeLSMCursorAfterFile(f, level)
					}
					fmt.Fprintf(&buf, "  file: %v  level: %d\n", f, level)

				case "iterate":
					for {
						f, level := nextExternalFile(cursor, cmp, objProvider, bounds, vers)
						if f == nil {
							fmt.Fprintf(&buf, "  no more files\n")
							break
						}
						fmt.Fprintf(&buf, "  file: %v  level: %d\n", f, level)
						cursor = makeLSMCursorAfterFile(f, level)
					}

				default:
					td.Fatalf(t, "unknown cursor command %q", cmd)
				}
			}
			return buf.String()

		default:
			td.Fatalf(t, "unknown command: %s", td.Cmd)
			return ""
		}
	})
}
