// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage"
)

// mockExternalObjProvider is a minimal objstorage.Provider implementation for testing.
// It only implements Lookup; other methods panic.
type mockExternalObjProvider struct {
	objstorage.Provider
	externalThreshold base.DiskFileNum
}

func (m *mockExternalObjProvider) Lookup(
	fileType base.FileType, fileNum base.DiskFileNum,
) (objstorage.ObjectMetadata, error) {
	meta := objstorage.ObjectMetadata{
		DiskFileNum: fileNum,
		FileType:    fileType,
	}
	if fileNum >= m.externalThreshold {
		// Set CustomObjectName to make IsExternal() return true.
		meta.Remote.CustomObjectName = "external"
	}
	return meta, nil
}

// TestScanCursor tests the ScanCursor with external file iteration.
// This verifies the cursor correctly tracks position across levels and advances
// past processed files.
func TestScanCursor(t *testing.T) {
	cmp := bytes.Compare
	// Backings >= 100 are external.
	objProvider := &mockExternalObjProvider{externalThreshold: 100}

	var vers *Version
	var cursor ScanCursor
	datadriven.RunTest(t, "testdata/scan_cursor", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			var err error
			const flushSplitBytes = 10 * 1024 * 1024
			l0Organizer := NewL0Organizer(base.DefaultComparer, flushSplitBytes)
			vers, err = ParseVersionDebug(base.DefaultComparer, l0Organizer, td.Input)
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
					cursor = ScanCursor{
						Level:  0,
						Key:    bounds.Start,
						SeqNum: 0,
					}
					fmt.Fprintf(&buf, "  %s\n", &cursor)

				case "reset":
					// Reset cursor to a specific level without bounds.
					level := 0
					if len(fields) > 1 {
						fmt.Sscanf(fields[1], "level=%d", &level)
					}
					cursor = ScanCursor{Level: level}
					fmt.Fprintf(&buf, "  %s\n", &cursor)

				case "next-external-file":
					f, level := cursor.NextExternalFile(cmp, objProvider, bounds, vers)
					if f != nil {
						// Verify that cursor still points to this file.
						f2, level2 := cursor.NextExternalFile(cmp, objProvider, bounds, vers)
						if f != f2 {
							td.Fatalf(t, "NextExternalFile returned different file")
						}
						if level != level2 {
							td.Fatalf(t, "NextExternalFile returned different level")
						}
						cursor = MakeScanCursorAfterFile(f, level)
					}
					fmt.Fprintf(&buf, "  file: %v  level: %d\n", f, level)

				case "iterate-external-files":
					for {
						f, level := cursor.NextExternalFile(cmp, objProvider, bounds, vers)
						if f == nil {
							fmt.Fprintf(&buf, "  no more files\n")
							break
						}
						fmt.Fprintf(&buf, "  file: %v  level: %d\n", f, level)
						cursor = MakeScanCursorAfterFile(f, level)
					}

				case "next-file":
					f, level := cursor.NextFile(cmp, vers)
					if f != nil {
						// Verify that cursor still points to this file.
						f2, level2 := cursor.NextFile(cmp, vers)
						if f != f2 {
							td.Fatalf(t, "NextFile returned different file")
						}
						if level != level2 {
							td.Fatalf(t, "NextFile returned different level")
						}
						cursor = MakeScanCursorAfterFile(f, level)
					}
					fmt.Fprintf(&buf, "  file: %v  level: %d\n", f, level)

				case "iterate-files":
					for {
						f, level := cursor.NextFile(cmp, vers)
						if f == nil {
							fmt.Fprintf(&buf, "  no more files\n")
							break
						}
						fmt.Fprintf(&buf, "  file: %v  level: %d\n", f, level)
						cursor = MakeScanCursorAfterFile(f, level)
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
