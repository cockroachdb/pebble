// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
)

type mockPrintLimiter struct {
	buf   bytes.Buffer
	burst int
}

func (m *mockPrintLimiter) DelayN(now time.Time, n int) time.Duration {
	fmt.Fprintf(&m.buf, "wait: %d\n", n)
	return 0
}

func (m *mockPrintLimiter) AllowN(now time.Time, n int) bool {
	fmt.Fprintf(&m.buf, "allow: %d\n", n)
	return true
}

func (m *mockPrintLimiter) Burst() int {
	return m.burst
}

func TestCompactionPacerMaybeThrottle(t *testing.T) {
	datadriven.RunTest(t, "testdata/compaction_pacer_maybe_throttle",
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				if len(d.CmdArgs) != 1 {
					return fmt.Sprintf("%s expects 1 argument", d.Cmd)
				}

				burst := uint64(1)
				var bytesIterated uint64
				var slowdownThreshold uint64
				var freeBytes, liveBytes, obsoleteBytes uint64
				if len(d.Input) > 0 {
					for _, data := range strings.Split(d.Input, "\n") {
						parts := strings.Split(data, ":")
						if len(parts) != 2 {
							return fmt.Sprintf("malformed test:\n%s", d.Input)
						}
						varKey := parts[0]
						varValue, err := strconv.ParseUint(strings.TrimSpace(parts[1]), 10, 64)
						if err != nil {
							return err.Error()
						}

						switch varKey {
						case "burst":
							burst = varValue
						case "bytesIterated":
							bytesIterated = varValue
						case "slowdownThreshold":
							slowdownThreshold = varValue
						case "freeBytes":
							freeBytes = varValue
						case "liveBytes":
							liveBytes = varValue
						case "obsoleteBytes":
							obsoleteBytes = varValue
						default:
							return fmt.Sprintf("unknown command: %s", varKey)
						}
					}
				}

				mockLimiter := mockPrintLimiter{burst: int(burst)}
				switch d.CmdArgs[0].Key {
				case "deletion":
					getInfo := func() deletionPacerInfo {
						return deletionPacerInfo{
							freeBytes:     freeBytes,
							liveBytes:     liveBytes,
							obsoleteBytes: obsoleteBytes,
						}
					}
					deletionPacer := newDeletionPacer(&mockLimiter, getInfo)
					deletionPacer.freeSpaceThreshold = slowdownThreshold
					err := deletionPacer.maybeThrottle(bytesIterated)
					if err != nil {
						return err.Error()
					}

					return mockLimiter.buf.String()
				default:
					return fmt.Sprintf("unknown command: %s", d.Cmd)
				}

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}
