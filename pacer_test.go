// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/petermattis/pebble/internal/datadriven"
)

type mockLimiter struct {
	waitCount  int
	allowCount int
	burst      int
}

func (m *mockLimiter) WaitN(ctx context.Context, n int) error {
	m.waitCount += n
	return nil
}

func (m *mockLimiter) AllowN(now time.Time, n int) bool {
	m.allowCount += n
	return true
}

func (m *mockLimiter) Burst() int {
	return m.burst
}

func TestCompactionPacerMaybeThrottle(t *testing.T) {
	datadriven.RunTest(t, "testdata/compaction_pacer_maybe_throttle",
		func(d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				if len(d.CmdArgs) != 1 {
					return fmt.Sprintf("%s expects 1 argument", d.Cmd)
				}

				var burst uint64
				var bytesIterated uint64
				var currentLevel uint64
				var slowdownThreshold uint64
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
						case "currentLevel":
							currentLevel = varValue
						case "slowdownThreshold":
							slowdownThreshold = varValue
						default:
							return fmt.Sprintf("unknown command: %s", varKey)
						}

					}
				}

				// Set default burst to 1. Having burst as 0 results in an infinite loop.
				if burst == 0 {
					burst = 1
				}

				mockLimiter := mockLimiter{burst: int(burst)}
				var buf bytes.Buffer
				switch d.CmdArgs[0].Key {
				case "compaction":
					getInfo := func() compactionPacerInfo {
						return compactionPacerInfo{
							slowdownThreshold:   slowdownThreshold,
							totalCompactionDebt: currentLevel,
						}
					}
					compactionPacer := newCompactionPacer(compactionPacerEnv{
						limiter:      &mockLimiter,
						memTableSize: 100,
						getInfo:      getInfo,
					})

					err := compactionPacer.maybeThrottle(bytesIterated)
					if err != nil {
						t.Fatal(err)
					}

					_, err = fmt.Fprintf(&buf, "%d, %d\n",
						mockLimiter.waitCount,
						mockLimiter.allowCount)
					if err != nil {
						t.Fatal(err)
					}

					return buf.String()
				case "flush":
					var bytesFlushed uint64
					getInfo := func() flushPacerInfo {
						return flushPacerInfo{
							totalBytes: currentLevel,
						}
					}
					flushPacer := newFlushPacer(flushPacerEnv{
						bytesFlushed: &bytesFlushed,
						limiter:      &mockLimiter,
						memTableSize: 100,
						getInfo:      getInfo,
					})
					flushPacer.slowdownThreshold = slowdownThreshold

					err := flushPacer.maybeThrottle(bytesIterated)
					if err != nil {
						t.Fatal(err)
					}

					_, err = fmt.Fprintf(&buf, "%d, %d, %d\n",
						mockLimiter.waitCount,
						mockLimiter.allowCount,
						bytesFlushed)
					if err != nil {
						t.Fatal(err)
					}

					return buf.String()
				default:
					return fmt.Sprintf("unknown command: %s", d.Cmd)
				}

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}
