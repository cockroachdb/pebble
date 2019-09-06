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

	"github.com/cockroachdb/pebble/internal/datadriven"
)

type mockCountLimiter struct {
	waitCount  int
	allowCount int
	burst      int
}

func (m *mockCountLimiter) WaitN(ctx context.Context, n int) error {
	m.waitCount += n
	return nil
}

func (m *mockCountLimiter) AllowN(now time.Time, n int) bool {
	m.allowCount += n
	return true
}

func (m *mockCountLimiter) Burst() int {
	return m.burst
}

type mockPrintLimiter struct {
	buf   bytes.Buffer
	burst int
}

func (m *mockPrintLimiter) WaitN(ctx context.Context, n int) error {
	fmt.Fprintf(&m.buf, "wait: %d\n", n)
	return nil
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
		func(d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				if len(d.CmdArgs) != 1 {
					return fmt.Sprintf("%s expects 1 argument", d.Cmd)
				}

				burst := uint64(1)
				dirtyBytes := uint64(1)
				var bytesIterated uint64
				var currentTotal uint64
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
						case "currentTotal":
							currentTotal = varValue
						case "dirtyBytes":
							dirtyBytes = varValue
						case "slowdownThreshold":
							slowdownThreshold = varValue
						default:
							return fmt.Sprintf("unknown command: %s", varKey)
						}
					}
				}

				mockLimiter := mockPrintLimiter{burst: int(burst)}
				switch d.CmdArgs[0].Key {
				case "compaction":
					getInfo := func() compactionPacerInfo {
						return compactionPacerInfo{
							slowdownThreshold:   slowdownThreshold,
							totalCompactionDebt: currentTotal,
							totalDirtyBytes:     dirtyBytes,
						}
					}
					compactionPacer := newCompactionPacer(compactionPacerEnv{
						limiter:      &mockLimiter,
						memTableSize: 100,
						getInfo:      getInfo,
					})

					err := compactionPacer.maybeThrottle(bytesIterated)
					if err != nil {
						return err.Error()
					}

					return mockLimiter.buf.String()
				case "flush":
					getInfo := func() flushPacerInfo {
						return flushPacerInfo{
							totalBytes: currentTotal,
						}
					}
					flushPacer := newFlushPacer(flushPacerEnv{
						limiter:      &mockLimiter,
						memTableSize: 100,
						getInfo:      getInfo,
					})
					flushPacer.slowdownThreshold = slowdownThreshold

					err := flushPacer.maybeThrottle(bytesIterated)
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
