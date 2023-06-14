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
	"github.com/cockroachdb/pebble/internal/rate"
)

func TestCompactionPacerMaybeThrottle(t *testing.T) {
	now := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	var buf bytes.Buffer
	nowFn := func() time.Time {
		return now
	}
	sleepFn := func(d time.Duration) {
		fmt.Fprintf(&buf, "wait: %s", d)
		now = now.Add(d)
	}

	var pacer *deletionPacer

	datadriven.RunTest(t, "testdata/compaction_pacer_maybe_throttle",
		func(t *testing.T, d *datadriven.TestData) string {
			buf.Reset()
			switch d.Cmd {
			case "init":
				burst := uint64(1)
				var slowdownThreshold uint64
				var freeBytes, liveBytes, obsoleteBytes uint64
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
					case "slowdownThreshold":
						slowdownThreshold = varValue
					case "freeBytes":
						freeBytes = varValue
					case "liveBytes":
						liveBytes = varValue
					case "obsoleteBytes":
						obsoleteBytes = varValue
					default:
						return fmt.Sprintf("unknown argument: %s", varKey)
					}
				}

				getInfo := func() deletionPacerInfo {
					return deletionPacerInfo{
						freeBytes:     freeBytes,
						liveBytes:     liveBytes,
						obsoleteBytes: obsoleteBytes,
					}
				}
				mockLimiter := rate.NewLimiterWithCustomTime(float64(burst), float64(burst), nowFn, sleepFn)
				pacer = newDeletionPacer(mockLimiter, getInfo)
				pacer.testingSleepFn = sleepFn
				pacer.freeSpaceThreshold = slowdownThreshold
				return ""

			case "delete":
				var bytesToDelete uint64
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
					case "bytesToDelete":
						bytesToDelete = varValue
					default:
						return fmt.Sprintf("unknown command: %s", varKey)
					}
				}
				pacer.maybeThrottle(bytesToDelete)
				return buf.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}
