// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package deletepacer

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/crhumanize"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestRateCalculatorDataDriven(t *testing.T) {
	datadriven.RunTest(t, "testdata/rate-calc", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "simulate":
			return runSimulation(t, td)
		default:
			td.Fatalf(t, "unknown command %q", td.Cmd)
			return ""
		}
	})
}

const MB = 1 << 20
const GB = 1 << 30

func runSimulation(t *testing.T, td *datadriven.TestData) string {
	// Parse configuration from arguments
	type testConfig struct {
		baselineRate            uint64
		freeSpaceThresholdBytes uint64
		freeSpaceTimeframe      time.Duration
		backlogTimeframe        time.Duration
		diskFreeSpace           uint64
	}
	cfg := &testConfig{
		baselineRate:            128 * MB,
		freeSpaceThresholdBytes: 10 * GB,
		freeSpaceTimeframe:      10 * time.Second,
		backlogTimeframe:        5 * time.Minute,
		diskFreeSpace:           100 * GB,
	}

	for _, arg := range td.CmdArgs {
		switch arg.Key {
		case "baseline-rate":
			rate, err := crhumanize.ParseBytesPerSec[uint64](arg.Vals[0])
			require.NoError(t, err)
			cfg.baselineRate = rate
		case "free-space-threshold":
			threshold, err := crhumanize.ParseBytes[uint64](arg.Vals[0])
			require.NoError(t, err)
			cfg.freeSpaceThresholdBytes = threshold
		case "free-space-timeframe":
			timeframe, err := time.ParseDuration(arg.Vals[0])
			require.NoError(t, err)
			cfg.freeSpaceTimeframe = timeframe
		case "backlog-timeframe":
			timeframe, err := time.ParseDuration(arg.Vals[0])
			require.NoError(t, err)
			cfg.backlogTimeframe = timeframe
		case "disk-free-space":
			freeSpace, err := crhumanize.ParseBytes[uint64](arg.Vals[0])
			require.NoError(t, err)
			cfg.diskFreeSpace = freeSpace
		default:
			td.Fatalf(t, "unknown argument %q", arg.Key)
		}
	}

	pacerCfg := &Options{
		FreeSpaceThresholdBytes: cfg.freeSpaceThresholdBytes,
		FreeSpaceTimeframe:      cfg.freeSpaceTimeframe,
		BacklogTimeframe:        cfg.backlogTimeframe,
		BaselineRate:            func() uint64 { return cfg.baselineRate },
	}

	diskFreeSpaceFn := func() uint64 { return cfg.diskFreeSpace }
	rc := makeRateCalculator(pacerCfg, diskFreeSpaceFn, crtime.Mono(0))

	var output strings.Builder

	var currentTime crtime.Mono
	for _, line := range strings.Split(strings.TrimSpace(td.Input), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.Fields(line)
		switch parts[0] {
		case "set-baseline-rate":
			if len(parts) < 2 {
				td.Fatalf(t, "invalid set-baseline-rate line: %q (expected: set-baseline-rate rate)", line)
			}
			rate, err := crhumanize.ParseBytesPerSec[uint64](parts[1])
			require.NoError(t, err, "failed to parse baseline rate %q", parts[1])
			cfg.baselineRate = rate
			continue

		case "set-free-space":
			if len(parts) < 2 {
				td.Fatalf(t, "invalid set-free-space line: %q (expected: set-free-space bytes)", line)
			}
			freeSpace, err := crhumanize.ParseBytes[uint64](parts[1])
			require.NoError(t, err, "failed to parse free space %q", parts[1])
			cfg.diskFreeSpace = freeSpace
			continue

		case "add-debt":
			if len(parts) < 2 {
				td.Fatalf(t, "invalid add-debt line: %q (expected: add-debt bytes)", line)
			}
			debtBytes, err := crhumanize.ParseBytes[uint64](parts[1])
			require.NoError(t, err, "failed to parse debt bytes %q", parts[1])
			rc.AddDebt(debtBytes)
			continue
		}

		// Parse format: "15s recent=1MiB queued=10MiB [disable-pacing]"
		duration, err := time.ParseDuration(parts[0])
		require.NoError(t, err, "failed to parse duration %q", parts[0])

		// Parse key=value pairs for recent and queued
		var recentBytes, queuedBytes uint64
		disablePacing := false

		for i := 1; i < len(parts); i++ {
			part := parts[i]
			// Handle disable-pacing flag
			if part == "disable-pacing" {
				disablePacing = true
				continue
			}

			// Parse key=value
			kv := strings.SplitN(part, "=", 2)
			if len(kv) != 2 {
				td.Fatalf(t, "invalid key=value pair %q in line %q", part, line)
			}

			switch kv[0] {
			case "recent":
				recentBytes, err = crhumanize.ParseBytes[uint64](kv[1])
				require.NoError(t, err, "failed to parse recent bytes %q", kv[1])
			case "queued":
				queuedBytes, err = crhumanize.ParseBytes[uint64](kv[1])
				require.NoError(t, err, "failed to parse queued bytes %q", kv[1])
			default:
				td.Fatalf(t, "unknown key %q in line %q", kv[0], line)
			}
		}

		currentTime = crtime.Mono(duration)
		rc.Update(currentTime, recentBytes, queuedBytes, disablePacing)

		// Report current rate, debt and wait time.
		currentRate := int(math.Round(rc.currentRate))
		backlogRate := int(math.Round(rc.backlogRate))
		freeSpaceRate := int(math.Round(rc.freeSpaceRate))

		rateStr := string(crhumanize.BytesPerSec(currentRate, crhumanize.Compact))
		if backlogRate > 0 || freeSpaceRate > 0 {
			var extraStrs []string
			if backlogRate > 0 {
				extraStrs = append(extraStrs, "backlog="+string(crhumanize.BytesPerSec(backlogRate, crhumanize.Compact)))
			}
			if freeSpaceRate > 0 {
				extraStrs = append(extraStrs, "free-space="+string(crhumanize.BytesPerSec(freeSpaceRate, crhumanize.Compact)))
			}
			rateStr += " (" + strings.Join(extraStrs, " ") + ")"
		}

		if rc.debtBytes == 0 {
			fmt.Fprintf(&output, "%s: rate=%s debt=0B\n", time.Duration(currentTime), rateStr)
		} else {
			waitTime := rc.DebtWaitTime().Round(time.Second)
			fmt.Fprintf(&output, "%s: rate=%s debt=%s (wait %v)\n", time.Duration(currentTime), rateStr, crhumanize.Bytes(uint64(rc.debtBytes), crhumanize.Compact), waitTime)
		}
	}

	return output.String()
}
