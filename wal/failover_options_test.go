// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package wal

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestFailoverOptionsValidate uses the same configurations that crash
// dirProber.getMeanMax, and asserts Validate rejects them before they can
// reach it.
func TestFailoverOptionsValidate(t *testing.T) {
	testCases := []struct {
		name      string
		probe     time.Duration
		healthy   time.Duration
		sampling  time.Duration
		wantError bool
	}{
		// Crashing configurations: "interval is too long".
		{"ratio-180", time.Second, 3 * time.Minute, 0, true},
		{"ratio-150-default-healthy", 100 * time.Millisecond, 0, 0, true},
		{"ratio-129-just-over", time.Second, 129 * time.Second, 0, true},
		// Crashing configuration: "interval is too short".
		{"negative-healthy", time.Second, -1 * time.Second, 0, true},
		// Crashing configurations: non-positive ticker intervals.
		{"negative-probe", -1 * time.Second, 0, 0, true},
		{"negative-sampling", 0, 0, -1 * time.Second, true},
		// Overflowing configurations: getMeanMax's samplesNeeded computation,
		// (HealthyInterval+PrimaryDirProbeInterval-1)/PrimaryDirProbeInterval,
		// wraps int64 when both intervals are huge. With equal intervals the sum
		// wraps to a value that truncates to 0, so getMeanMax panics ("interval
		// is too short"). With HealthyInterval at MaxInt64 the sum wraps negative,
		// getMeanMax skips both assertion arms and returns (0,0) -- which makes
		// the monitor fail back to a possibly-stalled primary with no probe
		// evidence, a silent misbehavior worse than the crash.
		{"overflow-equal-huge", 2540400 * time.Hour, 2540400 * time.Hour, 0, true},
		{"overflow-maxint-healthy", time.Second, time.Duration(math.MaxInt64), 0, true},
		// Valid configurations.
		{"all-defaults", 0, 0, 0, false},
		{"explicit-defaults", time.Second, 15 * time.Second, 100 * time.Millisecond, false},
		{"ratio-128-at-limit", time.Second, 128 * time.Second, 0, false},
		{"metamorphic-worst-case", 100 * time.Millisecond, 11900 * time.Millisecond, 0, false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			o := FailoverOptions{
				PrimaryDirProbeInterval:   tc.probe,
				HealthyInterval:           tc.healthy,
				UnhealthySamplingInterval: tc.sampling,
			}
			err := o.Validate()
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			// The invariant that matters: any configuration Validate accepts must
			// not panic getMeanMax. Only this direction holds -- a configuration
			// rejected for a non-positive UnhealthySamplingInterval does not make
			// getMeanMax panic, so do not assert the converse.
			if err == nil {
				effective := o
				effective.EnsureDefaults()
				require.NotPanics(t, func() {
					p := &dirProber{interval: effective.PrimaryDirProbeInterval}
					p.getMeanMax(effective.HealthyInterval)
				})
			}
		})
	}
}

// TestFailoverOptionsValidateIdempotentWithDefaults asserts Validate behaves
// the same whether or not EnsureDefaults has already been applied.
func TestFailoverOptionsValidateIdempotentWithDefaults(t *testing.T) {
	o := FailoverOptions{}
	require.NoError(t, o.Validate())
	o.EnsureDefaults()
	require.NoError(t, o.Validate())
}

// TestFailoverOptionsValidateThresholds covers the two options whose
// non-positive values do not crash but silently disable behavior: a negative
// HealthyProbeLatencyThreshold makes the failback comparison permanently
// false, and a negative ElevatedWriteStallThresholdLag means the write-stall
// threshold is never elevated after a failback.
func TestFailoverOptionsValidateThresholds(t *testing.T) {
	t.Run("negative-healthy-probe-latency", func(t *testing.T) {
		o := FailoverOptions{HealthyProbeLatencyThreshold: -time.Millisecond}
		require.Error(t, o.Validate())
	})
	t.Run("negative-elevated-write-stall-lag", func(t *testing.T) {
		o := FailoverOptions{ElevatedWriteStallThresholdLag: -time.Second}
		require.Error(t, o.Validate())
	})
	t.Run("zero-is-defaulted-and-valid", func(t *testing.T) {
		require.NoError(t, FailoverOptions{}.Validate())
	})
}
