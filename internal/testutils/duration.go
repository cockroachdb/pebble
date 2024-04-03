// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package testutils

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// DurationIsAtLeast verifies that the given duration is at least the given
// value.
func DurationIsAtLeast(t testing.TB, d, minValue time.Duration) {
	t.Helper()
	if runtime.GOOS == "windows" && minValue < 10*time.Millisecond {
		// Windows timer precision is coarse (on the order of 1 millisecond) and can
		// cause the duration for short operations to be 0.
		return
	}
	require.GreaterOrEqual(t, d, minValue)
}
