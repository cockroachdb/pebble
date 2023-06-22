// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeletionPacerMaybeThrottle(t *testing.T) {
	testCases := []struct {
		freeSpaceThreshold uint64
		freeBytes          uint64
		obsoleteBytes      uint64
		liveBytes          uint64
		shouldPace         bool
	}{
		{
			freeSpaceThreshold: 10,
			freeBytes:          100,
			obsoleteBytes:      1,
			liveBytes:          100,
			shouldPace:         true,
		},
		// As freeBytes < freeSpaceThreshold, there should be no throttling.
		{
			freeSpaceThreshold: 10,
			freeBytes:          5,
			obsoleteBytes:      1,
			liveBytes:          100,
			shouldPace:         false,
		},
		// As obsoleteBytesRatio > 0.20, there should be no throttling.
		{
			freeSpaceThreshold: 10,
			freeBytes:          500,
			obsoleteBytes:      50,
			liveBytes:          100,
			shouldPace:         false,
		},
		// When obsolete ratio unknown, there should be no throttling.
		{
			freeSpaceThreshold: 10,
			freeBytes:          500,
			obsoleteBytes:      0,
			liveBytes:          0,
			shouldPace:         false,
		},
	}
	for tcIdx, tc := range testCases {
		t.Run(fmt.Sprintf("%d", tcIdx), func(t *testing.T) {
			getInfo := func() deletionPacerInfo {
				return deletionPacerInfo{
					freeBytes:     tc.freeBytes,
					liveBytes:     tc.liveBytes,
					obsoleteBytes: tc.obsoleteBytes,
				}
			}
			pacer := newDeletionPacer(getInfo)
			pacer.freeSpaceThreshold = tc.freeSpaceThreshold
			result := pacer.shouldPace()
			require.Equal(t, tc.shouldPace, result)
		})
	}
}
