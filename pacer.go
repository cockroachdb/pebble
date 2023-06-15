// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"time"

	"github.com/cockroachdb/pebble/internal/rate"
)

// deletionPacerInfo contains any info from the db necessary to make deletion
// pacing decisions (to limit background IO usage so that it does not contend
// with foreground traffic).
type deletionPacerInfo struct {
	freeBytes     uint64
	obsoleteBytes uint64
	liveBytes     uint64
}

// deletionPacer rate limits deletions of obsolete files. This is necessary to
// prevent overloading the disk with too many deletions too quickly after a
// large compaction, or an iterator close. On some SSDs, disk performance can be
// negatively impacted if too many blocks are deleted very quickly, so this
// mechanism helps mitigate that.
type deletionPacer struct {
	limiter               *rate.Limiter
	freeSpaceThreshold    uint64
	obsoleteBytesMaxRatio float64

	getInfo func() deletionPacerInfo

	testingSleepFn func(delay time.Duration)
}

// newDeletionPacer instantiates a new deletionPacer for use when deleting
// obsolete files. The limiter passed in must be a singleton shared across this
// pebble instance.
func newDeletionPacer(limiter *rate.Limiter, getInfo func() deletionPacerInfo) *deletionPacer {
	return &deletionPacer{
		limiter: limiter,
		// If there are less than freeSpaceThreshold bytes of free space on
		// disk, do not pace deletions at all.
		freeSpaceThreshold: 16 << 30, // 16 GB
		// If the ratio of obsolete bytes to live bytes is greater than
		// obsoleteBytesMaxRatio, do not pace deletions at all.
		obsoleteBytesMaxRatio: 0.20,

		getInfo: getInfo,
	}
}

// limit applies rate limiting if the current free disk space is more than
// freeSpaceThreshold, and the ratio of obsolete to live bytes is less than
// obsoleteBytesMaxRatio.
func (p *deletionPacer) limit(amount uint64, info deletionPacerInfo) {
	obsoleteBytesRatio := float64(1.0)
	if info.liveBytes > 0 {
		obsoleteBytesRatio = float64(info.obsoleteBytes) / float64(info.liveBytes)
	}
	paceDeletions := info.freeBytes > p.freeSpaceThreshold &&
		obsoleteBytesRatio < p.obsoleteBytesMaxRatio
	if paceDeletions {
		p.limiter.Wait(float64(amount))
	} else {
		p.limiter.Remove(float64(amount))
	}
}

// maybeThrottle slows down a deletion of this file if it's faster than
// opts.TargetByteDeletionRate.
func (p *deletionPacer) maybeThrottle(bytesToDelete uint64) {
	p.limit(bytesToDelete, p.getInfo())
}
