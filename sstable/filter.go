// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"sync/atomic"

	"github.com/cockroachdb/pebble/internal/base"
)

// FilterMetrics holds metrics for the filter policy.
type FilterMetrics struct {
	// The number of hits for the filter policy. This is the
	// number of times the filter policy was successfully used to avoid access
	// of a data block.
	Hits int64
	// The number of misses for the filter policy. This is the number of times
	// the filter policy was checked but was unable to filter an access of a data
	// block.
	Misses int64
}

// FilterMetricsTracker is used to keep track of filter metrics. It contains the
// same metrics as FilterMetrics, but they can be updated atomically. An
// instance of FilterMetricsTracker can be passed to a Reader as a ReaderOption.
type FilterMetricsTracker struct {
	// See FilterMetrics.Hits.
	hits atomic.Int64
	// See FilterMetrics.Misses.
	misses atomic.Int64
}

// Load returns the current values as FilterMetrics.
func (m *FilterMetricsTracker) Load() FilterMetrics {
	return FilterMetrics{
		Hits:   m.hits.Load(),
		Misses: m.misses.Load(),
	}
}

type tableFilterReader struct {
	decoder base.TableFilterDecoder
	metrics *FilterMetricsTracker
}

func newTableFilterReader(
	decoder base.TableFilterDecoder, metrics *FilterMetricsTracker,
) *tableFilterReader {
	return &tableFilterReader{
		decoder: decoder,
		metrics: metrics,
	}
}

func (f *tableFilterReader) mayContain(data, key []byte) bool {
	mayContain := f.decoder.MayContain(data, key)
	if f.metrics != nil {
		if mayContain {
			f.metrics.misses.Add(1)
		} else {
			f.metrics.hits.Add(1)
		}
	}
	return mayContain
}
