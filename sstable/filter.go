// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"sync/atomic"

	"github.com/cockroachdb/pebble/bloom"
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

type filterWriter interface {
	addKey(key []byte)
	finish() ([]byte, error)
	policyName() string
	// estimatedSize returns an estimate of the filter block size based on the
	// number of keys added so far. This is used for size estimation before the
	// filter is finished.
	estimatedSize() uint64
}

type tableFilterReader struct {
	policy  FilterPolicy
	metrics *FilterMetricsTracker
}

func newTableFilterReader(policy FilterPolicy, metrics *FilterMetricsTracker) *tableFilterReader {
	return &tableFilterReader{
		policy:  policy,
		metrics: metrics,
	}
}

func (f *tableFilterReader) mayContain(data, key []byte) bool {
	mayContain := f.policy.MayContain(data, key)
	if f.metrics != nil {
		if mayContain {
			f.metrics.misses.Add(1)
		} else {
			f.metrics.hits.Add(1)
		}
	}
	return mayContain
}

type tableFilterWriter struct {
	policy FilterPolicy
	writer FilterWriter
	// count is the count of the number of keys added to the filter.
	count int
}

func newTableFilterWriter(policy FilterPolicy) *tableFilterWriter {
	return &tableFilterWriter{
		policy: policy,
		writer: policy.NewWriter(),
	}
}

func (f *tableFilterWriter) addKey(key []byte) {
	f.count++
	f.writer.AddKey(key)
}

func (f *tableFilterWriter) finish() ([]byte, error) {
	if f.count == 0 {
		return nil, nil
	}
	return f.writer.Finish(nil), nil
}

func (f *tableFilterWriter) policyName() string {
	return f.policy.Name()
}

// estimatedSize returns an estimate of the filter block size based on the
// number of keys added. For bloom filters, this uses the actual bitsPerKey
// from the policy. For other filter types, it falls back to a default of 10
// bits per key.
func (f *tableFilterWriter) estimatedSize() uint64 {
	// Get bitsPerKey from the policy. bloom.FilterPolicy is an int representing
	// bits per key. For other filter implementations (e.g., test wrappers),
	// fall back to the default of 10 (see bloom.FilterPolicy documentation).
	bitsPerKey := 10
	if bp, ok := f.policy.(bloom.FilterPolicy); ok {
		bitsPerKey = int(bp)
	}
	_, nBytes := bloom.FilterSize(f.count, bitsPerKey)
	return uint64(nBytes)
}
