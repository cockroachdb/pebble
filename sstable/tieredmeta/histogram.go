// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tieredmeta

import (
	"encoding/binary"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
)

type SpanIDStats struct {
	SpanID base.TieringSpanID
}

const tempAgeThreshold = base.TieringAttribute(40)

const numBuckets = 10

// StatsHistogram is a histogram that is bucketed on the TieringAttribute
// axis, and a bucket value records the total size in bytes in that bucket.
//
// We desire the histogram to be concise, while having enough fidelity to
// inform tiering decisions. Additionally, we need to sum and subtract
// histograms for the same SpanID. We could design something sophisticated,
// but instead of over-engineering here, we assume that the user has provided
// an age threshold (assuming the tiering attribute is some kind of
// timestamp), at which rows become cold. For now, to avoid plumbing, we
// hardcode it to 40, whatever the units (say 40 days), for histogram
// initialization.
//
// We break this up into 20% granularity buckets. So each bucket is 8 wide in
// the above example.
//
// When generating the histogram, we sample the current
// TieringPolicy.ColdTierLTThreshold. Say the value is 500. So the current
// time is ~500+40=540. We find the smallest multiple of the bucket
// granularity that is > current time, which is 544 in this case. Then we keep
// 10 samples in the histogram, each 8 wide, starting from 544+8, so going
// down to 472. That is, we've overlapped with the ColdTierLTThreshold
// sufficiently, if the user changes the policy slightly. Anything older than
// that is fused into one underflow bucket.
//
// Overflows are possible due to application errors (say accidentally writing
// data far in the future). We put these overflows into an unaccounted bucket.
//
// Underflow and overflows are treated differently since we need to add
// histograms that were generated at different times. A prefix of the oldest
// buckets of the older histogram will get merged into the underflow bucket in
// the sum. However, since the overflow will eventually become current, and
// then eventually become underflow, and we can't really track when that
// happens, they are permanently in the unaccounted bucket.
//
// Age plumbing and age changes: TODO(sumeer): we will need to interpolate the
// compute bucket counts, or reread all the old files in the background and
// recompute their histograms. Given that these histograms are stored in the
// files themselves, doing such a recomputation seems wasteful since the
// result will be lost on restart.
type StatsHistogram struct {
	// A BucketStart with a zero value, does not actually start at zero.
	// Instead, it is defined over the interval [1, BucketLength) and is the
	// only bucket which is smaller than BucketLength.
	BucketStart      base.TieringAttribute
	BucketLength     base.TieringAttribute
	Buckets          [numBuckets]Bucket
	UnderflowBytes   uint64
	UnaccountedBytes uint64
	ZeroBytes        uint64
	// Count is useful to compute the mean size.
	Count uint64
}

type Bucket struct {
	Bytes uint64
}

const MaxStatsHistogramSizeExcludingBuckets = 6 * binary.MaxVarintLen64

func (h *StatsHistogram) encode() []byte {
	buf := make([]byte, MaxStatsHistogramSizeExcludingBuckets+(numBuckets+1)*binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, uint64(h.BucketStart))
	n += binary.PutUvarint(buf[n:], uint64(h.BucketLength))
	// We encode the number of buckets, to allow for future changes in the struct.
	n += binary.PutUvarint(buf[n:], uint64(numBuckets))
	for i := 0; i < numBuckets; i++ {
		n += binary.PutUvarint(buf[n:], h.Buckets[i].Bytes)
	}
	n += binary.PutUvarint(buf[n:], h.UnderflowBytes)
	n += binary.PutUvarint(buf[n:], h.UnaccountedBytes)
	n += binary.PutUvarint(buf[n:], h.ZeroBytes)
	n += binary.PutUvarint(buf[n:], h.Count)
	return buf[:n]
}

func (h *StatsHistogram) decode(data []byte) error {
	uvarintAndCheck := func() (uint64, error) {
		v, n := binary.Uvarint(data)
		if n <= 0 {
			return 0, errors.AssertionFailedf("could not decode uvarint")
		}
		data = data[n:]
		return v, nil
	}
	if v, err := uvarintAndCheck(); err != nil {
		return err
	} else {
		h.BucketStart = base.TieringAttribute(v)
	}
	if v, err := uvarintAndCheck(); err != nil {
		return err
	} else {
		h.BucketLength = base.TieringAttribute(v)
	}
	if v, err := uvarintAndCheck(); err != nil {
		return err
	} else if v != numBuckets {
		return errors.AssertionFailedf("expected %d buckets, got %d", numBuckets, v)
	}
	for i := 0; i < numBuckets; i++ {
		if v, err := uvarintAndCheck(); err != nil {
			return err
		} else {
			h.Buckets[i].Bytes = v
		}
	}
	if v, err := uvarintAndCheck(); err != nil {
		return err
	} else {
		h.UnderflowBytes = v
	}
	if v, err := uvarintAndCheck(); err != nil {
		return err
	} else {
		h.UnaccountedBytes = v
	}
	if v, err := uvarintAndCheck(); err != nil {
		return err
	} else {
		h.ZeroBytes = v
	}
	if v, err := uvarintAndCheck(); err != nil {
		return err
	} else {
		h.Count = v
	}
	return nil
}

// Merge is used to sum histograms stored in different files. The bucket
// boundaries will be aligned, but they will be shifted since the histograms
// are generated at different times.
func (h *StatsHistogram) Merge(other *StatsHistogram) {
	if h.BucketLength != other.BucketLength {
		panic(errors.AssertionFailedf("bucket length %d != %d", h.BucketLength, other.BucketLength))
	}
	var h2 *StatsHistogram
	if other.BucketStart < h.BucketStart {
		otherCopy := *other
		otherCopy.ageTo(h.BucketStart)
		h2 = &otherCopy
	} else {
		h2 = other
		if other.BucketStart > h.BucketStart {
			h.ageTo(other.BucketStart)
		}
	}
	for i := 0; i < numBuckets; i++ {
		h.Buckets[i].Bytes += h2.Buckets[i].Bytes
	}
	h.UnderflowBytes += other.UnderflowBytes
	h.UnaccountedBytes += other.UnaccountedBytes
	h.ZeroBytes += other.ZeroBytes
	h.Count += other.Count
}

// Subtract requires that h represents a histogram to which other was
// previously added. It is used when a file corresponding to other has been
// compacted away.
func (h *StatsHistogram) Subtract(other *StatsHistogram) {
	if h.BucketLength != other.BucketLength {
		panic(errors.AssertionFailedf("bucket length %d != %d", h.BucketLength, other.BucketLength))
	}
	var h2 *StatsHistogram
	if other.BucketStart < h.BucketStart {
		otherCopy := *other
		otherCopy.ageTo(h.BucketStart)
		h2 = &otherCopy
	} else {
		h2 = other
		if other.BucketStart > h.BucketStart {
			h.ageTo(other.BucketStart)
		}
	}
	for i := 0; i < numBuckets; i++ {
		h.Buckets[i].Bytes = invariants.SafeSub(h.Buckets[i].Bytes, h2.Buckets[i].Bytes)
	}
	h.UnderflowBytes = invariants.SafeSub(h.UnderflowBytes, h2.UnderflowBytes)
	h.UnaccountedBytes = invariants.SafeSub(h.UnaccountedBytes, h2.UnaccountedBytes)
	h.ZeroBytes = invariants.SafeSub(h.ZeroBytes, h2.ZeroBytes)
	h.Count = invariants.SafeSub(h.Count, h2.Count)
}

// INVARIANT: bucketStart > h.BucketStart.
func (h *StatsHistogram) ageTo(bucketStart base.TieringAttribute) {
	offset := min((bucketStart-h.BucketStart)/h.BucketLength, numBuckets)
	// Oldest buckets are moved to the underflow.
	for i := base.TieringAttribute(0); i < offset; i++ {
		h.UnderflowBytes += h.Buckets[i].Bytes
	}
	// Newer buckets are shifted to be older.
	for i := offset; i < numBuckets; i++ {
		h.Buckets[i-offset].Bytes = h.Buckets[i].Bytes
	}
	// Remaining buckets are zeroed out.
	for i := numBuckets - offset; i < numBuckets; i++ {
		h.Buckets[i] = Bucket{}
	}
	h.BucketStart = bucketStart
}

type histogramWriter struct {
	stats StatsHistogram
}

func newHistogramWriter(curColdTierLTThreshold base.TieringAttribute) *histogramWriter {
	cur := curColdTierLTThreshold + tempAgeThreshold
	bucketLength := tempAgeThreshold / (numBuckets / 2) // 20% granularity
	lastBucketEnd := (cur + 2*bucketLength - 1) / bucketLength
	var bucketStart base.TieringAttribute
	if lastBucketEnd >= (bucketLength * numBuckets) {
		bucketStart = lastBucketEnd - (bucketLength * numBuckets)
	} else {
		bucketStart = 0
	}
	return &histogramWriter{
		stats: StatsHistogram{
			BucketStart:  bucketStart,
			BucketLength: bucketLength,
		},
	}
}

func (w *histogramWriter) record(attr base.TieringAttribute, bytes uint64) {
	w.stats.Count++
	if attr == 0 {
		w.stats.ZeroBytes += bytes
		return
	}
	if attr < w.stats.BucketStart {
		w.stats.UnderflowBytes += bytes
		return
	}
	bucketIndex := (attr - w.stats.BucketStart) / w.stats.BucketLength
	if bucketIndex >= numBuckets {
		w.stats.UnaccountedBytes += bytes
		return
	}
	w.stats.Buckets[bucketIndex].Bytes += bytes
}

func (w *histogramWriter) encode() []byte {
	return w.stats.encode()
}
