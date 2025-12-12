// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tieredmeta

import (
	"encoding/binary"

	"github.com/RaduBerinde/tdigest"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/pkg/errors"
)

// digestDelta is the compression argument for t-digest, used to specify the
// tradeoff between accuracy and memory consumption: meaning higher values give
// more accuracy, but use more memory/space. Specifically, the digest has at most
// 2*compression centroids, with ~1.3*compression in practice.
//
// A value of 100 is a common default for t-digest implementations, which should
// provide at least ±1% quantile accuracy (and much better at the tails).
const digestDelta = 100

// StatsHistogram is a quantile sketch using t-digest to track the distribution
// of a bytes value (with a caller-determined meaning) by TieringAttribute. It
// can efficiently answer questions like:
//   - What fraction of bytes have attribute <= threshold? (CDF)
//   - What attribute value covers q% of bytes? (Quantile)
//   - How many bytes are "cold" vs "hot"?
//
// The sketch is mergeable, making it easy to combine statistics from
// multiple files without bucket alignment issues.
type StatsHistogram struct {
	// TotalBytes is the sum of all bytes recorded (including BytesNoAttr).
	TotalBytes uint64
	// TotalCount is the number of records added.
	TotalCount uint64
	// BytesNoAttr tracks bytes with attribute=0 separately, as this typically
	// indicates unset/special values that shouldn't be tiered.
	BytesNoAttr uint64
	// digest is the t-digest for non-zero attributes (read-only).
	// May be nil if no non-zero samples have been recorded.
	digest *tdigest.TDigest
}

// CDF returns the fraction of non-zero bytes with attribute <= threshold.
// Returns a value in [0, 1].
// Example: CDF(coldThreshold) tells you what fraction of bytes are "cold".
func (s *StatsHistogram) CDF(threshold base.TieringAttribute) float64 {
	if s.digest == nil || s.BytesWithAttr() == 0 {
		return 0
	}
	return s.digest.CDF(float64(threshold))
}

// Quantile returns the attribute value at quantile ~q (where q is in [0, 1]) of
// bytes with a tiering attribute set.
// Example: Quantile(0.5) returns a value close to the median value.
func (s *StatsHistogram) Quantile(q float64) base.TieringAttribute {
	if s.digest == nil {
		return 0
	}
	return base.TieringAttribute(s.digest.Quantile(q))
}

// BytesWithAttr returns bytes that have a tiering attribute set, excluding
// bytes with attr=0 (which typically indicates unset/special values).
func (s *StatsHistogram) BytesWithAttr() uint64 {
	return s.TotalBytes - s.BytesNoAttr
}

// BytesBelowThreshold estimates how many bytes (excluding NoAttrBytes) have
// attribute <= threshold. This is useful for tiering decisions.
func (s *StatsHistogram) BytesBelowThreshold(threshold base.TieringAttribute) uint64 {
	return uint64(s.CDF(threshold) * float64(s.BytesWithAttr()))
}

// BytesAboveThreshold estimates how many non-zero bytes have attribute > threshold.
// This is useful for tiering decisions.
func (s *StatsHistogram) BytesAboveThreshold(threshold base.TieringAttribute) uint64 {
	return s.BytesWithAttr() - s.BytesBelowThreshold(threshold)
}

// encode serializes the histogram to bytes. The encoding format is:
//
//	<total bytes> <total count> <zero count> <digest size> <digest data>
func (s *StatsHistogram) encode() []byte {
	// Calculate the size needed for the t-digest.
	var digestSize int
	if s.digest != nil {
		digestSize = s.digest.SerializedSize()
	}

	// Pre-allocate with estimated capacity (4 uvarints + digest).
	// Each uvarint is at most 10 bytes for uint64.
	buf := make([]byte, 0, 4*binary.MaxVarintLen64+digestSize)

	buf = binary.AppendUvarint(buf, s.TotalBytes)
	buf = binary.AppendUvarint(buf, s.TotalCount)
	buf = binary.AppendUvarint(buf, s.BytesNoAttr)
	buf = binary.AppendUvarint(buf, uint64(digestSize))

	// Serialize the t-digest if present.
	if s.digest != nil {
		buf = s.digest.Serialize(buf)
	}

	return buf
}

// DecodeStatsHistogram decodes a StatsHistogram from the provided buffer.
// The encoding format is:
//
//	<total bytes> <total count> <zero bytes> <digest size> <digest data>
func DecodeStatsHistogram(buf []byte) (StatsHistogram, error) {
	var h StatsHistogram
	var n int

	h.TotalBytes, n = binary.Uvarint(buf)
	if n <= 0 {
		return StatsHistogram{}, base.CorruptionErrorf("cannot decode TotalBytes from buf %x", buf)
	}
	buf = buf[n:]

	h.TotalCount, n = binary.Uvarint(buf)
	if n <= 0 {
		return StatsHistogram{}, base.CorruptionErrorf("cannot decode TotalCount from buf %x", buf)
	}
	buf = buf[n:]

	h.BytesNoAttr, n = binary.Uvarint(buf)
	if n <= 0 {
		return StatsHistogram{}, base.CorruptionErrorf("cannot decode BytesNoAttr from buf %x", buf)
	}
	buf = buf[n:]

	digestSize, n := binary.Uvarint(buf)
	if n <= 0 {
		return StatsHistogram{}, base.CorruptionErrorf("cannot decode digest size from buf %x", buf)
	}
	buf = buf[n:]

	if int(digestSize) != len(buf) {
		return StatsHistogram{}, errors.Errorf("histogram digest size %d does not match remaining data %d",
			digestSize, len(buf))
	}

	if digestSize > 0 {
		var digest tdigest.TDigest
		_, err := tdigest.Deserialize(&digest, buf[:digestSize])
		if err != nil {
			return StatsHistogram{}, err
		}
		h.digest = &digest
	}

	return h, nil
}

// Merge combines another histogram into this one using a t-digest Merger.
// This is used to aggregate statistics from multiple files.
func (s *StatsHistogram) Merge(other *StatsHistogram) {
	s.TotalBytes += other.TotalBytes
	s.TotalCount += other.TotalCount
	s.BytesNoAttr += other.BytesNoAttr

	merger := tdigest.MakeMerger(digestDelta)
	if s.digest != nil {
		merger.Merge(s.digest)
	}
	if other.digest != nil {
		merger.Merge(other.digest)
	}
	merged := merger.Digest()
	s.digest = &merged
}

// histogramWriter wraps a tdigest.Builder for building during sstable/blob file
// writing.
type histogramWriter struct {
	builder    tdigest.Builder
	totalBytes uint64
	totalCount uint64
	zeroBytes  uint64
}

func newHistogramWriter() *histogramWriter {
	return &histogramWriter{
		builder: tdigest.MakeBuilder(digestDelta),
	}
}

// record adds a data point to the histogram. Records with attr=0 are tracked
// separately and excluded from the t-digest - as zero typically indicates
// an unset or special value that shouldn't influence tiering decisions.
func (w *histogramWriter) record(attr base.TieringAttribute, bytes uint64) {
	w.totalCount++
	w.totalBytes += bytes
	if attr == 0 {
		w.zeroBytes += bytes
		return
	}
	w.builder.Add(float64(attr), float64(bytes))
}

func (w *histogramWriter) encode() []byte {
	digest := w.builder.Digest()
	h := StatsHistogram{
		TotalBytes:  w.totalBytes,
		TotalCount:  w.totalCount,
		BytesNoAttr: w.zeroBytes,
		digest:      &digest,
	}
	return h.encode()
}
