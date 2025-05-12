// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compressionanalyzer

import (
	"fmt"
	"math"
	"strings"
	"text/tabwriter"

	"github.com/cockroachdb/pebble/internal/compression"
)

// BlockKind breaks down the types of blocks into categories which might benefit
// from individual compressions settings.
type BlockKind uint8

const (
	DataBlock BlockKind = iota
	SSTableValueBlock
	BlobValueBlock
	IndexBlock
	// OtherBlock includes range del/key blocks, and other top-level metadata
	// blocks.
	OtherBlock
	numBlockKinds
)

var blockKindString = [...]string{
	DataBlock:         "data",
	SSTableValueBlock: "sstval",
	BlobValueBlock:    "blobval",
	IndexBlock:        "index",
	OtherBlock:        "other",
}

func (k BlockKind) String() string {
	return blockKindString[k]
}

// BlockSize identifies a range of block sizes.
type BlockSize uint8

const (
	Small BlockSize = iota
	Medium
	Large
	Huge
	numBlockSizes
)

var blockSizeCutoffKB = [...]int{
	Small:  0,   // <24KB
	Medium: 24,  // 24-48KB
	Large:  48,  // 48-128KB
	Huge:   128, // >128KB
}

func (bs BlockSize) String() string {
	switch bs {
	case 0:
		return fmt.Sprintf("<%dKB", blockSizeCutoffKB[bs+1])
	case numBlockSizes - 1:
		return fmt.Sprintf(">%dKB", blockSizeCutoffKB[bs])
	default:
		return fmt.Sprintf("%d-%dKB", blockSizeCutoffKB[bs], blockSizeCutoffKB[bs+1])
	}
}

func MakeBlockSize(size int) BlockSize {
	for i := BlockSize(1); i < numBlockSizes; i++ {
		if size < blockSizeCutoffKB[i]*1024 {
			return i - 1
		}
	}
	return numBlockSizes - 1
}

// Compressibility indicates how compressible a block is. It is determined by
// applying MinLZFastest and noting the reduction.
type Compressibility uint8

const (
	Incompressible Compressibility = iota
	MarginallyCompressible
	ModeratelyCompressible
	HighlyCompressible
	numCompressibility
)

// Each cutoff is a compression ratio (i.e. uncompressed size / compressed
// size), based on MinLZFastest.
var compressibilityCutoffs = [...]float64{
	Incompressible:         0,   // compressed to >90%
	MarginallyCompressible: 1.1, // compressed to 67-90%
	ModeratelyCompressible: 1.5, // compressed to 40-67%
	HighlyCompressible:     2.5, // compressed to <40%
}

func MakeCompressibility(uncompressedSize, compressedSize int) Compressibility {
	ratio := float64(uncompressedSize) / float64(compressedSize)
	for i := Compressibility(1); i < numCompressibility; i++ {
		if ratio < compressibilityCutoffs[i] {
			return i - 1
		}
	}
	return numCompressibility - 1
}

func (c Compressibility) String() string {
	switch c {
	case 0:
		return fmt.Sprintf("<%.1f", compressibilityCutoffs[c+1])
	case numCompressibility - 1:
		return fmt.Sprintf(">%.1f", compressibilityCutoffs[c])
	default:
		return fmt.Sprintf("%.1f-%.1f", compressibilityCutoffs[c], compressibilityCutoffs[c+1])
	}
}

var Settings = [...]compression.Setting{
	compression.Snappy,
	compression.MinLZFastest,
	compression.MinLZBalanced,
	compression.ZstdLevel1,
	compression.ZstdLevel3,
	compression.ZstdLevel5,
	compression.ZstdLevel7,
}

const numSettings = 7

// Buckets holds the results of all experiments.
type Buckets [numBlockKinds][numBlockSizes][numCompressibility]Bucket

// Bucket aggregates results for blocks of the same kind, size range, and
// compressibility.
type Bucket struct {
	UncompressedSize Welford
	Experiments      [numSettings]PerSetting
}

// PerSetting holds statistics from experiments on blocks in a bucket with a
// specific compression.Setting.
type PerSetting struct {
	CompressionRatio Welford
	// CPU times are in microseconds.
	CompressionTime   Welford
	DecompressionTime Welford
}

func (b *Buckets) String(minSamples int) string {
	var buf strings.Builder
	tw := tabwriter.NewWriter(&buf, 2, 1, 2, ' ', 0)

	fmt.Fprintf(tw, "Kind\tSize Range\tTest CR\tSamples\tSize\t")
	for _, s := range Settings {
		fmt.Fprintf(tw, "\t%s", s.String())
	}
	fmt.Fprintf(tw, "\n")
	for k := BlockKind(0); k < numBlockKinds; k++ {
		for sz := BlockSize(0); sz < numBlockSizes; sz++ {
			for c := Compressibility(0); c < numCompressibility; c++ {
				bucket := &b[k][sz][c]
				if bucket.UncompressedSize.Count() < int64(minSamples) {
					continue
				}
				fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\tCR", k, sz, c, bucket.UncompressedSize.Count(), withStdDev(bucket.UncompressedSize, "KB", 1.0/1024))
				for _, e := range (*b)[k][sz][c].Experiments {
					fmt.Fprintf(tw, "\t%s", withStdDev(e.CompressionRatio, "", 1.0))
				}
				fmt.Fprintf(tw, "\n")
				fmt.Fprintf(tw, "\t\t\t\t\tComp")
				for _, e := range (*b)[k][sz][c].Experiments {
					fmt.Fprintf(tw, "\t%s", withStdDev(e.CompressionTime, "us", 1.0))
				}
				fmt.Fprintf(tw, "\n")
				fmt.Fprintf(tw, "\t\t\t\t\tDecomp")
				for _, e := range (*b)[k][sz][c].Experiments {
					fmt.Fprintf(tw, "\t%s", withStdDev(e.DecompressionTime, "us", 1.0))
				}
				fmt.Fprintf(tw, "\n")
			}
		}
	}
	_ = tw.Flush()
	return buf.String()
}

func withStdDev(w Welford, units string, scale float64) string {
	mean := w.Mean() * scale
	if math.IsNaN(mean) {
		mean = 0
	}
	stddev := 0
	if s := w.SampleStandardDeviation(); !math.IsNaN(s) {
		stddev = int(100 * s / w.Mean())
	}
	return fmt.Sprintf("%.1f%s ± %d%%", mean, units, stddev)
}

func (b *Buckets) ToCSV(minSamples int) string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "Kind,Size Range,Test CR,Samples,Size,Size±")
	for _, s := range Settings {
		fmt.Fprintf(&buf, ",%s CR", s.String())
		fmt.Fprintf(&buf, ",%s CR±", s.String())
		fmt.Fprintf(&buf, ",%s Comp us", s.String())
		fmt.Fprintf(&buf, ",%s Comp±", s.String())
		fmt.Fprintf(&buf, ",%s Decomp us", s.String())
		fmt.Fprintf(&buf, ",%s Decomp±", s.String())
	}
	fmt.Fprintf(&buf, "\n")
	for k := BlockKind(0); k < numBlockKinds; k++ {
		for sz := BlockSize(0); sz < numBlockSizes; sz++ {
			for c := Compressibility(0); c < numCompressibility; c++ {
				bucket := &b[k][sz][c]
				if bucket.UncompressedSize.Count() < int64(minSamples) {
					continue
				}
				fmt.Fprintf(&buf, "%s,%s,%s,%d,%.0f,%.0f", k, sz, c, bucket.UncompressedSize.Count(), bucket.UncompressedSize.Mean(), bucket.UncompressedSize.SampleStandardDeviation())
				for _, e := range (*b)[k][sz][c].Experiments {
					fmt.Fprintf(&buf, ",%.1f,%.1f", e.CompressionRatio.Mean(), e.CompressionRatio.SampleStandardDeviation())
					fmt.Fprintf(&buf, ",%.1f,%.1f", e.CompressionTime.Mean(), e.CompressionTime.SampleStandardDeviation())
					fmt.Fprintf(&buf, ",%.1f,%.1f", e.DecompressionTime.Mean(), e.DecompressionTime.SampleStandardDeviation())
				}
				fmt.Fprintf(&buf, "\n")
			}
		}
	}
	return buf.String()
}
