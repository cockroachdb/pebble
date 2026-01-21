// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/RaduBerinde/tdigest"
	"github.com/cockroachdb/crlib/crbytes"
	"github.com/cockroachdb/crlib/crhumanize"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/ascii"
	"github.com/cockroachdb/pebble/internal/ascii/table"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/metricsutil"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/spf13/cobra"
)

// digestDelta is the compression argument for t-digest, used to specify the
// tradeoff between accuracy and memory consumption. A value of 100 is a common
// default, providing at least +/-1% quantile accuracy.
const digestDelta = 100

// stat maintains running statistics for mean, variance, and percentiles using
// Welford's algorithm combined with a t-digest sketch.
type stat struct {
	metricsutil.Welford
	builder tdigest.Builder
}

// makeStat creates a new stat instance.
func makeStat() stat {
	return stat{builder: tdigest.MakeBuilder(digestDelta)}
}

// Add incorporates a new data point x into the running statistics.
func (s *stat) Add(x float64) {
	s.Welford.Add(x)
	s.builder.Add(x, 1)
}

// Percentile returns the value at the given percentile (0.0 to 1.0).
// For example, Percentile(0.5) returns the median.
func (s *stat) Percentile(p float64) float64 {
	d := s.builder.Digest()
	return d.Quantile(p)
}

// levelStats holds the running statistics for sstable metadata for a single level.
type levelStats struct {
	numTotalFiles   int64
	numSampledFiles int64

	// commonPrefix records the longest common prefix of user keys within each
	// sstable.
	commonPrefix stat

	// Size metrics (in bytes).
	sstableFileSize          stat
	sstableFileSizePlusBlobs stat

	// KV metrics.
	numKVsPerFile       stat
	bytesPerKV          stat // (RawKeySize + RawValueSize) / NumEntries
	bytesPerKVWithBlobs stat // includes blob value sizes

	// Index metrics.
	numFilesWithTwoLevelIndex int64
	// indexSize records the total size of the index block(s) within each sstable.
	indexSize stat // IndexSize
	// numEntriesPerIndexBlock records the number of entries per index block
	// (excluding top-level index blocks).
	numEntriesPerIndexBlock stat

	numDataBlocks stat

	// Filter metrics.
	filterBlockSize stat
}

func makeLevelStats() levelStats {
	return levelStats{
		commonPrefix:             makeStat(),
		sstableFileSize:          makeStat(),
		sstableFileSizePlusBlobs: makeStat(),
		numKVsPerFile:            makeStat(),
		bytesPerKV:               makeStat(),
		bytesPerKVWithBlobs:      makeStat(),
		indexSize:                makeStat(),
		numEntriesPerIndexBlock:  makeStat(),
		numDataBlocks:            makeStat(),
		filterBlockSize:          makeStat(),
	}
}

// metadataStats holds statistics for all levels.
type metadataStats struct {
	levels [manifest.NumLevels]levelStats
}

func makeMetadataStats() metadataStats {
	var stats metadataStats
	for i := range stats.levels {
		stats.levels[i] = makeLevelStats()
	}
	return stats
}

func (d *dbT) runAnalyzeMetadata(cmd *cobra.Command, args []string) {
	stdout, stderr := cmd.OutOrStdout(), cmd.ErrOrStderr()
	isTTY := isTTY(stdout)
	dirname := args[0]

	err := func() error {
		v, err := d.readCurrentVersion(dirname)
		if err != nil {
			return err
		}
		objProvider, err := objstorageprovider.Open(objstorageprovider.DefaultSettings(d.opts.FS, dirname))
		if err != nil {
			return err
		}
		defer func() { _ = objProvider.Close() }()

		// Build level-separated sstable list.
		var levelTables [manifest.NumLevels][]*manifest.TableMetadata
		var totalTables int
		for levelIdx, l := range v.Levels {
			for t := range l.All() {
				if t.Virtual {
					continue
				}
				levelTables[levelIdx] = append(levelTables[levelIdx], t.PhysicalMeta())
				totalTables++
			}
		}

		if totalTables == 0 {
			fmt.Fprintln(stderr, "no sstables found")
			return nil
		}

		if isTTY {
			fmt.Fprintf(stdout, "Found %d sstables across %d levels.\n", totalTables, manifest.NumLevels)
		}

		// Sampling loop.
		stats := makeMetadataStats()
		for i := range stats.levels {
			stats.levels[i].numTotalFiles = int64(len(levelTables[i]))
		}
		startTime := time.Now()
		lastReportTime := startTime
		const reportPeriod = 10 * time.Second

		rng := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
		levelIdx := 0
		sampledFiles := 0

		for {
			// Check stopping conditions.
			shouldStop := false
			if d.analyzeMetadata.timeout > 0 && time.Since(startTime) > d.analyzeMetadata.timeout {
				shouldStop = true
			}
			if d.analyzeMetadata.samplePercent > 0 && d.analyzeMetadata.samplePercent < 100 {
				percentage := float64(sampledFiles) * 100 / float64(totalTables)
				if percentage >= float64(d.analyzeMetadata.samplePercent) {
					shouldStop = true
				}
			}

			// Periodic reporting.
			if shouldStop || time.Since(lastReportTime) > reportPeriod {
				if isTTY {
					clearScreen(stdout)
				}
				if isTTY || shouldStop {
					printMetadataStats(stdout, &stats, sampledFiles, totalTables)
				}
				if shouldStop {
					return nil
				}
				lastReportTime = time.Now()
			}

			// Find next non-empty level (round-robin).
			found := false
			for i := 0; i < manifest.NumLevels; i++ {
				idx := (levelIdx + i) % manifest.NumLevels
				if len(levelTables[idx]) > 0 {
					levelIdx = idx
					found = true
					break
				}
			}
			if !found {
				// All tables sampled.
				if isTTY {
					clearScreen(stdout)
				}
				printMetadataStats(stdout, &stats, sampledFiles, totalTables)
				return nil
			}

			// Pick random sstable from this level.
			tableIdx := rng.IntN(len(levelTables[levelIdx]))
			table := levelTables[levelIdx][tableIdx]

			// Remove from list (sample without replacement).
			levelTables[levelIdx] = slices.Delete(levelTables[levelIdx], tableIdx, tableIdx+1)

			// Read and process metadata.
			if err := d.processSSTableMetadata(objProvider, v.Comparer(), table, &stats, levelIdx); err != nil {
				// Continue on individual file errors.
				fmt.Fprintf(stderr, "error reading file %s: %s\n", table.TableBacking.DiskFileNum, err)
			} else {
				sampledFiles++
			}

			levelIdx = (levelIdx + 1) % manifest.NumLevels
		}
	}()

	if err != nil {
		fmt.Fprintln(stderr, err)
	}
}

func (d *dbT) processSSTableMetadata(
	objProvider objstorage.Provider,
	cmp *base.Comparer,
	m *manifest.TableMetadata,
	stats *metadataStats,
	level int,
) error {
	ctx := context.Background()
	f, err := objProvider.OpenForReading(ctx, base.FileTypeTable, m.TableBacking.DiskFileNum, objstorage.OpenOptions{})
	if err != nil {
		return err
	}

	opts := d.opts.MakeReaderOptions()
	opts.Mergers = d.mergers
	opts.Comparers = d.comparers
	r, err := sstable.NewReader(ctx, f, opts)
	if err != nil {
		return errors.CombineErrors(err, f.Close())
	}
	defer func() { _ = r.Close() }()

	properties, err := r.ReadPropertiesBlock(ctx, nil /* buffer pool */)
	if err != nil {
		return err
	}

	// Update statistics for this level.
	ls := &stats.levels[level]
	ls.numSampledFiles++

	smallest := cmp.Split.Prefix(m.Smallest().UserKey)
	largest := cmp.Split.Prefix(m.Largest().UserKey)
	ls.commonPrefix.Add(float64(crbytes.CommonPrefix(smallest, largest)))

	ls.sstableFileSize.Add(float64(m.Size))
	ls.sstableFileSizePlusBlobs.Add(float64(m.Size + m.EstimatedReferenceSize()))

	ls.numKVsPerFile.Add(float64(properties.NumEntries))

	if properties.NumEntries > 0 {
		ls.bytesPerKV.Add(float64(m.Size) / float64(properties.NumEntries))
		ls.bytesPerKVWithBlobs.Add(float64(m.Size+m.EstimatedReferenceSize()) / float64(properties.NumEntries))
	}

	if properties.IndexPartitions > 0 {
		ls.numFilesWithTwoLevelIndex++
	}

	ls.indexSize.Add(float64(properties.IndexSize))
	numIndexBlocks := max(1, properties.IndexPartitions)
	for range numIndexBlocks {
		ls.numEntriesPerIndexBlock.Add(float64(properties.NumDataBlocks) / float64(numIndexBlocks))
	}
	ls.numDataBlocks.Add(float64(properties.NumDataBlocks))
	ls.filterBlockSize.Add(float64(properties.FilterSize))
	return nil
}

func printMetadataStats(w io.Writer, stats *metadataStats, sampled, total int) {
	fmt.Fprintf(w, "Sampled %d / %d files (%.1f%%)\n\n", sampled, total, float64(sampled)*100/float64(total))

	formatBytes := func(v float64) string {
		if v < 9.5 {
			return string(crhumanize.Float(v, 2)) + "B"
		}
		return string(crhumanize.Bytes(int64(v), crhumanize.Compact, crhumanize.OmitI))
	}
	formatCount := func(v float64) string {
		if v < 9.5 {
			return string(crhumanize.Float(v, 2))
		}
		return string(crhumanize.Count(int64(v), crhumanize.Compact))
	}

	formatStat := func(s *stat, format func(float64) string) string {
		if s.Count() == 0 {
			return ""
		}
		if s.Mean() == 0 {
			return format(0)
		}
		return fmt.Sprintf("%s ± %s", format(s.Mean()), crhumanize.Percent(s.StdDev(), s.Mean()))
	}

	formatStatWithTotal := func(s *stat, numFiles int64, format func(float64) string) string {
		if s.Count() == 0 {
			return ""
		}
		if s.Mean() == 0 {
			return format(0)
		}
		return fmt.Sprintf("%s ± %s (%s total)", format(s.Mean()), crhumanize.Percent(s.StdDev(), s.Mean()), format(s.Mean()*float64(numFiles)))
	}

	formatPercentiles := func(s *stat, format func(float64) string) string {
		if s.Count() == 0 {
			return ""
		}
		return fmt.Sprintf("p90=%s max=%s",
			format(s.Percentile(0.9)),
			format(s.Percentile(1.0)))
	}

	const titleWidth = 22

	type row struct {
		title   string
		value   func(*levelStats) string
		divider bool // if true, a horizontal divider is placed before this row
	}
	elems := []table.Element{
		table.String("", titleWidth, table.AlignCenter, func(r row) string { return r.title }),
	}
	for level := range manifest.NumLevels {
		elems = append(elems,
			table.Div(),
			table.String(fmt.Sprintf("L%d", level), 4, table.AlignCenter, func(r row) string { return r.value(&stats.levels[level]) }),
		)
	}
	tab := table.Define[row](elems...)

	pctTitle := ""

	rows := []row{
		{divider: true, title: "SSTables", value: func(ls *levelStats) string {
			if ls.numTotalFiles == 0 {
				return ""
			}
			return fmt.Sprintf("%d (%s sampled)", ls.numSampledFiles, crhumanize.Percent(ls.numSampledFiles, ls.numTotalFiles))
		}},
		{divider: true, title: "SSTable size", value: func(ls *levelStats) string {
			return formatStatWithTotal(&ls.sstableFileSize, ls.numTotalFiles, formatBytes)
		}},
		{title: pctTitle, value: func(ls *levelStats) string {
			return formatPercentiles(&ls.sstableFileSize, formatBytes)
		}},
		{divider: true, title: "SSTable+blob ref size", value: func(ls *levelStats) string {
			return formatStatWithTotal(&ls.sstableFileSizePlusBlobs, ls.numTotalFiles, formatBytes)
		}},
		{title: pctTitle, value: func(ls *levelStats) string {
			return formatPercentiles(&ls.sstableFileSizePlusBlobs, formatBytes)
		}},
		{divider: true, title: "KVs", value: func(ls *levelStats) string {
			return formatStatWithTotal(&ls.numKVsPerFile, ls.numTotalFiles, formatCount)
		}},
		{title: pctTitle, value: func(ls *levelStats) string {
			return formatPercentiles(&ls.numKVsPerFile, formatCount)
		}},
		{divider: true, title: "SSTable bytes per KV", value: func(ls *levelStats) string {
			return formatStat(&ls.bytesPerKV, formatBytes)
		}},
		{title: pctTitle, value: func(ls *levelStats) string {
			return formatPercentiles(&ls.bytesPerKV, formatBytes)
		}},
		{divider: true, title: "SSTable+blob bytes per KV", value: func(ls *levelStats) string {
			return formatStat(&ls.bytesPerKVWithBlobs, formatBytes)
		}},
		{title: pctTitle, value: func(ls *levelStats) string {
			return formatPercentiles(&ls.bytesPerKVWithBlobs, formatBytes)
		}},
		{divider: true, title: "SSTable common key prefix", value: func(ls *levelStats) string {
			return formatStat(&ls.commonPrefix, formatCount)
		}},
		{divider: true, title: "With two-level index", value: func(ls *levelStats) string {
			switch {
			case ls.numSampledFiles == 0:
				return ""
			case ls.numFilesWithTwoLevelIndex == 0:
				return "0"
			default:
				return fmt.Sprintf("%d (%s)", ls.numFilesWithTwoLevelIndex,
					crhumanize.Percent(ls.numFilesWithTwoLevelIndex, ls.numSampledFiles))
			}
		}},
		{divider: true, title: "Index size", value: func(ls *levelStats) string {
			return formatStatWithTotal(&ls.indexSize, ls.numTotalFiles, formatBytes)
		}},
		{title: pctTitle, value: func(ls *levelStats) string {
			return formatPercentiles(&ls.indexSize, formatBytes)
		}},
		{divider: true, title: "Entries per index block", value: func(ls *levelStats) string {
			return formatStat(&ls.numEntriesPerIndexBlock, formatCount)
		}},
		{title: pctTitle, value: func(ls *levelStats) string {
			return formatPercentiles(&ls.numEntriesPerIndexBlock, formatCount)
		}},
		{divider: true, title: "Data blocks", value: func(ls *levelStats) string {
			return formatStatWithTotal(&ls.numDataBlocks, ls.numTotalFiles, formatCount)
		}},
		{title: pctTitle, value: func(ls *levelStats) string {
			return formatPercentiles(&ls.numDataBlocks, formatCount)
		}},
		{divider: true, title: "Filter block size", value: func(ls *levelStats) string {
			return formatStatWithTotal(&ls.filterBlockSize, ls.numTotalFiles, formatBytes)
		}},
		{title: pctTitle, value: func(ls *levelStats) string {
			return formatPercentiles(&ls.filterBlockSize, formatBytes)
		}},
	}

	// Build horizontal dividers from rows that have divider: true.
	var dividerIndices []int
	for i, r := range rows {
		if r.divider {
			dividerIndices = append(dividerIndices, i)
		}
	}

	wb := ascii.Make(100, 100)
	tab.Render(wb.At(0, 0), table.RenderOptions{
		HorizontalDividers: table.MakeHorizontalDividers(dividerIndices...),
	}, rows...)
	fmt.Fprintln(w, wb.String())
}
