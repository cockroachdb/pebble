// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"cmp"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/humanize"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/spf13/cobra"
)

// manifestT implements manifest-level tools, including both configuration
// state and the commands themselves.
type manifestT struct {
	Root      *cobra.Command
	Dump      *cobra.Command
	Summarize *cobra.Command
	Check     *cobra.Command

	opts      *pebble.Options
	comparers sstable.Comparers
	fmtKey    keyFormatter
	verbose   bool

	filterStart key
	filterEnd   key

	summarizeDur time.Duration
}

func newManifest(opts *pebble.Options, comparers sstable.Comparers) *manifestT {
	m := &manifestT{
		opts:         opts,
		comparers:    comparers,
		summarizeDur: time.Hour,
	}
	m.fmtKey.mustSet("quoted")

	m.Root = &cobra.Command{
		Use:   "manifest",
		Short: "manifest introspection tools",
	}

	// Add dump command
	m.Dump = &cobra.Command{
		Use:   "dump <manifest-files>",
		Short: "print manifest contents",
		Long: `
Print the contents of the MANIFEST files.
`,
		Args: cobra.MinimumNArgs(1),
		Run:  m.runDump,
	}
	m.Dump.Flags().Var(&m.fmtKey, "key", "key formatter")
	m.Dump.Flags().Var(&m.filterStart, "filter-start", "start key filters out all version edits that only reference sstables containing keys strictly before the given key")
	m.Dump.Flags().Var(&m.filterEnd, "filter-end", "end key filters out all version edits that only reference sstables containing keys at or strictly after the given key")
	m.Root.AddCommand(m.Dump)
	m.Root.PersistentFlags().BoolVarP(&m.verbose, "verbose", "v", false, "verbose output")

	// Add summarize command
	m.Summarize = &cobra.Command{
		Use:   "summarize <manifest-files>",
		Short: "summarize manifest contents",
		Long: `
Summarize the edits to the MANIFEST files over time.
`,
		Args: cobra.MinimumNArgs(1),
		Run:  m.runSummarize,
	}
	m.Root.AddCommand(m.Summarize)
	m.Summarize.Flags().DurationVar(
		&m.summarizeDur, "dur", time.Hour, "bucket duration as a Go duration string (eg, '1h', '15m')")

	// Add check command
	m.Check = &cobra.Command{
		Use:   "check <manifest-files>",
		Short: "check manifest contents",
		Long: `
Check the contents of the MANIFEST files.
`,
		Args: cobra.MinimumNArgs(1),
		Run:  m.runCheck,
	}
	m.Root.AddCommand(m.Check)
	m.Check.Flags().Var(
		&m.fmtKey, "key", "key formatter")

	return m
}

func (m *manifestT) printLevels(cmp base.Compare, stdout io.Writer, v *manifest.Version) {
	for level := range v.Levels {
		if level == 0 && len(v.L0SublevelFiles) > 0 && !v.Levels[level].Empty() {
			for sublevel := len(v.L0SublevelFiles) - 1; sublevel >= 0; sublevel-- {
				fmt.Fprintf(stdout, "--- L0.%d ---\n", sublevel)
				for f := range v.L0SublevelFiles[sublevel].All() {
					if !anyOverlapFile(cmp, f, m.filterStart, m.filterEnd) {
						continue
					}
					fmt.Fprintf(stdout, "  %s:%d", f.FileNum, f.Size)
					formatSeqNumRange(stdout, f.SmallestSeqNum, f.LargestSeqNum)
					formatKeyRange(stdout, m.fmtKey, &f.Smallest, &f.Largest)
					if f.Virtual {
						fmt.Fprintf(stdout, "(virtual:backingNum=%s)", f.FileBacking.DiskFileNum)
					}
					fmt.Fprintf(stdout, "\n")
				}
			}
			continue
		}
		fmt.Fprintf(stdout, "--- L%d ---\n", level)
		for f := range v.Levels[level].All() {
			if !anyOverlapFile(cmp, f, m.filterStart, m.filterEnd) {
				continue
			}
			fmt.Fprintf(stdout, "  %s:%d", f.FileNum, f.Size)
			formatSeqNumRange(stdout, f.SmallestSeqNum, f.LargestSeqNum)
			formatKeyRange(stdout, m.fmtKey, &f.Smallest, &f.Largest)
			if f.Virtual {
				fmt.Fprintf(stdout, "(virtual:backingNum=%s)", f.FileBacking.DiskFileNum)
			}
			fmt.Fprintf(stdout, "\n")
		}
	}
}

func (m *manifestT) runDump(cmd *cobra.Command, args []string) {
	stdout, stderr := cmd.OutOrStdout(), cmd.OutOrStderr()
	for _, arg := range args {
		func() {
			f, err := m.opts.FS.Open(arg)
			if err != nil {
				fmt.Fprintf(stderr, "%s\n", err)
				return
			}
			defer f.Close()

			fmt.Fprintf(stdout, "%s\n", arg)

			var bve manifest.BulkVersionEdit
			bve.AddedTablesByFileNum = make(map[base.FileNum]*manifest.TableMetadata)
			var comparer *base.Comparer
			var editIdx int
			rr := record.NewReader(f, 0 /* logNum */)
			for {
				offset := rr.Offset()
				r, err := rr.Next()
				if err != nil {
					fmt.Fprintf(stdout, "%s\n", err)
					break
				}

				var ve manifest.VersionEdit
				err = ve.Decode(r)
				if err != nil {
					fmt.Fprintf(stdout, "%s\n", err)
					break
				}
				if err := bve.Accumulate(&ve); err != nil {
					fmt.Fprintf(stdout, "%s\n", err)
					break
				}

				if comparer != nil && !anyOverlap(comparer.Compare, &ve, m.filterStart, m.filterEnd) {
					continue
				}

				empty := true
				fmt.Fprintf(stdout, "%d/%d\n", offset, editIdx)
				if ve.ComparerName != "" {
					empty = false
					fmt.Fprintf(stdout, "  comparer:     %s", ve.ComparerName)
					comparer = m.comparers[ve.ComparerName]
					if comparer == nil {
						fmt.Fprintf(stdout, " (unknown)")
					}
					fmt.Fprintf(stdout, "\n")
					m.fmtKey.setForComparer(ve.ComparerName, m.comparers)
				}
				if ve.MinUnflushedLogNum != 0 {
					empty = false
					fmt.Fprintf(stdout, "  log-num:       %d\n", ve.MinUnflushedLogNum)
				}
				if ve.ObsoletePrevLogNum != 0 {
					empty = false
					fmt.Fprintf(stdout, "  prev-log-num:  %d\n", ve.ObsoletePrevLogNum)
				}
				if ve.NextFileNum != 0 {
					empty = false
					fmt.Fprintf(stdout, "  next-file-num: %d\n", ve.NextFileNum)
				}
				if ve.LastSeqNum != 0 {
					empty = false
					fmt.Fprintf(stdout, "  last-seq-num:  %d\n", ve.LastSeqNum)
				}
				entries := make([]manifest.DeletedTableEntry, 0, len(ve.DeletedTables))
				for df := range ve.DeletedTables {
					empty = false
					entries = append(entries, df)
				}
				slices.SortFunc(entries, func(a, b manifest.DeletedTableEntry) int {
					if v := cmp.Compare(a.Level, b.Level); v != 0 {
						return v
					}
					return cmp.Compare(a.FileNum, b.FileNum)
				})
				for _, df := range entries {
					fmt.Fprintf(stdout, "  deleted:       L%d %s\n", df.Level, df.FileNum)
				}
				for _, nf := range ve.NewTables {
					empty = false
					fmt.Fprintf(stdout, "  added:         L%d %s:%d",
						nf.Level, nf.Meta.FileNum, nf.Meta.Size)
					formatSeqNumRange(stdout, nf.Meta.SmallestSeqNum, nf.Meta.LargestSeqNum)
					formatKeyRange(stdout, m.fmtKey, &nf.Meta.Smallest, &nf.Meta.Largest)
					if nf.Meta.CreationTime != 0 {
						fmt.Fprintf(stdout, " (%s)",
							time.Unix(nf.Meta.CreationTime, 0).UTC().Format(time.RFC3339))
					}
					fmt.Fprintf(stdout, "\n")
				}
				if empty {
					// NB: An empty version edit can happen if we log a version edit with
					// a zero field. RocksDB does this with a version edit that contains
					// `LogNum == 0`.
					fmt.Fprintf(stdout, "  <empty>\n")
				}
				editIdx++
			}

			if comparer != nil {
				l0Organizer := manifest.NewL0Organizer(comparer, 0 /* flushSplitBytes */)
				emptyVersion := manifest.NewInitialVersion(comparer)
				v, err := bve.Apply(emptyVersion, m.opts.Experimental.ReadCompactionRate)
				if err != nil {
					fmt.Fprintf(stdout, "%s\n", err)
					return
				}
				l0Organizer.PerformUpdate(l0Organizer.PrepareUpdate(&bve, v), v)
				m.printLevels(comparer.Compare, stdout, v)
			}
		}()
	}
}

func anyOverlap(cmp base.Compare, ve *manifest.VersionEdit, start, end key) bool {
	if start == nil && end == nil {
		return true
	}
	for _, df := range ve.DeletedTables {
		if anyOverlapFile(cmp, df, start, end) {
			return true
		}
	}
	for _, nf := range ve.NewTables {
		if anyOverlapFile(cmp, nf.Meta, start, end) {
			return true
		}
	}
	return false
}

func anyOverlapFile(cmp base.Compare, f *manifest.TableMetadata, start, end key) bool {
	if f == nil {
		return true
	}
	if start != nil {
		if v := cmp(f.Largest.UserKey, start); v < 0 {
			return false
		} else if f.Largest.IsExclusiveSentinel() && v == 0 {
			return false
		}
	}
	if end != nil && cmp(f.Smallest.UserKey, end) >= 0 {
		return false
	}
	return true
}

func (m *manifestT) runSummarize(cmd *cobra.Command, args []string) {
	for _, arg := range args {
		err := m.runSummarizeOne(cmd.OutOrStdout(), arg)
		if err != nil {
			fmt.Fprintf(cmd.OutOrStderr(), "%s\n", err)
		}
	}
}

func (m *manifestT) runSummarizeOne(stdout io.Writer, arg string) error {
	f, err := m.opts.FS.Open(arg)
	if err != nil {
		return err
	}
	defer f.Close()
	fmt.Fprintf(stdout, "%s\n", arg)

	type summaryBucket struct {
		bytesAdded      [manifest.NumLevels]uint64
		bytesCompactOut [manifest.NumLevels]uint64
		bytesCompactIn  [manifest.NumLevels]uint64
		filesCompactIn  [manifest.NumLevels]uint64
		fileLifetimeSec [manifest.NumLevels]*hdrhistogram.Histogram
	}
	// 365 days. Arbitrary.
	const maxLifetimeSec = 365 * 24 * 60 * 60
	var (
		bve           manifest.BulkVersionEdit
		newestOverall time.Time
		oldestOverall time.Time // oldest after initial version edit
		buckets       = map[time.Time]*summaryBucket{}
		metadatas     = map[base.FileNum]*manifest.TableMetadata{}
	)
	bve.AddedTablesByFileNum = make(map[base.FileNum]*manifest.TableMetadata)
	rr := record.NewReader(f, 0 /* logNum */)
	numHistErrors := 0
	for i := 0; ; i++ {
		r, err := rr.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		var ve manifest.VersionEdit
		err = ve.Decode(r)
		if err != nil {
			return err
		}
		if err := bve.Accumulate(&ve); err != nil {
			return err
		}

		// !isLikelyCompaction corresponds to flushes or ingests, that will be
		// counted in bytesAdded. This is imperfect since ingests that excise can
		// have deleted files without creating backing tables, and be counted as
		// compactions. Also, copy compactions have no deleted files and create
		// backing tables, so will be counted as flush/ingest.
		//
		// The bytesAdded metric overcounts since existing files virtualized by an
		// ingest are also included.
		//
		// TODO(sumeer): this summarization needs a rewrite. We could do that
		// after adding an enum to the VersionEdit to aid the summarization.
		isLikelyCompaction := len(ve.NewTables) > 0 && len(ve.DeletedTables) > 0 && len(ve.CreatedBackingTables) == 0
		isIntraL0Compaction := isLikelyCompaction && ve.NewTables[0].Level == 0
		veNewest := newestOverall
		for _, nf := range ve.NewTables {
			_, seen := metadatas[nf.Meta.FileNum]
			if seen && !isLikelyCompaction {
				// Output error and continue processing as usual.
				fmt.Fprintf(stdout, "error: flush/ingest has file that is already known %d size %s\n",
					nf.Meta.FileNum, humanize.Bytes.Uint64(nf.Meta.Size))
			}
			metadatas[nf.Meta.FileNum] = nf.Meta
			if nf.Meta.CreationTime == 0 {
				continue
			}

			t := time.Unix(nf.Meta.CreationTime, 0).UTC()
			if veNewest.Before(t) {
				veNewest = t
			}
		}
		// Ratchet up the most recent timestamp we've seen.
		if newestOverall.Before(veNewest) {
			newestOverall = veNewest
		}

		if i == 0 || newestOverall.IsZero() {
			continue
		}
		// Update oldestOverall once, when we encounter the first version edit
		// at index >= 1. It should be approximately the start time of the
		// manifest.
		if !newestOverall.IsZero() && oldestOverall.IsZero() {
			oldestOverall = newestOverall
		}

		bucketKey := newestOverall.Truncate(m.summarizeDur)
		b := buckets[bucketKey]
		if b == nil {
			b = &summaryBucket{}
			buckets[bucketKey] = b
		}

		for _, nf := range ve.NewTables {
			if !isLikelyCompaction {
				b.bytesAdded[nf.Level] += nf.Meta.Size
			} else if !isIntraL0Compaction {
				b.bytesCompactIn[nf.Level] += nf.Meta.Size
				b.filesCompactIn[nf.Level]++
			}
		}

		for dfe := range ve.DeletedTables {
			// Increase `bytesCompactOut` for the input level of any compactions
			// that remove bytes from a level (excluding intra-L0 compactions).
			if isLikelyCompaction && !isIntraL0Compaction && dfe.Level != manifest.NumLevels-1 {
				b.bytesCompactOut[dfe.Level] += metadatas[dfe.FileNum].Size
			}
			meta, ok := metadatas[dfe.FileNum]
			if m.verbose && ok && meta.CreationTime > 0 {
				hist := b.fileLifetimeSec[dfe.Level]
				if hist == nil {
					hist = hdrhistogram.New(0, maxLifetimeSec, 1)
					b.fileLifetimeSec[dfe.Level] = hist
				}
				lifetimeSec := int64((newestOverall.Sub(time.Unix(meta.CreationTime, 0).UTC())) / time.Second)
				if lifetimeSec > maxLifetimeSec {
					lifetimeSec = maxLifetimeSec
				}
				if err := hist.RecordValue(lifetimeSec); err != nil {
					numHistErrors++
				}
			}
		}
	}

	formatUint64 := func(v uint64, _ time.Duration) string {
		if v == 0 {
			return "."
		}
		return humanize.Bytes.Uint64(v).String()
	}
	formatByteRate := func(v uint64, dur time.Duration) string {
		if v == 0 {
			return "."
		}
		secs := dur.Seconds()
		if secs == 0 {
			secs = 1
		}
		return humanize.Bytes.Uint64(uint64(float64(v)/secs)).String() + "/s"
	}
	formatRate := func(v uint64, dur time.Duration) string {
		if v == 0 {
			return "."
		}
		secs := dur.Seconds()
		if secs == 0 {
			secs = 1
		}
		return fmt.Sprintf("%.1f/s", float64(v)/secs)
	}

	if newestOverall.IsZero() {
		fmt.Fprintf(stdout, "(no timestamps)\n")
	} else {
		// NB: bt begins unaligned with the bucket duration (m.summarizeDur),
		// but after the first bucket will always be aligned.
		for bi, bt := 0, oldestOverall; !bt.After(newestOverall); bi, bt = bi+1, bt.Truncate(m.summarizeDur).Add(m.summarizeDur) {
			// Truncate the start time to calculate the bucket key, and
			// retrieve the appropriate bucket.
			bk := bt.Truncate(m.summarizeDur)
			var bucket summaryBucket
			if buckets[bk] != nil {
				bucket = *buckets[bk]
			}

			if bi%10 == 0 {
				fmt.Fprintf(stdout, "                        ")
				fmt.Fprintf(stdout, "_______L0_______L1_______L2_______L3_______L4_______L5_______L6_____TOTAL\n")
			}
			fmt.Fprintf(stdout, "%s\n", bt.Format(time.RFC3339))

			// Compute the bucket duration. It may < `m.summarizeDur` if this is
			// the first or last bucket.
			bucketEnd := bt.Truncate(m.summarizeDur).Add(m.summarizeDur)
			if bucketEnd.After(newestOverall) {
				bucketEnd = newestOverall
			}
			dur := bucketEnd.Sub(bt)

			stats := []struct {
				label  string
				format func(uint64, time.Duration) string
				vals   [manifest.NumLevels]uint64
			}{
				{"Ingest+Flush Bytes", formatUint64, bucket.bytesAdded},
				{"Ingest+Flush Bytes/s", formatByteRate, bucket.bytesAdded},
				{"Compact Out Bytes", formatUint64, bucket.bytesCompactOut},
				{"Compact Out Bytes/s", formatByteRate, bucket.bytesCompactOut},
				{"Compact In Bytes/s", formatByteRate, bucket.bytesCompactIn},
				{"Compact In Files/s", formatRate, bucket.filesCompactIn},
			}
			for _, stat := range stats {
				var sum uint64
				for _, v := range stat.vals {
					sum += v
				}
				fmt.Fprintf(stdout, "%23s   %8s %8s %8s %8s %8s %8s %8s %8s\n",
					stat.label,
					stat.format(stat.vals[0], dur),
					stat.format(stat.vals[1], dur),
					stat.format(stat.vals[2], dur),
					stat.format(stat.vals[3], dur),
					stat.format(stat.vals[4], dur),
					stat.format(stat.vals[5], dur),
					stat.format(stat.vals[6], dur),
					stat.format(sum, dur))
			}
		}
		fmt.Fprintf(stdout, "%s\n", newestOverall.Format(time.RFC3339))

		formatSec := func(sec int64) string {
			return (time.Second * time.Duration(sec)).String()
		}
		if m.verbose {
			fmt.Fprintf(stdout, "\nLifetime histograms\n")
			for bi, bt := 0, oldestOverall; !bt.After(newestOverall); bi, bt = bi+1, bt.Truncate(m.summarizeDur).Add(m.summarizeDur) {
				// Truncate the start time to calculate the bucket key, and
				// retrieve the appropriate bucket.
				bk := bt.Truncate(m.summarizeDur)
				var bucket summaryBucket
				if buckets[bk] != nil {
					bucket = *buckets[bk]
				}
				fmt.Fprintf(stdout, "%s\n", bt.Format(time.RFC3339))
				formatHist := func(level int, hist *hdrhistogram.Histogram) {
					if hist == nil {
						return
					}
					fmt.Fprintf(stdout, "  L%d: mean: %s p25: %s p50: %s p75: %s p90: %s\n", level,
						formatSec(int64(hist.Mean())), formatSec(hist.ValueAtPercentile(25)),
						formatSec(hist.ValueAtPercentile(50)), formatSec(hist.ValueAtPercentile(75)),
						formatSec(hist.ValueAtPercentile(90)))
				}
				for i := range bucket.fileLifetimeSec {
					formatHist(i, bucket.fileLifetimeSec[i])
				}
			}
			fmt.Fprintf(stdout, "%s\n", newestOverall.Format(time.RFC3339))
		}
	}

	dur := newestOverall.Sub(oldestOverall)
	fmt.Fprintf(stdout, "---\n")
	fmt.Fprintf(stdout, "Estimated start time: %s\n", oldestOverall.Format(time.RFC3339))
	fmt.Fprintf(stdout, "Estimated end time:   %s\n", newestOverall.Format(time.RFC3339))
	fmt.Fprintf(stdout, "Estimated duration:   %s\n", dur.String())
	if numHistErrors > 0 {
		fmt.Fprintf(stdout, "Errors in lifetime histograms: %d\n", numHistErrors)
	}

	return nil
}

func (m *manifestT) runCheck(cmd *cobra.Command, args []string) {
	stdout, stderr := cmd.OutOrStdout(), cmd.OutOrStderr()
	ok := true
	for _, arg := range args {
		func() {
			f, err := m.opts.FS.Open(arg)
			if err != nil {
				fmt.Fprintf(stderr, "%s\n", err)
				ok = false
				return
			}
			defer f.Close()

			var l0Organizer *manifest.L0Organizer
			var v *manifest.Version
			var cmp *base.Comparer
			rr := record.NewReader(f, 0 /* logNum */)
			// Contains the TableMetadata needed by BulkVersionEdit.Apply.
			// It accumulates the additions since later edits contain
			// deletions of earlier added files.
			addedByFileNum := make(map[base.FileNum]*manifest.TableMetadata)
			for {
				offset := rr.Offset()
				r, err := rr.Next()
				if err != nil {
					if err == io.EOF {
						break
					}
					fmt.Fprintf(stdout, "%s: offset: %d err: %s\n", arg, offset, err)
					ok = false
					break
				}

				var ve manifest.VersionEdit
				err = ve.Decode(r)
				if err != nil {
					fmt.Fprintf(stdout, "%s: offset: %d err: %s\n", arg, offset, err)
					ok = false
					break
				}
				var bve manifest.BulkVersionEdit
				bve.AddedTablesByFileNum = addedByFileNum
				if err := bve.Accumulate(&ve); err != nil {
					fmt.Fprintf(stderr, "%s\n", err)
					ok = false
					return
				}

				empty := true
				if ve.ComparerName != "" {
					empty = false
					cmp = m.comparers[ve.ComparerName]
					if cmp == nil {
						fmt.Fprintf(stdout, "%s: offset: %d comparer %s not found",
							arg, offset, ve.ComparerName)
						ok = false
						break
					}
					m.fmtKey.setForComparer(ve.ComparerName, m.comparers)
				}
				empty = empty && ve.MinUnflushedLogNum == 0 && ve.ObsoletePrevLogNum == 0 &&
					ve.LastSeqNum == 0 && len(ve.DeletedTables) == 0 &&
					len(ve.NewTables) == 0
				if empty {
					continue
				}
				// TODO(sbhola): add option to Apply that reports all errors instead of
				// one error.
				if v == nil {
					l0Organizer = manifest.NewL0Organizer(cmp, 0 /* flushSplitBytes */)
					v = manifest.NewInitialVersion(cmp)
				}
				newv, err := bve.Apply(v, m.opts.Experimental.ReadCompactionRate)
				if err != nil {
					fmt.Fprintf(stdout, "%s: offset: %d err: %s\n",
						arg, offset, err)
					fmt.Fprintf(stdout, "Version state before failed Apply\n")
					m.printLevels(cmp.Compare, stdout, v)
					fmt.Fprintf(stdout, "Version edit that failed\n")
					for df := range ve.DeletedTables {
						fmt.Fprintf(stdout, "  deleted: L%d %s\n", df.Level, df.FileNum)
					}
					for _, nf := range ve.NewTables {
						fmt.Fprintf(stdout, "  added: L%d %s:%d",
							nf.Level, nf.Meta.FileNum, nf.Meta.Size)
						formatSeqNumRange(stdout, nf.Meta.SmallestSeqNum, nf.Meta.LargestSeqNum)
						formatKeyRange(stdout, m.fmtKey, &nf.Meta.Smallest, &nf.Meta.Largest)
						fmt.Fprintf(stdout, "\n")
					}
					ok = false
					break
				}
				l0Organizer.PerformUpdate(l0Organizer.PrepareUpdate(&bve, newv), newv)
				v = newv
			}
		}()
	}
	if ok {
		fmt.Fprintf(stdout, "OK\n")
	}
}
