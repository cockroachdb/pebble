// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sort"
	"text/tabwriter"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/private"
	"github.com/cockroachdb/pebble/internal/rangedel"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/spf13/cobra"
)

// sstableT implements sstable-level tools, including both configuration state
// and the commands themselves.
type sstableT struct {
	Root       *cobra.Command
	Check      *cobra.Command
	Layout     *cobra.Command
	Properties *cobra.Command
	Scan       *cobra.Command
	Space      *cobra.Command

	// Configuration and state.
	opts      *pebble.Options
	comparers sstable.Comparers
	mergers   sstable.Mergers

	// Flags.
	fmtKey   formatter
	fmtValue formatter
	start    key
	end      key
	filter   key
	count    int64
	verbose  bool
}

func newSSTable(
	opts *pebble.Options, comparers sstable.Comparers, mergers sstable.Mergers,
) *sstableT {
	s := &sstableT{
		opts:      opts,
		comparers: comparers,
		mergers:   mergers,
	}
	s.fmtKey.mustSet("quoted")
	s.fmtValue.mustSet("[%x]")

	s.Root = &cobra.Command{
		Use:   "sstable",
		Short: "sstable introspection tools",
	}
	s.Check = &cobra.Command{
		Use:   "check <sstables>",
		Short: "verify checksums and metadata",
		Long:  ``,
		Args:  cobra.MinimumNArgs(1),
		Run:   s.runCheck,
	}
	s.Layout = &cobra.Command{
		Use:   "layout <sstables>",
		Short: "print sstable block and record layout",
		Long: `
Print the layout for the sstables. The -v flag controls whether record layout
is displayed or omitted.
`,
		Args: cobra.MinimumNArgs(1),
		Run:  s.runLayout,
	}
	s.Properties = &cobra.Command{
		Use:   "properties <sstables>",
		Short: "print sstable properties",
		Long: `
Print the properties for the sstables. The -v flag controls whether the
properties are pretty-printed or displayed in a verbose/raw format.
`,
		Args: cobra.MinimumNArgs(1),
		Run:  s.runProperties,
	}
	s.Scan = &cobra.Command{
		Use:   "scan <sstables>",
		Short: "print sstable records",
		Long: `
Print the records in the sstables. The sstables are scanned in command line
order which means the records will be printed in that order. Raw range
tombstones are displayed interleaved with point records.
`,
		Args: cobra.MinimumNArgs(1),
		Run:  s.runScan,
	}
	s.Space = &cobra.Command{
		Use:   "space <sstables>",
		Short: "print filesystem space used",
		Long: `
Print the estimated space usage in the specified files for the
inclusive-inclusive range specified by --start and --end.
`,
		Args: cobra.MinimumNArgs(1),
		Run:  s.runSpace,
	}

	s.Root.AddCommand(s.Check, s.Layout, s.Properties, s.Scan, s.Space)
	s.Root.PersistentFlags().BoolVarP(&s.verbose, "verbose", "v", false, "verbose output")

	s.Check.Flags().Var(
		&s.fmtKey, "key", "key formatter")
	s.Layout.Flags().Var(
		&s.fmtKey, "key", "key formatter")
	s.Layout.Flags().Var(
		&s.fmtValue, "value", "value formatter")
	s.Scan.Flags().Var(
		&s.fmtKey, "key", "key formatter")
	s.Scan.Flags().Var(
		&s.fmtValue, "value", "value formatter")
	for _, cmd := range []*cobra.Command{s.Scan, s.Space} {
		cmd.Flags().Var(
			&s.start, "start", "start key for the range")
		cmd.Flags().Var(
			&s.end, "end", "end key for the range")
	}
	s.Scan.Flags().Var(
		&s.filter, "filter", "only output records with matching prefix or overlapping range tombstones")
	s.Scan.Flags().Int64Var(
		&s.count, "count", 0, "key count for scan (0 is unlimited)")

	return s
}

func (s *sstableT) newReader(f vfs.File) (*sstable.Reader, error) {
	o := sstable.ReaderOptions{
		Cache:    s.opts.Cache,
		Comparer: s.opts.Comparer,
		Filters:  s.opts.Filters,
	}
	return sstable.NewReader(f, o, s.comparers, s.mergers,
		private.SSTableRawTombstonesOpt.(sstable.ReaderOption))
}

func (s *sstableT) runCheck(cmd *cobra.Command, args []string) {
	s.foreachSstable(args, func(arg string) {
		f, err := s.opts.FS.Open(arg)
		if err != nil {
			fmt.Fprintf(stderr, "%s\n", err)
			return
		}

		fmt.Fprintf(stdout, "%s\n", arg)

		r, err := s.newReader(f)

		if err != nil {
			fmt.Fprintf(stdout, "%s\n", err)
			return
		}
		defer r.Close()

		// Update the internal formatter if this comparator has one specified.
		s.fmtKey.setForComparer(r.Properties.ComparerName, s.comparers)

		iter := r.NewIter(nil, nil)
		var lastKey base.InternalKey
		for key, _ := iter.First(); key != nil; key, _ = iter.Next() {
			if base.InternalCompare(r.Compare, lastKey, *key) >= 0 {
				fmt.Fprintf(stdout, "WARNING: OUT OF ORDER KEYS!\n")
				if s.fmtKey.spec != "null" {
					fmt.Fprintf(stdout, "    %s >= %s\n",
						lastKey.Pretty(s.fmtKey.fn), key.Pretty(s.fmtKey.fn))
				}
			}
			lastKey.Trailer = key.Trailer
			lastKey.UserKey = append(lastKey.UserKey[:0], key.UserKey...)
		}
		if err := iter.Close(); err != nil {
			fmt.Fprintf(stdout, "%s\n", err)
		}
	})
}

func (s *sstableT) runLayout(cmd *cobra.Command, args []string) {
	s.foreachSstable(args, func(arg string) {
		f, err := s.opts.FS.Open(arg)
		if err != nil {
			fmt.Fprintf(stderr, "%s\n", err)
			return
		}

		fmt.Fprintf(stdout, "%s\n", arg)

		r, err := s.newReader(f)
		defer r.Close()

		if err != nil {
			fmt.Fprintf(stdout, "%s\n", err)
			return
		}

		// Update the internal formatter if this comparator has one specified.
		s.fmtKey.setForComparer(r.Properties.ComparerName, s.comparers)

		l, err := r.Layout()
		if err != nil {
			fmt.Fprintf(stderr, "%s\n", err)
			return
		}
		fmtRecord := func(key *base.InternalKey, value []byte) {
			formatKeyValue(stdout, s.fmtKey, s.fmtValue, key, value)
		}
		if s.fmtKey.spec == "null" && s.fmtValue.spec == "null" {
			fmtRecord = nil
		}
		l.Describe(stdout, s.verbose, r, fmtRecord)
	})
}

func (s *sstableT) runProperties(cmd *cobra.Command, args []string) {
	s.foreachSstable(args, func(arg string) {
		f, err := s.opts.FS.Open(arg)
		if err != nil {
			fmt.Fprintf(stderr, "%s\n", err)
			return
		}

		fmt.Fprintf(stdout, "%s\n", arg)

		r, err := s.newReader(f)
		defer r.Close()

		if err != nil {
			fmt.Fprintf(stdout, "%s\n", err)
			return
		}

		if s.verbose {
			fmt.Fprintf(stdout, "%s", r.Properties.String())
			return
		}

		stat, err := f.Stat()
		if err != nil {
			fmt.Fprintf(stderr, "%s\n", err)
			return
		}

		formatNull := func(s string) string {
			switch s {
			case "", "nullptr":
				return "-"
			}
			return s
		}

		tw := tabwriter.NewWriter(stdout, 2, 1, 2, ' ', 0)
		fmt.Fprintf(tw, "version\t%d\n", r.Properties.FormatVersion)
		fmt.Fprintf(tw, "size\t\n")
		fmt.Fprintf(tw, "  file\t%d\n", stat.Size())
		fmt.Fprintf(tw, "  data\t%d\n", r.Properties.DataSize)
		fmt.Fprintf(tw, "    blocks\t%d\n", r.Properties.NumDataBlocks)
		fmt.Fprintf(tw, "  index\t%d\n", r.Properties.IndexSize)
		fmt.Fprintf(tw, "    blocks\t%d\n", 1+r.Properties.IndexPartitions)
		fmt.Fprintf(tw, "    top-level\t%d\n", r.Properties.TopLevelIndexSize)
		fmt.Fprintf(tw, "  filter\t%d\n", r.Properties.FilterSize)
		fmt.Fprintf(tw, "  raw-key\t%d\n", r.Properties.RawKeySize)
		fmt.Fprintf(tw, "  raw-value\t%d\n", r.Properties.RawValueSize)
		fmt.Fprintf(tw, "records\t%d\n", r.Properties.NumEntries)
		fmt.Fprintf(tw, "  set\t%d\n", r.Properties.NumEntries-
			(r.Properties.NumDeletions+r.Properties.NumMergeOperands))
		fmt.Fprintf(tw, "  delete\t%d\n", r.Properties.NumDeletions-r.Properties.NumRangeDeletions)
		fmt.Fprintf(tw, "  range-delete\t%d\n", r.Properties.NumRangeDeletions)
		fmt.Fprintf(tw, "  merge\t%d\n", r.Properties.NumMergeOperands)
		fmt.Fprintf(tw, "  global-seq-num\t%d\n", r.Properties.GlobalSeqNum)
		fmt.Fprintf(tw, "index\t\n")
		fmt.Fprintf(tw, "  key\t")
		if r.Properties.IndexKeyIsUserKey != 0 {
			fmt.Fprintf(tw, "user key\n")
		} else {
			fmt.Fprintf(tw, "internal key\n")
		}
		fmt.Fprintf(tw, "  value\t")
		if r.Properties.IndexValueIsDeltaEncoded != 0 {
			fmt.Fprintf(tw, "delta encoded\n")
		} else {
			fmt.Fprintf(tw, "raw encoded\n")
		}
		fmt.Fprintf(tw, "comparer\t%s\n", r.Properties.ComparerName)
		fmt.Fprintf(tw, "merger\t%s\n", formatNull(r.Properties.MergerName))
		fmt.Fprintf(tw, "filter\t%s\n", formatNull(r.Properties.FilterPolicyName))
		fmt.Fprintf(tw, "  prefix\t%t\n", r.Properties.PrefixFiltering)
		fmt.Fprintf(tw, "  whole-key\t%t\n", r.Properties.WholeKeyFiltering)
		fmt.Fprintf(tw, "compression\t%s\n", r.Properties.CompressionName)
		fmt.Fprintf(tw, "  options\t%s\n", r.Properties.CompressionOptions)
		fmt.Fprintf(tw, "user properties\t\n")
		fmt.Fprintf(tw, "  collectors\t%s\n", r.Properties.PropertyCollectorNames)
		keys := make([]string, 0, len(r.Properties.UserProperties))
		for key := range r.Properties.UserProperties {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			fmt.Fprintf(tw, "  %s\t%s\n", key, r.Properties.UserProperties[key])
		}
		tw.Flush()
	})
}

func (s *sstableT) runScan(cmd *cobra.Command, args []string) {
	s.foreachSstable(args, func(arg string) {
		f, err := s.opts.FS.Open(arg)
		if err != nil {
			fmt.Fprintf(stderr, "%s\n", err)
			return
		}

		// In filter-mode, we prefix ever line that is output with the sstable
		// filename.
		var prefix string
		if s.filter == nil {
			fmt.Fprintf(stdout, "%s\n", arg)
		} else {
			prefix = fmt.Sprintf("%s: ", arg)
		}

		r, err := s.newReader(f)
		defer r.Close()

		if err != nil {
			fmt.Fprintf(stdout, "%s%s\n", prefix, err)
			return
		}

		// Update the internal formatter if this comparator has one specified.
		s.fmtKey.setForComparer(r.Properties.ComparerName, s.comparers)

		iter := r.NewIter(nil, s.end)
		defer iter.Close()
		key, value := iter.SeekGE(s.start)

		// We configured sstable.Reader to return raw tombstones which requires a
		// bit more work here to put them in a form that can be iterated in
		// parallel with the point records.
		rangeDelIter, err := func() (base.InternalIterator, error) {
			iter, err := r.NewRangeDelIter()
			if err != nil {
				return nil, err
			}
			if iter == nil {
				return rangedel.NewIter(r.Compare, nil), nil
			}
			defer iter.Close()

			var tombstones []rangedel.Tombstone
			for key, value := iter.First(); key != nil; key, value = iter.Next() {
				t := rangedel.Tombstone{
					Start: *key,
					End:   value,
				}
				if s.end != nil && r.Compare(s.end, t.Start.UserKey) <= 0 {
					// The range tombstone lies after the scan range.
					continue
				}
				if r.Compare(s.start, t.End) >= 0 {
					// The range tombstone lies before the scan range.
					continue
				}
				tombstones = append(tombstones, t)
			}

			sort.Slice(tombstones, func(i, j int) bool {
				return r.Compare(tombstones[i].Start.UserKey, tombstones[j].Start.UserKey) < 0
			})
			return rangedel.NewIter(r.Compare, tombstones), nil
		}()
		if err != nil {
			fmt.Fprintf(stdout, "%s%s\n", prefix, err)
			return
		}

		defer rangeDelIter.Close()
		rangeDelKey, rangeDelValue := rangeDelIter.First()
		count := s.count

		var lastKey base.InternalKey
		for key != nil || rangeDelKey != nil {
			if key != nil &&
				(rangeDelKey == nil ||
					base.InternalCompare(r.Compare, *key, *rangeDelKey) < 0) {
				// The filter specifies a prefix of the key.
				//
				// TODO(peter): Is using prefix comparison like this kosher for all
				// comparers? Probably not, but it is for common ones such as the
				// Pebble default and CockroachDB's comparer.
				if s.filter == nil || bytes.HasPrefix(key.UserKey, s.filter) {
					fmt.Fprint(stdout, prefix)
					formatKeyValue(stdout, s.fmtKey, s.fmtValue, key, value)
				}
				if base.InternalCompare(r.Compare, lastKey, *key) >= 0 {
					fmt.Fprintf(stdout, "%s    WARNING: OUT OF ORDER KEYS!\n", prefix)
				}
				lastKey.Trailer = key.Trailer
				lastKey.UserKey = append(lastKey.UserKey[:0], key.UserKey...)
				key, value = iter.Next()
			} else {
				// If a filter is specified, we want to output any range tombstone
				// which overlaps the prefix. The comparison on the start key is
				// somewhat complex. Consider the tombstone [aaa,ccc). We want to
				// output this tombstone if filter is "aa", and if it "bbb".
				if s.filter == nil ||
					((r.Compare(s.filter, rangeDelKey.UserKey) >= 0 ||
						bytes.HasPrefix(rangeDelKey.UserKey, s.filter)) &&
						r.Compare(s.filter, rangeDelValue) < 0) {
					fmt.Fprint(stdout, prefix)
					formatKeyValue(stdout, s.fmtKey, s.fmtValue, rangeDelKey, rangeDelValue)
				}
				rangeDelKey, rangeDelValue = rangeDelIter.Next()
			}

			if count > 0 {
				count--
				if count == 0 {
					break
				}
			}
		}

		if err := iter.Close(); err != nil {
			fmt.Fprintf(stdout, "%s\n", err)
		}
	})
}

func (s *sstableT) runSpace(cmd *cobra.Command, args []string) {
	s.foreachSstable(args, func(arg string) {
		f, err := s.opts.FS.Open(arg)
		if err != nil {
			fmt.Fprintf(stderr, "%s\n", err)
			return
		}
		r, err := s.newReader(f)
		if err != nil {
			fmt.Fprintf(stderr, "%s\n", err)
			return
		}
		defer r.Close()

		bytes, err := r.EstimateDiskUsage(s.start, s.end)
		if err != nil {
			fmt.Fprintf(stderr, "%s\n", err)
			return
		}
		fmt.Fprintf(stdout, "%s: %d\n", arg, bytes)
	})
}

func (s *sstableT) foreachSstable(args []string, fn func(arg string)) {
	// Loop over args, invoking fn for each file. Each directory is recursively
	// listed and fn is invoked on any file with an .sst or .ldb suffix.
	for _, arg := range args {
		info, err := s.opts.FS.Stat(arg)
		if err != nil || !info.IsDir() {
			fn(arg)
			continue
		}
		walk(s.opts.FS, arg, func(path string) {
			switch filepath.Ext(path) {
			case ".sst", ".ldb":
				fn(path)
			}
		})
	}
}
