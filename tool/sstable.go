// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"fmt"
	"sort"
	"text/tabwriter"

	"github.com/cockroachdb/pebble/internal/base"
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

	// Configuration and state.
	opts      *sstable.Options
	comparers sstable.Comparers
	mergers   sstable.Mergers
	dbNum     uint64

	// Flags.
	fmtKey   formatter
	fmtValue formatter
	start    key
	end      key
	verbose  bool
}

func newSSTable(
	opts *base.Options, comparers sstable.Comparers, mergers sstable.Mergers,
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
order which means the records will be printed in that order.
`,
		Args: cobra.MinimumNArgs(1),
		Run:  s.runScan,
	}

	s.Root.AddCommand(s.Check, s.Layout, s.Properties, s.Scan)
	for _, cmd := range []*cobra.Command{s.Layout, s.Properties} {
		cmd.Flags().BoolVarP(
			&s.verbose, "verbose", "v", false,
			"verbose output")
	}

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
	s.Scan.Flags().Var(
		&s.start, "start", "start key for the scan")
	s.Scan.Flags().Var(
		&s.end, "end", "end key for the scan")

	return s
}

func (s *sstableT) newReader(f vfs.File) (*sstable.Reader, error) {
	s.dbNum++
	return sstable.NewReader(f, s.dbNum, 0, s.opts, s.comparers, s.mergers)
}

func (s *sstableT) runCheck(cmd *cobra.Command, args []string) {
	for _, arg := range args {
		func() {
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
		}()
	}
}

func (s *sstableT) runLayout(cmd *cobra.Command, args []string) {
	for _, arg := range args {
		func() {
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
		}()
	}
}

func (s *sstableT) runProperties(cmd *cobra.Command, args []string) {
	for _, arg := range args {
		func() {
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
			fmt.Fprintf(tw, "version\t%d\n", r.Properties.Version)
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
				(r.Properties.NumDeletions+r.Properties.NumRangeDeletions+r.Properties.NumMergeOperands))
			fmt.Fprintf(tw, "  delete\t%d\n", r.Properties.NumDeletions)
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
		}()
	}
}

func (s *sstableT) runScan(cmd *cobra.Command, args []string) {
	for _, arg := range args {
		func() {
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

			iter := r.NewIter(nil, s.end)
			var lastKey base.InternalKey
			for key, value := iter.SeekGE(s.start); key != nil; key, value = iter.Next() {
				formatKeyValue(stdout, s.fmtKey, s.fmtValue, key, value)
				if base.InternalCompare(r.Compare, lastKey, *key) >= 0 {
					fmt.Fprintf(stdout, "    WARNING: OUT OF ORDER KEYS!\n")
				}
				lastKey.Trailer = key.Trailer
				lastKey.UserKey = append(lastKey.UserKey[:0], key.UserKey...)
			}
			if err := iter.Close(); err != nil {
				fmt.Fprintf(stdout, "%s\n", err)
			}
		}()
	}
}
