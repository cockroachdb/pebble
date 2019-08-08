// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"fmt"
	"sort"
	"text/tabwriter"

	"github.com/petermattis/pebble/bloom"
	"github.com/petermattis/pebble/internal/base"
	"github.com/petermattis/pebble/sstable"
	"github.com/petermattis/pebble/vfs"
	"github.com/spf13/cobra"
)

var sstableConfig struct {
	// TODO(peter): Make the options configurable. In particular, we want to be
	// able to register comparers, and mergers. We also want to be able to
	// register key/value pretty printers.
	opts    *sstable.Options
	start   string
	end     string
	verbose bool
}

// SSTableCmd is the root of the sstable commands.
var SSTableCmd = &cobra.Command{
	Use:   "sstable",
	Short: "sstable introspection tools",
}

// SSTableCheckCmd implements sstable check.
var SSTableCheckCmd = &cobra.Command{
	Use:   "check <sstables>",
	Short: "verify checksums and metadata",
	Long:  ``,
	Args:  cobra.MinimumNArgs(1),
	Run:   runSSTableCheck,
}

// SSTableLayoutCmd implements sstable layout.
var SSTableLayoutCmd = &cobra.Command{
	Use:   "layout <sstables>",
	Short: "print sstable block and record layout",
	Long: `
Print the layout for the sstables. The -v flag controls whether record layout
is displayed or omitted.
`,
	Args: cobra.MinimumNArgs(1),
	Run:  runSSTableLayout,
}

// SSTablePropertiesCmd implements sstable properties.
var SSTablePropertiesCmd = &cobra.Command{
	Use:   "properties <sstables>",
	Short: "print sstable properties",
	Long: `
Print the properties for the sstables. The -v flag controls whether the
properties are pretty-printed or displayed in a verbose/raw format.
`,
	Args: cobra.MinimumNArgs(1),
	Run:  runSSTableProperties,
}

// SSTableScanCmd implements sstable scan.
var SSTableScanCmd = &cobra.Command{
	Use:   "scan <sstables>",
	Short: "print sstable records",
	Long: `
Print the records in the sstables. The sstables are scanned in command line
order which means the records will be printed in that order.
`,
	Args: cobra.MinimumNArgs(1),
	Run:  runSSTableScan,
}

func init() {
	SSTableCmd.AddCommand(
		SSTableCheckCmd,
		SSTableLayoutCmd,
		SSTablePropertiesCmd,
		SSTableScanCmd,
	)
	for _, cmd := range []*cobra.Command{SSTableLayoutCmd, SSTablePropertiesCmd} {
		cmd.Flags().BoolVarP(
			&sstableConfig.verbose, "verbose", "v", false,
			"verbose output")
	}

	SSTableScanCmd.Flags().StringVar(
		&sstableConfig.start, "start", "", "start key for the scan")
	SSTableScanCmd.Flags().StringVar(
		&sstableConfig.end, "end", "", "end key for the scan")

	sstableConfig.opts = &sstable.Options{
		Levels: []sstable.TableOptions{{
			FilterPolicy: bloom.FilterPolicy(10),
			FilterType:   base.TableFilter,
		}},
	}
}

func runSSTableCheck(cmd *cobra.Command, args []string) {
	for _, arg := range args {
		func() {
			f, err := vfs.Default.Open(arg)
			if err != nil {
				fmt.Fprintf(stderr, "%s\n", err)
				return
			}

			fmt.Fprintf(stdout, "%s\n", arg)

			r, err := sstable.NewReader(f, nextDBNum(), 0, sstableConfig.opts)
			defer r.Close()

			if err != nil {
				fmt.Fprintf(stdout, "%s\n", err)
				return
			}

			iter := r.NewIter(nil, nil)
			for key, _ := iter.First(); key != nil; key, _ = iter.Next() {
			}
			if err := iter.Close(); err != nil {
				fmt.Fprintf(stdout, "%s\n", err)
			}
		}()
	}
}

func runSSTableLayout(cmd *cobra.Command, args []string) {
	for _, arg := range args {
		func() {
			f, err := vfs.Default.Open(arg)
			if err != nil {
				fmt.Fprintf(stderr, "%s\n", err)
				return
			}

			fmt.Fprintf(stdout, "%s\n", arg)

			r, err := sstable.NewReader(f, nextDBNum(), 0, sstableConfig.opts)
			defer r.Close()

			if err != nil {
				fmt.Fprintf(stdout, "%s\n", err)
				return
			}

			l, err := r.Layout()
			if err != nil {
				fmt.Fprintf(stderr, "%s\n", err)
				return
			}
			l.Describe(stdout, sstableConfig.verbose, r)
		}()
	}
}

func runSSTableProperties(cmd *cobra.Command, args []string) {
	for _, arg := range args {
		func() {
			f, err := vfs.Default.Open(arg)
			if err != nil {
				fmt.Fprintf(stderr, "%s\n", err)
				return
			}

			fmt.Fprintf(stdout, "%s\n", arg)

			r, err := sstable.NewReader(f, nextDBNum(), 0, sstableConfig.opts)
			defer r.Close()

			if err != nil {
				fmt.Fprintf(stdout, "%s\n", err)
				return
			}

			if sstableConfig.verbose {
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

func runSSTableScan(cmd *cobra.Command, args []string) {
	for _, arg := range args {
		func() {
			f, err := vfs.Default.Open(arg)
			if err != nil {
				fmt.Fprintf(stderr, "%s\n", err)
				return
			}

			fmt.Fprintf(stdout, "%s\n", arg)

			r, err := sstable.NewReader(f, nextDBNum(), 0, sstableConfig.opts)
			defer r.Close()

			if err != nil {
				fmt.Fprintf(stdout, "%s\n", err)
				return
			}

			// TODO(peter): Allow the start and end key to be specified in hex.
			var start, end []byte
			if sstableConfig.start != "" {
				start = []byte(sstableConfig.start)
			}
			if sstableConfig.end != "" {
				end = []byte(sstableConfig.end)
			}

			iter := r.NewIter(nil, end)
			for key, value := iter.SeekGE(start); key != nil; key, value = iter.Next() {
				// TODO(peter): Allow a pretty printer to be provided for keys and
				// values.
				fmt.Fprintf(stdout, "%s [% x]\n", key, value)
			}
			if err := iter.Close(); err != nil {
				fmt.Fprintf(stdout, "%s\n", err)
			}
		}()
	}
}
