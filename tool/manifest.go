// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/record"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/spf13/cobra"
)

// manifestT implements manifest-level tools, including both configuration
// state and the commands themselves.
type manifestT struct {
	Root  *cobra.Command
	Dump  *cobra.Command
	Check *cobra.Command

	opts      *pebble.Options
	comparers sstable.Comparers
	fmtKey    keyFormatter
	verbose   bool
}

func newManifest(opts *pebble.Options, comparers sstable.Comparers) *manifestT {
	m := &manifestT{
		opts:      opts,
		comparers: comparers,
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

	m.Root.AddCommand(m.Dump)
	m.Root.PersistentFlags().BoolVarP(&m.verbose, "verbose", "v", false, "verbose output")

	m.Dump.Flags().Var(
		&m.fmtKey, "key", "key formatter")

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

func (m *manifestT) printLevels(v *manifest.Version) {
	for level := range v.Levels {
		if level == 0 && len(v.L0SublevelFiles) > 0 && !v.Levels[level].Empty() {
			for sublevel := len(v.L0SublevelFiles) - 1; sublevel >= 0; sublevel-- {
				fmt.Fprintf(stdout, "--- L0.%d ---\n", sublevel)
				v.L0SublevelFiles[sublevel].Each(func(f *manifest.FileMetadata) {
					fmt.Fprintf(stdout, "  %s:%d", f.FileNum, f.Size)
					formatSeqNumRange(stdout, f.SmallestSeqNum, f.LargestSeqNum)
					formatKeyRange(stdout, m.fmtKey, &f.Smallest, &f.Largest)
					fmt.Fprintf(stdout, "\n")
				})
			}
			continue
		}
		fmt.Fprintf(stdout, "--- L%d ---\n", level)
		iter := v.Levels[level].Iter()
		for f := iter.First(); f != nil; f = iter.Next() {
			fmt.Fprintf(stdout, "  %s:%d", f.FileNum, f.Size)
			formatSeqNumRange(stdout, f.SmallestSeqNum, f.LargestSeqNum)
			formatKeyRange(stdout, m.fmtKey, &f.Smallest, &f.Largest)
			fmt.Fprintf(stdout, "\n")
		}
	}
}

func (m *manifestT) runDump(cmd *cobra.Command, args []string) {
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
			bve.AddedByFileNum = make(map[base.FileNum]*manifest.FileMetadata)
			var cmp *base.Comparer
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

				empty := true
				fmt.Fprintf(stdout, "%d\n", offset)
				if ve.ComparerName != "" {
					empty = false
					fmt.Fprintf(stdout, "  comparer:     %s", ve.ComparerName)
					cmp = m.comparers[ve.ComparerName]
					if cmp == nil {
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
				entries := make([]manifest.DeletedFileEntry, 0, len(ve.DeletedFiles))
				for df := range ve.DeletedFiles {
					empty = false
					entries = append(entries, df)
				}
				sort.Slice(entries, func(i, j int) bool {
					if entries[i].Level != entries[j].Level {
						return entries[i].Level < entries[j].Level
					}
					return entries[i].FileNum < entries[j].FileNum
				})
				for _, df := range entries {
					fmt.Fprintf(stdout, "  deleted:       L%d %s\n", df.Level, df.FileNum)
				}
				for _, nf := range ve.NewFiles {
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
			}

			if cmp != nil {
				v, _, err := bve.Apply(nil /* version */, cmp.Compare, m.fmtKey.fn, 0, m.opts.Experimental.ReadCompactionRate)
				if err != nil {
					fmt.Fprintf(stdout, "%s\n", err)
					return
				}
				m.printLevels(v)
			}
		}()
	}
}

func (m *manifestT) runCheck(cmd *cobra.Command, args []string) {
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

			var v *manifest.Version
			var cmp *base.Comparer
			rr := record.NewReader(f, 0 /* logNum */)
			// Contains the FileMetadata needed by BulkVersionEdit.Apply.
			// It accumulates the additions since later edits contain
			// deletions of earlier added files.
			addedByFileNum := make(map[base.FileNum]*manifest.FileMetadata)
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
				bve.AddedByFileNum = addedByFileNum
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
					ve.LastSeqNum == 0 && len(ve.DeletedFiles) == 0 &&
					len(ve.NewFiles) == 0
				if empty {
					continue
				}
				// TODO(sbhola): add option to Apply that reports all errors instead of
				// one error.
				newv, _, err := bve.Apply(v, cmp.Compare, m.fmtKey.fn, 0, m.opts.Experimental.ReadCompactionRate)
				if err != nil {
					fmt.Fprintf(stdout, "%s: offset: %d err: %s\n",
						arg, offset, err)
					fmt.Fprintf(stdout, "Version state before failed Apply\n")
					m.printLevels(v)
					fmt.Fprintf(stdout, "Version edit that failed\n")
					for df := range ve.DeletedFiles {
						fmt.Fprintf(stdout, "  deleted: L%d %s\n", df.Level, df.FileNum)
					}
					for _, nf := range ve.NewFiles {
						fmt.Fprintf(stdout, "  added: L%d %s:%d",
							nf.Level, nf.Meta.FileNum, nf.Meta.Size)
						formatSeqNumRange(stdout, nf.Meta.SmallestSeqNum, nf.Meta.LargestSeqNum)
						formatKeyRange(stdout, m.fmtKey, &nf.Meta.Smallest, &nf.Meta.Largest)
						fmt.Fprintf(stdout, "\n")
					}
					ok = false
					break
				}
				v = newv
			}
		}()
	}
	if ok {
		fmt.Fprintf(stdout, "OK\n")
	}
}
