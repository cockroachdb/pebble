// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"fmt"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/record"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/spf13/cobra"
)

// manifestT implements manifest-level tools, including both configuration
// state and the commands themselves.
type manifestT struct {
	Root *cobra.Command
	Dump *cobra.Command

	opts      *sstable.Options
	comparers sstable.Comparers
	fmtKey    formatter
}

func newManifest(opts *base.Options, comparers sstable.Comparers) *manifestT {
	m := &manifestT{
		opts:      opts,
		comparers: comparers,
	}
	m.fmtKey.mustSet("quoted")

	m.Root = &cobra.Command{
		Use:   "manifest",
		Short: "manifest introspection tools",
	}
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

	m.Dump.Flags().Var(
		&m.fmtKey, "key", "key formatter")
	return m
}

func (m *manifestT) runDump(cmd *cobra.Command, args []string) {
	for _, arg := range args {
		func() {
			f, err := vfs.Default.Open(arg)
			if err != nil {
				fmt.Fprintf(stderr, "%s\n", err)
				return
			}
			defer f.Close()

			fmt.Fprintf(stdout, "%s\n", arg)

			var bve manifest.BulkVersionEdit
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
				bve.Accumulate(&ve)

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
				}
				m.fmtKey.setForComparer(ve.ComparerName, m.comparers)
				if ve.LogNum != 0 {
					empty = false
					fmt.Fprintf(stdout, "  log-num:      %d\n", ve.LogNum)
				}
				if ve.PrevLogNum != 0 {
					empty = false
					fmt.Fprintf(stdout, "  prev-log-num: %d\n", ve.PrevLogNum)
				}
				if ve.LastSeqNum != 0 {
					empty = false
					fmt.Fprintf(stdout, "  last-seq-num: %d\n", ve.LastSeqNum)
				}
				for df := range ve.DeletedFiles {
					empty = false
					fmt.Fprintf(stdout, "  deleted:      L%d %d\n", df.Level, df.FileNum)
				}
				for _, nf := range ve.NewFiles {
					empty = false
					fmt.Fprintf(stdout, "  added:        L%d %d:%d",
						nf.Level, nf.Meta.FileNum, nf.Meta.Size)
					formatKeyRange(stdout, m.fmtKey, &nf.Meta.Smallest, &nf.Meta.Largest)
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
				v, err := bve.Apply(nil, cmp.Compare, m.fmtKey.fn)
				if err != nil {
					fmt.Fprintf(stdout, "%s\n", err)
					return
				}
				for level := range v.Files {
					fmt.Fprintf(stdout, "--- L%d ---\n", level)
					for j := range v.Files[level] {
						f := &v.Files[level][j]
						fmt.Fprintf(stdout, "  %d:%d", f.FileNum, f.Size)
						formatKeyRange(stdout, m.fmtKey, &f.Smallest, &f.Largest)
						fmt.Fprintf(stdout, "\n")
					}
				}
			}
		}()
	}
}
