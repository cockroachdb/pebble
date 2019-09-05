// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/spf13/cobra"
)

// dbT implements db-level tools, including both configuration state and the
// commands themselves.
type dbT struct {
	Root  *cobra.Command
	Check *cobra.Command
	LSM   *cobra.Command
	Scan  *cobra.Command

	// Configuration.
	opts      *sstable.Options
	comparers sstable.Comparers
	mergers   sstable.Mergers

	// Flags.
	comparerName string
	mergerName   string
	fmtKey       formatter
	fmtValue     formatter
	start        key
	end          key
}

func newDB(
	opts *base.Options, comparers sstable.Comparers, mergers sstable.Mergers,
) *dbT {
	d := &dbT{
		opts:      opts,
		comparers: comparers,
		mergers:   mergers,
	}
	d.fmtKey.mustSet("quoted")
	d.fmtValue.mustSet("[%x]")

	d.Root = &cobra.Command{
		Use:   "db",
		Short: "DB introspection tools",
	}
	d.Check = &cobra.Command{
		Use:   "check <dir>",
		Short: "verify checksums and metadata",
		Long: `
Verify sstable, manifest, and WAL checksums. Requires that the specified
database not be in use by another process.
`,
		Args: cobra.ExactArgs(1),
		Run:  d.runCheck,
	}
	d.LSM = &cobra.Command{
		Use:   "lsm <dir>",
		Short: "print LSM structure",
		Long: `
Print the structure of the LSM tree. Requires that the specified database not
be in use by another process.
`,
		Args: cobra.ExactArgs(1),
		Run:  d.runLSM,
	}
	d.Scan = &cobra.Command{
		Use:   "scan <dir>",
		Short: "print db records",
		Long: `
Print the records in the DB. Requires that the specified database not be in use
by another process.
`,
		Args: cobra.ExactArgs(1),
		Run:  d.runScan,
	}

	d.Root.AddCommand(d.Check, d.LSM, d.Scan)

	for _, cmd := range []*cobra.Command{d.Check, d.LSM, d.Scan} {
		cmd.Flags().StringVar(
			&d.comparerName, "comparer", "", "comparer name (use default if empty)")
		cmd.Flags().StringVar(
			&d.mergerName, "merger", "", "merger name (use default if empty)")
	}

	d.Scan.Flags().Var(
		&d.fmtKey, "key", "key formatter")
	d.Scan.Flags().Var(
		&d.fmtValue, "value", "value formatter")
	d.Scan.Flags().Var(
		&d.start, "start", "start key for the scan")
	d.Scan.Flags().Var(
		&d.end, "end", "end key for the scan")
	return d
}

func (d *dbT) openDB(dir string) (*pebble.DB, error) {
	if d.comparerName != "" {
		d.opts.Comparer = d.comparers[d.comparerName]
		if d.opts.Comparer == nil {
			return nil, fmt.Errorf("unknown comparer %q", d.comparerName)
		}
	}
	if d.mergerName != "" {
		d.opts.Merger = d.mergers[d.mergerName]
		if d.opts.Merger == nil {
			return nil, fmt.Errorf("unknown merger %q", d.mergerName)
		}
	}
	return pebble.Open(dir, d.opts)
}

func (d *dbT) runCheck(cmd *cobra.Command, args []string) {
	// The check command is equivalent to scanning over all of the records, but
	// not outputting anything.
	d.fmtKey.Set("null")
	d.fmtValue.Set("null")
	d.start, d.end = nil, nil
	d.runScan(cmd, args)
}

func (d *dbT) runLSM(cmd *cobra.Command, args []string) {
	db, err := d.openDB(args[0])
	if err != nil {
		fmt.Fprintf(stdout, "%s\n", err)
		return
	}

	fmt.Fprintf(stdout, "%s", db.Metrics())

	if err := db.Close(); err != nil {
		fmt.Fprintf(stdout, "%s\n", err)
	}
}

func (d *dbT) runScan(cmd *cobra.Command, args []string) {
	db, err := d.openDB(args[0])
	if err != nil {
		fmt.Fprintf(stdout, "%s\n", err)
		return
	}

	start := timeNow()
	fmtKeys := d.fmtKey.spec != "null"
	fmtValues := d.fmtValue.spec != "null"
	var count int64

	iter := db.NewIter(&pebble.IterOptions{
		UpperBound: d.end,
	})
	for valid := iter.SeekGE(d.start); valid; valid = iter.Next() {
		if fmtKeys || fmtValues {
			needDelimiter := false
			if fmtKeys {
				fmt.Fprintf(stdout, "%s", d.fmtKey.fn(iter.Key()))
				needDelimiter = true
			}
			if fmtValues {
				if needDelimiter {
					stdout.Write([]byte{' '})
				}
				fmt.Fprintf(stdout, "%s", d.fmtValue.fn(iter.Value()))
			}
			stdout.Write([]byte{'\n'})
		}

		count++
	}

	if err := iter.Close(); err != nil {
		fmt.Fprintf(stdout, "%s\n", err)
	}

	elapsed := timeNow().Sub(start)

	plural := ""
	if count != 1 {
		plural = "s"
	}
	fmt.Fprintf(stdout, "scanned %d record%s in %0.1fs\n",
		count, plural, elapsed.Seconds())

	if err := db.Close(); err != nil {
		fmt.Fprintf(stdout, "%s\n", err)
	}
}
