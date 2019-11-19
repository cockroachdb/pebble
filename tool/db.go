// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"fmt"
	"io/ioutil"

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
	Space *cobra.Command

	// Configuration.
	opts      *pebble.Options
	comparers sstable.Comparers
	mergers   sstable.Mergers

	// Flags.
	comparerName string
	mergerName   string
	fmtKey       formatter
	fmtValue     formatter
	start        key
	end          key
	count        int64
	verbose      bool
}

func newDB(opts *pebble.Options, comparers sstable.Comparers, mergers sstable.Mergers) *dbT {
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
	d.Space = &cobra.Command{
		Use:   "space <dir>",
		Short: "print filesystem space used",
		Long: `
Print the estimated filesystem space usage for the inclusive-inclusive range
specified by --start and --end. Requires that the specified database not be in
use by another process.
`,
		Args: cobra.ExactArgs(1),
		Run:  d.runSpace,
	}

	d.Root.AddCommand(d.Check, d.LSM, d.Scan, d.Space)
	d.Root.PersistentFlags().BoolVarP(&d.verbose, "verbose", "v", false, "verbose output")

	for _, cmd := range []*cobra.Command{d.Check, d.LSM, d.Scan, d.Space} {
		cmd.Flags().StringVar(
			&d.comparerName, "comparer", "", "comparer name (use default if empty)")
		cmd.Flags().StringVar(
			&d.mergerName, "merger", "", "merger name (use default if empty)")
	}

	for _, cmd := range []*cobra.Command{d.Scan, d.Space} {
		cmd.Flags().Var(
			&d.start, "start", "start key for the range")
		cmd.Flags().Var(
			&d.end, "end", "end key for the range")
	}

	d.Scan.Flags().Var(
		&d.fmtKey, "key", "key formatter")
	d.Scan.Flags().Var(
		&d.fmtValue, "value", "value formatter")
	d.Scan.Flags().Int64Var(
		&d.count, "count", 0, "key count for scan (0 is unlimited)")
	return d
}

func (d *dbT) loadOptions(dir string) error {
	ls, err := d.opts.FS.List(dir)
	if err != nil || len(ls) == 0 {
		// NB: We don't return the error here as we prefer to return the error from
		// pebble.Open. Another way to put this is that a non-existent directory is
		// not a failure in loading the options.
		return nil
	}

	hooks := &pebble.ParseHooks{
		NewComparer: func(name string) (*pebble.Comparer, error) {
			if c := d.comparers[name]; c != nil {
				return c, nil
			}
			return nil, fmt.Errorf("unknown comparer %q", name)
		},
		NewMerger: func(name string) (*pebble.Merger, error) {
			if m := d.mergers[name]; m != nil {
				return m, nil
			}
			return nil, fmt.Errorf("unknown merger %q", name)
		},
		SkipUnknown: func(name string) bool {
			return true
		},
	}

	// TODO(peter): RocksDB sometimes leaves multiple OPTIONS files in
	// existence. We parse all of them as the comparer and merger shouldn't be
	// changing. We could parse only the first or the latest. Not clear if this
	// matters.
	var dbOpts pebble.Options
	for _, filename := range ls {
		ft, _, ok := base.ParseFilename(d.opts.FS, filename)
		if !ok {
			continue
		}
		switch ft {
		case base.FileTypeOptions:
			err := func() error {
				f, err := d.opts.FS.Open(d.opts.FS.PathJoin(dir, filename))
				if err != nil {
					return err
				}
				defer f.Close()

				data, err := ioutil.ReadAll(f)
				if err != nil {
					return err
				}

				if err := dbOpts.Parse(string(data), hooks); err != nil {
					return err
				}
				return nil
			}()
			if err != nil {
				return err
			}
		}
	}

	if dbOpts.Comparer != nil {
		d.opts.Comparer = dbOpts.Comparer
	}
	if dbOpts.Merger != nil {
		d.opts.Merger = dbOpts.Merger
	}
	return nil
}

func (d *dbT) openDB(dir string) (*pebble.DB, error) {
	if err := d.loadOptions(dir); err != nil {
		return nil, err
	}
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
	db, err := d.openDB(args[0])
	if err != nil {
		fmt.Fprintf(stdout, "%s\n", err)
		return
	}
	var stats pebble.CheckLevelsStats
	if err := db.CheckLevels(&stats); err != nil {
		fmt.Fprintf(stdout, "%s\n", err)
	}
	fmt.Fprintf(stdout, "checked %d %s and %d %s\n",
		stats.NumPoints, makePlural("point", stats.NumPoints), stats.NumTombstones, makePlural("tombstone", int64(stats.NumTombstones)))
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

	// Update the internal formatter if this comparator has one specified.
	if d.opts.Comparer != nil {
		d.fmtKey.setForComparer(d.opts.Comparer.Name, d.comparers)
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
		if d.count > 0 && count >= d.count {
			break
		}
	}

	if err := iter.Close(); err != nil {
		fmt.Fprintf(stdout, "%s\n", err)
	}

	elapsed := timeNow().Sub(start)

	fmt.Fprintf(stdout, "scanned %d %s in %0.1fs\n",
		count, makePlural("record", count), elapsed.Seconds())

	if err := db.Close(); err != nil {
		fmt.Fprintf(stdout, "%s\n", err)
	}
}

func (d *dbT) runSpace(cmd *cobra.Command, args []string) {
	db, err := d.openDB(args[0])
	if err != nil {
		fmt.Fprintf(stderr, "%s\n", err)
		return
	}
	defer func() {
		if err := db.Close(); err != nil {
			fmt.Fprintf(stdout, "%s\n", err)
		}
	}()
	bytes, err := db.EstimateDiskUsage(d.start, d.end)
	if err != nil {
		fmt.Fprintf(stderr, "%s\n", err)
		return
	}
	fmt.Fprintf(stdout, "%d\n", bytes)
}

func makePlural(singular string, count int64) string {
	if count > 1 {
		return fmt.Sprintf("%ss", singular)
	}
	return singular
}
