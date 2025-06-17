// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/humanize"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/sstableinternal"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/tool/logs"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/spf13/cobra"
)

// dbT implements db-level tools, including both configuration state and the
// commands themselves.
type dbT struct {
	Root        *cobra.Command
	Check       *cobra.Command
	Upgrade     *cobra.Command
	Checkpoint  *cobra.Command
	Get         *cobra.Command
	Logs        *cobra.Command
	LSM         *cobra.Command
	Properties  *cobra.Command
	Scan        *cobra.Command
	Set         *cobra.Command
	Space       *cobra.Command
	IOBench     *cobra.Command
	Excise      *cobra.Command
	AnalyzeData *cobra.Command

	// Configuration.
	opts            *pebble.Options
	comparers       sstable.Comparers
	mergers         sstable.Mergers
	openErrEnhancer func(error) error
	openOptions     []OpenOption
	exciseSpanFn    DBExciseSpanFn
	remoteStorageFn DBRemoteStorageFn

	// Flags.
	comparerName string
	mergerName   string
	fmtKey       keyFormatter
	fmtValue     valueFormatter
	start        key
	end          key
	count        int64
	// TOOD(radu): move flags specific to a single command to substructs.
	allLevels     bool
	ioCount       int
	ioParallelism int
	ioSizes       string
	verbose       bool
	bypassPrompt  bool
	lsmURL        bool
	analyzeData   struct {
		samplePercent int
		timeout       time.Duration
		readMBPerSec  int
		outputCSVFile string
	}
}

func newDB(
	opts *pebble.Options,
	comparers sstable.Comparers,
	mergers sstable.Mergers,
	openErrEnhancer func(error) error,
	openOptions []OpenOption,
	exciseSpanFn DBExciseSpanFn,
) *dbT {
	d := &dbT{
		opts:            opts,
		comparers:       comparers,
		mergers:         mergers,
		openErrEnhancer: openErrEnhancer,
		openOptions:     openOptions,
		exciseSpanFn:    exciseSpanFn,
	}
	d.fmtKey.mustSet("quoted")
	d.fmtValue.mustSet("[%x]")

	d.Root = &cobra.Command{
		Use:     "db",
		Short:   "DB introspection tools",
		Version: fmt.Sprintf("supported Pebble format versions: %d-%d", pebble.FormatMinSupported, pebble.FormatNewest),
	}
	d.Root.SetVersionTemplate(`{{printf "%s" .Short}}
{{printf "%s" .Version}}
`)
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
	d.Upgrade = &cobra.Command{
		Use:   "upgrade <dir>",
		Short: "upgrade the DB internal format version",
		Long: `
Upgrades the DB internal format version to the latest version.
It is recommended to make a backup copy of the DB directory before upgrading.
Requires that the specified database not be in use by another process.
`,
		Args: cobra.ExactArgs(1),
		Run:  d.runUpgrade,
	}
	d.Checkpoint = &cobra.Command{
		Use:   "checkpoint <src-dir> <dest-dir>",
		Short: "create a checkpoint",
		Long: `
Creates a Pebble checkpoint in the specified destination directory. A checkpoint
is a point-in-time snapshot of DB state. Requires that the specified
database not be in use by another process.
`,
		Args: cobra.ExactArgs(2),
		Run:  d.runCheckpoint,
	}
	d.Get = &cobra.Command{
		Use:   "get <dir> <key>",
		Short: "get value for a key",
		Long: `
Gets a value for a key, if it exists in DB. Prints a "not found" error if key
does not exist. Requires that the specified database not be in use by another
process.
`,
		Args: cobra.ExactArgs(2),
		Run:  d.runGet,
	}
	d.Logs = logs.NewCmd()
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
	d.Properties = &cobra.Command{
		Use:   "properties <dir>",
		Short: "print aggregated sstable properties",
		Long: `
Print SSTable properties, aggregated per level of the LSM.
`,
		Args: cobra.ExactArgs(1),
		Run:  d.runProperties,
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
	d.Set = &cobra.Command{
		Use:   "set <dir> <key> <value>",
		Short: "set a value for a key",
		Long: `
Adds a new key/value to the DB. Requires that the specified database
not be in use by another process.
`,
		Args: cobra.ExactArgs(3),
		Run:  d.runSet,
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
	d.Excise = &cobra.Command{
		Use:   "excise <dir>",
		Short: "excise a key range",
		Long: `
Excise a key range, removing all SSTs inside the range and virtualizing any SSTs
that partially overlap the range.
`,
		Args: cobra.ExactArgs(1),
		Run:  d.runExcise,
	}
	d.IOBench = &cobra.Command{
		Use:   "io-bench <dir>",
		Short: "perform sstable IO benchmark",
		Long: `
Run a random IO workload with various IO sizes against the sstables in the
specified database.
`,
		Args: cobra.ExactArgs(1),
		Run:  d.runIOBench,
	}
	d.AnalyzeData = &cobra.Command{
		Use:   "analyze-data <dir>",
		Short: "analyze data compressibility",
		Long: `
Sample blocks from the files in the database directory to run compression
experiments and produce a CSV file of the results.
`,
		Args: cobra.ExactArgs(1),
		Run:  d.runAnalyzeData,
	}

	d.Root.AddCommand(d.Check, d.Upgrade, d.Checkpoint, d.Get, d.Logs, d.LSM, d.Properties, d.Scan, d.Set, d.Space, d.Excise, d.IOBench, d.AnalyzeData)
	d.Root.PersistentFlags().BoolVarP(&d.verbose, "verbose", "v", false, "verbose output")

	for _, cmd := range []*cobra.Command{d.Check, d.Upgrade, d.Checkpoint, d.Get, d.LSM, d.Properties, d.Scan, d.Set, d.Space, d.Excise, d.AnalyzeData} {
		cmd.Flags().StringVar(
			&d.comparerName, "comparer", "", "comparer name (use default if empty)")
		cmd.Flags().StringVar(
			&d.mergerName, "merger", "", "merger name (use default if empty)")
	}

	for _, cmd := range []*cobra.Command{d.Scan, d.Get} {
		cmd.Flags().Var(
			&d.fmtValue, "value", "value formatter")
	}
	for _, cmd := range []*cobra.Command{d.Upgrade, d.Excise} {
		cmd.Flags().BoolVarP(
			&d.bypassPrompt, "yes", "y", false, "bypass prompt")
	}

	d.LSM.Flags().BoolVar(
		&d.lsmURL, "url", false, "generate LSM viewer URL")

	d.Space.Flags().Var(
		&d.start, "start", "start key for the range")
	d.Space.Flags().Var(
		&d.end, "end", "inclusive end key for the range")

	d.Scan.Flags().Var(
		&d.fmtKey, "key", "key formatter")
	d.Scan.Flags().Var(
		&d.start, "start", "start key for the range")
	d.Scan.Flags().Var(
		&d.end, "end", "exclusive end key for the range")
	d.Scan.Flags().Int64Var(
		&d.count, "count", 0, "key count for scan (0 is unlimited)")

	d.Excise.Flags().Var(
		&d.start, "start", "start key for the excised range")
	d.Excise.Flags().Var(
		&d.end, "end", "exclusive end key for the excised range")

	d.IOBench.Flags().BoolVar(
		&d.allLevels, "all-levels", false, "if set, benchmark all levels (default is only L5/L6)")
	d.IOBench.Flags().IntVar(
		&d.ioCount, "io-count", 10000, "number of IOs (per IO size) to benchmark")
	d.IOBench.Flags().IntVar(
		&d.ioParallelism, "io-parallelism", 16, "number of goroutines issuing IO")
	d.IOBench.Flags().StringVar(
		&d.ioSizes, "io-sizes-kb", "4,16,64,128,256,512,1024", "comma separated list of IO sizes in KB")

	d.AnalyzeData.Flags().IntVar(
		&d.analyzeData.samplePercent, "sample-percent", 100, "percentage of data to sample (default 100%)")

	d.AnalyzeData.Flags().DurationVar(
		&d.analyzeData.timeout, "timeout", 0, "stop after this much time has passed (default 0 = no timeout)")

	d.AnalyzeData.Flags().IntVar(
		&d.analyzeData.readMBPerSec, "read-mb-per-sec", 0, "limit read IO bandwidth (default 0 = no limit)")

	d.AnalyzeData.Flags().StringVar(
		&d.analyzeData.outputCSVFile, "output", "", "path for the output CSV file")
	_ = d.AnalyzeData.MarkFlagRequired("output")

	return d
}

func (d *dbT) initOptions(dir string) error {
	if err := d.loadOptions(dir); err != nil {
		return errors.Wrap(err, "error loading options")
	}
	if d.comparerName != "" {
		d.opts.Comparer = d.comparers[d.comparerName]
		if d.opts.Comparer == nil {
			return errors.Errorf("unknown comparer %q", errors.Safe(d.comparerName))
		}
	}
	if d.mergerName != "" {
		d.opts.Merger = d.mergers[d.mergerName]
		if d.opts.Merger == nil {
			return errors.Errorf("unknown merger %q", errors.Safe(d.mergerName))
		}
	}
	return nil
}

func (d *dbT) loadOptions(dir string) error {
	ls, err := d.opts.FS.List(dir)
	if err != nil || len(ls) == 0 {
		// nolint:returnerrcheck
		// We don't return the error here as we prefer to return the error from
		// pebble.Open. Another way to put this is that a non-existent directory is
		// not a failure in loading the options.
		return nil
	}

	hooks := &pebble.ParseHooks{
		NewComparer: func(name string) (*pebble.Comparer, error) {
			if c := d.comparers[name]; c != nil {
				return c, nil
			}
			return nil, errors.Errorf("unknown comparer %q", errors.Safe(name))
		},
		NewMerger: func(name string) (*pebble.Merger, error) {
			if m := d.mergers[name]; m != nil {
				return m, nil
			}
			return nil, errors.Errorf("unknown merger %q", errors.Safe(name))
		},
		SkipUnknown: func(name, value string) bool {
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

				data, err := io.ReadAll(f)
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

// OpenOption is an option that may be applied to the *pebble.Options before
// calling pebble.Open.
type OpenOption interface {
	Apply(dirname string, opts *pebble.Options)
}

func (d *dbT) openDB(dir string, openOptions ...OpenOption) (*pebble.DB, error) {
	db, err := d.openDBInternal(dir, openOptions...)
	if err != nil {
		if d.openErrEnhancer != nil {
			err = d.openErrEnhancer(err)
		}
		return nil, err
	}
	return db, nil
}

func (d *dbT) openDBInternal(dir string, openOptions ...OpenOption) (*pebble.DB, error) {
	if err := d.initOptions(dir); err != nil {
		return nil, err
	}
	opts := *d.opts
	for _, opt := range openOptions {
		opt.Apply(dir, &opts)
	}
	for _, opt := range d.openOptions {
		opt.Apply(dir, &opts)
	}
	opts.Cache = nil
	opts.CacheSize = 128 * 1024 * 1024
	return pebble.Open(dir, &opts)
}

func (d *dbT) closeDB(stderr io.Writer, db *pebble.DB) {
	if err := db.Close(); err != nil {
		fmt.Fprintf(stderr, "%s\n", err)
	}
}

func (d *dbT) runCheck(cmd *cobra.Command, args []string) {
	stdout, stderr := cmd.OutOrStdout(), cmd.ErrOrStderr()
	db, err := d.openDB(args[0])
	if err != nil {
		fmt.Fprintf(stderr, "%s\n", err)
		return
	}
	defer d.closeDB(stderr, db)

	var stats pebble.CheckLevelsStats
	if err := db.CheckLevels(&stats); err != nil {
		fmt.Fprintf(stderr, "%s\n", err)
	}
	fmt.Fprintf(stdout, "checked %d %s and %d %s\n",
		stats.NumPoints, makePlural("point", stats.NumPoints), stats.NumTombstones, makePlural("tombstone", int64(stats.NumTombstones)))
}

func (d *dbT) runUpgrade(cmd *cobra.Command, args []string) {
	stdout, stderr := cmd.OutOrStdout(), cmd.ErrOrStderr()
	db, err := d.openDB(args[0], nonReadOnly{})
	if err != nil {
		fmt.Fprintf(stderr, "%s\n", err)
		return
	}
	defer d.closeDB(stderr, db)

	targetVersion := pebble.FormatNewest
	current := db.FormatMajorVersion()
	if current >= targetVersion {
		fmt.Fprintf(stdout, "DB is already at internal version %d.\n", current)
		return
	}
	fmt.Fprintf(stdout, "Upgrading DB from internal version %d to %d.\n", current, targetVersion)

	prompt := `WARNING!!!
This DB will not be usable with older versions of Pebble!

It is strongly recommended to back up the data before upgrading.
`

	if len(d.opts.BlockPropertyCollectors) == 0 {
		prompt += `
If this DB uses custom block property collectors, the upgrade should be invoked
through a custom binary that configures them. Otherwise, any new tables created
during upgrade will not have the relevant block properties.
`
	}
	if !d.promptForConfirmation(prompt, cmd.InOrStdin(), stdout, stderr) {
		return
	}
	if err := db.RatchetFormatMajorVersion(targetVersion); err != nil {
		fmt.Fprintf(stderr, "error: %s\n", err)
	}
	fmt.Fprintf(stdout, "Upgrade complete.\n")
}

func (d *dbT) runCheckpoint(cmd *cobra.Command, args []string) {
	stderr := cmd.ErrOrStderr()
	db, err := d.openDB(args[0], nonReadOnly{})
	if err != nil {
		fmt.Fprintf(stderr, "%s\n", err)
		return
	}
	defer d.closeDB(stderr, db)
	destDir := args[1]

	if err := db.Checkpoint(destDir); err != nil {
		fmt.Fprintf(stderr, "%s\n", err)
	}
}

func (d *dbT) runGet(cmd *cobra.Command, args []string) {
	stdout, stderr := cmd.OutOrStdout(), cmd.ErrOrStderr()
	db, err := d.openDB(args[0])
	if err != nil {
		fmt.Fprintf(stderr, "%s\n", err)
		return
	}
	defer d.closeDB(stderr, db)
	var k key
	if err := k.Set(args[1]); err != nil {
		fmt.Fprintf(stderr, "%s\n", err)
		return
	}

	val, closer, err := db.Get(k)
	if err != nil {
		fmt.Fprintf(stderr, "%s\n", err)
		return
	}
	defer func() {
		if closer != nil {
			closer.Close()
		}
	}()
	if val != nil {
		fmt.Fprintf(stdout, "%s\n", d.fmtValue.fn(k, val))
	}
}

func (d *dbT) runLSM(cmd *cobra.Command, args []string) {
	stdout, stderr := cmd.OutOrStdout(), cmd.ErrOrStderr()
	db, err := d.openDB(args[0])
	if err != nil {
		fmt.Fprintf(stderr, "%s\n", err)
		return
	}
	defer d.closeDB(stderr, db)

	fmt.Fprintf(stdout, "%s", db.Metrics())
	if d.lsmURL {
		fmt.Fprintf(stdout, "\nLSM viewer: %s\n", db.LSMViewURL())
	}
}

func (d *dbT) runScan(cmd *cobra.Command, args []string) {
	stdout, stderr := cmd.OutOrStdout(), cmd.ErrOrStderr()
	db, err := d.openDB(args[0])
	if err != nil {
		fmt.Fprintf(stderr, "%s\n", err)
		return
	}
	defer d.closeDB(stderr, db)

	// Update the internal formatter if this comparator has one specified.
	if d.opts.Comparer != nil {
		d.fmtKey.setForComparer(d.opts.Comparer.Name, d.comparers)
		d.fmtValue.setForComparer(d.opts.Comparer.Name, d.comparers)
	}

	start := timeNow()
	fmtKeys := d.fmtKey.spec != "null"
	fmtValues := d.fmtValue.spec != "null"
	var count int64

	iter, _ := db.NewIter(&pebble.IterOptions{
		UpperBound: d.end,
	})
	var valid bool
	if len(d.start) == 0 {
		valid = iter.First()
	} else {
		valid = iter.SeekGE(d.start)
	}
	for ; valid; valid = iter.Next() {
		if fmtKeys || fmtValues {
			needDelimiter := false
			if fmtKeys {
				fmt.Fprintf(stdout, "%s", d.fmtKey.fn(iter.Key()))
				needDelimiter = true
			}
			if fmtValues {
				if needDelimiter {
					_, _ = stdout.Write([]byte{' '})
				}
				fmt.Fprintf(stdout, "%s", d.fmtValue.fn(iter.Key(), iter.Value()))
			}
			_, _ = stdout.Write([]byte{'\n'})
		}

		count++
		if d.count > 0 && count >= d.count {
			break
		}
	}

	if err := iter.Close(); err != nil {
		fmt.Fprintf(stderr, "%s\n", err)
	}

	elapsed := timeNow().Sub(start)

	fmt.Fprintf(stdout, "scanned %d %s in %0.1fs\n",
		count, makePlural("record", count), elapsed.Seconds())
}

func (d *dbT) runSpace(cmd *cobra.Command, args []string) {
	stdout, stderr := cmd.OutOrStdout(), cmd.ErrOrStderr()
	db, err := d.openDB(args[0])
	if err != nil {
		fmt.Fprintf(stderr, "%s\n", err)
		return
	}
	defer d.closeDB(stdout, db)

	bytes, err := db.EstimateDiskUsage(d.start, d.end)
	if err != nil {
		fmt.Fprintf(stderr, "%s\n", err)
		return
	}
	fmt.Fprintf(stdout, "%d\n", bytes)
}

func (d *dbT) getExciseSpan() (pebble.KeyRange, error) {
	// If a DBExciseSpanFn is specified, try to use it and see if it returns a
	// valid span.
	if d.exciseSpanFn != nil {
		span, err := d.exciseSpanFn()
		if err != nil {
			return pebble.KeyRange{}, err
		}
		if span.Valid() {
			if d.start != nil || d.end != nil {
				return pebble.KeyRange{}, errors.Errorf(
					"--start/--end cannot be used when span is specified by other methods.")
			}
			return span, nil
		}
	}
	if d.start == nil || d.end == nil {
		return pebble.KeyRange{}, errors.Errorf("excise range not specified.")
	}
	return pebble.KeyRange{
		Start: d.start,
		End:   d.end,
	}, nil
}

func (d *dbT) runExcise(cmd *cobra.Command, args []string) {
	stdout, stderr := cmd.OutOrStdout(), cmd.ErrOrStderr()

	span, err := d.getExciseSpan()
	if err != nil {
		fmt.Fprintf(stderr, "Error: %v\n", err)
		return
	}

	d.opts.EnsureDefaults()
	dbOpts := d.opts
	// Disable all processes that would try to open tables: table stats,
	// consistency check, automatic compactions.
	dbOpts.DisableTableStats = true
	dbOpts.DisableConsistencyCheck = true
	dbOpts.DisableAutomaticCompactions = true

	dbDir := args[0]
	db, err := d.openDB(dbDir, nonReadOnly{})
	if err != nil {
		fmt.Fprintf(stderr, "%s\n", err)
		return
	}
	defer d.closeDB(stdout, db)

	// Update the internal formatter if this comparator has one specified.
	if d.opts.Comparer != nil {
		d.fmtKey.setForComparer(d.opts.Comparer.Name, d.comparers)
	}

	fmt.Fprintf(stdout, "Excising range:\n")
	fmt.Fprintf(stdout, "  start: %s\n", d.fmtKey.fn(span.Start))
	fmt.Fprintf(stdout, "  end:   %s\n", d.fmtKey.fn(span.End))

	prompt := `WARNING!!!
This command will remove all keys in this range!`
	if !d.promptForConfirmation(prompt, cmd.InOrStdin(), stdout, stderr) {
		return
	}

	// Write a temporary sst that only has excise tombstones. We write it inside
	// the database directory so that the command works against any FS.
	// TODO(radu): remove this if we add a separate DB.Excise method.
	path := dbOpts.FS.PathJoin(dbDir, fmt.Sprintf("excise-%0x.sst", rand.Uint32()))
	defer func() { _ = dbOpts.FS.Remove(path) }()
	f, err := dbOpts.FS.Create(path, vfs.WriteCategoryUnspecified)
	if err != nil {
		fmt.Fprintf(stderr, "Error creating temporary sst file %q: %s\n", path, err)
		return
	}
	writable := objstorageprovider.NewFileWritable(f)
	writerOpts := dbOpts.MakeWriterOptions(0, db.TableFormat())
	w := sstable.NewWriter(writable, writerOpts)
	err = w.DeleteRange(span.Start, span.End)
	err = errors.CombineErrors(err, w.RangeKeyDelete(span.Start, span.End))
	err = errors.CombineErrors(err, w.Close())
	if err != nil {
		fmt.Fprintf(stderr, "Error writing temporary sst file %q: %s\n", path, err)
		return
	}

	_, err = db.IngestAndExcise(context.Background(), []string{path}, nil, nil, span)
	if err != nil {
		fmt.Fprintf(stderr, "Error excising: %s\n", err)
		return
	}
	fmt.Fprintf(stdout, "Excise complete.\n")
}

func (d *dbT) runProperties(cmd *cobra.Command, args []string) {
	stdout, stderr := cmd.OutOrStdout(), cmd.ErrOrStderr()
	dirname := args[0]
	err := func() error {
		desc, err := pebble.Peek(dirname, d.opts.FS)
		if err != nil {
			return err
		} else if !desc.Exists {
			return oserror.ErrNotExist
		}
		manifestFilename := d.opts.FS.PathBase(desc.ManifestFilename)

		// Replay the manifest to get the current version.
		f, err := d.opts.FS.Open(desc.ManifestFilename)
		if err != nil {
			return errors.Wrapf(err, "pebble: could not open MANIFEST file %q", manifestFilename)
		}
		defer f.Close()

		cmp := base.DefaultComparer
		var bve manifest.BulkVersionEdit
		bve.AllAddedTables = make(map[base.FileNum]*manifest.TableMetadata)
		rr := record.NewReader(f, 0 /* logNum */)
		for {
			r, err := rr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return errors.Wrapf(err, "pebble: reading manifest %q", manifestFilename)
			}
			var ve manifest.VersionEdit
			err = ve.Decode(r)
			if err != nil {
				return err
			}
			if err := bve.Accumulate(&ve); err != nil {
				return err
			}
			if ve.ComparerName != "" {
				cmp = d.comparers[ve.ComparerName]
				d.fmtKey.setForComparer(ve.ComparerName, d.comparers)
				d.fmtValue.setForComparer(ve.ComparerName, d.comparers)
			}
		}
		l0Organizer := manifest.NewL0Organizer(cmp, d.opts.FlushSplitBytes)
		emptyVersion := manifest.NewInitialVersion(cmp)
		v, err := bve.Apply(emptyVersion, d.opts.Experimental.ReadCompactionRate)
		if err != nil {
			return err
		}
		l0Organizer.PerformUpdate(l0Organizer.PrepareUpdate(&bve, v), v)

		objProvider, err := objstorageprovider.Open(objstorageprovider.DefaultSettings(d.opts.FS, dirname))
		if err != nil {
			return err
		}
		defer func() { _ = objProvider.Close() }()

		// Load and aggregate sstable properties.
		tw := tabwriter.NewWriter(stdout, 2, 1, 4, ' ', 0)
		var total props
		var all []props
		for _, l := range v.Levels {
			var level props
			for t := range l.All() {
				if t.Virtual {
					// TODO(bananabrick): Handle virtual sstables here. We don't
					// really have any stats or properties at this point. Maybe
					// we could approximate some of these properties for virtual
					// sstables by first grabbing properties for the backing
					// physical sstable, and then extrapolating.
					continue
				}
				err := d.addProps(objProvider, t.PhysicalMeta(), &level)
				if err != nil {
					return err
				}
			}
			all = append(all, level)
			total.update(level)
		}
		all = append(all, total)

		fmt.Fprintln(tw, "\tL0\tL1\tL2\tL3\tL4\tL5\tL6\tTOTAL")

		fmt.Fprintf(tw, "count\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n",
			propArgs(all, func(p *props) interface{} { return p.Count })...)

		fmt.Fprintln(tw, "seq num\t\t\t\t\t\t\t\t")
		fmt.Fprintf(tw, "  smallest\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n",
			propArgs(all, func(p *props) interface{} { return p.SmallestSeqNum })...)
		fmt.Fprintf(tw, "  largest\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n",
			propArgs(all, func(p *props) interface{} { return p.LargestSeqNum })...)

		fmt.Fprintln(tw, "size\t\t\t\t\t\t\t\t")
		fmt.Fprintf(tw, "  data\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			propArgs(all, func(p *props) interface{} { return humanize.Bytes.Uint64(p.DataSize) })...)
		fmt.Fprintf(tw, "    blocks\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n",
			propArgs(all, func(p *props) interface{} { return p.NumDataBlocks })...)
		fmt.Fprintf(tw, "  index\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			propArgs(all, func(p *props) interface{} { return humanize.Bytes.Uint64(p.IndexSize) })...)
		fmt.Fprintf(tw, "    blocks\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n",
			propArgs(all, func(p *props) interface{} { return p.NumIndexBlocks })...)
		fmt.Fprintf(tw, "    top-level\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			propArgs(all, func(p *props) interface{} { return humanize.Bytes.Uint64(p.TopLevelIndexSize) })...)
		fmt.Fprintf(tw, "  filter\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			propArgs(all, func(p *props) interface{} { return humanize.Bytes.Uint64(p.FilterSize) })...)
		fmt.Fprintf(tw, "  raw-key\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			propArgs(all, func(p *props) interface{} { return humanize.Bytes.Uint64(p.RawKeySize) })...)
		fmt.Fprintf(tw, "  raw-value\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			propArgs(all, func(p *props) interface{} { return humanize.Bytes.Uint64(p.RawValueSize) })...)
		fmt.Fprintf(tw, "  pinned-key\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			propArgs(all, func(p *props) interface{} { return humanize.Bytes.Uint64(p.SnapshotPinnedKeySize) })...)
		fmt.Fprintf(tw, "  pinned-value\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			propArgs(all, func(p *props) interface{} { return humanize.Bytes.Uint64(p.SnapshotPinnedValueSize) })...)
		fmt.Fprintf(tw, "  point-del-key-size\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			propArgs(all, func(p *props) interface{} { return humanize.Bytes.Uint64(p.RawPointTombstoneKeySize) })...)
		fmt.Fprintf(tw, "  point-del-value-size\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			propArgs(all, func(p *props) interface{} { return humanize.Bytes.Uint64(p.RawPointTombstoneValueSize) })...)

		fmt.Fprintln(tw, "records\t\t\t\t\t\t\t\t")
		fmt.Fprintf(tw, "  set\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			propArgs(all, func(p *props) interface{} {
				return humanize.Count.Uint64(p.NumEntries - p.NumDeletions - p.NumMergeOperands)
			})...)
		fmt.Fprintf(tw, "  delete\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			propArgs(all, func(p *props) interface{} { return humanize.Count.Uint64(p.NumDeletions - p.NumRangeDeletions) })...)
		fmt.Fprintf(tw, "  delete-sized\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			propArgs(all, func(p *props) interface{} { return humanize.Count.Uint64(p.NumSizedDeletions) })...)
		fmt.Fprintf(tw, "  range-delete\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			propArgs(all, func(p *props) interface{} { return humanize.Count.Uint64(p.NumRangeDeletions) })...)
		fmt.Fprintf(tw, "  range-key-sets\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			propArgs(all, func(p *props) interface{} { return humanize.Count.Uint64(p.NumRangeKeySets) })...)
		fmt.Fprintf(tw, "  range-key-unsets\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			propArgs(all, func(p *props) interface{} { return humanize.Count.Uint64(p.NumRangeKeyUnSets) })...)
		fmt.Fprintf(tw, "  range-key-deletes\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			propArgs(all, func(p *props) interface{} { return humanize.Count.Uint64(p.NumRangeKeyDeletes) })...)
		fmt.Fprintf(tw, "  merge\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			propArgs(all, func(p *props) interface{} { return humanize.Count.Uint64(p.NumMergeOperands) })...)
		fmt.Fprintf(tw, "  pinned\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			propArgs(all, func(p *props) interface{} { return humanize.Count.Uint64(p.SnapshotPinnedKeys) })...)

		if err := tw.Flush(); err != nil {
			return err
		}
		return nil
	}()
	if err != nil {
		fmt.Fprintln(stderr, err)
	}
}

func (d *dbT) runSet(cmd *cobra.Command, args []string) {
	stderr := cmd.ErrOrStderr()
	db, err := d.openDB(args[0], nonReadOnly{})
	if err != nil {
		fmt.Fprintf(stderr, "%s\n", err)
		return
	}
	defer d.closeDB(stderr, db)
	var k, v key
	if err := k.Set(args[1]); err != nil {
		fmt.Fprintf(stderr, "%s\n", err)
		return
	}
	if err := v.Set(args[2]); err != nil {
		fmt.Fprintf(stderr, "%s\n", err)
		return
	}

	if err := db.Set(k, v, nil); err != nil {
		fmt.Fprintf(stderr, "%s\n", err)
	}
}

func (d *dbT) promptForConfirmation(prompt string, stdin io.Reader, stdout, stderr io.Writer) bool {
	if d.bypassPrompt {
		return true
	}
	if _, err := fmt.Fprintf(stdout, "%s\n", prompt); err != nil {
		fmt.Fprintf(stderr, "Error: %v\n", err)
		return false
	}
	reader := bufio.NewReader(stdin)
	for {
		if _, err := fmt.Fprintf(stdout, "Continue? [Y/N] "); err != nil {
			fmt.Fprintf(stderr, "Error: %v\n", err)
			return false
		}
		answer, err := reader.ReadString('\n')
		if err != nil {
			fmt.Fprintf(stderr, "Error: %v\n", err)
			return false
		}
		answer = strings.ToLower(strings.TrimSpace(answer))
		if answer == "y" || answer == "yes" {
			return true
		}

		if answer == "n" || answer == "no" {
			_, _ = fmt.Fprintf(stderr, "Aborting\n")
			return false
		}
	}
}

type nonReadOnly struct{}

func (n nonReadOnly) Apply(dirname string, opts *pebble.Options) {
	opts.ReadOnly = false
	// Increase the L0 compaction threshold to reduce the likelihood of an
	// unintended compaction changing test output.
	opts.L0CompactionThreshold = 10
}

func propArgs(props []props, getProp func(*props) interface{}) []interface{} {
	args := make([]interface{}, 0, len(props))
	for _, p := range props {
		args = append(args, getProp(&p))
	}
	return args
}

type props struct {
	Count                      uint64
	SmallestSeqNum             base.SeqNum
	LargestSeqNum              base.SeqNum
	DataSize                   uint64
	FilterSize                 uint64
	IndexSize                  uint64
	NumDataBlocks              uint64
	NumIndexBlocks             uint64
	NumDeletions               uint64
	NumSizedDeletions          uint64
	NumEntries                 uint64
	NumMergeOperands           uint64
	NumRangeDeletions          uint64
	NumRangeKeySets            uint64
	NumRangeKeyUnSets          uint64
	NumRangeKeyDeletes         uint64
	RawKeySize                 uint64
	RawPointTombstoneKeySize   uint64
	RawPointTombstoneValueSize uint64
	RawValueSize               uint64
	SnapshotPinnedKeys         uint64
	SnapshotPinnedKeySize      uint64
	SnapshotPinnedValueSize    uint64
	TopLevelIndexSize          uint64
}

func (p *props) update(o props) {
	p.Count += o.Count
	if o.SmallestSeqNum != 0 && (o.SmallestSeqNum < p.SmallestSeqNum || p.SmallestSeqNum == 0) {
		p.SmallestSeqNum = o.SmallestSeqNum
	}
	if o.LargestSeqNum > p.LargestSeqNum {
		p.LargestSeqNum = o.LargestSeqNum
	}
	p.DataSize += o.DataSize
	p.FilterSize += o.FilterSize
	p.IndexSize += o.IndexSize
	p.NumDataBlocks += o.NumDataBlocks
	p.NumIndexBlocks += o.NumIndexBlocks
	p.NumDeletions += o.NumDeletions
	p.NumSizedDeletions += o.NumSizedDeletions
	p.NumEntries += o.NumEntries
	p.NumMergeOperands += o.NumMergeOperands
	p.NumRangeDeletions += o.NumRangeDeletions
	p.NumRangeKeySets += o.NumRangeKeySets
	p.NumRangeKeyUnSets += o.NumRangeKeyUnSets
	p.NumRangeKeyDeletes += o.NumRangeKeyDeletes
	p.RawKeySize += o.RawKeySize
	p.RawPointTombstoneKeySize += o.RawPointTombstoneKeySize
	p.RawPointTombstoneValueSize += o.RawPointTombstoneValueSize
	p.RawValueSize += o.RawValueSize
	p.SnapshotPinnedKeySize += o.SnapshotPinnedKeySize
	p.SnapshotPinnedValueSize += o.SnapshotPinnedValueSize
	p.SnapshotPinnedKeys += o.SnapshotPinnedKeys
	p.TopLevelIndexSize += o.TopLevelIndexSize
}

func (d *dbT) addProps(objProvider objstorage.Provider, m *manifest.TableMetadata, p *props) error {
	ctx := context.Background()
	f, err := objProvider.OpenForReading(ctx, base.FileTypeTable, m.TableBacking.DiskFileNum, objstorage.OpenOptions{})
	if err != nil {
		return err
	}
	opts := d.opts.MakeReaderOptions()
	opts.Mergers = d.mergers
	opts.Comparers = d.comparers
	opts.ReaderOptions.CacheOpts = sstableinternal.CacheOptions{
		FileNum: m.TableBacking.DiskFileNum,
	}
	r, err := sstable.NewReader(ctx, f, opts)
	if err != nil {
		return errors.CombineErrors(err, f.Close())
	}

	properties, err := r.ReadPropertiesBlock(context.TODO(), nil /* buffer pool */)
	if err != nil {
		return errors.CombineErrors(err, r.Close())
	}

	p.update(props{
		Count:                      1,
		SmallestSeqNum:             m.SmallestSeqNum,
		LargestSeqNum:              m.LargestSeqNum,
		DataSize:                   properties.DataSize,
		FilterSize:                 properties.FilterSize,
		IndexSize:                  properties.IndexSize,
		NumDataBlocks:              properties.NumDataBlocks,
		NumIndexBlocks:             1 + properties.IndexPartitions,
		NumDeletions:               properties.NumDeletions,
		NumSizedDeletions:          properties.NumSizedDeletions,
		NumEntries:                 properties.NumEntries,
		NumMergeOperands:           properties.NumMergeOperands,
		NumRangeDeletions:          properties.NumRangeDeletions,
		NumRangeKeySets:            properties.NumRangeKeySets,
		NumRangeKeyUnSets:          properties.NumRangeKeyUnsets,
		NumRangeKeyDeletes:         properties.NumRangeKeyDels,
		RawKeySize:                 properties.RawKeySize,
		RawPointTombstoneKeySize:   properties.RawPointTombstoneKeySize,
		RawPointTombstoneValueSize: properties.RawPointTombstoneValueSize,
		RawValueSize:               properties.RawValueSize,
		SnapshotPinnedKeySize:      properties.SnapshotPinnedKeySize,
		SnapshotPinnedValueSize:    properties.SnapshotPinnedValueSize,
		SnapshotPinnedKeys:         properties.SnapshotPinnedKeys,
		TopLevelIndexSize:          properties.TopLevelIndexSize,
	})
	return r.Close()
}

func makePlural(singular string, count int64) string {
	if count > 1 {
		return fmt.Sprintf("%ss", singular)
	}
	return singular
}
