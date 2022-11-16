// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/errorfs"
	"github.com/cockroachdb/pebble/internal/randvar"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/pmezard/go-difflib/difflib"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
	"golang.org/x/sync/errgroup"
)

// TODO(peter):
//
// Miscellaneous:
// - Add support for different comparers. In particular, allow reverse
//   comparers and a comparer which supports Comparer.Split (by splitting off
//   a variable length suffix).
// - DeleteRange can be used to replace Delete, stressing the DeleteRange
//   implementation.
// - Add support for Writer.LogData

var (
	dir = flag.String("dir", "_meta",
		"the directory storing test state")
	fs = flag.String("fs", "rand",
		`force the tests to use either memory or disk-backed filesystems (valid: "mem", "disk", "rand")`)
	// TODO: default error rate to a non-zero value. Currently, retrying is
	// non-deterministic because of the Ierator.*WithLimit() methods since
	// they may say that the Iterator is not valid, but be positioned at a
	// certain key that can be returned in the future if the limit is changed.
	// Since that key is hidden from clients of Iterator, the retryableIter
	// using SeekGE will not necessarily position the Iterator that saw an
	// injected error at the same place as an Iterator that did not see that
	// error.
	errorRate = flag.Float64("error-rate", 0.0,
		"rate of errors injected into filesystem operations (0 ≤ r < 1)")
	failRE = flag.String("fail", "",
		"fail the test if the supplied regular expression matches the output")
	traceFile = flag.String("trace-file", "",
		"write an execution trace to `<run-dir>/file`")
	keep = flag.Bool("keep", false,
		"keep the DB directory even on successful runs")
	seed = flag.Uint64("seed", 0,
		"a pseudorandom number generator seed")
	ops        = randvar.NewFlag("uniform:5000-10000")
	maxThreads = flag.Int("max-threads", math.MaxInt,
		"limit execution of a single run to the provided number of threads; must be ≥ 1")
	runDir = flag.String("run-dir", "",
		"the specific configuration to (re-)run (used for post-mortem debugging)")
	compare = flag.String("compare", "",
		`comma separated list of options files to compare. The result of each run is compared with
the result of the run from the first options file in the list. Example, -compare
random-003,standard-000. The dir flag should have the directory containing these directories.
Example, -dir _meta/200610-203012.077`)

	// The following options may be used for split-version metamorphic testing.
	// To perform split-version testing, the client runs the metamorphic tests
	// on an earlier Pebble SHA passing the `--keep` flag. The client then
	// switches to the later Pebble SHA, setting the below options to point to
	// the `ops` file and one of the previous run's data directories.
	previousOps = flag.String("previous-ops", "",
		"path to an ops file, used to prepopulate the set of keys operations draw from")
	initialStatePath = flag.String("initial-state", "",
		"path to a database's data directory, used to prepopulate the test run's databases")
	initialStateDesc = flag.String("initial-state-desc", "",
		`a human-readable description of the initial database state.
		If set this parameter is written to the OPTIONS to aid in
		debugging. It's intended to describe the lineage of a
		database's state, including sufficient information for
		reproduction (eg, SHA, prng seed, etc).`)
)

func init() {
	flag.Var(ops, "ops", "")
}

func testCompareRun(t *testing.T, compare string) {
	runDirs := strings.Split(compare, ",")
	historyPaths := make([]string, len(runDirs))
	for i := 0; i < len(runDirs); i++ {
		historyPath := filepath.Join(*dir, runDirs[i]+"-"+time.Now().Format("060102-150405.000"))
		runDirs[i] = filepath.Join(*dir, runDirs[i])
		_ = os.Remove(historyPath)
		historyPaths[i] = historyPath
	}
	defer func() {
		for _, path := range historyPaths {
			_ = os.Remove(path)
		}
	}()

	for i, runDir := range runDirs {
		testMetaRun(t, runDir, *seed, historyPaths[i])
	}

	if t.Failed() {
		return
	}

	i, diff := CompareHistories(t, historyPaths)
	if i != 0 {
		fmt.Printf(`
===== DIFF =====
%s/{%s,%s}
%s
`, *dir, runDirs[0], runDirs[i], diff)
		os.Exit(1)
	}
}

func testMetaRun(t *testing.T, runDir string, seed uint64, historyPath string) {
	opsPath := filepath.Join(filepath.Dir(filepath.Clean(runDir)), "ops")
	opsData, err := os.ReadFile(opsPath)
	require.NoError(t, err)

	ops, err := parse(opsData)
	require.NoError(t, err)
	_ = ops

	optionsPath := filepath.Join(runDir, "OPTIONS")
	optionsData, err := os.ReadFile(optionsPath)
	require.NoError(t, err)

	opts := &pebble.Options{}
	testOpts := &testOptions{opts: opts}
	require.NoError(t, parseOptions(testOpts, string(optionsData)))

	// Always use our custom comparer which provides a Split method, splitting
	// keys at the trailing '@'.
	opts.Comparer = testkeys.Comparer
	// Use an archive cleaner to ease post-mortem debugging.
	opts.Cleaner = base.ArchiveCleaner{}

	// Set up the filesystem to use for the test. Note that by default we use an
	// in-memory FS.
	if testOpts.useDisk {
		opts.FS = vfs.Default
		require.NoError(t, os.RemoveAll(opts.FS.PathJoin(runDir, "data")))
	} else {
		opts.Cleaner = base.ArchiveCleaner{}
		if testOpts.strictFS {
			opts.FS = vfs.NewStrictMem()
		} else {
			opts.FS = vfs.NewMem()
		}
	}
	threads := testOpts.threads
	if *maxThreads < threads {
		threads = *maxThreads
	}

	dir := opts.FS.PathJoin(runDir, "data")
	// Set up the initial database state if configured to start from a non-empty
	// database. By default tests start from an empty database, but split
	// version testing may configure a previous metamorphic tests's database
	// state as the initial state.
	if testOpts.initialStatePath != "" {
		require.NoError(t, setupInitialState(dir, testOpts))
	}

	// Wrap the filesystem with one that will inject errors into read
	// operations with *errorRate probability.
	opts.FS = errorfs.Wrap(opts.FS, errorfs.WithProbability(errorfs.OpKindRead, *errorRate))

	if opts.WALDir != "" {
		opts.WALDir = opts.FS.PathJoin(runDir, opts.WALDir)
	}

	historyFile, err := os.Create(historyPath)
	require.NoError(t, err)
	defer historyFile.Close()
	writers := []io.Writer{historyFile}

	if testing.Verbose() {
		writers = append(writers, os.Stdout)
	}
	h := newHistory(*failRE, writers...)

	m := newTest(ops)
	require.NoError(t, m.init(h, dir, testOpts))

	if threads <= 1 {
		for m.step(h) {
			if err := h.Error(); err != nil {
				fmt.Fprintf(os.Stderr, "Seed: %d\n", seed)
				fmt.Fprintln(os.Stderr, err)
				m.maybeSaveData()
				os.Exit(1)
			}
		}
	} else {
		eg, ctx := errgroup.WithContext(context.Background())
		for t := 0; t < threads; t++ {
			t := t // bind loop var to scope
			eg.Go(func() error {
				for idx := 0; idx < len(m.ops); idx++ {
					// Skip any operations whose receiver object hashes to a
					// different thread. All operations with the same receiver
					// are performed from the same thread. This goroutine is
					// only responsible for executing operations that hash to
					// `t`.
					if hashThread(m.ops[idx].receiver(), threads) != t {
						continue
					}

					// Some operations have additional synchronization
					// dependencies. If this operation has any, wait for its
					// dependencies to complete before executing.
					for _, waitOnIdx := range m.opsWaitOn[idx] {
						select {
						case <-ctx.Done():
							// Exit if some other thread already errored out.
							return ctx.Err()
						case <-m.opsDone[waitOnIdx]:
						}
					}

					m.ops[idx].run(m, h.recorder(t, idx))

					// If this operation has a done channel, close it so that
					// other operations that synchronize on this operation know
					// that it's been completed.
					if ch := m.opsDone[idx]; ch != nil {
						close(ch)
					}

					if err := h.Error(); err != nil {
						return err
					}
				}
				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			fmt.Fprintf(os.Stderr, "Seed: %d\n", seed)
			fmt.Fprintln(os.Stderr, err)
			m.maybeSaveData()
			os.Exit(1)
		}
	}

	if *keep && !testOpts.useDisk {
		m.maybeSaveData()
	}
}

func hashThread(objID objID, numThreads int) int {
	// Fibonacci hash https://probablydance.com/2018/06/16/fibonacci-hashing-the-optimization-that-the-world-forgot-or-a-better-alternative-to-integer-modulo/
	return int((11400714819323198485 * uint64(objID)) % uint64(numThreads))
}

// TestMeta generates a random set of operations to run, then runs the test
// with different options. See standardOptions() for the set of options that
// are always run, and randomOptions() for the randomly generated options. The
// number of operations to generate is determined by the `--ops` flag. If a
// failure occurs, the output is kept in `_meta/<test>`, though note that a
// subsequent invocation will overwrite that output. A test can be re-run by
// using the `--run-dir` flag. For example:
//
//   go test -v -run TestMeta --run-dir _meta/standard-017
//
// This will reuse the existing operations present in _meta/ops, rather than
// generating a new set.
//
// The generated operations and options are generated deterministically from a
// pseudorandom number generator seed. If a failure occurs, the seed is
// printed, and the full suite of tests may be re-run using the `--seed` flag:
//
//   go test -v -run TestMeta --seed 1594395154492165000
//
// This will generate a new `_meta/<test>` directory, with the same operations
// and options. This must be run on the same commit SHA as the original
// failure, otherwise changes to the metamorphic tests may cause the generated
// operations and options to differ.
func TestMeta(t *testing.T) {
	if *compare != "" {
		testCompareRun(t, *compare)
		return
	}

	if *runDir != "" {
		// The --run-dir flag is specified either in the child process (see
		// runOptions() below) or the user specified it manually in order to re-run
		// a test.
		testMetaRun(t, *runDir, *seed, filepath.Join(*runDir, "history"))
		return
	}

	// Setting the default seed here rather than in the flag's default value
	// ensures each run uses a new seed when using the Go test `-count` flag.
	seed := *seed
	if seed == 0 {
		seed = uint64(time.Now().UnixNano())
	}

	// Create a directory for test state.
	require.NoError(t, os.MkdirAll(*dir, 0755))
	metaDir, err := os.MkdirTemp(*dir, time.Now().Format("060102-150405.000"))
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(metaDir, 0755))
	defer func() {
		if !t.Failed() && !*keep {
			_ = os.RemoveAll(metaDir)
		}
	}()

	rng := rand.New(rand.NewSource(seed))
	opCount := ops.Uint64(rng)

	// Generate a new set of random ops, writing them to <dir>/ops. These will be
	// read by the child processes when performing a test run.
	km := newKeyManager()
	cfg := defaultConfig()
	if *previousOps != "" {
		// During split-version testing, we load keys from an `ops` file
		// produced by a metamorphic test run of an earlier Pebble version.
		// Seeding the keys ensure we generate interesting operations, including
		// ones with key shadowing, merging, etc.
		opsPath := filepath.Join(filepath.Dir(filepath.Clean(*previousOps)), "ops")
		opsData, err := os.ReadFile(opsPath)
		require.NoError(t, err)
		ops, err := parse(opsData)
		require.NoError(t, err)
		loadPrecedingKeys(t, ops, &cfg, km)
	}
	ops := generate(rng, opCount, cfg, km)
	opsPath := filepath.Join(metaDir, "ops")
	formattedOps := formatOps(ops)
	require.NoError(t, os.WriteFile(opsPath, []byte(formattedOps), 0644))

	// runOptions performs a particular test run with the specified options. The
	// options are written to <run-dir>/OPTIONS and a child process is created to
	// actually execute the test.
	runOptions := func(t *testing.T, opts *testOptions) {
		if opts.opts.Cache != nil {
			defer opts.opts.Cache.Unref()
		}

		runDir := filepath.Join(metaDir, path.Base(t.Name()))
		require.NoError(t, os.MkdirAll(runDir, 0755))

		// If the filesystem type was forced, all tests will use that value.
		switch *fs {
		case "rand":
			// No-op. Use the generated value for the filesystem.
		case "disk":
			opts.useDisk = true
		case "mem":
			opts.useDisk = false
		default:
			t.Fatalf("unknown forced filesystem type: %q", *fs)
		}

		optionsPath := filepath.Join(runDir, "OPTIONS")
		optionsStr := optionsToString(opts)
		require.NoError(t, os.WriteFile(optionsPath, []byte(optionsStr), 0644))

		args := []string{
			"-keep=" + fmt.Sprint(*keep),
			"-run-dir=" + runDir,
			"-test.run=" + t.Name() + "$",
		}
		if *traceFile != "" {
			args = append(args, "-test.trace="+filepath.Join(runDir, *traceFile))
		}

		cmd := exec.Command(os.Args[0], args...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf(`
===== SEED =====
%d
===== ERR =====
%v
===== OUT =====
%s
===== OPTIONS =====
%s
===== OPS =====
%s
===== HISTORY =====
%s`, seed, err, out, optionsStr, formattedOps, readFile(filepath.Join(runDir, "history")))
		}
	}

	// Create the standard options.
	var names []string
	options := map[string]*testOptions{}
	for i, opts := range standardOptions() {
		name := fmt.Sprintf("standard-%03d", i)
		names = append(names, name)
		options[name] = opts
	}

	// Create random options. We make an arbitrary choice to run with as many
	// random options as we have standard options.
	nOpts := len(options)
	for i := 0; i < nOpts; i++ {
		name := fmt.Sprintf("random-%03d", i)
		names = append(names, name)
		opts := randomOptions(rng)
		options[name] = opts
	}

	// If the user provided the path to an initial database state to use, update
	// all the options to pull from it.
	if *initialStatePath != "" {
		for _, o := range options {
			var err error
			o.initialStatePath, err = filepath.Abs(*initialStatePath)
			require.NoError(t, err)
			o.initialStateDesc = *initialStateDesc
		}
	}

	// Run the options.
	t.Run("execution", func(t *testing.T) {
		for _, name := range names {
			name := name
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				runOptions(t, options[name])
			})
		}
	})
	// NB: The above 'execution' subtest will not complete until all of the
	// individual execution/ subtests have completed. The grouping within the
	// `execution` subtest ensures all the histories are available when we
	// proceed to comparing against the base history.

	// Don't bother comparing output if we've already failed.
	if t.Failed() {
		return
	}

	t.Run("compare", func(t *testing.T) {
		getHistoryPath := func(name string) string {
			return filepath.Join(metaDir, name, "history")
		}

		base := readHistory(t, getHistoryPath(names[0]))
		base = reorderHistory(base)
		for i := 1; i < len(names); i++ {
			t.Run(names[i], func(t *testing.T) {
				lines := readHistory(t, getHistoryPath(names[i]))
				lines = reorderHistory(lines)
				diff := difflib.UnifiedDiff{
					A:       base,
					B:       lines,
					Context: 5,
				}
				text, err := difflib.GetUnifiedDiffString(diff)
				require.NoError(t, err)
				if text != "" {
					// NB: We force an exit rather than using t.Fatal because the latter
					// will run another instance of the test if -count is specified, while
					// we're happy to exit on the first failure.
					optionsStrA := optionsToString(options[names[0]])
					optionsStrB := optionsToString(options[names[i]])

					fmt.Printf(`
		===== SEED =====
		%d
		===== DIFF =====
		%s/{%s,%s}
		%s
		===== OPTIONS %s =====
		%s
		===== OPTIONS %s =====
		%s
		===== OPS =====
		%s
		`, seed, metaDir, names[0], names[i], text, names[0], optionsStrA, names[i], optionsStrB, formattedOps)
					os.Exit(1)
				}
			})
		}
	})
}

func readFile(path string) string {
	history, err := os.ReadFile(path)
	if err != nil {
		return fmt.Sprintf("err: %v", err)
	}

	return string(history)
}
