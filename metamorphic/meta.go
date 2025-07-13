// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package metamorphic provides a testing framework for running randomized tests
// over multiple Pebble databases with varying configurations. Logically
// equivalent operations should result in equivalent output across all
// configurations.
package metamorphic

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/dsl"
	"github.com/cockroachdb/pebble/internal/randvar"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type runAndCompareOptions struct {
	seed              uint64
	ops               randvar.Static
	previousOpsPaths  []string
	initialStatePath  string
	initialStateDesc  string
	traceFile         string
	innerBinary       string
	mutateTestOptions []func(*TestOptions)
	customRuns        map[string]string
	numInstances      int
	runOnceOptions
}

// A RunOption configures the behavior of RunAndCompare.
type RunOption interface {
	apply(*runAndCompareOptions)
}

// Seed configures generation to use the provided seed. Seed may be used to
// deterministically reproduce the same run.
type Seed uint64

func (s Seed) apply(ro *runAndCompareOptions) { ro.seed = uint64(s) }

// ExtendPreviousRun configures RunAndCompare to use the output of a previous
// metamorphic test run to seed the this run. It's used in the crossversion
// metamorphic tests, in which a data directory is upgraded through multiple
// versions of Pebble, exercising upgrade code paths and cross-version
// compatibility.
//
// The opsPaths should be the filesystem paths for the ops files containing the
// runs that resulted in the initial state. It's used to inform operation
// generation to prefer using keys used in the previous run, which are therefore
// more likely to be "interesting."; it is also required in order to issue
// SingleDelete operations correctly.
//
// The initialStatePath argument should be the filesystem path to the directory
// containing the test state for the previous run of the metamorphic test left
// off (with the store in a "data" subdirectory).
//
// The initialStateDesc argument is presentational and should hold a
// human-readable description of the initial state.
func ExtendPreviousRun(opsPaths []string, initialStatePath, initialStateDesc string) RunOption {
	return closureOpt(func(ro *runAndCompareOptions) {
		ro.previousOpsPaths = opsPaths
		ro.initialStatePath = initialStatePath
		ro.initialStateDesc = initialStateDesc
	})
}

var (
	// UseDisk configures RunAndCompare to use the physical filesystem for all
	// generated runs.
	UseDisk = closureOpt(func(ro *runAndCompareOptions) {
		ro.mutateTestOptions = append(ro.mutateTestOptions, func(to *TestOptions) { to.useDisk = true })
	})
	// UseInMemory configures RunAndCompare to use an in-memory virtual
	// filesystem for all generated runs.
	UseInMemory = closureOpt(func(ro *runAndCompareOptions) {
		ro.mutateTestOptions = append(ro.mutateTestOptions, func(to *TestOptions) { to.useDisk = false })
	})
)

// OpCount configures the random variable for the number of operations to
// generate.
func OpCount(rv randvar.Static) RunOption {
	return closureOpt(func(ro *runAndCompareOptions) { ro.ops = rv })
}

// RuntimeTrace configures each test run to collect a runtime trace and output
// it with the provided filename.
func RuntimeTrace(name string) RunOption {
	return closureOpt(func(ro *runAndCompareOptions) { ro.traceFile = name })
}

// InnerBinary configures the binary that is called for each run. If not
// specified, this binary (os.Args[0]) is called.
func InnerBinary(path string) RunOption {
	return closureOpt(func(ro *runAndCompareOptions) { ro.innerBinary = path })
}

// ParseCustomTestOption adds support for parsing the provided CustomOption from
// OPTIONS files serialized by the metamorphic tests. This RunOption alone does
// not cause the metamorphic tests to run with any variant of the provided
// CustomOption set.
func ParseCustomTestOption(name string, parseFn func(value string) (CustomOption, bool)) RunOption {
	return closureOpt(func(ro *runAndCompareOptions) { ro.customOptionParsers[name] = parseFn })
}

// AddCustomRun adds an additional run of the metamorphic tests, using the
// provided OPTIONS file contents. The default options will be used, except
// those options that are overriden by the provided OPTIONS string.
func AddCustomRun(name string, serializedOptions string) RunOption {
	return closureOpt(func(ro *runAndCompareOptions) { ro.customRuns[name] = serializedOptions })
}

type closureOpt func(*runAndCompareOptions)

func (f closureOpt) apply(ro *runAndCompareOptions) { f(ro) }

func buildRunAndCompareOpts(rOpts []RunOption) runAndCompareOptions {
	runOpts := runAndCompareOptions{
		ops:        randvar.NewUniform(1000, 10000),
		customRuns: map[string]string{},
		runOnceOptions: runOnceOptions{
			customOptionParsers: map[string]func(string) (CustomOption, bool){},
		},
	}
	for _, o := range rOpts {
		o.apply(&runOpts)
	}
	if runOpts.seed == 0 {
		runOpts.seed = uint64(time.Now().UnixNano())
	}
	return runOpts
}

// RunAndCompare runs the metamorphic tests, using the provided root directory
// to hold test data.
func RunAndCompare(t *testing.T, rootDir string, rOpts ...RunOption) {
	runOpts := buildRunAndCompareOpts(rOpts)
	require.NoError(t, os.MkdirAll(rootDir, 0755))
	metaDir, err := os.MkdirTemp(rootDir, time.Now().Format("060102-150405.000"))
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(metaDir, 0755))
	defer func() {
		if !t.Failed() && !runOpts.keep {
			_ = os.RemoveAll(metaDir)
		}
	}()

	topLevelTestName := t.Name()
	for path.Dir(topLevelTestName) != "." {
		topLevelTestName = path.Dir(topLevelTestName)
	}

	rng := rand.New(rand.NewPCG(0, runOpts.seed))
	opCount := runOpts.ops.Uint64(rng)

	// Generate a new set of random ops, writing them to <dir>/ops. These will be
	// read by the child processes when performing a test run.
	km := newKeyManager(runOpts.numInstances, runOpts.keyFormat)
	cfg := presetConfigs[rng.IntN(len(presetConfigs))]
	if runOpts.numInstances > 1 {
		// The multi-instance variant does not support all operations yet.
		//
		// TODO(bilal): Address this and use the default configs.
		cfg = multiInstancePresetConfig
		cfg.numInstances = runOpts.numInstances
	}
	g := newGenerator(rng, cfg, km)
	for _, opsPath := range runOpts.previousOpsPaths {
		// During cross-version testing, we load keys from an `ops` file
		// produced by a metamorphic test run of an earlier Pebble version.
		// Seeding the keys ensure we generate interesting operations, including
		// ones with key shadowing, merging, etc.
		opsData, err := os.ReadFile(opsPath)
		require.NoError(t, err)
		ops, err := parse(opsData, parserOpts{})
		if err != nil {
			t.Fatalf("failed to parse previous ops file %q: %v", opsPath, err)
		}
		loadPrecedingKeys(ops, g.keyGenerator, km)
	}

	ops := g.generate(opCount)
	opsPath := filepath.Join(metaDir, "ops")
	formattedOps := formatOps(km.kf, ops)
	require.NoError(t, os.WriteFile(opsPath, []byte(formattedOps), 0644))

	// runOptions performs a particular test run with the specified options. The
	// options are written to <run-dir>/OPTIONS and a child process is created to
	// actually execute the test.
	runOptions := func(t *testing.T, opts *TestOptions) {
		if opts.Opts.Cache != nil {
			defer opts.Opts.Cache.Unref()
		}
		for _, fn := range runOpts.mutateTestOptions {
			fn(opts)
		}
		runDir := filepath.Join(metaDir, path.Base(t.Name()))
		require.NoError(t, os.MkdirAll(runDir, 0755))

		optionsPath := filepath.Join(runDir, "OPTIONS")
		optionsStr := optionsToString(opts)
		require.NoError(t, os.WriteFile(optionsPath, []byte(optionsStr), 0644))

		args := []string{
			"-keep=" + fmt.Sprint(runOpts.keep),
			"-run-dir=" + runDir,
			"-test.run=" + topLevelTestName + "$",
			"--op-timeout=" + runOpts.opTimeout.String(),
			"--key-format=" + runOpts.keyFormat.Name,
		}
		if runOpts.numInstances > 1 {
			args = append(args, "--num-instances="+strconv.Itoa(runOpts.numInstances))
		}
		if runOpts.traceFile != "" {
			args = append(args, "-test.trace="+filepath.Join(runDir, runOpts.traceFile))
		}

		binary := os.Args[0]
		if runOpts.innerBinary != "" {
			binary = runOpts.innerBinary
		}
		cmd := exec.Command(binary, args...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			historyPath := filepath.Join(runDir, "history")
			historyLines := strings.Split(readFile(historyPath), "\n")
			// Show only the last 30 lines of history.
			if maxLines := 30; len(historyLines) > maxLines {
				start := len(historyLines) - maxLines
				historyLines = append([]string{fmt.Sprintf("...[%d lines skipped]...", start)}, historyLines[start:]...)
			}

			t.Fatalf(`error running %v
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
%s
%s
To reduce:  go test ./internal/metamorphic -tags invariants -run '%s$' --run-dir %s --try-to-reduce -v`,
				cmd.String(),
				runOpts.seed,
				err,
				out,
				optionsStr,
				opsPath,
				historyPath,
				strings.Join(historyLines, "\n"),
				topLevelTestName, runDir,
			)
		}
	}

	// Construct the various OPTIONS to test with.
	names, options, err := buildOptions(rng, runOpts)
	if err != nil {
		t.Fatal(err)
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
				diff := lineByLineDiff(base, lines)
				if diff != "" {
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
To reduce:  go test ./internal/metamorphic -tags invariants -run '%s$' --compare "%s/{%s,%s}" --try-to-reduce -v
`,
						runOpts.seed,
						metaDir, names[0], names[i], diff,
						names[0], optionsStrA,
						names[i], optionsStrB,
						opsPath,
						topLevelTestName, metaDir, names[0], names[i])
					os.Exit(1)
				}
			})
		}
	})
}

func buildOptions(
	rng *rand.Rand, runOpts runAndCompareOptions,
) (names []string, options map[string]*TestOptions, err error) {
	options = map[string]*TestOptions{}

	// Create the standard options.
	for i, opts := range standardOptions(runOpts.keyFormat) {
		name := fmt.Sprintf("standard-%03d", i)
		names = append(names, name)
		options[name] = opts
	}

	// Create the custom option runs, if any.
	for name, customOptsStr := range runOpts.customRuns {
		options[name] = defaultTestOptions(runOpts.keyFormat)
		if err = parseOptions(options[name], customOptsStr, runOpts.customOptionParsers); err != nil {
			return nil, nil, errors.Wrapf(err, "custom opts %q: %s", name, err)
		}
	}
	// Sort the custom options names for determinism (they're currently in
	// random order from map iteration).
	sort.Strings(names[len(names)-len(runOpts.customRuns):])

	// Create random options. We make an arbitrary choice to run with as many
	// random options as we have standard options.
	nOpts := len(options)
	for i := 0; i < nOpts; i++ {
		name := fmt.Sprintf("random-%03d", i)
		names = append(names, name)
		opts := RandomOptions(rng, runOpts.keyFormat, RandomOptionsCfg{
			CustomOptionParsers: runOpts.customOptionParsers,
		})
		options[name] = opts
	}

	// If the user provided the path to an initial database state to use, update
	// all the options to pull from it.
	if runOpts.initialStatePath != "" {
		for _, o := range options {
			o.initialStatePath, err = filepath.Abs(runOpts.initialStatePath)
			if err != nil {
				return nil, nil, err
			}
			o.initialStateDesc = runOpts.initialStateDesc
		}
	}
	return names, options, nil
}

type runOnceOptions struct {
	// Note: when adding a new option, a new flag might need to be passed to the
	// "inner" execution of the test binary in RunAndCompare.

	keep       bool
	maxThreads int
	// opTimeout causes the test to fail if any one op takes longer than this.
	opTimeout           time.Duration
	errorRate           float64
	failRegexp          *regexp.Regexp
	numInstances        int
	keyFormat           KeyFormat
	initialStatePath    string
	customOptionParsers map[string]func(string) (CustomOption, bool)
}

// A RunOnceOption configures the behavior of a single run of the metamorphic
// tests.
type RunOnceOption interface {
	applyOnce(*runOnceOptions)
}

// KeepData keeps the database directory, even on successful runs. If the test
// used an in-memory filesystem, the in-memory filesystem will be persisted to
// the run directory.
type KeepData struct{}

func (KeepData) apply(ro *runAndCompareOptions) { ro.keep = true }
func (KeepData) applyOnce(ro *runOnceOptions)   { ro.keep = true }

// InjectErrorsRate configures the run to inject errors into read-only
// filesystem operations and retry injected errors.
type InjectErrorsRate float64

func (r InjectErrorsRate) apply(ro *runAndCompareOptions) { ro.errorRate = float64(r) }
func (r InjectErrorsRate) applyOnce(ro *runOnceOptions)   { ro.errorRate = float64(r) }

// MaxThreads sets an upper bound on the number of parallel execution threads
// during replay.
type MaxThreads int

func (m MaxThreads) apply(ro *runAndCompareOptions) { ro.maxThreads = int(m) }
func (m MaxThreads) applyOnce(ro *runOnceOptions)   { ro.maxThreads = int(m) }

// OpTimeout sets a timeout for each executed operation. A value of 0 means no
// timeout.
type OpTimeout time.Duration

func (t OpTimeout) apply(ro *runAndCompareOptions) { ro.opTimeout = time.Duration(t) }
func (t OpTimeout) applyOnce(ro *runOnceOptions)   { ro.opTimeout = time.Duration(t) }

// FailOnMatch configures the run to fail immediately if the history matches the
// provided regular expression.
type FailOnMatch struct {
	*regexp.Regexp
}

func (f FailOnMatch) apply(ro *runAndCompareOptions) { ro.failRegexp = f.Regexp }
func (f FailOnMatch) applyOnce(ro *runOnceOptions)   { ro.failRegexp = f.Regexp }

// MultiInstance configures the number of pebble instances to create.
type MultiInstance int

func (m MultiInstance) apply(ro *runAndCompareOptions) { ro.numInstances = int(m) }
func (m MultiInstance) applyOnce(ro *runOnceOptions)   { ro.numInstances = int(m) }

// RunOnceInitialStatePath is used to set an initial database state path for a
// single run.
type RunOnceInitialStatePath string

func (i RunOnceInitialStatePath) applyOnce(ro *runOnceOptions) { ro.initialStatePath = string(i) }

// RunOnce performs one run of the metamorphic tests. RunOnce expects the
// directory named by `runDir` to already exist and contain an `OPTIONS` file
// containing the test run's configuration. The history of the run is persisted
// to a file at the path `historyPath`.
//
// The `seed` parameter is not functional; it's used for context in logging.
func RunOnce(t TestingT, runDir string, seed uint64, historyPath string, rOpts ...RunOnceOption) {
	runOpts := runOnceOptions{
		customOptionParsers: map[string]func(string) (CustomOption, bool){},
	}
	for _, o := range rOpts {
		o.applyOnce(&runOpts)
	}

	opsPath := filepath.Join(filepath.Dir(filepath.Clean(runDir)), "ops")
	opsData, err := os.ReadFile(opsPath)
	require.NoError(t, err)

	optionsPath := filepath.Join(runDir, "OPTIONS")
	optionsData, err := os.ReadFile(optionsPath)
	require.NoError(t, err)

	// NB: It's important to use defaultTestOptions() here as the base into
	// which we parse the serialized options. It contains the relevant defaults,
	// like the appropriate block-property collectors.
	testOpts := defaultTestOptions(runOpts.keyFormat)
	opts := testOpts.Opts
	require.NoError(t, parseOptions(testOpts, string(optionsData), runOpts.customOptionParsers))

	if runOpts.initialStatePath != "" {
		testOpts.initialStatePath = runOpts.initialStatePath
	}

	ops, err := parse(opsData, parserOpts{
		parseFormattedUserKey:       testOpts.KeyFormat.ParseFormattedKey,
		parseFormattedUserKeySuffix: testOpts.KeyFormat.ParseFormattedKeySuffix,
	})
	require.NoError(t, err)

	// Set up the filesystem to use for the test. Note that by default we use an
	// in-memory FS.
	if testOpts.useDisk {
		opts.FS = vfs.Default
		require.NoError(t, os.RemoveAll(opts.FS.PathJoin(runDir, "data")))
	} else {
		opts.Cleaner = base.ArchiveCleaner{}
	}
	// Wrap the filesystem with a VFS that will inject random latency if the
	// test options require it. We cap the overlal injected latency to ten
	// seconds to avoid excessive test run times when paired with small target
	// file sizes, block sizes, etc.
	if testOpts.ioLatencyProbability > 0 {
		opts.FS = errorfs.Wrap(opts.FS, errorfs.RandomLatency(
			errorfs.Randomly(testOpts.ioLatencyProbability, testOpts.ioLatencySeed),
			testOpts.ioLatencyMean,
			testOpts.ioLatencySeed,
			10*time.Second,
		))
	}

	// Wrap the filesystem with one that will inject errors into read
	// operations with *errorRate probability.
	opts.FS = errorfs.Wrap(opts.FS, errorfs.ErrInjected.If(
		dsl.And(errorfs.Reads, errorfs.Randomly(runOpts.errorRate, int64(seed))),
	))

	// Bound testOpts.threads to runOpts.maxThreads.
	if runOpts.maxThreads < testOpts.Threads {
		testOpts.Threads = runOpts.maxThreads
	}

	dataDir := opts.FS.PathJoin(runDir, "data")
	// Set up the initial database state if configured to start from a non-empty
	// database. By default tests start from an empty database, but split
	// version testing may configure a previous metamorphic tests's database
	// state as the initial state.
	if testOpts.initialStatePath != "" {
		require.NoError(t, setupInitialState(dataDir, testOpts))
	}

	if testOpts.Opts.WALFailover != nil {
		if runOpts.numInstances > 1 {
			// TODO(bilal,jackson): Allow opts to diverge on a per-instance
			// basis, and use that to set unique WAL dirs for all instances in
			// multi-instance mode.
			testOpts.Opts.WALFailover = nil
		} else {
			testOpts.Opts.WALFailover.Secondary.FS = opts.FS
		}
	}

	if runOpts.numInstances > 1 {
		// TODO(bilal): Allow opts to diverge on a per-instance basis, and use
		// that to set unique WAL dirs for all instances in multi-instance mode.
		opts.WALDir = ""
	}

	historyFile, err := os.Create(historyPath)
	require.NoError(t, err)
	defer historyFile.Close()
	writers := []io.Writer{historyFile}

	if testing.Verbose() {
		writers = append(writers, os.Stdout)
	}
	h := newHistory(runOpts.failRegexp, writers...)
	defer h.Close()

	m := newTest(ops)
	require.NoError(t, m.init(h, dataDir, testOpts, runOpts.numInstances, runOpts.opTimeout))

	if err := Execute(m); err != nil {
		fmt.Fprintf(os.Stderr, "Seed: %d\n", seed)
		fmt.Fprintln(os.Stderr, err)
	}

	// If we have unclosed databases, print LSM details. This will be the case
	// when we use --try-to-reduce.
	for i, db := range m.dbs {
		if db != nil {
			fmt.Fprintf(os.Stderr, "\ndb%d:\n%s", i+1, db.DebugString())
			fmt.Fprintf(os.Stderr, "\n%s\n", db.LSMViewURL())
		}
	}
	// Don't let the test pass if it issued a Fatalf.
	err = errors.CombineErrors(err, h.Error())

	if err != nil {
		m.saveInMemoryData()
		os.Exit(1)
	}

	if runOpts.keep && !testOpts.useDisk {
		m.saveInMemoryData()
	}
}

// Execute runs the provided test, writing the execution history into the Test's
// sink.
//
// Note that the test execution might change the filesystems in the TestOptions
// used with New(); see the restart operation.
func Execute(m *Test) error {
	if m.testOpts.Threads <= 1 {
		for m.step(m.h, nil /* optionalRecordf */) {
			if err := m.h.Error(); err != nil {
				return err
			}
		}
		return nil
	}

	// Run in parallel using an errgroup.
	eg, ctx := errgroup.WithContext(context.Background())
	for t := 0; t < m.testOpts.Threads; t++ {
		t := t // bind loop var to scope
		eg.Go(func() error {
			for idx := 0; idx < len(m.ops); idx++ {
				// Skip any operations whose receiver object hashes to a
				// different thread. All operations with the same receiver
				// are performed from the same thread. This goroutine is
				// only responsible for executing operations that hash to
				// `t`.
				if hashThread(m.ops[idx].receiver(), m.testOpts.Threads) != t {
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

				m.runOp(idx, m.h.recorder(t, idx, nil /* optionalRecordf */))

				// If this operation has a done channel, close it so that
				// other operations that synchronize on this operation know
				// that it's been completed.
				if ch := m.opsDone[idx]; ch != nil {
					close(ch)
				}

				if err := m.h.Error(); err != nil {
					return err
				}
			}
			return nil
		})
	}
	return eg.Wait()
}

func hashThread(objID objID, numThreads int) int {
	// Fibonacci hash https://probablydance.com/2018/06/16/fibonacci-hashing-the-optimization-that-the-world-forgot-or-a-better-alternative-to-integer-modulo/
	return int((11400714819323198485 * uint64(objID)) % uint64(numThreads))
}

// Compare runs the metamorphic tests in the provided runDirs and compares their
// histories.
func Compare(t TestingT, rootDir string, seed uint64, runDirs []string, rOpts ...RunOnceOption) {
	historyPaths := make([]string, len(runDirs))
	for i := 0; i < len(runDirs); i++ {
		historyPath := filepath.Join(rootDir, runDirs[i]+"-"+time.Now().Format("060102-150405.000"))
		runDirs[i] = filepath.Join(rootDir, runDirs[i])
		_ = os.Remove(historyPath)
		historyPaths[i] = historyPath
	}
	defer func() {
		for _, path := range historyPaths {
			_ = os.Remove(path)
		}
	}()

	for i, runDir := range runDirs {
		RunOnce(t, runDir, seed, historyPaths[i], rOpts...)
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
`, rootDir, runDirs[0], runDirs[i], diff)
		os.Exit(1)
	}
}

// TestingT is an interface wrapper around *testing.T
type TestingT interface {
	require.TestingT
	Failed() bool
}

func readFile(path string) string {
	history, err := os.ReadFile(path)
	if err != nil {
		return fmt.Sprintf("err: %v", err)
	}

	return string(history)
}

// lineByLineDiff performs a line-by-line diff of two histories and returns the
// first chunk of differences.
//
// This is preferable to unified diffs (which groups differences into sections)
// because it's easier to see the difference in each particular operation.
//
// Returns "" if the two slices are equal (modulo any trailing empty lines).
func lineByLineDiff(a, b []string) string {
	// Make the two slices the same length.
	for len(a) < len(b) {
		a = append(a, "")
	}
	for len(b) < len(a) {
		b = append(b, "")
	}
	if slices.Equal(a, b) {
		return ""
	}
	firstDiff := 0
	for ; a[firstDiff] == b[firstDiff]; firstDiff++ {
	}
	start := max(0, firstDiff-8)
	end := min(firstDiff+8, len(a))
	var buf strings.Builder
	extraLine := false
	for i := start; i < end; i++ {
		if a[i] == b[i] {
			if extraLine {
				fmt.Fprintf(&buf, "\n")
				extraLine = false
			}
			fmt.Fprintf(&buf, "%s\n", strings.TrimSuffix(a[i], "\n"))
		} else {
			fmt.Fprintf(&buf, "\n-%s\n+%s\n", strings.TrimSuffix(a[i], "\n"), strings.TrimSuffix(b[i], "\n"))
			extraLine = true
		}
	}
	return buf.String()
}
