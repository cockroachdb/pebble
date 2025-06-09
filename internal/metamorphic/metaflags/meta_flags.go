// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package metaflags defines command-line flags for the metamorphic tests and
// provides functionality to construct the respective
// metamorphic.RunOptions/RunOnceOptions.
package metaflags

import (
	"flag"
	"fmt"
	"math"
	"os"
	"regexp"
	"strings"
	"time"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/buildtags"
	"github.com/cockroachdb/pebble/internal/randvar"
	"github.com/cockroachdb/pebble/metamorphic"
)

// CommonFlags contains flags that apply to both metamorphic.Run and
// metamorphic.RunOnce/Compare.
type CommonFlags struct {
	// Dir is the directory storing test state. See "dir" flag below.
	Dir string
	// Seed for generation of random operations. See "seed" flag below.
	Seed uint64
	// ErrorRate is the rate of injected filesystem errors. See "error-rate" flag
	// below.
	ErrorRate float64
	// FailRE causes the test to fail if the output matches this regex. See "fail"
	// flag below.
	FailRE string
	// Keep determines if the DB directory is kept on successful runs. See "keep"
	// flag below.
	Keep bool
	// MaxThreads used by a single run. See "max-threads" flag below.
	MaxThreads int
	// NumInstances is the number of Pebble instances to create in one run. See
	// "num-instances" flag below.
	NumInstances int
	// OpTimeout is a per-operation timeout.
	OpTimeout time.Duration
	// KeyFormatName is the name of the KeyFormat to use. Defaults to "testkeys".
	// Acceptable values are "testkeys" and "cockroachkvs".
	KeyFormatName string
	// InitialStatePath is the path to a database data directory from a previous
	// run. See the "initial-state" flag below.
	InitialStatePath string
}

// KeyFormat returns the KeyFormat indicated by the flags KeyFormatName.
func (c *CommonFlags) KeyFormat() metamorphic.KeyFormat {
	keyFormat, ok := KeyFormats[c.KeyFormatName]
	if !ok {
		panic(fmt.Sprintf("unknown key format: %q", c.KeyFormatName))
	}
	return keyFormat
}

// KeyFormats is a map of available key formats.
var KeyFormats = map[string]metamorphic.KeyFormat{
	"testkeys":     metamorphic.TestkeysKeyFormat,
	"cockroachkvs": metamorphic.CockroachKeyFormat,
}

func initCommonFlags() *CommonFlags {
	c := &CommonFlags{}
	flag.StringVar(&c.Dir, "dir", "_meta",
		"the directory storing test state")

	flag.Uint64Var(&c.Seed, "seed", 0,
		"a pseudorandom number generator seed")

	// TODO: default error rate to a non-zero value. Currently, retrying is
	// non-deterministic because of the Ierator.*WithLimit() methods since
	// they may say that the Iterator is not valid, but be positioned at a
	// certain key that can be returned in the future if the limit is changed.
	// Since that key is hidden from clients of Iterator, the retryableIter
	// using SeekGE will not necessarily position the Iterator that saw an
	// injected error at the same place as an Iterator that did not see that
	// error.
	flag.Float64Var(&c.ErrorRate, "error-rate", 0.0,
		"rate of errors injected into filesystem operations (0 ≤ r < 1)")

	flag.StringVar(&c.FailRE, "fail", "",
		"fail the test if the supplied regular expression matches the output")

	flag.BoolVar(&c.Keep, "keep", false,
		"keep the DB directory even on successful runs")

	flag.IntVar(&c.MaxThreads, "max-threads", math.MaxInt,
		"limit execution of a single run to the provided number of threads; must be ≥ 1")

	flag.IntVar(&c.NumInstances, "num-instances", 1, "number of pebble instances to create (default: 1)")

	defaultOpTimeout := 2 * time.Minute
	if buildtags.SlowBuild {
		defaultOpTimeout *= 5
	}
	flag.DurationVar(&c.OpTimeout, "op-timeout", defaultOpTimeout, "per-op timeout")

	flag.StringVar(&c.KeyFormatName, "key-format", "testkeys",
		"name of the key format to use")

	flag.StringVar(&c.InitialStatePath, "initial-state", "",
		`path to a database's data directory, used to prepopulate the test run's databases.
		Must be used in conjunction with --previous-ops (unless --run or --compare is used).`)

	return c
}

// RunOnceFlags contains flags that apply only to metamorphic.RunOnce/Compare.
type RunOnceFlags struct {
	*CommonFlags
	// RunDir applies to metamorphic.RunOnce and contains the specific
	// configuration of the run. See "run-dir" flag below.
	RunDir string
	// Compare applies to metamorphic.Compare. See "compare" flag below.
	Compare string
	// TryToReduce enables a mode where we try to find a minimal subset of
	// operations that reproduce a problem during a test run (e.g. panic or
	// internal error).
	TryToReduce bool
	// ReduceAttempts is the number of attempts to reduce (for each op removal
	// probability).
	ReduceAttempts int
}

func initRunOnceFlags(c *CommonFlags) *RunOnceFlags {
	ro := &RunOnceFlags{CommonFlags: c}
	flag.StringVar(&ro.RunDir, "run-dir", "",
		`directory containing the specific configuration to (re-)run (used for post-mortem debugging).
Example: --run-dir _meta/231220-164251.3552792807512/random-003`)

	flag.StringVar(&ro.Compare, "compare", "",
		`runs to compare, in the format _meta/test-root-dir/{run1,run2,...}. The result
of each run is compared with the result of the first run.
Example, --compare '_meta/231220-164251.3552792807512/{standard-000,random-025}'`)

	flag.BoolVar(&ro.TryToReduce, "try-to-reduce", false,
		`if set, we will try to reduce the number of operations that cause a failure. The
verbose flag should be used with this flag.`)

	flag.IntVar(&ro.ReduceAttempts, "reduce-attempts", 100,
		`the number of attempts to reduce, for each probability; only used with --try-to-reduce.`)

	return ro
}

// RunFlags contains flags that apply only to metamorphic.Run.
type RunFlags struct {
	*CommonFlags
	// FS controls the type of filesystems to use. See "fs" flag below.
	FS string
	// TraceFile for execution tracing. See "trace-file" flag below.
	TraceFile string
	// Ops describes how the total number of operations is generated. See "ops" flags below.
	Ops randvar.Flag
	// InnerBinary is the binary to invoke for a single run. See "inner-binary"
	// flag below.
	InnerBinary string
	// PreviousOps is the path to the ops file of a previous run. See the
	// "previous-ops" flag below.
	PreviousOps string
	// InitialStateDesc is a human-readable description of the initial database
	// state. See "initial-state-desc" flag below.
	InitialStateDesc string
}

func initRunFlags(c *CommonFlags) *RunFlags {
	r := &RunFlags{CommonFlags: c}
	flag.StringVar(&r.FS, "fs", "rand",
		`force the tests to use either memory or disk-backed filesystems (valid: "mem", "disk", "rand")`)

	flag.StringVar(&r.TraceFile, "trace-file", "",
		"write an execution trace to `<run-dir>/file`")

	ops := "uniform:5000-10000"
	if buildtags.SlowBuild || unsafe.Sizeof(uintptr(0)) == 4 {
		// Reduce the size of the test in slow builds (to avoid timeouts) or 32-bit
		// builds (to avoid OOMs).
		ops = "uniform:1000-2000"
	}
	if err := r.Ops.Set(ops); err != nil {
		panic(err)
	}
	flag.Var(&r.Ops, "ops", "uniform:min-max")

	flag.StringVar(&r.InnerBinary, "inner-binary", "",
		`binary to run for each instance of the test (this same binary by default); cannot be used
with --run-dir or --compare`)

	// The following options may be used for split-version metamorphic testing.
	// To perform split-version testing, the client runs the metamorphic tests
	// on an earlier Pebble SHA passing the `--keep` flag. The client then
	// switches to the later Pebble SHA, setting the below options to point to
	// the `ops` file and one of the previous run's data directories.

	flag.StringVar(&r.PreviousOps, "previous-ops", "",
		`path to an ops file, used to prepopulate the set of keys operations draw from." +
		Must be used in conjunction with --initial-state`)

	flag.StringVar(&r.InitialStateDesc, "initial-state-desc", "",
		`a human-readable description of the initial database state.
		If set this parameter is written to the OPTIONS to aid in
		debugging. It's intended to describe the lineage of a
		database's state, including sufficient information for
		reproduction (eg, SHA, prng seed, etc).`)
	return r
}

// InitRunOnceFlags initializes the flags that are used for a single run of the
// metamorphic test.
func InitRunOnceFlags() *RunOnceFlags {
	return initRunOnceFlags(initCommonFlags())
}

// InitAllFlags initializes all metamorphic test flags: those used for a
// single run, and those used for a "top level" run.
func InitAllFlags() (*RunOnceFlags, *RunFlags) {
	c := initCommonFlags()
	return initRunOnceFlags(c), initRunFlags(c)
}

// MakeRunOnceOptions constructs RunOnceOptions based on the flags.
func (ro *RunOnceFlags) MakeRunOnceOptions() []metamorphic.RunOnceOption {
	onceOpts := []metamorphic.RunOnceOption{
		metamorphic.MaxThreads(ro.MaxThreads),
		metamorphic.OpTimeout(ro.OpTimeout),
		ro.KeyFormat(),
	}
	if ro.Keep {
		onceOpts = append(onceOpts, metamorphic.KeepData{})
	}
	if ro.FailRE != "" {
		onceOpts = append(onceOpts, metamorphic.FailOnMatch{Regexp: regexp.MustCompile(ro.FailRE)})
	}
	if ro.ErrorRate > 0 {
		onceOpts = append(onceOpts, metamorphic.InjectErrorsRate(ro.ErrorRate))
	}
	if ro.NumInstances > 1 {
		onceOpts = append(onceOpts, metamorphic.MultiInstance(ro.NumInstances))
	}
	if ro.InitialStatePath != "" {
		onceOpts = append(onceOpts, metamorphic.RunOnceInitialStatePath(ro.InitialStatePath))
	}
	return onceOpts
}

// ParseCompare parses the value of the compare flag, in format
// "test-root-dir/{run1,run2,...}". Exits if the value is not valid.
//
// Returns the common test root dir (e.g. "test-root-dir") and a list of
// subdirectories (e.g. {"run1", "run2"}).
func (ro *RunOnceFlags) ParseCompare() (testRootDir string, runSubdirs []string) {
	testRootDir, runSubdirs, ok := ro.tryParseCompare()
	if !ok {
		fmt.Fprintf(os.Stderr, `cannot parse compare flag value %q; format is "test-root-dir/{run1,run2,..}"`, ro.Compare)
		os.Exit(1)
	}
	return testRootDir, runSubdirs
}

func (ro *RunOnceFlags) tryParseCompare() (testRootDir string, runSubdirs []string, ok bool) {
	value := ro.Compare
	brace := strings.Index(value, "{")
	if brace == -1 || !strings.HasSuffix(value, "}") {
		return "", nil, false
	}

	testRootDir = value[:brace]
	runSubdirs = strings.Split(value[brace+1:len(value)-1], ",")
	if len(runSubdirs) < 2 {
		return "", nil, false
	}
	return testRootDir, runSubdirs, true
}

// MakeRunOptions constructs RunOptions based on the flags.
func (r *RunFlags) MakeRunOptions() ([]metamorphic.RunOption, error) {
	opts := []metamorphic.RunOption{
		metamorphic.Seed(r.Seed),
		metamorphic.OpCount(r.Ops.Static),
		metamorphic.MaxThreads(r.MaxThreads),
		metamorphic.OpTimeout(r.OpTimeout),
		r.KeyFormat(),
	}
	if r.Keep {
		opts = append(opts, metamorphic.KeepData{})
	}
	if r.FailRE != "" {
		opts = append(opts, metamorphic.FailOnMatch{Regexp: regexp.MustCompile(r.FailRE)})
	}
	if r.ErrorRate > 0 {
		opts = append(opts, metamorphic.InjectErrorsRate(r.ErrorRate))
	}
	if r.TraceFile != "" {
		opts = append(opts, metamorphic.RuntimeTrace(r.TraceFile))
	}
	if r.PreviousOps != "" {
		if r.InitialStatePath == "" {
			return nil, errors.Newf("--previous-ops requires --initial-state")
		}
		opts = append(opts, metamorphic.ExtendPreviousRun(r.PreviousOps, r.InitialStatePath, r.InitialStateDesc))
	} else if r.InitialStatePath != "" {
		return nil, errors.Newf("--initial-state requires --previous-ops")
	}
	if r.NumInstances > 1 {
		opts = append(opts, metamorphic.MultiInstance(r.NumInstances))
	}

	// If the filesystem type was forced, all tests will use that value.
	switch r.FS {
	case "", "rand", "default":
		// No-op. Use the generated value for the filesystem.
	case "disk":
		opts = append(opts, metamorphic.UseDisk)
	case "mem":
		opts = append(opts, metamorphic.UseInMemory)
	default:
		panic(fmt.Sprintf("unknown forced filesystem type: %q", r.FS))
	}
	if r.InnerBinary != "" {
		opts = append(opts, metamorphic.InnerBinary(r.InnerBinary))
	}
	return opts, nil
}
