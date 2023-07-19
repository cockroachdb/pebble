// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"flag"
	"math"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/internal/randvar"
	"github.com/cockroachdb/pebble/metamorphic"
)

// TODO(peter):
//
// Miscellaneous:
// - Add support for different comparers. In particular, allow reverse
//   comparers and a comparer which supports Comparer.Split (by splitting off
//   a variable length suffix).
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

// TestMeta generates a random set of operations to run, then runs the test
// with different options. See standardOptions() for the set of options that
// are always run, and randomOptions() for the randomly generated options. The
// number of operations to generate is determined by the `--ops` flag. If a
// failure occurs, the output is kept in `_meta/<test>`, though note that a
// subsequent invocation will overwrite that output. A test can be re-run by
// using the `--run-dir` flag. For example:
//
//	go test -v -run TestMeta --run-dir _meta/standard-017
//
// This will reuse the existing operations present in _meta/ops, rather than
// generating a new set.
//
// The generated operations and options are generated deterministically from a
// pseudorandom number generator seed. If a failure occurs, the seed is
// printed, and the full suite of tests may be re-run using the `--seed` flag:
//
//	go test -v -run TestMeta --seed 1594395154492165000
//
// This will generate a new `_meta/<test>` directory, with the same operations
// and options. This must be run on the same commit SHA as the original
// failure, otherwise changes to the metamorphic tests may cause the generated
// operations and options to differ.
func TestMeta(t *testing.T) {
	opts := []metamorphic.RunOption{
		metamorphic.Seed(*seed),
		metamorphic.OpCount(ops.Static),
	}
	onceOpts := []metamorphic.RunOnceOption{
		metamorphic.MaxThreads(*maxThreads),
	}
	if *keep {
		onceOpts = append(onceOpts, metamorphic.KeepData{})
	}
	if *failRE != "" {
		onceOpts = append(onceOpts, metamorphic.FailOnMatch{Regexp: regexp.MustCompile(*failRE)})
	}
	if *errorRate > 0 {
		onceOpts = append(onceOpts, metamorphic.InjectErrorsRate(*errorRate))
	}
	if *traceFile != "" {
		opts = append(opts, metamorphic.RuntimeTrace(*traceFile))
	}
	if *previousOps != "" {
		opts = append(opts, metamorphic.ExtendPreviousRun(*previousOps, *initialStatePath, *initialStateDesc))
	}
	// If the filesystem type was forced, all tests will use that value.
	switch *fs {
	case "", "rand", "default":
		// No-op. Use the generated value for the filesystem.
	case "disk":
		opts = append(opts, metamorphic.UseDisk)
	case "mem":
		opts = append(opts, metamorphic.UseInMemory)
	default:
		t.Fatalf("unknown forced filesystem type: %q", *fs)
	}

	if *compare != "" {
		runDirs := strings.Split(*compare, ",")
		metamorphic.Compare(t, *dir, *seed, runDirs, onceOpts...)
		return
	}

	if *runDir != "" {
		// The --run-dir flag is specified either in the child process (see
		// runOptions() below) or the user specified it manually in order to re-run
		// a test.
		metamorphic.RunOnce(t, *runDir, *seed, filepath.Join(*runDir, "history"), onceOpts...)
		return
	}
	opts = append(opts, metamorphic.RunOnceOptions(onceOpts...))
	metamorphic.RunAndCompare(t, *dir, opts...)
}
