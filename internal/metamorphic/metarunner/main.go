// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// metarunner is a utility which runs metamorphic.RunOnce or Compare. It is
// equivalent to executing `internal/metamorphic.TestMeta` with `--run-dir` or
// `--compare`. It is used for code coverage instrumentation.
package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/cockroachdb/pebble/metamorphic"
)

var (
	dir = flag.String("dir", "_meta",
		"the directory storing test state")
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
	keep = flag.Bool("keep", false,
		"keep the DB directory even on successful runs")
	seed = flag.Uint64("seed", 0,
		"a pseudorandom number generator seed")
	maxThreads = flag.Int("max-threads", math.MaxInt,
		"limit execution of a single run to the provided number of threads; must be ≥ 1")
	runDir = flag.String("run-dir", "",
		"the specific configuration to (re-)run (used for post-mortem debugging)")
	compare = flag.String("compare", "",
		`comma separated list of options files to compare. The result of each run is compared with
the result of the run from the first options file in the list. Example, -compare
random-003,standard-000. The dir flag should have the directory containing these directories.
Example, -dir _meta/200610-203012.077`)
	_ = flag.String("test.run", "", `ignored; used for compatibility with TestMeta`)
)

func main() {
	flag.Parse()
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

	t := &mockT{}
	switch {
	case *compare != "":
		runDirs := strings.Split(*compare, ",")
		metamorphic.Compare(t, *dir, *seed, runDirs, onceOpts...)

	case *runDir != "":
		// The --run-dir flag is specified either in the child process (see
		// runOptions() below) or the user specified it manually in order to re-run
		// a test.
		metamorphic.RunOnce(t, *runDir, *seed, filepath.Join(*runDir, "history"), onceOpts...)

	default:
		t.Errorf("--compare or --run-dir must be used")
	}
	if t.Failed() {
		t.FailNow()
	}
}

type mockT struct {
	failed bool
}

var _ metamorphic.TestingT = (*mockT)(nil)

func (t *mockT) Errorf(format string, args ...interface{}) {
	t.failed = true
	fmt.Fprintf(os.Stderr, format+"\n", args...)
}

func (t *mockT) FailNow() {
	os.Exit(2)
}

func (t *mockT) Failed() bool {
	return t.failed
}
