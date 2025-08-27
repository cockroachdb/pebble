// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// metarunner is a utility which runs metamorphic.RunOnce or Compare. It is
// equivalent to executing `internal/metamorphic.TestMeta` with `--run-dir` or
// `--compare`. It is used for code coverage instrumentation.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble/v2/internal/metamorphic/metaflags"
	"github.com/cockroachdb/pebble/v2/metamorphic"
)

var runOnceFlags = metaflags.InitRunOnceFlags()
var _ = flag.String("test.run", "", `ignored; used for compatibility with TestMeta`)

func main() {
	flag.Parse()
	onceOpts := runOnceFlags.MakeRunOnceOptions()
	t := &mockT{}
	switch {
	case runOnceFlags.Compare != "":
		testRootDir, runSubdirs := runOnceFlags.ParseCompare()
		metamorphic.Compare(t, testRootDir, runOnceFlags.Seed, runSubdirs, onceOpts...)

	case runOnceFlags.RunDir != "":
		// The --run-dir flag is specified either in the child process (see
		// runOptions() below) or the user specified it manually in order to re-run
		// a test.
		metamorphic.RunOnce(t, runOnceFlags.RunDir, runOnceFlags.Seed,
			filepath.Join(runOnceFlags.RunDir, "history"), onceOpts...)

	default:
		t.Errorf("--compare or --run-dir must be used")
	}

	if t.Failed() {
		// Make sure we return an error code.
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
