// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/randvar"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/pmezard/go-difflib/difflib"
	"golang.org/x/exp/rand"
)

// TODO(peter):
//
// Verify the metamorphic test catchs various bugs:
// - Instability of keys returned from range-del-iters and used by truncate
// - Lack of support for lower/upper bound in flushableBatchIter
//
// Miscellaneous:
// - Add support for different comparers. In particular, allow reverse
//   comparers and a comparer which supports Comparer.Split (by splitting off
//   a variable length suffix).
// - Ingest and Apply can be randomly swapped leading to testing of
//   interesting cases.
// - DeleteRange can be used to replace Delete, stressing the DeleteRange
//   implementation.
// - Add support for Writer.LogData

var (
	// TODO(peter): Enable comparing of output by default. Currently disabled as
	// comparing output shows differences between runs that appear to be due to
	// bugs in Pebble.
	compare = flag.Bool("compare", false, "")
	dir     = flag.String("dir", "_meta", "")
	disk    = flag.Bool("disk", false, "")
	failRE  = flag.String("fail", "",
		"fail the test if the supplied regular expression matches the output")
	keep   = flag.Bool("keep", false, "")
	ops    = randvar.NewFlag("uniform:5000-10000")
	runDir = flag.String("run-dir", "", "")
)

func init() {
	flag.Var(ops, "ops", "")
}

func testMetaRun(t *testing.T, runDir string) {
	opsPath := filepath.Join(filepath.Dir(filepath.Clean(runDir)), "ops")
	opsData, err := ioutil.ReadFile(opsPath)
	if err != nil {
		t.Fatal(err)
	}
	ops, err := parse(opsData)
	if err != nil {
		t.Fatal(err)
	}
	_ = ops

	optionsPath := filepath.Join(runDir, "OPTIONS")
	optionsData, err := ioutil.ReadFile(optionsPath)
	if err != nil {
		t.Fatal(err)
	}
	opts := &pebble.Options{}
	testOpts := &testOptions{opts: opts}
	if err := parseOptions(testOpts, string(optionsData)); err != nil {
		t.Fatal(err)
	}

	// Always use our custom comparer which provides a Split method.
	opts.Comparer = &comparer
	// Use an archive cleaner to ease post-mortem debugging.
	opts.Cleaner = base.ArchiveCleaner{}

	// Set up the filesystem to use for the test. Note that by default we use an
	// in-memory FS.
	if *disk && !testOpts.strictFS {
		opts.FS = vfs.Default
		if err := os.RemoveAll(opts.FS.PathJoin(runDir, "data")); err != nil {
			t.Fatal(err)
		}
	} else {
		opts.Cleaner = base.ArchiveCleaner{}
		if testOpts.strictFS {
			opts.FS = vfs.NewStrictMem()
		} else {
			opts.FS = vfs.NewMem()
		}
	}
	if opts.WALDir != "" {
		opts.WALDir = opts.FS.PathJoin(runDir, opts.WALDir)
	}

	historyPath := filepath.Join(runDir, "history")
	historyFile, err := os.Create(historyPath)
	if err != nil {
		t.Fatal(err)
	}
	defer historyFile.Close()

	writers := []io.Writer{historyFile}
	if testing.Verbose() {
		writers = append(writers, os.Stdout)
	}
	h := newHistory(*failRE, writers...)

	m := newTest(ops)
	if err := m.init(h, opts.FS.PathJoin(runDir, "data"), testOpts); err != nil {
		t.Fatal(err)
	}
	for m.step(h) {
		if h.Failed() {
			if len(*failRE) > 0 {
				fmt.Fprintf(os.Stderr, "failure regex %q matched\n", *failRE)
			}
			m.maybeSaveData()
			os.Exit(1)
		}
	}

	if *keep && !*disk {
		m.maybeSaveData()
	}
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
func TestMeta(t *testing.T) {
	if *runDir != "" {
		// The --run-dir flag is specified either in the child process (see
		// runOptions() below) or the user specified it manually in order to re-run
		// a test.
		testMetaRun(t, *runDir)
		return
	}

	rootName := t.Name()

	// Cleanup any previous state.
	metaDir := filepath.Join(*dir, time.Now().Format("060102-150405.000"))
	if err := os.RemoveAll(metaDir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(metaDir, 0755); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if !t.Failed() && !*keep {
			_ = os.RemoveAll(metaDir)
		}
	}()

	// Generate a new set of random ops, writing them to <dir>/ops. These will be
	// read by the child processes when performing a test run.
	ops := generate(ops.Uint64(), defaultConfig)
	opsPath := filepath.Join(metaDir, "ops")
	if err := ioutil.WriteFile(opsPath, []byte(formatOps(ops)), 0644); err != nil {
		t.Fatal(err)
	}

	// Perform a particular test run with the specified options. The options are
	// written to <run-dir>/OPTIONS and a child process is created to actually
	// execute the test.
	runOptions := func(t *testing.T, opts *testOptions) {
		runDir := filepath.Join(metaDir, path.Base(t.Name()))
		if err := os.MkdirAll(runDir, 0755); err != nil {
			t.Fatal(err)
		}

		optionsPath := filepath.Join(runDir, "OPTIONS")
		str := optionsToString(opts)
		if err := ioutil.WriteFile(optionsPath, []byte(str), 0644); err != nil {
			t.Fatal(err)
		}

		cmd := exec.Command(os.Args[0],
			"-disk="+fmt.Sprint(*disk),
			"-keep="+fmt.Sprint(*keep),
			"-run-dir="+runDir,
			"-test.run="+rootName+"$")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%v\n%s\n\n%s", err, filepath.Join(runDir, "history"), out)
		}
	}

	// Perform runs with the standard options.
	var names []string
	for i, opts := range standardOptions() {
		name := fmt.Sprintf("standard-%03d", i)
		names = append(names, name)
		t.Run(name, func(t *testing.T) {
			runOptions(t, opts)
		})
	}

	// Perform runs with random options.
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	for i := 0; i < 20; i++ {
		name := fmt.Sprintf("random-%03d", i)
		names = append(names, name)
		t.Run(name, func(t *testing.T) {
			runOptions(t, randomOptions(rng))
		})
	}

	// Don't bother comparing output if we've already failed.
	if t.Failed() {
		return
	}

	if *compare {
		// Read a history file, stripping out lines that begin with a comment.
		readHistory := func(name string) []string {
			historyPath := filepath.Join(metaDir, name, "history")
			data, err := ioutil.ReadFile(historyPath)
			if err != nil {
				t.Fatal(err)
			}
			lines := difflib.SplitLines(string(data))
			newLines := make([]string, 0, len(lines))
			for _, line := range lines {
				if strings.HasPrefix(line, "// ") {
					continue
				}
				newLines = append(newLines, line)
			}
			return newLines
		}

		base := readHistory(names[0])
		for i := 1; i < len(names); i++ {
			lines := readHistory(names[i])
			diff := difflib.UnifiedDiff{
				A:       base,
				B:       lines,
				Context: 5,
			}
			text, err := difflib.GetUnifiedDiffString(diff)
			if err != nil {
				t.Fatal(err)
			}
			if text != "" {
				// NB: We force an exit rather than using t.Fatal because the later
				// will run another instance of the test if -count is specified, while
				// we're happy to exit on the first failure.
				fmt.Printf("diff %s/{%s,%s}\n%s\n", metaDir, names[0], names[i], text)
				os.Exit(1)
			}
		}
	}
}
