// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package crossversion builds on the metamorphic testing implemented in
// internal/metamorphic, performing metamorphic testing across versions of
// Pebble. This improves test coverage of upgrade and migration code paths.
package crossversion

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/metamorphic"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

var (
	factor       int
	seed         int64
	versions     pebbleVersions
	artifactsDir string
	streamOutput bool
)

func init() {
	// NB: If you add new command-line flags, you should update the
	// reproductionCommand function.
	flag.Int64Var(&seed, "seed", 0,
		`a pseudorandom number generator seed`)
	flag.IntVar(&factor, "factor", 10,
		`the number of data directories to carry forward
from one version's run to the subsequent version's runs.`)
	flag.Var(&versions, "version",
		`a comma-separated 3-tuple defining a Pebble version to test.
The expected format is <label>,<SHA>,<test-binary-path>.
The label should be a human-readable label describing the
version, for example, 'CRDB-22.1'. The SHA indicates the
exact commit sha of the version, and may be abbreviated.
The test binary path must point to a test binary of the
internal/metamorphic package built on the indicated SHA.
A test binary may be built with 'go test -c'.

This flag should be provided multiple times to indicate
the set of versions to test. The order of the versions
is significant and database states generated from earlier
versions will be used to initialize runs of subsequent
versions.`)
	flag.StringVar(&artifactsDir, "artifacts", "",
		`the path to a directory where test artifacts should be
moved on failure. Defaults to the current working directory.`)
	flag.BoolVar(&streamOutput, "stream-output", false,
		`stream TestMeta output to standard output`)
}

func reproductionCommand() string {
	return fmt.Sprintf(
		"SEED=%d FACTOR=%d ./scripts/run-crossversion-meta.sh %s\n",
		seed, factor, versions.String(),
	)
}

// TestMetaCrossVersion performs cross-version metamorphic testing.
//
// It runs tests against the internal/metamorphic test binaries specified with
// multiple instances of the -version flag, exercising upgrade and migration
// code paths.
//
// More specifically, assume we are passed the following versions:
//
//	--version 22.2,<sha>,meta-22-2.test --version 23.1,<sha>,meta-23-1.test
//
// TestMetaCrossVersion will:
//   - run TestMeta on meta-22-2.test;
//   - retain a random subset of the resulting directories (each directory is a
//     store after a sequence of operations);
//   - run TestMeta on meta-23.1.test once for every retained directory from the
//     previous version (using it as initial state).
func TestMetaCrossVersion(t *testing.T) {
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	tempDir := t.TempDir()
	t.Logf("Test directory: %s\n", tempDir)
	t.Logf("Reproduction:\n  %s\n", reproductionCommand())

	// Print all the versions supplied and ensure all the test binaries
	// actually exist before proceeding.
	for i, v := range versions {
		if len(v.SHA) > 8 {
			// Use shortened SHAs for readability.
			versions[i].SHA = versions[i].SHA[:8]
		}
		absPath, err := filepath.Abs(v.TestBinaryPath)
		if err != nil {
			t.Fatal(err)
		}
		fi, err := os.Stat(absPath)
		if err != nil {
			t.Fatal(err)
		}
		versions[i].TestBinaryPath = absPath
		t.Logf("%d: %s (Mode = %s)", i, v.String(), fi.Mode())
	}

	// All randomness should be derived from `seed`. This makes reproducing a
	// failure locally easier.
	ctx := context.Background()
	require.NoError(t, runCrossVersion(ctx, t, tempDir, versions, seed, factor))
}

type pebbleVersion struct {
	Label          string
	SHA            string
	TestBinaryPath string
}

func runCrossVersion(
	ctx context.Context,
	t *testing.T,
	tempDir string,
	versions pebbleVersions,
	seed int64,
	factor int,
) error {
	prng := rand.New(rand.NewPCG(0, uint64(seed)))
	// Use prng to derive deterministic seeds to provide to the child
	// metamorphic runs. The same seed is used for all runs on a particular
	// Pebble version.
	versionSeeds := make([]uint64, len(versions))
	for i := range versions {
		versionSeeds[i] = prng.Uint64()
	}

	rootDir := filepath.Join(tempDir, fmt.Sprint(seed))
	if err := os.MkdirAll(rootDir, os.ModePerm); err != nil {
		return err
	}

	// When run with test parallelism, multiple tests may fail concurrently.
	// Only one should actually run the test failure logic which copies the root
	// dir into the artifacts directory.
	var fatalOnce sync.Once

	// The outer for loop executes once per version being tested. It takes a
	// list of initial states, populated by the previous version. The inner loop
	// executes once per initial state, running the metamorphic test against the
	// initial state.
	//
	// The number of states that are carried forward from one version to the
	// next is fixed by `factor`.
	initialStates := []initialState{{}}
	for i := range versions {
		subrunResults := runVersion(ctx, t, &fatalOnce, rootDir, versions[i], versionSeeds[i], initialStates)

		// All the initial states described the same state and all of this
		// version's metamorphic runs used the same seed, so all of the
		// resulting histories should be identical.
		histories := subrunResults.historyPaths()
		if len(histories) == 0 {
			t.Fatal("no subrun histories")
		}
		if h, diff := metamorphic.CompareHistories(t, histories); h > 0 {
			var dirs dirsToSave
			dirs.add(rootDir, fmt.Sprint(seed))
			fatalf(t, &fatalOnce, dirs,
				"Divergence when using different initial states.\n"+
					"  initial state 1: %s (%s)\n"+
					"  initial state 2: %s (%s)\n"+
					"Diff:\n"+
					"%s",
				subrunResults[0].initialState.desc,
				subrunResults[0].initialState.path,
				subrunResults[h].initialState.desc,
				subrunResults[h].initialState.path,
				diff)
		}

		// Prune the set of initial states we collected for this version, using
		// the deterministic randomness of prng to pick which states we keep.
		nextInitialStates := subrunResults.initialStates()
		if len(subrunResults) > factor {
			prng.Shuffle(len(nextInitialStates), func(i, j int) {
				nextInitialStates[i], nextInitialStates[j] = nextInitialStates[j], nextInitialStates[i]
			})
			// Delete the states that we're not going to use.
			for _, s := range nextInitialStates[factor:] {
				require.NoError(t, os.RemoveAll(s.path))
			}
			nextInitialStates = nextInitialStates[:factor]
		}
		initialStates = nextInitialStates
	}
	return nil
}

// subrunResult describes the output of a single run of the metamorphic test
// against a particular version.
type subrunResult struct {
	// runDir is the path to the directory containing the output of this
	// TestMeta run.
	runDir string
	// historyPath is the path to the history file recording the output of this
	// run. All the histories produced by a single call to runVersion should be
	// semantically equivalent.
	historyPath string
	// initialState describes the location of the database on disk after the run
	// completed. It can be used to seed a new run of the metamorphic test in a
	// later Pebble version.
	initialState initialState
}

type initialState struct {
	desc     string
	path     string
	opsPaths []string
}

func (s initialState) String() string {
	if s.desc == "" {
		return "<empty>"
	}
	return s.desc
}

type subrunResults []subrunResult

func (r subrunResults) historyPaths() []string {
	paths := make([]string, len(r))
	for i := range r {
		paths[i] = r[i].historyPath
	}
	return paths
}

func (r subrunResults) initialStates() []initialState {
	states := make([]initialState, len(r))
	for i := range r {
		states[i] = r[i].initialState
	}
	return states
}

func runVersion(
	ctx context.Context,
	t *testing.T,
	fatalOnce *sync.Once,
	rootDir string,
	vers pebbleVersion,
	seed uint64,
	initialStates []initialState,
) (results subrunResults) {
	// mu guards histories and nextInitialStates. The subtests may be run in
	// parallel (via t.Parallel()).
	var mu sync.Mutex

	// The outer 'execution-<label>' subtest will block until all of the
	// individual subtests have completed.
	t.Run(fmt.Sprintf("execution-%s", vers.Label), func(t *testing.T) {
		if len(initialStates) == 1 && initialStates[0].path == "" {
			t.Logf("Will run the metamorphic test to generate first set of initial states.")
		} else {
			t.Logf("Will run the metamorphic test %d times, each time with a different initial state.", len(initialStates))
		}
		t.Logf("  Version: %s (%s)", vers.Label, vers.SHA)
		for j, s := range initialStates {
			j, s := j, s // re-bind loop vars to scope

			runID := fmt.Sprintf("%s_%s_%d_%03d", vers.Label, vers.SHA, seed, j)
			r := metamorphicTestRun{
				seed:           seed,
				dir:            filepath.Join(rootDir, runID),
				vers:           vers,
				initialState:   s,
				testBinaryPath: vers.TestBinaryPath,
			}
			desc := s.desc
			if s.desc == "" {
				desc = "no-initial-state"
			}
			t.Run(desc, func(t *testing.T) {
				t.Parallel()
				require.NoError(t, os.MkdirAll(r.dir, os.ModePerm))

				var buf bytes.Buffer
				var out io.Writer = &buf
				if streamOutput {
					out = io.MultiWriter(out, os.Stderr)
				}
				t.Logf("Running metamorphic test binary.")
				t.Logf("  Version: %s (%s)", vers.Label, vers.SHA)
				t.Logf("  Test state dir: %s", r.dir)
				if s.desc == "" {
					t.Logf("  No initial state.")
				} else {
					t.Logf("  Initial state: %s (%s)", s.desc, s.path)
					t.Logf("  Previous ops:")
					for _, p := range s.opsPaths {
						t.Logf("    %s", p)
					}
				}

				if err := r.run(ctx, t, out); err != nil {
					var dirs dirsToSave
					dirs.add(r.dir, runID)
					if s.path != "" {
						dirs.add(s.path, runID+"-initial-state")
					}
					fatalf(t, fatalOnce, dirs, "Metamorphic test failed: %s. Output:\n%s\n", err, buf.String())
				}

				// dir is a directory containing the ops file and subdirectories for
				// each run with a particular set of OPTIONS. For example:
				//
				// dir/
				//   ops
				//   random-000/
				//   random-001/
				//   ...
				//   standard-000/
				//   standard-001/
				//   ...
				dir := getRunDir(t, r.dir)
				// subrunDirs contains the names of all dir's subdirectories.
				subrunDirs := getDirs(t, dir)

				mu.Lock()
				defer mu.Unlock()
				for _, subrunDir := range subrunDirs {
					results = append(results, subrunResult{
						runDir:      dir,
						historyPath: filepath.Join(dir, subrunDir, "history"),
						initialState: initialState{
							desc:     fmt.Sprintf("sha=%s-seed=%d-opts=%s(%s)", vers.SHA, seed, subrunDir, s.String()),
							path:     filepath.Join(dir, subrunDir),
							opsPaths: append(s.opsPaths, filepath.Join(dir, "ops")),
						},
					})
				}
			})
		}
	})
	return results
}

type dirToSave struct {
	path string
	// name of the directory to create in the artifacts dir.
	name string
}

type dirsToSave struct {
	dirs []dirToSave
}

// add appends a directory to the list of directories to save on test failure.
// The path is copied to the artifacts subdir with the given name.
func (d *dirsToSave) add(path string, name string) {
	d.dirs = append(d.dirs, dirToSave{path: path, name: name})
}

func saveDirs(t testing.TB, d dirsToSave) {
	if len(d.dirs) == 0 {
		return
	}
	outDir := artifactsDir
	if outDir == "" {
		var err error
		outDir, err = os.Getwd()
		if err != nil {
			t.Errorf("failed to determine current working directory: %s", err)
			return
		}
	}
	t.Logf("Saving artifacts:")
	for _, dir := range d.dirs {
		dst := filepath.Join(outDir, dir.name)
		t.Logf("  %s", dir.name)
		t.Logf("    src: %s", dir.path)
		t.Logf("    dst: %s", dst)
		_, err := vfs.Clone(vfs.Default, vfs.Default, dir.path, dst, vfs.CloneTryLink)
		if err != nil {
			t.Error(err)
		}
	}
}

func fatalf(t testing.TB, fatalOnce *sync.Once, dirs dirsToSave, msg string, args ...interface{}) {
	fatalOnce.Do(func() {
		saveDirs(t, dirs)
		t.Fatalf(msg, args...)
	})
}

type metamorphicTestRun struct {
	seed           uint64
	dir            string
	vers           pebbleVersion
	initialState   initialState
	testBinaryPath string
}

func (r *metamorphicTestRun) run(ctx context.Context, t *testing.T, output io.Writer) error {
	var args []string

	// We also build a readable string of the command that will be run.
	var prettyCmdLin strings.Builder
	prettyCmdLin.WriteString(r.dir)

	add := func(a ...string) {
		args = append(args, a...)
		prettyCmdLin.WriteString(" \\\n  ")
		prettyCmdLin.WriteString(strings.Join(a, " "))
	}

	add("-test.run", "TestMeta$")
	if testing.Verbose() {
		add("-test.v")
	}
	add("-seed", strconv.FormatUint(r.seed, 10))
	add("-keep")

	// Use an op-count distribution that includes a low lower bound, so that
	// some intermediary versions do very little work besides opening the
	// database. This helps exercise state from version n that survives to
	// versions ≥ n+2.
	add("-ops", "uniform:1-10000")

	// Explicitly specify the location of the _meta directory. In Cockroach
	// CI when built using bazel, the subprocesses may be given a different
	// current working directory than the one provided below. To ensure we
	// can find this run's artifacts, explicitly pass the intended dir.
	add("-dir", filepath.Join(r.dir, "_meta"))

	// Crossversion tests run with high parallelism, so the chances of a timeout
	// are high. Increase the per-operation timeout.
	add("--op-timeout", "10m")

	if r.initialState.path != "" {
		add("--initial-state-desc", r.initialState.desc)
		add("--initial-state", r.initialState.path)
		add("--previous-ops", strings.Join(r.initialState.opsPaths, ","))
	}
	cmd := exec.CommandContext(ctx, r.testBinaryPath, args...)
	cmd.Dir = r.dir
	cmd.Stderr = output
	cmd.Stdout = output

	t.Logf("running:\n%s", prettyCmdLin.String())

	return cmd.Run()
}

func (v pebbleVersion) String() string {
	return fmt.Sprintf("%s,%s,%s", v.Label, v.SHA, v.TestBinaryPath)
}

// pebbleVersions implements flag.Value for the -version flag.
type pebbleVersions []pebbleVersion

var _ flag.Value = (*pebbleVersions)(nil)

// String returns the SHAs of the versions.
func (f *pebbleVersions) String() string {
	var buf bytes.Buffer
	for i, v := range *f {
		if i > 0 {
			fmt.Fprint(&buf, " ")
		}
		fmt.Fprintf(&buf, v.SHA)
	}
	return buf.String()
}

// Set is part of the flag.Value interface; it is called once for every
// occurrence of the version flag.
func (f *pebbleVersions) Set(value string) error {
	// Expected format is `<label>,<sha>,<path>`.
	fields := strings.FieldsFunc(value, func(r rune) bool { return r == ',' || unicode.IsSpace(r) })
	if len(fields) != 3 {
		return errors.Newf("unable to parse version %q", value)
	}
	*f = append(*f, pebbleVersion{
		Label:          fields[0],
		SHA:            fields[1],
		TestBinaryPath: fields[2],
	})
	return nil
}

func getDirs(t testing.TB, dir string) (names []string) {
	dirents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, dirent := range dirents {
		if dirent.IsDir() {
			names = append(names, dirent.Name())
		}
	}
	return names
}

func getRunDir(t testing.TB, dir string) string {
	metaDir := filepath.Join(dir, "_meta")
	dirs := getDirs(t, metaDir)
	if len(dirs) != 1 {
		t.Fatalf("expected 1 directory, found %d", len(dirs))
	}
	return filepath.Join(metaDir, dirs[0])
}
