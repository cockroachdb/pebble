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
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/metamorphic"
	"github.com/stretchr/testify/require"
)

var (
	factor       int
	seed         int64
	versions     pebbleVersions
	artifactsDir string
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
version, for example, 'CRDB 22.1'. The SHA indicates the
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
}

func reproductionCommand() string {
	return fmt.Sprintf("go test -v -run 'TestMetaCrossVersion' --seed %d --factor %d %s\n",
		seed, factor, versions.String())
}

// TestMetaCrossVersion performs cross-version metamorphic testing.
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

type initialState struct {
	desc string
	path string
}

func (s initialState) String() string {
	if s.desc == "" {
		return "<empty>"
	}
	return s.desc
}

func runCrossVersion(
	ctx context.Context,
	t *testing.T,
	tempDir string,
	versions pebbleVersions,
	seed int64,
	factor int,
) error {
	prng := rand.New(rand.NewSource(seed))
	// Use prng to derive deterministic seeds to provide to the child
	// metamorphic runs. The same seed is used for all runs on a particular
	// Pebble version.
	versionSeeds := make([]uint64, len(versions))
	for i := range versions {
		versionSeeds[i] = prng.Uint64()
	}

	rootDir := filepath.Join(tempDir, strconv.FormatInt(seed, 10))
	if err := os.MkdirAll(rootDir, os.ModePerm); err != nil {
		return err
	}

	// The outer for loop executes once per version being tested. It takes a
	// list of initial states, populated by the previous version. The inner loop
	// executes once per initial state, running the metamorphic test against the
	// initial state.
	//
	// The number of states that are carried forward from one version to the
	// next is fixed by `factor`.
	var buf bytes.Buffer
	initialStates := []initialState{{}}
	for i := range versions {
		t.Logf("Running tests with version %s with %d initial state(s).", versions[i].SHA, len(initialStates))
		var nextInitialStates []initialState
		var histories []string
		for j, s := range initialStates {
			runID := fmt.Sprintf("%s_%d_%03d", versions[i].SHA, seed, j)

			r := metamorphicTestRun{
				seed:           versionSeeds[i],
				dir:            filepath.Join(rootDir, runID),
				vers:           versions[i],
				initialState:   s,
				testBinaryPath: versions[i].TestBinaryPath,
			}
			if err := os.MkdirAll(r.dir, os.ModePerm); err != nil {
				return err
			}

			var out io.Writer = &buf
			if testing.Verbose() {
				out = io.MultiWriter(out, os.Stderr)
			}
			t.Logf("  Running test with version %s with initial state %s.",
				versions[i].SHA, s)
			if err := r.run(ctx, out); err != nil {
				fatalf(t, rootDir, "Metamorphic test failed: %s\nOutput:%s\n", err, buf.String())
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
			for _, subrunDir := range subrunDirs {
				// Record the subrun as an initial state for the next version.
				nextInitialStates = append(nextInitialStates, initialState{
					path: filepath.Join(dir, subrunDir),
					desc: fmt.Sprintf("sha=%s-seed=%d-opts=%s(%s)", versions[i].SHA, versionSeeds[i], subrunDir, s.String()),
				})
				histories = append(histories, filepath.Join(dir, subrunDir, "history"))
			}

			buf.Reset()
		}

		// All the initial states described the same state and all of this
		// version's metamorphic runs used the same seed, so all of the
		// resulting histories should be identical.
		if h, diff := metamorphic.CompareHistories(t, histories); h > 0 {
			fatalf(t, rootDir, "Metamorphic test divergence between %q and %q:\nDiff:\n%s",
				nextInitialStates[0].desc, nextInitialStates[h].desc, diff)
		}

		// Prune the set of initial states we collected for this version, using
		// the deterministic randomness of prng to pick which states we keep.
		if len(nextInitialStates) > factor {
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

func fatalf(t testing.TB, dir string, msg string, args ...interface{}) {
	if artifactsDir == "" {
		var err error
		artifactsDir, err = os.Getwd()
		require.NoError(t, err)
	}
	dst := filepath.Join(artifactsDir, filepath.Base(dir))
	t.Logf("Moving test dir %q to %q.", dir, dst)
	require.NoError(t, os.Rename(dir, dst))
	t.Fatalf(msg, args...)
}

type metamorphicTestRun struct {
	seed           uint64
	dir            string
	vers           pebbleVersion
	initialState   initialState
	testBinaryPath string
}

func (r *metamorphicTestRun) run(ctx context.Context, output io.Writer) error {
	args := []string{
		"-test.run", "TestMeta$",
		"-seed", strconv.FormatUint(r.seed, 10),
		"-keep",
		// Use an op-count distribution that includes a low lower bound, so that
		// some intermediary versions do very little work besides opening the
		// database. This helps exercise state from version n that survives to
		// versions ≥ n+2.
		"-ops", "uniform:1-10000",
		// Explicitly specify the location of the _meta directory. In Cockroach
		// CI when built using bazel, the subprocesses may be given a different
		// current working directory than the one provided below. To ensure we
		// can find this run's artifacts, explicitly pass the intended dir.
		"-dir", filepath.Join(r.dir, "_meta"),
	}
	// Propagate the verbose flag, if necessary.
	if testing.Verbose() {
		args = append(args, "-test.v")
	}
	if r.initialState.path != "" {
		args = append(args,
			"--initial-state", r.initialState.path,
			"--initial-state-desc", r.initialState.desc)
	}
	cmd := exec.CommandContext(ctx, r.testBinaryPath, args...)
	cmd.Dir = r.dir
	cmd.Stderr = output
	cmd.Stdout = output

	// Print the command itself before executing it.
	if testing.Verbose() {
		fmt.Fprintln(output, cmd)
	}

	return cmd.Run()
}

func (v pebbleVersion) String() string {
	return fmt.Sprintf("%s,%s,%s", v.Label, v.SHA, v.TestBinaryPath)
}

type pebbleVersions []pebbleVersion

func (f *pebbleVersions) String() string {
	var buf bytes.Buffer
	for i, v := range *f {
		if i > 0 {
			fmt.Fprint(&buf, " ")
		}
		fmt.Fprintf(&buf, "--version %s", v.String())
	}
	return buf.String()
}

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
