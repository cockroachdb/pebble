// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/metamorphic"
	"github.com/stretchr/testify/require"
)

// tryToReduce starts with a run that reproduces a "run once" failure of
// t.Name() and tries to reduce the number of ops to find a minimal
// reproduction.
//
// Sample usage:
//
//	go test -run TestMetaTwoInstance ./internal/metamorphic \
//	  --run-dir _meta/231220-073015.8533726292752/random-027 \
//	  -tags invariants --try-to-reduce -v
//
// The test will save the smallest reproduction found and print out the relevant
// information.
func tryToReduce(t *testing.T, testStateDir string, runDir string, reduceAttempts int) {
	testRootDir := filepath.Dir(runDir)
	runSubdir := filepath.Base(runDir)
	r := makeReducer(t, testStateDir, testRootDir, []string{runSubdir}, reduceAttempts)
	r.Run(t)
}

// tryToReduceCompare starts with a run that reproduces a compare failure of
// t.Name() and tries to reduce the number of ops to find a minimal
// reproduction.
//
// Sample usage:
//
//	go test -run TestMetaTwoInstance ./internal/metamorphic \
//	  --compare '_meta/231220-073015.8533726292752/{standard-001,random-027}' \
//	  -tags invariants --try-to-reduce -v
//
// The test will save the smallest reproduction found and print out the relevant
// information.
func tryToReduceCompare(
	t *testing.T, testStateDir string, testRootDir string, runSubdirs []string, reduceAttempts int,
) {
	r := makeReducer(t, testStateDir, testRootDir, runSubdirs, reduceAttempts)
	r.Run(t)
}

// reducer is a helper that starts with a reproduction of a RunOnce failure and
// tries to reduce the number of operations.
type reducer struct {
	// testRootDir is the directory of the test, which contains the "ops" file.
	testRootDir    string
	configs        []testConfig
	reduceAttempts int

	ops []string

	// rootDir is the directory storing test state (normally _meta). See
	// CommonFlags.Dir.
	testStateDir string

	// lastSavedDir keeps track of the last saved test root directory, so we can
	// delete it once we save a new one.
	lastSavedDir string
}

type testConfig struct {
	// name of the test; matches the basename of the run dir path.
	name        string
	optionsData []byte
}

func makeReducer(
	t *testing.T, testStateDir string, testRootDir string, runSubdirs []string, reduceAttempts int,
) *reducer {
	// All run dirs should have the same parent path.
	opsData, err := os.ReadFile(filepath.Join(testRootDir, "ops"))
	require.NoError(t, err)
	ops := strings.Split(strings.TrimSpace(string(opsData)), "\n")

	tc := make([]testConfig, len(runSubdirs))
	for i := range runSubdirs {
		// Load options file.
		optionsData, err := os.ReadFile(filepath.Join(testRootDir, runSubdirs[i], "OPTIONS"))
		require.NoError(t, err)
		tc[i] = testConfig{
			name:        runSubdirs[i],
			optionsData: optionsData,
		}
	}

	t.Logf("Starting with %d operations", len(ops))

	return &reducer{
		testRootDir:    testRootDir,
		configs:        tc,
		ops:            ops,
		reduceAttempts: reduceAttempts,
		testStateDir:   testStateDir,
	}
}

// setupRunDirs creates a test root directory with the given ops and a
// subdirectory for each config.
func (r *reducer) setupRunDirs(
	t *testing.T, ops []string,
) (testRootDir string, runSubdirs []string) {
	testRootDir, err := os.MkdirTemp(r.testStateDir, "reduce-"+time.Now().Format("060102-150405.000"))
	require.NoError(t, err)
	// Write the ops file.
	require.NoError(t, os.WriteFile(filepath.Join(testRootDir, "ops"), []byte(strings.Join(ops, "\n")), 0644))

	for _, c := range r.configs {
		runDir := filepath.Join(testRootDir, c.name)
		require.NoError(t, os.MkdirAll(runDir, 0755))

		// Write the OPTIONS file.
		require.NoError(t, os.WriteFile(filepath.Join(runDir, "OPTIONS"), c.optionsData, 0644))
		runSubdirs = append(runSubdirs, c.name)
	}
	return testRootDir, runSubdirs
}

func (r *reducer) try(t *testing.T, ops []string) bool {
	testRootDir, runSubdirs := r.setupRunDirs(t, ops)

	args := []string{
		"-test.run", t.Name() + "$",
		"-test.v",
		"-test.timeout", "10s",
		"--keep",
	}

	var runFlags []string
	if len(runSubdirs) == 1 {
		// RunOnce mode.
		runFlags = []string{"--run-dir", filepath.Join(testRootDir, runSubdirs[0])}
	} else {
		// Compare mode.
		runFlags = []string{"--compare", filepath.Join(testRootDir, fmt.Sprintf("{%s}", strings.Join(runSubdirs, ",")))}
	}
	args = append(args, runFlags...)

	var output bytes.Buffer
	cmd := exec.CommandContext(context.Background(), os.Args[0], args...)
	cmd.Stderr = &output
	cmd.Stdout = &output
	err := cmd.Run()
	// If the test succeeds or fails with an internal test error or a timeout, we
	// removed important ops.
	if err == nil ||
		strings.Contains(output.String(), "metamorphic test internal error") ||
		strings.Contains(output.String(), "element has outstanding references") ||
		strings.Contains(output.String(), "leaked iterators") ||
		strings.Contains(output.String(), "leaked snapshots") ||
		strings.Contains(output.String(), "test timed out") {
		require.NoError(t, os.RemoveAll(testRootDir))
		return false
	}

	logFile := filepath.Join(testRootDir, "log")
	require.NoError(t, os.WriteFile(logFile, output.Bytes(), 0644))
	t.Logf("Reduced to %d ops.", len(ops))
	t.Logf("  Log: %v", logFile)

	// Try to generate a diagram.
	diagram, err := metamorphic.TryToGenerateDiagram(
		metamorphic.TestkeysKeyFormat,
		[]byte(strings.Join(ops, "\n")),
	)
	require.NoError(t, err)
	if diagram != "" {
		diagramPath := filepath.Join(testRootDir, "diagram")
		require.NoError(t, os.WriteFile(diagramPath, []byte(diagram+"\n"), 0644))
		t.Logf("  Diagram: %s", diagramPath)
	}

	t.Logf(`  go test ./internal/metamorphic -tags invariants -run "%s$" -v %s %q`, t.Name(), runFlags[0], runFlags[1])
	if r.lastSavedDir != "" {
		require.NoError(t, os.RemoveAll(r.lastSavedDir))
	}
	r.lastSavedDir = testRootDir
	return true
}

func (r *reducer) Run(t *testing.T) {
	ops := r.ops
	// We start with a high probability of removing elements, and once we can't
	// find any reductions we decrease it. This works well even if the problem is
	// not deterministic and isn't reproduced on every run.
	for removeProbability := 0.1; removeProbability > 1e-5 && removeProbability > 0.1/float64(len(ops)); removeProbability *= 0.5 {
		t.Logf("removeProbability %.2f%%", removeProbability*100.0)
		for i := 0; i < r.reduceAttempts; i++ {
			if o := randomSubset(t, ops, removeProbability); r.try(t, o) {
				ops = o
				// Reset the counter.
				i = -1
			}
		}
	}
	// Try to simplify the keys.
	opsData := []byte(strings.Join(ops, "\n"))
	for _, retainSuffixes := range []bool{false, true} {
		newOpsData := metamorphic.TryToSimplifyKeys(metamorphic.TestkeysKeyFormat, opsData, retainSuffixes)
		o := strings.Split(strings.TrimSpace(string(newOpsData)), "\n")
		if r.try(t, o) {
			return
		}
	}
}

func randomSubset(t *testing.T, ops []string, removeProbability float64) []string {
	require.Greater(t, len(ops), 1)
	// The first op is always Init; we need to keep it.
	res := ops[:1:1]
	ops = ops[1:]
	// Regardless of the probability, we choose at least one op to remove.
	x := rand.IntN(len(ops))
	for i := range ops {
		if i == x || rand.Float64() < removeProbability {
			// Remove this op.
			continue
		}
		res = append(res, ops[i])
	}
	return res
}
