// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"bytes"
	"context"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// tryToReduce starts with a run that reproduces a failure of t.Name() and tries
// to reduce the number of ops to find a minimal reproduction.
//
// Sample usage:
//
//	go test -run TestMetaTwoInstance ./internal/metamorphic \
//	  --run-dir _meta/231220-073015.8533726292752/random-027 \
//	  --tags invariants --try-to-reduce -v
//
// The test will save the smallest reproduction found and print out the relevant
// information.
func tryToReduce(t *testing.T, rootDir string, runDir string) {
	r := makeReducer(t, rootDir, runDir)
	r.Run(t)
}

// reducer is a helper that starts with a reproduction of a RunOnce failure and
// tries to reduce the number of operations.
type reducer struct {
	optionsData []byte
	ops         []string

	// rootDir is the directory storing test state (normally _meta). See
	// CommonFlags.Dir.
	rootDir string

	lastSavedDir string
}

func makeReducer(t *testing.T, rootDir string, runDir string) *reducer {
	runDir = filepath.Clean(runDir)
	opsPath := filepath.Join(filepath.Dir(runDir), "ops")
	opsData, err := os.ReadFile(opsPath)
	require.NoError(t, err)
	ops := strings.Split(strings.TrimSpace(string(opsData)), "\n")

	// Load options file.
	options, err := os.ReadFile(filepath.Join(runDir, "OPTIONS"))
	require.NoError(t, err)

	t.Logf("Starting with %d operations", len(ops))

	return &reducer{
		optionsData: options,
		ops:         ops,
		rootDir:     rootDir,
	}
}

// setupRunDir creates a run directory with the given ops and returns it.
func (r *reducer) setupRunDir(t *testing.T, ops []string) (metaDir, testDir string) {
	metaDir, err := os.MkdirTemp(r.rootDir, "reduce-"+time.Now().Format("060102-150405.000"))
	require.NoError(t, err)
	testDir = filepath.Join(metaDir, "run")
	require.NoError(t, os.MkdirAll(testDir, 0755))

	// Write the OPTIONS file.
	require.NoError(t, os.WriteFile(filepath.Join(testDir, "OPTIONS"), r.optionsData, 0644))
	// Write the ops file.
	require.NoError(t, os.WriteFile(filepath.Join(metaDir, "ops"), []byte(strings.Join(ops, "\n")), 0644))
	return metaDir, testDir
}

func (r *reducer) try(t *testing.T, ops []string) bool {
	metaDir, testDir := r.setupRunDir(t, ops)

	args := []string{
		"-test.run", t.Name() + "$",
		"-test.v",
		"--keep",
		"--dir", metaDir,
		"--run-dir", testDir,
	}

	var output bytes.Buffer
	cmd := exec.CommandContext(context.Background(), os.Args[0], args...)
	cmd.Stderr = &output
	cmd.Stdout = &output
	err := cmd.Run()
	// If the test succeeds or fails with an internal test error, we removed
	// important ops.
	if err == nil || strings.Contains(output.String(), "metamorphic test internal error") {
		require.NoError(t, os.RemoveAll(metaDir))
		return false
	}

	logFile := filepath.Join(metaDir, "log")
	require.NoError(t, os.WriteFile(logFile, output.Bytes(), 0644))
	t.Logf("Reduced to %d ops; saved log %v and run dir %v", len(ops), logFile, testDir)
	// Only keep one saved dir.
	if r.lastSavedDir != "" {
		require.NoError(t, os.RemoveAll(r.lastSavedDir))
	}
	r.lastSavedDir = metaDir
	return true
}

func (r *reducer) Run(t *testing.T) {
	ops := r.ops
	// We start with a high probability of removing elements, and once we can't
	// find any reductions we decrease it. This works well even if the problem is
	// not deterministic and isn't reproduced on every run.
	for removeProbability := 0.1; removeProbability > 1e-5; removeProbability *= 0.5 {
		t.Logf("removeProbability %.2f", removeProbability)
		for i := 0; i < 100; i++ {
			o := randomSubset(t, ops, removeProbability)
			if r.try(t, o) {
				ops = o
				// Reset the counter.
				i = -1
			}
		}
	}
}

func randomSubset(t *testing.T, ops []string, removeProbability float64) []string {
	require.Greater(t, len(ops), 1)
	// The first op is always Init; we need to keep it.
	res := ops[:1:1]
	ops = ops[1:]
	// Regardless of the probability, we choose at least one op to remove.
	x := rand.Intn(len(ops))
	for i := range ops {
		if i == x || rand.Float64() < removeProbability {
			// Remove this op.
			continue
		}
		res = append(res, ops[i])
	}
	return res
}
