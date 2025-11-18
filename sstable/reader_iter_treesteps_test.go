// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/internal/treesteps"
	"github.com/stretchr/testify/require"
)

// TestTreeSteps tests the treesteps recording for singleLevelIterator and
// twoLevelIterator operations, generating visualization URLs showing iterator
// behavior.
func TestIterTreeSteps(t *testing.T) {
	if !treesteps.Enabled {
		t.Skip("treesteps not available in this build")
	}

	t.Run("single-level", func(t *testing.T) {
		testIterTreeSteps(t, "testdata/treesteps_single_level_iter")
	})

	t.Run("two-level", func(t *testing.T) {
		testIterTreeSteps(t, "testdata/treesteps_two_level_iter")
	})
}

func testIterTreeSteps(t *testing.T, testdataPath string) {
	var r *Reader
	defer func() {
		if r != nil {
			_ = r.Close()
			r = nil
		}
	}()
	datadriven.RunTest(t, testdataPath, func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "build":
			if r != nil {
				_ = r.Close()
				r = nil
			}
			opts := &WriterOptions{
				Comparer:    testkeys.Comparer,
				TableFormat: TableFormatMax,
			}
			var err error
			_, r, err = runBuildCmd(td, opts, nil /* cacheHandle */)
			if err != nil {
				t.Fatalf("%s", err)
			}
			return ""

		case "iter-treesteps":
			return runIterTreeStepsCmd(t, r, td)

		default:
			return "unknown command"
		}
	})
}

func runIterTreeStepsCmd(t *testing.T, r *Reader, td *datadriven.TestData) string {
	require.NotNil(t, r, "must run 'build' before 'iter-treesteps'")

	iter, err := r.NewIter(NoTransforms, nil /* lower */, nil /* upper */, TableBlobContext{})
	require.NoError(t, err)

	rec := treesteps.StartRecording(iter, td.Pos)
	runIterCmd(td, iter, false)

	steps := rec.Finish()
	url := steps.URL()
	return url.String()
}
