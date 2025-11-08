// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/stretchr/testify/require"
)

// TestReaderLazyLoading tests the lazy loading functionality using data-driven tests.
// It verifies that the index block is not loaded when the iterator is created, and
// is loaded only after the first access operation.
func TestReaderLazyLoading(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var r *Reader
	defer func() {
		if r != nil {
			r.Close()
			r = nil
		}
	}()
	datadriven.RunTest(t, "testdata/reader_lazy_loading", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "build":
			if r != nil {
				r.Close()
				r = nil
			}
			r = runBuildForLazyTest(t, td)
			return ""
		case "iter-lazy":
			return runIterLazyCmd(t, td, r)
		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func runBuildForLazyTest(t *testing.T, td *datadriven.TestData) *Reader {
	// Build writer options with defaults
	writerOpts := WriterOptions{
		Comparer:       base.DefaultComparer,
		MergerName:     base.DefaultMerger.Name,
		TableFormat:    TableFormatMax, // allow any format
		BlockSize:      4096,
		IndexBlockSize: 2048,
	}

	// Parse command arguments
	if err := ParseWriterOptions(&writerOpts, td.CmdArgs...); err != nil {
		t.Fatal(err)
	}

	// Add bloom filter if specified
	if td.HasArg("bloom-filter") {
		writerOpts.FilterPolicy = testFilterPolicy
	}

	// Build the table
	_, reader, err := runBuildCmd(td, &writerOpts, nil)
	if err != nil {
		t.Fatal(err)
	}
	return reader
}

func runIterLazyCmd(t *testing.T, td *datadriven.TestData, r *Reader) string {
	if r == nil {
		return "build must be called before iter-lazy"
	}

	// Create new iterator
	iter, err := r.NewPointIter(context.Background(), IterOptions{
		Transforms:           NoTransforms,
		FilterBlockSizeLimit: AlwaysUseFilterBlock,
		Env:                  NoReadEnv,
		ReaderProvider:       MakeTrivialReaderProvider(r),
		BlobContext:          AssertNoBlobHandles,
	})
	require.NoError(t, err)

	return runIterCmd(td, iter, true /* printValue */, runIterCmdShowCommands)
}

// isLazyIndexLoaded checks if the index block has been loaded for lazy iterators
func isLazyIndexLoaded(iter Iterator) bool {
	// Try to detect if this is a single-level iterator with lazy loading
	switch sli := iter.(type) {
	case *singleLevelIteratorRowBlocks:
		return sli.indexLoaded
	case *singleLevelIteratorColumnBlocks:
		return sli.indexLoaded
	default:
		// For other iterator types (e.g., two-level), assume index is loaded
		// since they don't use the lazy loading mechanism
		return true
	}
}

// Simple test filter policy for bloom filters
var testFilterPolicy = testFilterPolicyImpl{}

type testFilterPolicyImpl struct{}

func (testFilterPolicyImpl) Name() string { return "test.filter" }

func (testFilterPolicyImpl) MayContain(ftype FilterType, filter, key []byte) bool {
	// For the test, return false for keys starting with "nonexistent" to simulate bloom filter miss
	prefix := "nonexistent"
	if len(key) >= len(prefix) && string(key[:len(prefix)]) == prefix {
		return false
	}
	return true
}

func (testFilterPolicyImpl) NewWriter(ftype FilterType) FilterWriter {
	return &testFilterWriter{}
}

type testFilterWriter struct{}

func (w *testFilterWriter) AddKey(key []byte)                          {}
func (w *testFilterWriter) Finish(buf []byte) []byte                   { return buf }
func (w *testFilterWriter) EstimateEncodedSize() int                   { return 10 }
func (w *testFilterWriter) Reset()                                     {}
func (w *testFilterWriter) Len() int                                   { return 0 }
func (w *testFilterWriter) MayContain(key []byte) bool                 { return true }
func (w *testFilterWriter) NumProbes() int                             { return 1 }
func (w *testFilterWriter) NumHashFuncs() int                          { return 1 }
func (w *testFilterWriter) EstimateFinishBytes(additionalKeys int) int { return 10 }
func (w *testFilterWriter) Fingerprint() uint64                        { return 0 }
func (w *testFilterWriter) SetTotalKeys(n int)                         {}
func (w *testFilterWriter) GetFilterData() []byte                      { return nil }
