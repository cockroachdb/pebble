// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
)

// TestReaderLazyLoading tests the lazy loading functionality using data-driven tests.
// It verifies that the index block is not loaded when the iterator is created, and
// is loaded only after the first access operation.
func TestReaderLazyLoading(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.RunTest(t, "testdata/reader_lazy_loading", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "build":
			return runBuildForLazyTest(t, td)
		case "iter-lazy":
			return runIterLazyCmd(t, td)
		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

var (
	lazyTestReader *Reader
	lazyTestIter   Iterator
)

func runBuildForLazyTest(t *testing.T, td *datadriven.TestData) string {
	// Close existing reader if any
	if lazyTestReader != nil {
		lazyTestReader.Close()
		lazyTestReader = nil
	}
	if lazyTestIter != nil {
		lazyTestIter.Close()
		lazyTestIter = nil
	}

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
		return err.Error()
	}

	// Add bloom filter if specified
	if td.HasArg("bloom-filter") {
		writerOpts.FilterPolicy = testFilterPolicy
	}

	// Build the table
	_, reader, err := runBuildCmd(td, &writerOpts, nil)
	if err != nil {
		return err.Error()
	}

	lazyTestReader = reader
	return ""
}

func runIterLazyCmd(t *testing.T, td *datadriven.TestData) string {
	if lazyTestReader == nil {
		return "build must be called before iter-lazy"
	}

	// Close existing iterator if any
	if lazyTestIter != nil {
		lazyTestIter.Close()
		lazyTestIter = nil
	}

	// Create new iterator
	iter, err := lazyTestReader.NewPointIter(context.Background(), IterOptions{
		Transforms:           NoTransforms,
		FilterBlockSizeLimit: AlwaysUseFilterBlock,
		Env:                  NoReadEnv,
		ReaderProvider:       MakeTrivialReaderProvider(lazyTestReader),
		BlobContext:          AssertNoBlobHandles,
	})
	if err != nil {
		return err.Error()
	}
	lazyTestIter = iter

	// Process the operations
	var output strings.Builder
	lines := strings.Split(strings.TrimSpace(td.Input), "\n")

	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		if i > 0 {
			output.WriteString("\n")
		}

		switch {
		case line == "check-index-loaded":
			loaded := isLazyIndexLoaded(iter)
			output.WriteString(fmt.Sprintf("index-loaded: %v", loaded))

		case line == "first":
			kv := iter.First()
			if kv != nil {
				output.WriteString(fmt.Sprintf("first: <%s:%d>:%s", kv.K.UserKey, kv.K.SeqNum(), kv.InPlaceValue()))
			} else {
				output.WriteString("first: .")
			}

		case line == "last":
			kv := iter.Last()
			if kv != nil {
				output.WriteString(fmt.Sprintf("last: <%s:%d>:%s", kv.K.UserKey, kv.K.SeqNum(), kv.InPlaceValue()))
			} else {
				output.WriteString("last: .")
			}

		case strings.HasPrefix(line, "seek-ge "):
			key := strings.TrimPrefix(line, "seek-ge ")
			kv := iter.SeekGE([]byte(key), base.SeekGEFlagsNone)
			if kv != nil {
				output.WriteString(fmt.Sprintf("seek-ge %s: <%s:%d>:%s", key, kv.K.UserKey, kv.K.SeqNum(), kv.InPlaceValue()))
			} else {
				output.WriteString(fmt.Sprintf("seek-ge %s: .", key))
			}

		case strings.HasPrefix(line, "seek-lt "):
			key := strings.TrimPrefix(line, "seek-lt ")
			kv := iter.SeekLT([]byte(key), base.SeekLTFlagsNone)
			if kv != nil {
				output.WriteString(fmt.Sprintf("seek-lt %s: <%s:%d>:%s", key, kv.K.UserKey, kv.K.SeqNum(), kv.InPlaceValue()))
			} else {
				output.WriteString(fmt.Sprintf("seek-lt %s: .", key))
			}

		case strings.HasPrefix(line, "seek-prefix-ge "):
			parts := strings.Fields(line)
			if len(parts) != 3 {
				return fmt.Sprintf("seek-prefix-ge requires prefix and key: %s", line)
			}
			prefix, key := parts[1], parts[2]
			kv := iter.SeekPrefixGE([]byte(prefix), []byte(key), base.SeekGEFlagsNone)
			if kv != nil {
				output.WriteString(fmt.Sprintf("seek-prefix-ge %s: <%s:%d>:%s", key, kv.K.UserKey, kv.K.SeqNum(), kv.InPlaceValue()))
			} else {
				output.WriteString(fmt.Sprintf("seek-prefix-ge %s: .", key))
			}

		case line == "next":
			kv := iter.Next()
			if kv != nil {
				output.WriteString(fmt.Sprintf("next: <%s:%d>:%s", kv.K.UserKey, kv.K.SeqNum(), kv.InPlaceValue()))
			} else {
				output.WriteString("next: .")
			}

		case line == "prev":
			kv := iter.Prev()
			if kv != nil {
				output.WriteString(fmt.Sprintf("prev: <%s:%d>:%s", kv.K.UserKey, kv.K.SeqNum(), kv.InPlaceValue()))
			} else {
				output.WriteString("prev: .")
			}

		default:
			return fmt.Sprintf("unknown iter-lazy operation: %s", line)
		}

		if err := iter.Error(); err != nil {
			output.WriteString(fmt.Sprintf(" <err: %s>", err))
		}
	}

	return output.String()
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
