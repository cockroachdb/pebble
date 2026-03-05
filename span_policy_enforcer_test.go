// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/stretchr/testify/require"
)

func makeTestTableMeta(tableNum int, smallest, largest string) *manifest.TableMetadata {
	meta := &manifest.TableMetadata{
		TableNum: base.TableNum(tableNum),
		Size:     1,
	}
	meta.SeqNums.Low = base.SeqNum(tableNum)
	meta.SeqNums.High = base.SeqNum(tableNum)
	meta.LargestSeqNumAbsolute = base.SeqNum(tableNum)
	smallestKey := base.MakeInternalKey([]byte(smallest), meta.SeqNums.Low, base.InternalKeyKindSet)
	largestKey := base.MakeInternalKey([]byte(largest), meta.SeqNums.High, base.InternalKeyKindSet)
	meta.ExtendPointKeyBounds(base.DefaultComparer.Compare, smallestKey, largestKey)
	meta.InitPhysicalBacking()
	return meta
}

func TestSpanPolicyEnforcerCompressionViolation(t *testing.T) {
	enforcer := &spanPolicyEnforcer{}
	fastPolicy := base.SpanPolicy{PreferFastCompression: true}
	noPolicy := base.SpanPolicy{}

	for _, tc := range []struct {
		stats     string
		policy    base.SpanPolicy
		violation bool
	}{
		{"Zstd1:100/200", fastPolicy, true},
		{"Zstd1:100/200", noPolicy, false},
		{"Snappy:100/200", fastPolicy, false},
		{"MinLZ1:100/200", fastPolicy, false},
		{"None:100", fastPolicy, false},
		{"Snappy:50/100,Zstd1:50/100", fastPolicy, true},
	} {
		compressionStats, err := block.ParseCompressionStats(tc.stats)
		if err != nil {
			t.Fatalf("error parsing compression stats: %v", err)
		}
		backingProps := &manifest.TableBackingProperties{CompressionStats: compressionStats}
		require.Equal(t, tc.violation, enforcer.checkCompressionViolation(backingProps, tc.policy),
			"stats=%s", tc.stats)
	}
}

func TestSpanPolicyEnforcerCheckPolicyViolation(t *testing.T) {
	opts := &Options{Comparer: base.DefaultComparer}
	opts.Experimental.SpanPolicyFunc = func(bounds base.UserKeyBounds) (base.SpanPolicy, error) {
		// Policy changes at "m": no preference before, fast preference after.
		if string(bounds.Start) < "m" {
			return base.SpanPolicy{KeyRange: base.KeyRange{Start: []byte("a"), End: []byte("m")}}, nil
		}
		return base.SpanPolicy{KeyRange: base.KeyRange{Start: []byte("m"), End: []byte("z")}, PreferFastCompression: true}, nil
	}
	db := &DB{opts: opts, cmp: base.DefaultComparer.Compare}
	enforcer := &spanPolicyEnforcer{db: db, cmp: base.DefaultComparer.Compare}

	testCases := []struct {
		tableMeta         *manifest.TableMetadata
		compressionStats  string
		expectedViolation bool
	}{
		// File in first span (no compression preference) - no violation.
		{makeTestTableMeta(1, "a", "l"), "Zstd1:100/200", false},
		// File in second span (fast compression) - violation.
		{makeTestTableMeta(2, "n", "z"), "Zstd1:100/200", true},
		// File spanning both policies - violation in second policy.
		{makeTestTableMeta(3, "a", "z"), "Zstd1:100/200", true},
	}
	for _, tc := range testCases {
		tc.tableMeta.TableBacking.PopulateProperties(&sstable.Properties{CompressionStats: tc.compressionStats})
		require.Equal(t, tc.expectedViolation, enforcer.checkPolicyViolation(tc.tableMeta))
	}
}
