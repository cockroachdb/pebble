// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/pebble/internal/invariants"
)

// SpanPolicy contains policies that can vary by key range.
type SpanPolicy struct {
	// KeyRange defines the key range for which this policy is valid.
	//
	// If Start is empty, the policy is valid for the entire keyspace up to End.
	// If End is empty, the policy is valid for the entire keyspace after Start.
	// If both are empty, this is the only policy across the entire keyspace.
	//
	// The Start and End keys are not required to encompass the whole KeyRange over
	// which this policy applies, i.e., they should be interpreted as a subset of
	// the real interval for the policy.
	KeyRange KeyRange

	// Prefer a faster compression algorithm for the keys in this span.
	//
	// This is useful for keys that are frequently read or written but which don't
	// amount to a significant amount of space.
	PreferFastCompression bool

	// ValueStoragePolicy is a hint used to determine where to store the values
	// for KVs.
	ValueStoragePolicy ValueStoragePolicyAdjustment

	// NOTE: update the IsDefault() method if you add new fields to this struct.
}

// IsDefault returns true if the SpanPolicy is the default policy, i.e. none of
// the fields other than KeyRange are set.
func (p *SpanPolicy) IsDefault() bool {
	return !p.PreferFastCompression &&
		p.ValueStoragePolicy == (ValueStoragePolicyAdjustment{})
}

// StillCovers takes a key that is no smaller than the span policy start key (if
// any) and returns whether the key is still within the span policy end key.
func (p *SpanPolicy) StillCovers(cmp Compare, key []byte) bool {
	if invariants.Enabled && len(p.KeyRange.Start) > 0 && cmp(p.KeyRange.Start, key) > 0 {
		panic("key too small")
	}
	return len(p.KeyRange.End) == 0 || cmp(key, p.KeyRange.End) < 0
}

// String returns a string representation of the SpanPolicy.
func (p SpanPolicy) String() string {
	var b strings.Builder
	if len(p.KeyRange.Start) > 0 {
		fmt.Fprintf(&b, "[%s, ", DefaultFormatter(p.KeyRange.Start))
	} else {
		b.WriteString("[-∞, ")
	}
	if len(p.KeyRange.End) > 0 {
		fmt.Fprintf(&b, "%s", DefaultFormatter(p.KeyRange.End))
	} else {
		b.WriteString("∞")
	}
	b.WriteString(") -> ")

	var policy string
	if p.PreferFastCompression {
		policy = crstrings.WithSep(policy, ",", "fast-compression")
	}
	if vsp := p.ValueStoragePolicy.String(); vsp != "" {
		policy = crstrings.WithSep(policy, ",", vsp)
	}
	if policy == "" {
		policy = "default"
	}
	b.WriteString(policy)
	return b.String()
}

// ValueStoragePolicyAdjustment is used to determine where to store the values for
// KVs, overriding global policies. Values can be configured to be stored in-place,
// in value blocks, or in blob files.
type ValueStoragePolicyAdjustment struct {
	// DisableSeparationBySuffix disables discriminating KVs depending on
	// suffix.
	//
	// Among a set of keys with the same prefix, Pebble's default heuristics
	// optimize access to the KV with the smallest suffix. This is useful for MVCC
	// keys (where the smallest suffix is the latest version), but should be
	// disabled for keys where the suffix does not correspond to a version.
	// See sstable.IsLikelyMVCCGarbage for the exact criteria we use to
	// determine whether a value is likely MVCC garbage.
	//
	// If separation by suffix is enabled, KVs with older suffix values will be
	// written according to the following rules:
	// - If the value is empty, no separation is performed.
	// - If blob separation is enabled the value will be separated into a blob
	// file even if its size is smaller than the minimum value size.
	// - If blob separation is disabled, the value will be written to a value
	// block within the sstable.
	DisableSeparationBySuffix bool

	// DisableBlobSeparation disables separating values into blob files.
	DisableBlobSeparation bool

	// OverrideBlobSeparationMinimumSize overrides the minimum size required
	// for value separation into a blob file. Note that value separation must
	// be enabled globally for this to take effect.
	OverrideBlobSeparationMinimumSize int

	// MinimumMVCCGarbageSize, when non-zero, imposes a new minimum size required
	// for value separation into a blob file only if the value is likely MVCC
	// garbage. Note that value separation must be enabled globally for this to
	// take effect.
	MinimumMVCCGarbageSize int
}

func (vsp *ValueStoragePolicyAdjustment) ContainsOverrides() bool {
	return vsp.OverrideBlobSeparationMinimumSize > 0 || vsp.DisableSeparationBySuffix ||
		vsp.MinimumMVCCGarbageSize > 0
}

func (vsp ValueStoragePolicyAdjustment) String() string {
	var f []string
	if vsp.DisableSeparationBySuffix {
		f = append(f, "disable-value-separation-by-suffix")
	}
	if vsp.DisableBlobSeparation {
		f = append(f, "no-blob-value-separation")
	}
	if vsp.OverrideBlobSeparationMinimumSize > 0 {
		f = append(f, fmt.Sprintf("override-value-separation-min-size=%d", vsp.OverrideBlobSeparationMinimumSize))
	}
	if vsp.MinimumMVCCGarbageSize > 0 {
		f = append(f, fmt.Sprintf("minimum-mvcc-garbage-size=%d", vsp.MinimumMVCCGarbageSize))
	}
	return strings.Join(f, ",")
}
