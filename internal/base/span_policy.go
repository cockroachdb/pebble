// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import (
	"fmt"
	"strings"
)

// SpanPolicy contains policies that can vary by key range.
type SpanPolicy struct {
	// Prefer a faster compression algorithm for the keys in this span.
	//
	// This is useful for keys that are frequently read or written but which don't
	// amount to a significant amount of space.
	PreferFastCompression bool

	// ValueStoragePolicy is a hint used to determine where to store the values
	// for KVs.
	ValueStoragePolicy ValueStoragePolicyAdjustment
}

// String returns a string representation of the SpanPolicy.
func (p SpanPolicy) String() string {
	var f []string
	if p.PreferFastCompression {
		f = append(f, "fast-compression")
	}
	if vsp := p.ValueStoragePolicy.String(); vsp != "" {
		f = append(f, vsp)
	}
	return strings.Join(f, ",")
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
