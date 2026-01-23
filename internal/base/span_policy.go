// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import (
	"fmt"
	"strings"
	"time"

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

	// TieringPolicy is an optional policy for specifying which key-value pairs
	// should be stored in the warm or cold tier. Once a SpanPolicy specifies a
	// non-zero TieringPolicy, all subsequent requests for a SpanPolicy for a key
	// k in [KeyRange.Start, KeyRange.End) must not return a TieringPolicy that
	// returns a different value from TieringPolicy.SpanID.
	//
	// Due to eventual consistency at the CockroachDB layer, we tolerate the
	// TieringPolicy field to appear and disappear (become zero), as long as in
	// all appearances the SpanID is the same.
	//
	// Additionally, a span may stop being used permanently by a higher layer,
	// and the data deleted, in which case it can reuse the same SpanID for a
	// different span. This must only be done when the deletion has destroyed
	// the data in the LSM, say via an excise.
	//
	// Pebble will remember the set of (KeyRange, SpanID) pairs that it has seen
	// in its history, for error checking the aforementioned invariant.
	TieringPolicy TieringPolicyAndExtractor

	// NOTE: update the IsDefault() method if you add new fields to this struct.
}

// IsDefault returns true if the SpanPolicy is the default policy, i.e. none of
// the fields other than KeyRange are set.
func (p *SpanPolicy) IsDefault() bool {
	return !p.PreferFastCompression &&
		!p.ValueStoragePolicy.IsSet() &&
		!p.TieringPolicy.IsSet()
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
	if p.ValueStoragePolicy.IsSet() {
		policy = crstrings.WithSep(policy, ",", p.ValueStoragePolicy.String())
	}
	if p.TieringPolicy.IsSet() {
		policy = crstrings.WithSep(policy, ",", p.TieringPolicy.String())
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

func (vsp *ValueStoragePolicyAdjustment) IsSet() bool {
	return *vsp != (ValueStoragePolicyAdjustment{})
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

// TieringAttribute is a user-specified attribute for the key-value pair.
//
// Currently, the value is always a unix timestamp in seconds (what
// time.Time.Unix() returns), but this could be extended in the future.
// and should be opaque to most of the Pebble code.
//
// The zero value is reserved to mean "no attribute" or "unknown".
type TieringAttribute uint64

// TieringSpanID is an identifier that provides a context for interpreting a
// TieringAttribute value. At any point in time, the span policies determine
// a set of non-overlapping key regions with distinct TieringSpanIDs.
type TieringSpanID uint64

// TieringPolicy defines a policy for tiering key-value pairs into warm and
// cold tiers.
//
// The default (zero) value indicates no tiering.
type TieringPolicy struct {
	// SpanID is an immutable id for the key span to which this policy applies.
	// The actual span is specified by the SpanPolicy.KeyRange context in which
	// this policy is returned.
	SpanID TieringSpanID

	// AgeThreshold defines a threshold based on age. Each TieringAttribute for
	// this SpanID is assumed to be a Unix time in seconds (see time.Time.Unix()).
	// KVs with TieringAttribute time older than this threshold belongs in the cold
	// tier.
	//
	// The threshold for a SpanID can change (rarely), as a user reconfiguration.
	//
	// Note: in the future, we may add other mechanisms to set the cold/hot
	// threshold; we should not assume that TieringAttribute is a timestamp
	// except in the context of interpreting a specific policy.
	AgeThreshold time.Duration
}

func (tp TieringPolicy) IsSet() bool {
	return tp.SpanID != 0
}

func (tp TieringPolicy) String() string {
	if tp.SpanID == 0 {
		return ""
	}
	return fmt.Sprintf("tiering:span=%d/age=%s", tp.SpanID, tp.AgeThreshold)
}

// TieringPolicyAndExtractor defines a tiering policy and an extractor for the
// tiering attribute for that policy.
//
// Currently, the only way to retrieve a TieringPolicyAndExtractor is via
// SpanPolicyFunc, by passing a key parameter. The policy is needed by Pebble
// in the following cases:
//
//   - During the execution phase of a flush or a sstable compaction, to do
//     attribute extraction, or to decide which tier a particular row belongs
//     to. Since the key is known, the SpanPolicy can be retrieved with that
//     key. Typically, attribute extraction is done during flushes, and we
//     never re-extract during compactions. However, due to the eventual
//     consistency of the tiering policies, we may need to extract for the
//     first time during a sstable compaction. Note that we cannot extract for
//     the first time when doing a blob file rewrite compaction since the key
//     that determines the policy is not known.
//
//   - During a blob file rewrite compaction. We do not store the key with
//     each value in the blob file, but we store a non-tight key span for the
//     whole blob file. The start key of that span is used to retrieve the
//     first SpanPolicy, and the SpanPolicy.KeyRange.End is used to iterate
//     until we reach the blob file end key. Since the blob file may have
//     been rewritten in the past (hence the key span is not tight), we may
//     retrieve some unnecessary policies, but we will have all the SpanIDs
//     that could possibly apply to these values and can stash them into a
//     SpanID => TieringPolicy map, for use in this compaction. NB: due to
//     eventual consistency, the SpanIDs in this map may be a subset of the
//     SpanIDs in the blob file. For the ones with an unknown policy, we will
//     not change the tier.
//
//   - Before starting a sstable compaction, a decision needs to be made
//     whether to rewrite certain warm and cold blob files referenced in the
//     compaction. This rewrite decision uses the latest tiering policies for
//     all the spanIDs in the inputs of the compactions, and their tiering
//     attribute histograms. It may result in a decision to rewrite a blob
//     file, if it allows for significant movement of data between tiers. In a
//     similar vein, when writing new blob files, a decision needs to be made
//     up front whether there is enough cold data to justify writing a cold
//     blob file (to avoid having tiny files). The same iteration approach
//     mentioned earlier is used.
//
//   - Periodic calls, to learn the latest AgeThreshold, so that it can
//     initiate explicit rewrites of files that are not being rewritten
//     normally, to move data between tiers. The same iteration approach
//     mentioned earlier is used to iterate over *all* tiering policies.
type TieringPolicyAndExtractor struct {
	TieringPolicy

	// ExtractAttribute extracts the tiering attribute from the key-value pair.
	// Once extracted, the attribute can be remembered since it must never
	// change for this key-value pair during the lifetime of the DB.
	//
	// Successful extraction must not return a 0 attribute. Pebble reserves the
	// 0 attribute (with a non-zero SpanID) to represent an extraction error,
	// and stats are maintained for this so that users can enquire about the
	// bytes in the system that have such errors.
	ExtractAttribute func(userKey []byte, value []byte) (TieringAttribute, error)
}
