// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/stretchr/testify/require"
)

// makeTestTableMeta creates a TableMetadata suitable for testing
// shouldWriteBlobFiles. It initializes the physical backing and populates
// properties relevant to value separation.
func makeTestTableMeta(
	cmp base.Compare,
	tableNum base.TableNum,
	smallest, largest base.InternalKey,
	blobRefDepth manifest.BlobReferenceDepth,
	valSepMinSize uint64,
	valSepBySuffixDisabled bool,
) *manifest.TableMetadata {
	m := &manifest.TableMetadata{TableNum: tableNum, Size: 1024}
	m.BlobReferenceDepth = blobRefDepth
	m.ExtendPointKeyBounds(cmp, smallest, largest)
	m.InitPhysicalBacking()
	m.TableBacking.PopulateProperties(&sstable.Properties{
		ValueSeparationMinSize:          valSepMinSize,
		ValueSeparationBySuffixDisabled: valSepBySuffixDisabled,
	})
	return m
}

func TestShouldWriteBlobFiles(t *testing.T) {
	cmp := base.DefaultComparer.Compare
	ikey := func(userKey string, seqNum uint64) base.InternalKey {
		return base.MakeInternalKey([]byte(userKey), base.SeqNum(seqNum), base.InternalKeyKindSet)
	}
	defaultPolicy := ValueSeparationPolicy{
		Enabled:               true,
		MinimumSize:           512,
		MaxBlobReferenceDepth: 5,
	}
	defaultSpanPolicyFunc := SpanPolicyFunc(func(bounds base.UserKeyBounds) (base.SpanPolicy, error) {
		return base.SpanPolicy{}, nil
	})

	type testCase struct {
		name               string
		kind               compactionKind
		inputs             []compactionLevel
		policy             ValueSeparationPolicy
		spanPolicyFunc     SpanPolicyFunc
		wantWriteBlobs     bool
		wantReferenceDepth manifest.BlobReferenceDepth
		wantAnnotations    []string
	}
	tests := []testCase{
		{
			name: "flush",
			kind: compactionKindFlush,
			// Flushes always write new blob files regardless of inputs.
			wantWriteBlobs:     true,
			wantReferenceDepth: 0,
		},
		{
			// A virtual rewrite materializes a virtual table without writing
			// new blob files. The reference depth is preserved unchanged.
			name: "virtual-rewrite",
			kind: compactionKindVirtualRewrite,
			inputs: []compactionLevel{
				{
					level: 6,
					files: manifest.NewLevelSliceKeySorted(cmp, []*manifest.TableMetadata{
						makeTestTableMeta(
							cmp, 1, ikey("a", 5), ikey("m", 5), 3, 512, false),
					}),
				},
			},
			wantWriteBlobs:     false,
			wantReferenceDepth: 3,
		},
		{
			name: "default-low-reference-depth",
			kind: compactionKindDefault,
			inputs: []compactionLevel{
				{
					level: 5,
					files: manifest.NewLevelSliceKeySorted(cmp, []*manifest.TableMetadata{
						makeTestTableMeta(
							cmp, 1, ikey("a", 5), ikey("m", 5), 3, 512, false),
					}),
				},
				{
					level: 6,
					files: manifest.NewLevelSliceKeySorted(cmp, []*manifest.TableMetadata{
						makeTestTableMeta(
							cmp, 2, ikey("a", 4), ikey("m", 4), 2, 512, false),
					}),
				},
			},
			wantWriteBlobs:     false,
			wantReferenceDepth: 5, // 3 + 2
		},
		{
			name: "all-pre-valsep",
			kind: compactionKindDefault,
			inputs: []compactionLevel{
				{
					level: 5,
					files: manifest.NewLevelSliceKeySorted(cmp, []*manifest.TableMetadata{
						makeTestTableMeta(
							cmp, 1, ikey("a", 5), ikey("m", 5), 0, 0, false),
					}),
				},
				{
					level: 6,
					files: manifest.NewLevelSliceKeySorted(cmp, []*manifest.TableMetadata{
						makeTestTableMeta(
							cmp, 2, ikey("a", 4), ikey("m", 4), 0, 0, false),
					}),
				},
			},
			wantWriteBlobs:     true,
			wantReferenceDepth: 0,
			wantAnnotations:    []string{"write-blobs-input-depth-zero"},
		},
		{
			// A compaction mixing post-valsep L5 files with pre-valsep L6 files
			// should NOT force a rewrite of the L5 blob references.
			name: "mixed-pre-and-post-valsep",
			kind: compactionKindDefault,
			inputs: []compactionLevel{
				{
					level: 5,
					files: manifest.NewLevelSliceKeySorted(cmp, []*manifest.TableMetadata{
						makeTestTableMeta(
							cmp, 1, ikey("a", 5), ikey("m", 5), 1, 512, false),
					}),
				},
				{
					level: 6,
					files: manifest.NewLevelSliceKeySorted(cmp, []*manifest.TableMetadata{
						makeTestTableMeta(
							cmp, 2, ikey("a", 4), ikey("m", 4), 0, 0, false),
					}),
				},
			},
			wantWriteBlobs:     false,
			wantReferenceDepth: 1, // max(1) + max(0)
		},
		{
			name: "depth-exceeded",
			kind: compactionKindDefault,
			inputs: []compactionLevel{
				{
					level: 5,
					files: manifest.NewLevelSliceKeySorted(cmp, []*manifest.TableMetadata{
						makeTestTableMeta(
							cmp, 1, ikey("a", 5), ikey("m", 5), 3, 512, false),
					}),
				},
				{
					level: 6,
					files: manifest.NewLevelSliceKeySorted(cmp, []*manifest.TableMetadata{
						makeTestTableMeta(
							cmp, 2, ikey("a", 4), ikey("m", 4), 3, 512, false),
					}),
				},
			},
			// inputReferenceDepth = 3 + 3 = 6 > MaxBlobReferenceDepth (5)
			wantWriteBlobs:     true,
			wantReferenceDepth: 0,
			wantAnnotations:    []string{"write-blobs-input-depth-exceeded"},
		},
		{
			name: "min-size-mismatch",
			kind: compactionKindDefault,
			inputs: []compactionLevel{
				{
					level: 5,
					files: manifest.NewLevelSliceKeySorted(cmp, []*manifest.TableMetadata{
						// MinSize=256 differs from policy MinimumSize=512.
						makeTestTableMeta(
							cmp, 1, ikey("a", 5), ikey("m", 5), 1, 256, false),
					}),
				},
				{
					level: 6,
					files: manifest.NewLevelSliceKeySorted(cmp, []*manifest.TableMetadata{
						makeTestTableMeta(
							cmp, 2, ikey("a", 4), ikey("m", 4), 1, 512, false),
					}),
				},
			},
			wantWriteBlobs:     true,
			wantReferenceDepth: 0,
			wantAnnotations:    []string{"write-blobs-min-size-mismatch"},
		},
		{
			name: "suffix-disabled-mismatch",
			kind: compactionKindDefault,
			inputs: []compactionLevel{
				{
					level: 5,
					files: manifest.NewLevelSliceKeySorted(cmp, []*manifest.TableMetadata{
						// BySuffixDisabled=true but the default span policy expects false.
						makeTestTableMeta(
							cmp, 1, ikey("a", 5), ikey("m", 5), 1, 512, true),
					}),
				},
				{
					level: 6,
					files: manifest.NewLevelSliceKeySorted(cmp, []*manifest.TableMetadata{
						makeTestTableMeta(
							cmp, 2, ikey("a", 4), ikey("m", 4), 1, 512, false),
					}),
				},
			},
			wantWriteBlobs:     true,
			wantReferenceDepth: 0,
			wantAnnotations:    []string{"write-blobs-suffix-disabled-mismatch"},
		},
		{
			name: "multiple-span-policies",
			kind: compactionKindDefault,
			inputs: []compactionLevel{
				{
					level: 5,
					files: manifest.NewLevelSliceKeySorted(cmp, []*manifest.TableMetadata{
						// File spans a→z but the span policy only covers a→m.
						makeTestTableMeta(
							cmp, 1, ikey("a", 5), ikey("z", 5), 1, 512, false),
					}),
				},
				{
					level: 6,
					files: manifest.NewLevelSliceKeySorted(cmp, []*manifest.TableMetadata{
						makeTestTableMeta(
							cmp, 2, ikey("a", 4), ikey("m", 4), 1, 512, false),
					}),
				},
			},
			spanPolicyFunc: func(bounds base.UserKeyBounds) (base.SpanPolicy, error) {
				return base.SpanPolicy{
					KeyRange: base.KeyRange{Start: []byte("a"), End: []byte("m")},
				}, nil
			},
			wantWriteBlobs:     true,
			wantReferenceDepth: 0,
			wantAnnotations:    []string{"write-blobs-multiple-policies"},
		},
		{
			name: "all-matching",
			kind: compactionKindDefault,
			inputs: []compactionLevel{
				{
					level: 5,
					files: manifest.NewLevelSliceKeySorted(cmp, []*manifest.TableMetadata{
						makeTestTableMeta(
							cmp, 1, ikey("a", 5), ikey("m", 5), 2, 512, false),
					}),
				},
				{
					level: 6,
					files: manifest.NewLevelSliceKeySorted(cmp, []*manifest.TableMetadata{
						makeTestTableMeta(
							cmp, 2, ikey("a", 4), ikey("m", 4), 1, 512, false),
					}),
				},
			},
			wantWriteBlobs:     false,
			wantReferenceDepth: 3, // 2 + 1
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := tt.policy
			if policy == (ValueSeparationPolicy{}) {
				policy = defaultPolicy
			}
			spanPolicyFunc := tt.spanPolicyFunc
			if spanPolicyFunc == nil {
				spanPolicyFunc = defaultSpanPolicyFunc
			}
			c := &tableCompaction{
				kind:   tt.kind,
				inputs: tt.inputs,
			}
			writeBlobs, refDepth := shouldWriteBlobFiles(c, policy, spanPolicyFunc, cmp)
			require.Equal(t, tt.wantWriteBlobs, writeBlobs, "writeBlobs")
			require.Equal(t, tt.wantReferenceDepth, refDepth, "referenceDepth")
			if tt.wantAnnotations == nil {
				require.Empty(t, c.annotations, "annotations")
			} else {
				require.Equal(t, tt.wantAnnotations, c.annotations, "annotations")
			}
		})
	}
}
