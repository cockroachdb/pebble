// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package valsep

import (
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/blob"
)

// ValueSeparationOutputConfig is used to configure value separation for an
// individual compaction output.
type ValueSeparationOutputConfig struct {
	// MinimumSize is the minimum size of a value that will be separated into a
	// blob file.
	MinimumSize                    int
	DisableValueSeparationBySuffix bool
}

// ValueSeparation defines an interface for writing some values to separate blob
// files.
type ValueSeparation interface {
	// SetNextOutputConfig is called when a compaction is starting a new output
	// sstable. It can be used to configure value separation specifically for
	// the next compaction output.
	SetNextOutputConfig(config ValueSeparationOutputConfig)
	// Kind returns the kind of value separation strategy being used.
	Kind() sstable.ValueSeparationKind
	// MinimumSize returns the minimum size a value must be in order to be
	// separated into a blob file.
	MinimumSize() int
	// EstimatedFileSize returns an estimate of the disk space consumed by the
	// current, pending blob file if it were closed now. If no blob file has
	// been created, it returns 0.
	EstimatedFileSize() uint64
	// EstimatedReferenceSize returns an estimate of the disk space consumed by
	// the current output sstable's blob references so far.
	EstimatedReferenceSize() uint64
	// Add adds the provided key-value pair to the provided sstable writer,
	// possibly separating the value into a blob file.
	Add(tw sstable.RawWriter, kv *base.InternalKV, forceObsolete bool, isLikelyMVCCGarbage bool) error
	// FinishOutput is called when a compaction is finishing an output sstable.
	// It returns the table's blob references, which will be added to the
	// table's TableMetadata, and stats and metadata describing a newly
	// constructed blob file if any.
	FinishOutput() (ValueSeparationMetadata, error)
}

// NewBlobFileInfo describes a newly written blob file
// created by value separation. This is returned by
// ValueSeparation.FinishOutput.
type NewBlobFileInfo struct {
	FileStats    blob.FileWriterStats
	FileObject   objstorage.ObjectMetadata
	FileMetadata *manifest.PhysicalBlobFile
}

// ValueSeparationMetadata describes metadata about a table's blob references,
// and optionally a newly constructed blob file.
type ValueSeparationMetadata struct {
	BlobReferences     manifest.BlobReferences
	BlobReferenceSize  uint64
	BlobReferenceDepth manifest.BlobReferenceDepth

	// The below fields are only populated if a new blob file was created.
	NewBlobFiles []NewBlobFileInfo
}

// NeverSeparateValues is a ValueSeparation implementation that never separates
// values into external blob files. It is the default value if no
// ValueSeparation implementation is explicitly provided.
type NeverSeparateValues struct{}

// Assert that NeverSeparateValues implements the ValueSeparation interface.
var _ ValueSeparation = NeverSeparateValues{}

// SetNextOutputConfig implements the ValueSeparation interface.
func (NeverSeparateValues) SetNextOutputConfig(config ValueSeparationOutputConfig) {}

// Kind implements the ValueSeparation interface.
func (NeverSeparateValues) Kind() sstable.ValueSeparationKind {
	return sstable.ValueSeparationNone
}

// MinimumSize implements the ValueSeparation interface.
func (v NeverSeparateValues) MinimumSize() int { return 0 }

// EstimatedFileSize implements the ValueSeparation interface.
func (NeverSeparateValues) EstimatedFileSize() uint64 { return 0 }

// EstimatedReferenceSize implements the ValueSeparation interface.
func (NeverSeparateValues) EstimatedReferenceSize() uint64 { return 0 }

// Add implements the ValueSeparation interface.
func (NeverSeparateValues) Add(
	tw sstable.RawWriter, kv *base.InternalKV, forceObsolete bool, _ bool,
) error {
	v, _, err := kv.Value(nil)
	if err != nil {
		return err
	}
	return tw.Add(kv.K, v, forceObsolete)
}

// FinishOutput implements the ValueSeparation interface.
func (NeverSeparateValues) FinishOutput() (ValueSeparationMetadata, error) {
	return ValueSeparationMetadata{}, nil
}
