// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"bytes"
	stdcmp "cmp"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/strparse"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/block"
)

// Compare exports the base.Compare type.
type Compare = base.Compare

// InternalKey exports the base.InternalKey type.
type InternalKey = base.InternalKey

// TableInfo contains the common information for table related events.
type TableInfo struct {
	// FileNum is the internal DB identifier for the table.
	FileNum base.FileNum
	// Size is the size of the file in bytes.
	Size uint64
	// Smallest is the smallest internal key in the table.
	Smallest InternalKey
	// Largest is the largest internal key in the table.
	Largest InternalKey
	// SmallestSeqNum is the smallest sequence number in the table.
	SmallestSeqNum base.SeqNum
	// LargestSeqNum is the largest sequence number in the table.
	LargestSeqNum base.SeqNum
}

// TableStats contains statistics on a table used for compaction heuristics,
// and export via Metrics.
type TableStats struct {
	// The total number of entries in the table.
	NumEntries uint64
	// The number of point and range deletion entries in the table.
	NumDeletions uint64
	// NumRangeKeySets is the total number of range key sets in the table.
	//
	// NB: If there's a chance that the sstable contains any range key sets,
	// then NumRangeKeySets must be > 0.
	NumRangeKeySets uint64
	// Estimate of the total disk space that may be dropped by this table's
	// point deletions by compacting them.
	PointDeletionsBytesEstimate uint64
	// Estimate of the total disk space that may be dropped by this table's
	// range deletions by compacting them. This estimate is at data-block
	// granularity and is not updated if compactions beneath the table reduce
	// the amount of reclaimable disk space. It also does not account for
	// overlapping data in L0 and ignores L0 sublevels, but the error that
	// introduces is expected to be small.
	//
	// Tables in the bottommost level of the LSM may have a nonzero estimate if
	// snapshots or move compactions prevented the elision of their range
	// tombstones. A table in the bottommost level that was ingested into L6
	// will have a zero estimate, because the file's sequence numbers indicate
	// that the tombstone cannot drop any data contained within the file itself.
	RangeDeletionsBytesEstimate uint64
	// Total size of value blocks and value index block.
	ValueBlocksSize uint64
	// CompressionType is the compression type of the table.
	CompressionType block.Compression
	// TombstoneDenseBlocksRatio is the ratio of data blocks in this table that
	// fulfills at least one of the following:
	// 1. The block contains at least options.Experimental.NumDeletionsThreshold
	//    point tombstones.
	// 2. The ratio of the uncompressed size of point tombstones to the
	//    uncompressed size of the block is at least
	//    options.Experimental.DeletionSizeRatioThreshold.
	// This statistic is used to determine eligibility for a tombstone density
	// compaction.
	TombstoneDenseBlocksRatio float64
}

// boundType represents the type of key (point or range) present as the smallest
// and largest keys.
type boundType uint8

const (
	boundTypePointKey boundType = iota + 1
	boundTypeRangeKey
)

// CompactionState is the compaction state of a file.
//
// The following shows the valid state transitions:
//
//	NotCompacting --> Compacting --> Compacted
//	      ^               |
//	      |               |
//	      +-------<-------+
//
// Input files to a compaction transition to Compacting when a compaction is
// picked. A file that has finished compacting typically transitions into the
// Compacted state, at which point it is effectively obsolete ("zombied") and
// will eventually be removed from the LSM. A file that has been move-compacted
// will transition from Compacting back into the NotCompacting state, signaling
// that the file may be selected for a subsequent compaction. A failed
// compaction will result in all input tables transitioning from Compacting to
// NotCompacting.
//
// This state is in-memory only. It is not persisted to the manifest.
type CompactionState uint8

// CompactionStates.
const (
	CompactionStateNotCompacting CompactionState = iota
	CompactionStateCompacting
	CompactionStateCompacted
)

// String implements fmt.Stringer.
func (s CompactionState) String() string {
	switch s {
	case CompactionStateNotCompacting:
		return "NotCompacting"
	case CompactionStateCompacting:
		return "Compacting"
	case CompactionStateCompacted:
		return "Compacted"
	default:
		panic(fmt.Sprintf("pebble: unknown compaction state %d", s))
	}
}

// TableMetadata is maintained for leveled-ssts, i.e., they belong to a level of
// some version. TableMetadata does not contain the actual level of the sst,
// since such leveled-ssts can move across levels in different versions, while
// sharing the same TableMetadata. There are two kinds of leveled-ssts, physical
// and virtual. Underlying both leveled-ssts is a backing-sst, for which the
// only state is FileBacking. A backing-sst is level-less. It is possible for a
// backing-sst to be referred to by a physical sst in one version and by one or
// more virtual ssts in one or more versions. A backing-sst becomes obsolete and
// can be deleted once it is no longer required by any physical or virtual sst
// in any version.
//
// We maintain some invariants:
//
//  1. Each physical and virtual sst will have a unique TableMetadata.FileNum,
//     and there will be exactly one TableMetadata associated with the FileNum.
//
//  2. Within a version, a backing-sst is either only referred to by one
//     physical sst or one or more virtual ssts.
//
//  3. Once a backing-sst is referred to by a virtual sst in the latest version,
//     it cannot go back to being referred to by a physical sst in any future
//     version.
//
// Once a physical sst is no longer needed by any version, we will no longer
// maintain the table metadata associated with it. We will still maintain the
// FileBacking associated with the physical sst if the backing sst is required
// by any virtual ssts in any version.
type TableMetadata struct {
	// AllowedSeeks is used to determine if a file should be picked for
	// a read triggered compaction. It is decremented when read sampling
	// in pebble.Iterator after every after every positioning operation
	// that returns a user key (eg. Next, Prev, SeekGE, SeekLT, etc).
	AllowedSeeks atomic.Int64

	// statsValid indicates if stats have been loaded for the table. The
	// TableStats structure is populated only if valid is true.
	statsValid atomic.Bool

	// FileBacking is the state which backs either a physical or virtual
	// sstables.
	FileBacking *FileBacking

	// InitAllowedSeeks is the inital value of allowed seeks. This is used
	// to re-set allowed seeks on a file once it hits 0.
	InitAllowedSeeks int64
	// FileNum is the file number.
	//
	// INVARIANT: when !TableMetadata.Virtual, FileNum == FileBacking.DiskFileNum.
	FileNum base.FileNum
	// Size is the size of the file, in bytes. Size is an approximate value for
	// virtual sstables.
	//
	// INVARIANTS:
	// - When !TableMetadata.Virtual, Size == FileBacking.Size.
	// - Size should be non-zero. Size 0 virtual sstables must not be created.
	Size uint64
	// File creation time in seconds since the epoch (1970-01-01 00:00:00
	// UTC). For ingested sstables, this corresponds to the time the file was
	// ingested. For virtual sstables, this corresponds to the wall clock time
	// when the TableMetadata for the virtual sstable was first created.
	CreationTime int64
	// LargestSeqNumAbsolute is an upper bound for the largest sequence number
	// in the table. This upper bound is guaranteed to be higher than any
	// sequence number any of the table's keys have held at any point in time
	// while the database has been open. Specifically, if the table contains
	// keys that have had their sequence numbers zeroed during a compaction,
	// LargestSeqNumAbsolute will be at least as high as the pre-zeroing
	// sequence number. LargestSeqNumAbsolute is NOT durably persisted, so after
	// a database restart it takes on the value of LargestSeqNum.
	LargestSeqNumAbsolute base.SeqNum
	// Lower and upper bounds for the smallest and largest sequence numbers in
	// the table, across both point and range keys. For physical sstables, these
	// values are tight bounds. For virtual sstables, there is no guarantee that
	// there will be keys with SmallestSeqNum or LargestSeqNum within virtual
	// sstable bounds.
	SmallestSeqNum base.SeqNum
	LargestSeqNum  base.SeqNum
	// SmallestPointKey and LargestPointKey are the inclusive bounds for the
	// internal point keys stored in the table. This includes RANGEDELs, which
	// alter point keys.
	// NB: these field should be set using ExtendPointKeyBounds. They are left
	// exported for reads as an optimization.
	SmallestPointKey InternalKey
	LargestPointKey  InternalKey
	// SmallestRangeKey and LargestRangeKey are the inclusive bounds for the
	// internal range keys stored in the table.
	// NB: these field should be set using ExtendRangeKeyBounds. They are left
	// exported for reads as an optimization.
	SmallestRangeKey InternalKey
	LargestRangeKey  InternalKey
	// Smallest and Largest are the inclusive bounds for the internal keys stored
	// in the table, across both point and range keys.
	// NB: these fields are derived from their point and range key equivalents,
	// and are updated via the MaybeExtend{Point,Range}KeyBounds methods.
	Smallest InternalKey
	Largest  InternalKey
	// BlobReferences is a list of blob files containing values that are
	// referenced by this sstable.
	BlobReferences []BlobReference
	// BlobReferenceDepth is an upper bound on the number of blob files that a
	// reader scanning the table would need to keep open if they only open and
	// close referenced blob files once. In other words, it's the stack depth of
	// blob files referenced by this sstable. If a flush or compaction rewrites
	// an sstable's values to a new blob file, the resulting sstable has a blob
	// reference depth of 1. When a compaction reuses blob references, the max
	// blob reference depth of the files in each level is used, and then the
	// depth is summed, and assigned to the output. This is a loose upper bound
	// (assuming worst case distribution of keys in all inputs) but avoids
	// tracking key spans for references and using key comparisons.
	//
	// Because the blob reference depth is the size of the working set of blob
	// files referenced by the table, it cannot exceed the count of distinct
	// blob file references.
	//
	// Example: Consider a compaction of file f0 from L0 and files f1, f2, f3
	// from L1, where the former has blob reference depth of 1 and files f1, f2,
	// f3 all happen to have a blob-reference-depth of 1. Say we produce many
	// output files, one of which is f4. We are assuming here that the blobs
	// referenced by f0 whose keys happened to be written to f4 are spread all
	// across the key span of f4. Say keys from f1 and f2 also made their way to
	// f4. Then we will first have keys that refer to blobs referenced by f1,f0
	// and at some point once we move past the keys of f1, we will have keys
	// that refer to blobs referenced by f2,f0. In some sense, we have a working
	// set of 2 blob files at any point in time, and this is similar to the idea
	// of level stack depth for reads -- hence we adopt the depth terminology.
	// We want to keep this stack depth in check, since locality is important,
	// while allowing it to be higher than 1, since otherwise we will need to
	// rewrite blob files in every compaction (defeating the write amp benefit
	// we are looking for). Similar to the level depth, this simplistic analysis
	// does not take into account distribution of keys involved in the
	// compaction and which of them have blob references. Also the locality is
	// actually better than in this analysis because more of the keys will be
	// from the lower level.
	//
	// INVARIANT: BlobReferenceDepth == 0 iff len(BlobReferences) == 0
	// INVARIANT: BlobReferenceDepth <= len(BlobReferences)
	BlobReferenceDepth int

	// refs is the reference count for the table, used to determine when a table
	// is obsolete. When a table's reference count falls to zero, the table is
	// considered obsolete and the table's references on its associated files
	// (backing file, blob references) are released.
	//
	// The tables in each version are maintained in a copy-on-write B-tree and
	// each B-tree node keeps a reference on the contained tables.
	refs atomic.Int32

	// Stats describe table statistics. Protected by DB.mu.
	//
	// For virtual sstables, set stats upon virtual sstable creation as
	// asynchronous computation of stats is not currently supported.
	//
	// TODO(bananabrick): To support manifest replay for virtual sstables, we
	// probably need to compute virtual sstable stats asynchronously. Otherwise,
	// we'd have to write virtual sstable stats to the version edit.
	Stats TableStats

	// For L0 files only. Protected by DB.mu. Used to generate L0 sublevels and
	// pick L0 compactions. Only accurate for the most recent Version.
	// TODO(radu): this is very hacky and fragile. This information should live
	// inside L0Sublevels.
	SubLevel         int
	L0Index          int
	minIntervalIndex int
	maxIntervalIndex int

	// NB: the alignment of this struct is 8 bytes. We pack all the bools to
	// ensure an optimal packing.

	// IsIntraL0Compacting is set to True if this file is part of an intra-L0
	// compaction. When it's true, IsCompacting must also return true. If
	// Compacting is true and IsIntraL0Compacting is false for an L0 file, the
	// file must be part of a compaction to Lbase.
	IsIntraL0Compacting bool
	CompactionState     CompactionState
	// True if compaction of this file has been explicitly requested.
	// Previously, RocksDB and earlier versions of Pebble allowed this
	// flag to be set by a user table property collector. Some earlier
	// versions of Pebble respected this flag, while other more recent
	// versions ignored this flag.
	//
	// More recently this flag has been repurposed to facilitate the
	// compaction of 'atomic compaction units'. Files marked for
	// compaction are compacted in a rewrite compaction at the lowest
	// possible compaction priority.
	//
	// NB: A count of files marked for compaction is maintained on
	// Version, and compaction picking reads cached annotations
	// determined by this field.
	//
	// Protected by DB.mu.
	MarkedForCompaction bool
	// HasPointKeys tracks whether the table contains point keys (including
	// RANGEDELs). If a table contains only range deletions, HasPointsKeys is
	// still true.
	HasPointKeys bool
	// HasRangeKeys tracks whether the table contains any range keys.
	HasRangeKeys bool
	// smallestSet and largestSet track whether the overall bounds have been set.
	boundsSet bool
	// boundTypeSmallest and boundTypeLargest provide an indication as to which
	// key type (point or range) corresponds to the smallest and largest overall
	// table bounds.
	boundTypeSmallest, boundTypeLargest boundType
	// Virtual is true if the TableMetadata belongs to a virtual sstable.
	Virtual bool

	// SyntheticPrefix is used to prepend a prefix to all keys and/or override all
	// suffixes in a table; used for some virtual tables.
	SyntheticPrefixAndSuffix sstable.SyntheticPrefixAndSuffix
}

// Ref increments the table's ref count. If this is the table's first reference,
// Ref will increment the reference of the table's FileBacking and referenced
// blob files.
func (m *TableMetadata) Ref() {
	if v := m.refs.Add(1); v == 1 {
		m.FileBacking.Ref()
		for _, blobRef := range m.BlobReferences {
			// BlobReference.Metadata is allowed to be nil when first decoded
			// from a VersionEdit, but BulkVersionEdit.Accumulate should have
			// populated the field (during manifest replay), or we should have
			// set it explicitly when constructing the TableMetadata (during
			// compactions, flushes).
			if blobRef.Metadata == nil {
				panic(errors.AssertionFailedf("%s reference to blob %s has no metadata", m.FileNum, blobRef.FileNum))
			}
			blobRef.Metadata.ref()
		}
	}
}

// Unref decrements the table's reference count. If the count reaches zero, the
// table releases its references on associated files. If any referenced files
// become obsolete, they're inserted into the provided ObsoleteFiles.
func (m *TableMetadata) Unref(obsoleteFiles ObsoleteFilesSet) {
	v := m.refs.Add(-1)
	if invariants.Enabled && v < 0 {
		panic(errors.AssertionFailedf("pebble: invalid TableMetadata refcounting for table %s", m.FileNum))
	}
	// When the reference count reaches zero, release the table's references.
	if v == 0 {
		if m.FileBacking.Unref() == 0 {
			obsoleteFiles.AddBacking(m.FileBacking)
		}
		for _, blobRef := range m.BlobReferences {
			if blobRef.Metadata.unref() == 0 {
				obsoleteFiles.AddBlob(blobRef.Metadata)
			}
		}
	}
}

// InternalKeyBounds returns the set of overall table bounds.
func (m *TableMetadata) InternalKeyBounds() (InternalKey, InternalKey) {
	return m.Smallest, m.Largest
}

// UserKeyBounds returns the user key bounds that correspond to m.Smallest and
// Largest. Because we do not allow split user keys, the user key bounds of
// files within a level do not overlap.
func (m *TableMetadata) UserKeyBounds() base.UserKeyBounds {
	return base.UserKeyBoundsFromInternal(m.Smallest, m.Largest)
}

// UserKeyBoundsByType returns the user key bounds for the given key types.
// Note that the returned bounds are invalid when requesting KeyTypePoint but
// HasPointKeys is false, or when requesting KeyTypeRange and HasRangeKeys is
// false.
func (m *TableMetadata) UserKeyBoundsByType(keyType KeyType) base.UserKeyBounds {
	switch keyType {
	case KeyTypePoint:
		return base.UserKeyBoundsFromInternal(m.SmallestPointKey, m.LargestPointKey)
	case KeyTypeRange:
		return base.UserKeyBoundsFromInternal(m.SmallestRangeKey, m.LargestRangeKey)
	default:
		return base.UserKeyBoundsFromInternal(m.Smallest, m.Largest)
	}
}

// SyntheticSeqNum returns a SyntheticSeqNum which is set when SmallestSeqNum
// equals LargestSeqNum.
func (m *TableMetadata) SyntheticSeqNum() sstable.SyntheticSeqNum {
	if m.SmallestSeqNum == m.LargestSeqNum {
		return sstable.SyntheticSeqNum(m.SmallestSeqNum)
	}
	return sstable.NoSyntheticSeqNum
}

// IterTransforms returns an sstable.IterTransforms populated according to the
// file.
func (m *TableMetadata) IterTransforms() sstable.IterTransforms {
	return sstable.IterTransforms{
		SyntheticSeqNum:          m.SyntheticSeqNum(),
		SyntheticPrefixAndSuffix: m.SyntheticPrefixAndSuffix,
	}
}

// FragmentIterTransforms returns an sstable.FragmentIterTransforms populated
// according to the file.
func (m *TableMetadata) FragmentIterTransforms() sstable.FragmentIterTransforms {
	return sstable.FragmentIterTransforms{
		SyntheticSeqNum:          m.SyntheticSeqNum(),
		SyntheticPrefixAndSuffix: m.SyntheticPrefixAndSuffix,
	}
}

// PhysicalTableMeta is used by functions which want a guarantee that their input
// belongs to a physical sst and not a virtual sst.
//
// NB: This type should only be constructed by calling
// TableMetadata.PhysicalMeta.
type PhysicalTableMeta struct {
	*TableMetadata
}

// VirtualTableMeta is used by functions which want a guarantee that their input
// belongs to a virtual sst and not a physical sst.
//
// A VirtualTableMeta inherits all the same fields as a TableMetadata. These
// fields have additional invariants imposed on them, and/or slightly varying
// meanings:
//   - Smallest and Largest (and their counterparts
//     {Smallest, Largest}{Point,Range}Key) remain tight bounds that represent a
//     key at that exact bound. We make the effort to determine the next smallest
//     or largest key in an sstable after virtualizing it, to maintain this
//     tightness. If the largest is a sentinel key (IsExclusiveSentinel()), it
//     could mean that a rangedel or range key ends at that user key, or has been
//     truncated to that user key.
//   - One invariant is that if a rangedel or range key is truncated on its
//     upper bound, the virtual sstable *must* have a rangedel or range key
//     sentinel key as its upper bound. This is because truncation yields
//     an exclusive upper bound for the rangedel/rangekey, and if there are
//     any points at that exclusive upper bound within the same virtual
//     sstable, those could get uncovered by this truncation. We enforce this
//     invariant in calls to keyspan.Truncate.
//   - Size is an estimate of the size of the virtualized portion of this sstable.
//     The underlying file's size is stored in FileBacking.Size, though it could
//     also be estimated or could correspond to just the referenced portion of
//     a file (eg. if the file originated on another node).
//   - Size must be > 0.
//   - SmallestSeqNum and LargestSeqNum are loose bounds for virtual sstables.
//     This means that all keys in the virtual sstable must have seqnums within
//     [SmallestSeqNum, LargestSeqNum], however there's no guarantee that there's
//     a key with a seqnum at either of the bounds. Calculating tight seqnum
//     bounds would be too expensive and deliver little value.
//
// NB: This type should only be constructed by calling TableMetadata.VirtualMeta.
type VirtualTableMeta struct {
	*TableMetadata
}

// VirtualReaderParams fills in the parameters necessary to create a virtual
// sstable reader.
func (m VirtualTableMeta) VirtualReaderParams(isShared bool) sstable.VirtualReaderParams {
	return sstable.VirtualReaderParams{
		Lower:            m.Smallest,
		Upper:            m.Largest,
		FileNum:          m.FileNum,
		IsSharedIngested: isShared && m.SyntheticSeqNum() != 0,
		Size:             m.Size,
		BackingSize:      m.FileBacking.Size,
	}
}

// PhysicalMeta should be the only source of creating the PhysicalFileMeta
// wrapper type.
func (m *TableMetadata) PhysicalMeta() PhysicalTableMeta {
	if m.Virtual {
		panic("pebble: table metadata does not belong to a physical sstable")
	}
	return PhysicalTableMeta{
		m,
	}
}

// VirtualMeta should be the only source of creating the VirtualFileMeta wrapper
// type.
func (m *TableMetadata) VirtualMeta() VirtualTableMeta {
	if !m.Virtual {
		panic("pebble: table metadata does not belong to a virtual sstable")
	}
	return VirtualTableMeta{
		m,
	}
}

// FileBacking either backs a single physical sstable, or one or more virtual
// sstables.
//
// See the comment above the TableMetadata type for sstable terminology.
type FileBacking struct {
	DiskFileNum base.DiskFileNum
	Size        uint64

	// Reference count for the backing file, used to determine when a backing file
	// is obsolete and can be removed.
	//
	// The reference count is at least the number of distinct tables that use this
	// backing across all versions that have a non-zero reference count. The tables
	// in each version are maintained in a copy-on-write B-tree and each B-tree node
	// keeps a reference on the respective backings.
	//
	// In addition, a reference count is taken for every backing in the latest
	// version's VirtualBackings (necessary to support Protect/Unprotect).
	refs atomic.Int32
}

// MustHaveRefs asserts that the backing has a positive refcount.
func (b *FileBacking) MustHaveRefs() {
	if refs := b.refs.Load(); refs <= 0 {
		panic(errors.AssertionFailedf("backing %s must have positive refcount (refs=%d)",
			b.DiskFileNum, refs))
	}
}

// Ref increments the backing's ref count.
func (b *FileBacking) Ref() {
	b.refs.Add(1)
}

// IsUnused returns if the backing is not being used by any tables in a version
// or btree.
func (b *FileBacking) IsUnused() bool {
	return b.refs.Load() == 0
}

// Unref decrements the backing's ref count (and returns the new count).
func (b *FileBacking) Unref() int32 {
	v := b.refs.Add(-1)
	if invariants.Enabled && v < 0 {
		panic(errors.AssertionFailedf("pebble: invalid FileBacking refcounting: file %s has refcount %d", b.DiskFileNum, v))
	}
	return v
}

// InitPhysicalBacking allocates and sets the FileBacking which is required by a
// physical sstable TableMetadata.
//
// Ensure that the state required by FileBacking, such as the FileNum, is
// already set on the TableMetadata before InitPhysicalBacking is called.
// Calling InitPhysicalBacking only after the relevant state has been set in the
// TableMetadata is not necessary in tests which don't rely on FileBacking.
func (m *TableMetadata) InitPhysicalBacking() {
	if m.Virtual {
		panic("pebble: virtual sstables should use a pre-existing FileBacking")
	}
	if m.FileBacking == nil {
		m.FileBacking = &FileBacking{
			DiskFileNum: base.PhysicalTableDiskFileNum(m.FileNum),
			Size:        m.Size,
		}
	}
}

// InitProviderBacking creates a new FileBacking for a file backed by
// an objstorage.Provider.
func (m *TableMetadata) InitProviderBacking(fileNum base.DiskFileNum, size uint64) {
	if !m.Virtual {
		panic("pebble: provider-backed sstables must be virtual")
	}
	if m.FileBacking == nil {
		m.FileBacking = &FileBacking{DiskFileNum: fileNum}
	}
	m.FileBacking.Size = size
}

// ValidateVirtual should be called once the TableMetadata for a virtual sstable
// is created to verify that the fields of the virtual sstable are sound.
func (m *TableMetadata) ValidateVirtual(createdFrom *TableMetadata) {
	switch {
	case !m.Virtual:
		panic("pebble: invalid virtual sstable")
	case createdFrom.SmallestSeqNum != m.SmallestSeqNum:
		panic("pebble: invalid smallest sequence number for virtual sstable")
	case createdFrom.LargestSeqNum != m.LargestSeqNum:
		panic("pebble: invalid largest sequence number for virtual sstable")
	case createdFrom.LargestSeqNumAbsolute != m.LargestSeqNumAbsolute:
		panic("pebble: invalid largest absolute sequence number for virtual sstable")
	case createdFrom.FileBacking != nil && createdFrom.FileBacking != m.FileBacking:
		panic("pebble: invalid physical sstable state for virtual sstable")
	case m.Size == 0:
		panic("pebble: virtual sstable size must be set upon creation")
	}
}

// SetCompactionState transitions this file's compaction state to the given
// state. Protected by DB.mu.
func (m *TableMetadata) SetCompactionState(to CompactionState) {
	if invariants.Enabled {
		transitionErr := func() error {
			return errors.Newf("pebble: invalid compaction state transition: %s -> %s", m.CompactionState, to)
		}
		switch m.CompactionState {
		case CompactionStateNotCompacting:
			if to != CompactionStateCompacting {
				panic(transitionErr())
			}
		case CompactionStateCompacting:
			if to != CompactionStateCompacted && to != CompactionStateNotCompacting {
				panic(transitionErr())
			}
		case CompactionStateCompacted:
			panic(transitionErr())
		default:
			panic(fmt.Sprintf("pebble: unknown compaction state: %d", m.CompactionState))
		}
	}
	m.CompactionState = to
}

// IsCompacting returns true if this file's compaction state is
// CompactionStateCompacting. Protected by DB.mu.
func (m *TableMetadata) IsCompacting() bool {
	return m.CompactionState == CompactionStateCompacting
}

// StatsValid returns true if the table stats have been populated. If StatValid
// returns true, the Stats field may be read (with or without holding the
// database mutex).
func (m *TableMetadata) StatsValid() bool {
	return m.statsValid.Load()
}

// StatsMarkValid marks the TableStats as valid. The caller must hold DB.mu
// while populating TableStats and calling StatsMarkValud. Once stats are
// populated, they must not be mutated.
func (m *TableMetadata) StatsMarkValid() {
	m.statsValid.Store(true)
}

// ExtendPointKeyBounds attempts to extend the lower and upper point key bounds
// and overall table bounds with the given smallest and largest keys. The
// smallest and largest bounds may not be extended if the table already has a
// bound that is smaller or larger, respectively. The receiver is returned.
// NB: calling this method should be preferred to manually setting the bounds by
// manipulating the fields directly, to maintain certain invariants.
func (m *TableMetadata) ExtendPointKeyBounds(
	cmp Compare, smallest, largest InternalKey,
) *TableMetadata {
	// Update the point key bounds.
	if !m.HasPointKeys {
		m.SmallestPointKey, m.LargestPointKey = smallest, largest
		m.HasPointKeys = true
	} else {
		if base.InternalCompare(cmp, smallest, m.SmallestPointKey) < 0 {
			m.SmallestPointKey = smallest
		}
		if base.InternalCompare(cmp, largest, m.LargestPointKey) > 0 {
			m.LargestPointKey = largest
		}
	}
	// Update the overall bounds.
	m.extendOverallBounds(cmp, m.SmallestPointKey, m.LargestPointKey, boundTypePointKey)
	return m
}

// ExtendRangeKeyBounds attempts to extend the lower and upper range key bounds
// and overall table bounds with the given smallest and largest keys. The
// smallest and largest bounds may not be extended if the table already has a
// bound that is smaller or larger, respectively. The receiver is returned.
// NB: calling this method should be preferred to manually setting the bounds by
// manipulating the fields directly, to maintain certain invariants.
func (m *TableMetadata) ExtendRangeKeyBounds(
	cmp Compare, smallest, largest InternalKey,
) *TableMetadata {
	// Update the range key bounds.
	if !m.HasRangeKeys {
		m.SmallestRangeKey, m.LargestRangeKey = smallest, largest
		m.HasRangeKeys = true
	} else {
		if base.InternalCompare(cmp, smallest, m.SmallestRangeKey) < 0 {
			m.SmallestRangeKey = smallest
		}
		if base.InternalCompare(cmp, largest, m.LargestRangeKey) > 0 {
			m.LargestRangeKey = largest
		}
	}
	// Update the overall bounds.
	m.extendOverallBounds(cmp, m.SmallestRangeKey, m.LargestRangeKey, boundTypeRangeKey)
	return m
}

// extendOverallBounds attempts to extend the overall table lower and upper
// bounds. The given bounds may not be used if a lower or upper bound already
// exists that is smaller or larger than the given keys, respectively. The given
// boundType will be used if the bounds are updated.
func (m *TableMetadata) extendOverallBounds(
	cmp Compare, smallest, largest InternalKey, bTyp boundType,
) {
	if !m.boundsSet {
		m.Smallest, m.Largest = smallest, largest
		m.boundsSet = true
		m.boundTypeSmallest, m.boundTypeLargest = bTyp, bTyp
	} else {
		if base.InternalCompare(cmp, smallest, m.Smallest) < 0 {
			m.Smallest = smallest
			m.boundTypeSmallest = bTyp
		}
		if base.InternalCompare(cmp, largest, m.Largest) > 0 {
			m.Largest = largest
			m.boundTypeLargest = bTyp
		}
	}
}

// Overlaps returns true if the file key range overlaps with the given user key bounds.
func (m *TableMetadata) Overlaps(cmp Compare, bounds *base.UserKeyBounds) bool {
	b := m.UserKeyBounds()
	return b.Overlaps(cmp, bounds)
}

// ContainedWithinSpan returns true if the file key range completely overlaps with the
// given range ("end" is assumed to exclusive).
func (m *TableMetadata) ContainedWithinSpan(cmp Compare, start, end []byte) bool {
	lowerCmp, upperCmp := cmp(m.Smallest.UserKey, start), cmp(m.Largest.UserKey, end)
	return lowerCmp >= 0 && (upperCmp < 0 || (upperCmp == 0 && m.Largest.IsExclusiveSentinel()))
}

// ContainsKeyType returns whether or not the file contains keys of the provided
// type.
func (m *TableMetadata) ContainsKeyType(kt KeyType) bool {
	switch kt {
	case KeyTypePointAndRange:
		return true
	case KeyTypePoint:
		return m.HasPointKeys
	case KeyTypeRange:
		return m.HasRangeKeys
	default:
		panic("unrecognized key type")
	}
}

// SmallestBound returns the file's smallest bound of the key type. It returns a
// false second return value if the file does not contain any keys of the key
// type.
func (m *TableMetadata) SmallestBound(kt KeyType) (*InternalKey, bool) {
	switch kt {
	case KeyTypePointAndRange:
		return &m.Smallest, true
	case KeyTypePoint:
		return &m.SmallestPointKey, m.HasPointKeys
	case KeyTypeRange:
		return &m.SmallestRangeKey, m.HasRangeKeys
	default:
		panic("unrecognized key type")
	}
}

// LargestBound returns the file's largest bound of the key type. It returns a
// false second return value if the file does not contain any keys of the key
// type.
func (m *TableMetadata) LargestBound(kt KeyType) (*InternalKey, bool) {
	switch kt {
	case KeyTypePointAndRange:
		return &m.Largest, true
	case KeyTypePoint:
		return &m.LargestPointKey, m.HasPointKeys
	case KeyTypeRange:
		return &m.LargestRangeKey, m.HasRangeKeys
	default:
		panic("unrecognized key type")
	}
}

const (
	maskContainsPointKeys = 1 << 0
	maskSmallest          = 1 << 1
	maskLargest           = 1 << 2
)

// boundsMarker returns a marker byte whose bits encode the following
// information (in order from least significant bit):
// - if the table contains point keys
// - if the table's smallest key is a point key
// - if the table's largest key is a point key
func (m *TableMetadata) boundsMarker() (sentinel uint8, err error) {
	if m.HasPointKeys {
		sentinel |= maskContainsPointKeys
	}
	switch m.boundTypeSmallest {
	case boundTypePointKey:
		sentinel |= maskSmallest
	case boundTypeRangeKey:
		// No op - leave bit unset.
	default:
		return 0, base.CorruptionErrorf("file %s has neither point nor range key as smallest key", m.FileNum)
	}
	switch m.boundTypeLargest {
	case boundTypePointKey:
		sentinel |= maskLargest
	case boundTypeRangeKey:
		// No op - leave bit unset.
	default:
		return 0, base.CorruptionErrorf("file %s has neither point nor range key as largest key", m.FileNum)
	}
	return
}

// String implements fmt.Stringer, printing the file number and the overall
// table bounds.
func (m *TableMetadata) String() string {
	return fmt.Sprintf("%s:[%s-%s]", m.FileNum, m.Smallest, m.Largest)
}

// DebugString returns a verbose representation of TableMetadata, typically for
// use in tests and debugging, returning the file number and the point, range
// and overall bounds for the table.
func (m *TableMetadata) DebugString(format base.FormatKey, verbose bool) string {
	var b bytes.Buffer
	if m.Virtual {
		fmt.Fprintf(&b, "%s(%s):[%s-%s]",
			m.FileNum, m.FileBacking.DiskFileNum, m.Smallest.Pretty(format), m.Largest.Pretty(format))
	} else {
		fmt.Fprintf(&b, "%s:[%s-%s]",
			m.FileNum, m.Smallest.Pretty(format), m.Largest.Pretty(format))
	}
	if !verbose {
		return b.String()
	}
	fmt.Fprintf(&b, " seqnums:[%d-%d]", m.SmallestSeqNum, m.LargestSeqNum)
	if m.HasPointKeys {
		fmt.Fprintf(&b, " points:[%s-%s]",
			m.SmallestPointKey.Pretty(format), m.LargestPointKey.Pretty(format))
	}
	if m.HasRangeKeys {
		fmt.Fprintf(&b, " ranges:[%s-%s]",
			m.SmallestRangeKey.Pretty(format), m.LargestRangeKey.Pretty(format))
	}
	if m.Size != 0 {
		fmt.Fprintf(&b, " size:%d", m.Size)
	}
	if len(m.BlobReferences) > 0 {
		fmt.Fprint(&b, " blobrefs:[")
		for i, r := range m.BlobReferences {
			if i > 0 {
				fmt.Fprint(&b, ", ")
			}
			fmt.Fprintf(&b, "(%s: %d)", r.FileNum, r.ValueSize)
		}
		fmt.Fprintf(&b, "; depth:%d]", m.BlobReferenceDepth)
	}
	return b.String()
}

const debugParserSeparators = ":-[]();"

// errFromPanic can be used in a recover block to convert panics into errors.
func errFromPanic(r any) error {
	if err, ok := r.(error); ok {
		return err
	}
	return errors.Errorf("%v", r)
}

// ParseTableMetadataDebug parses a TableMetadata from its DebugString
// representation.
func ParseTableMetadataDebug(s string) (_ *TableMetadata, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.CombineErrors(err, errFromPanic(r))
		}
	}()

	// Input format:
	//	000000:[a#0,SET-z#0,SET] seqnums:[5-5] points:[...] ranges:[...] size:5
	m := &TableMetadata{}
	p := strparse.MakeParser(debugParserSeparators, s)
	m.FileNum = p.FileNum()
	var backingNum base.DiskFileNum
	if p.Peek() == "(" {
		p.Expect("(")
		backingNum = p.DiskFileNum()
		p.Expect(")")
	}
	p.Expect(":", "[")
	m.Smallest = p.InternalKey()
	p.Expect("-")
	m.Largest = p.InternalKey()
	p.Expect("]")

	for !p.Done() {
		field := p.Next()
		p.Expect(":")
		switch field {
		case "seqnums":
			p.Expect("[")
			m.SmallestSeqNum = p.SeqNum()
			p.Expect("-")
			m.LargestSeqNum = p.SeqNum()
			p.Expect("]")
			m.LargestSeqNumAbsolute = m.LargestSeqNum

		case "points":
			p.Expect("[")
			m.SmallestPointKey = p.InternalKey()
			p.Expect("-")
			m.LargestPointKey = p.InternalKey()
			m.HasPointKeys = true
			p.Expect("]")

		case "ranges":
			p.Expect("[")
			m.SmallestRangeKey = p.InternalKey()
			p.Expect("-")
			m.LargestRangeKey = p.InternalKey()
			m.HasRangeKeys = true
			p.Expect("]")

		case "size":
			m.Size = p.Uint64()

		case "blobrefs":
			p.Expect("[")
			for p.Peek() != ";" {
				if p.Peek() == "," {
					p.Expect(",")
				}
				p.Expect("(")
				var ref BlobReference
				ref.FileNum = base.DiskFileNum(p.FileNum())
				p.Expect(":")
				ref.ValueSize = p.Uint64()
				m.BlobReferences = append(m.BlobReferences, ref)
				p.Expect(")")
			}
			p.Expect(";")
			p.Expect("depth")
			p.Expect(":")
			m.BlobReferenceDepth = int(p.Uint64())
			p.Expect("]")

		default:
			p.Errf("unknown field %q", field)
		}
	}

	// By default, when the parser sees just the overall bounds, we set the point
	// keys. This preserves backwards compatability with existing test cases that
	// specify only the overall bounds.
	if !m.HasPointKeys && !m.HasRangeKeys {
		m.SmallestPointKey, m.LargestPointKey = m.Smallest, m.Largest
		m.HasPointKeys = true
	}
	if backingNum == 0 {
		m.InitPhysicalBacking()
	} else {
		m.Virtual = true
		m.InitProviderBacking(backingNum, 0 /* size */)
	}
	return m, nil
}

// Validate validates the metadata for consistency with itself, returning an
// error if inconsistent.
func (m *TableMetadata) Validate(cmp Compare, formatKey base.FormatKey) error {
	// Combined range and point key validation.

	if !m.HasPointKeys && !m.HasRangeKeys {
		return base.CorruptionErrorf("file %s has neither point nor range keys",
			errors.Safe(m.FileNum))
	}
	if base.InternalCompare(cmp, m.Smallest, m.Largest) > 0 {
		return base.CorruptionErrorf("file %s has inconsistent bounds: %s vs %s",
			errors.Safe(m.FileNum), m.Smallest.Pretty(formatKey),
			m.Largest.Pretty(formatKey))
	}
	if m.SmallestSeqNum > m.LargestSeqNum {
		return base.CorruptionErrorf("file %s has inconsistent seqnum bounds: %d vs %d",
			errors.Safe(m.FileNum), m.SmallestSeqNum, m.LargestSeqNum)
	}
	if m.LargestSeqNumAbsolute < m.LargestSeqNum {
		return base.CorruptionErrorf("file %s has inconsistent absolute largest seqnum bounds: %d vs %d",
			errors.Safe(m.FileNum), m.LargestSeqNumAbsolute, m.LargestSeqNum)
	}

	// Point key validation.

	if m.HasPointKeys {
		if base.InternalCompare(cmp, m.SmallestPointKey, m.LargestPointKey) > 0 {
			return base.CorruptionErrorf("file %s has inconsistent point key bounds: %s vs %s",
				errors.Safe(m.FileNum), m.SmallestPointKey.Pretty(formatKey),
				m.LargestPointKey.Pretty(formatKey))
		}
		if base.InternalCompare(cmp, m.SmallestPointKey, m.Smallest) < 0 ||
			base.InternalCompare(cmp, m.LargestPointKey, m.Largest) > 0 {
			return base.CorruptionErrorf(
				"file %s has inconsistent point key bounds relative to overall bounds: "+
					"overall = [%s-%s], point keys = [%s-%s]",
				errors.Safe(m.FileNum),
				m.Smallest.Pretty(formatKey), m.Largest.Pretty(formatKey),
				m.SmallestPointKey.Pretty(formatKey), m.LargestPointKey.Pretty(formatKey),
			)
		}
		if !isValidPointBoundKeyKind[m.SmallestPointKey.Kind()] {
			return base.CorruptionErrorf("file %s has invalid smallest point key kind", m)
		}
		if !isValidPointBoundKeyKind[m.LargestPointKey.Kind()] {
			return base.CorruptionErrorf("file %s has invalid largest point key kind", m)
		}
	}

	// Range key validation.

	if m.HasRangeKeys {
		if base.InternalCompare(cmp, m.SmallestRangeKey, m.LargestRangeKey) > 0 {
			return base.CorruptionErrorf("file %s has inconsistent range key bounds: %s vs %s",
				errors.Safe(m.FileNum), m.SmallestRangeKey.Pretty(formatKey),
				m.LargestRangeKey.Pretty(formatKey))
		}
		if base.InternalCompare(cmp, m.SmallestRangeKey, m.Smallest) < 0 ||
			base.InternalCompare(cmp, m.LargestRangeKey, m.Largest) > 0 {
			return base.CorruptionErrorf(
				"file %s has inconsistent range key bounds relative to overall bounds: "+
					"overall = [%s-%s], range keys = [%s-%s]",
				errors.Safe(m.FileNum),
				m.Smallest.Pretty(formatKey), m.Largest.Pretty(formatKey),
				m.SmallestRangeKey.Pretty(formatKey), m.LargestRangeKey.Pretty(formatKey),
			)
		}
		if !isValidRangeKeyBoundKeyKind[m.SmallestRangeKey.Kind()] {
			return base.CorruptionErrorf("file %s has invalid smallest range key kind", m)
		}
		if !isValidRangeKeyBoundKeyKind[m.LargestRangeKey.Kind()] {
			return base.CorruptionErrorf("file %s has invalid largest range key kind", m)
		}
	}

	// Ensure that TableMetadata.Init was called.
	if m.FileBacking == nil {
		return base.CorruptionErrorf("table metadata FileBacking not set")
	}
	// Assert that there's a nonzero blob reference depth if and only if the
	// table has a nonzero count of blob references. Additionally, the file's
	// blob reference depth should be bounded by the number of blob references.
	if (len(m.BlobReferences) == 0) != (m.BlobReferenceDepth == 0) || m.BlobReferenceDepth > len(m.BlobReferences) {
		return base.CorruptionErrorf("table %s with %d blob refs but %d blob ref depth",
			m.FileNum, len(m.BlobReferences), m.BlobReferenceDepth)
	}
	if m.SyntheticPrefixAndSuffix.HasPrefix() {
		if !m.Virtual {
			return base.CorruptionErrorf("non-virtual file with synthetic prefix")
		}
		if !bytes.HasPrefix(m.Smallest.UserKey, m.SyntheticPrefixAndSuffix.Prefix()) {
			return base.CorruptionErrorf("virtual file with synthetic prefix has smallest key with a different prefix: %s", m.Smallest.Pretty(formatKey))
		}
		if !bytes.HasPrefix(m.Largest.UserKey, m.SyntheticPrefixAndSuffix.Prefix()) {
			return base.CorruptionErrorf("virtual file with synthetic prefix has largest key with a different prefix: %s", m.Largest.Pretty(formatKey))
		}
	}
	if m.SyntheticPrefixAndSuffix.HasSuffix() {
		if !m.Virtual {
			return base.CorruptionErrorf("non-virtual file with synthetic suffix")
		}
	}

	return nil
}

var (
	isValidPointBoundKeyKind = [base.InternalKeyKindMax + 1]bool{
		base.InternalKeyKindDelete:        true,
		base.InternalKeyKindSet:           true,
		base.InternalKeyKindMerge:         true,
		base.InternalKeyKindSingleDelete:  true,
		base.InternalKeyKindRangeDelete:   true,
		base.InternalKeyKindSetWithDelete: true,
		base.InternalKeyKindDeleteSized:   true,
	}
	isValidRangeKeyBoundKeyKind = [base.InternalKeyKindMax + 1]bool{
		base.InternalKeyKindRangeKeySet:    true,
		base.InternalKeyKindRangeKeyUnset:  true,
		base.InternalKeyKindRangeKeyDelete: true,
	}
)

// TableInfo returns a subset of the TableMetadata state formatted as a
// TableInfo.
func (m *TableMetadata) TableInfo() TableInfo {
	return TableInfo{
		FileNum:        m.FileNum,
		Size:           m.Size,
		Smallest:       m.Smallest,
		Largest:        m.Largest,
		SmallestSeqNum: m.SmallestSeqNum,
		LargestSeqNum:  m.LargestSeqNum,
	}
}

func (m *TableMetadata) cmpSeqNum(b *TableMetadata) int {
	// NB: This is the same ordering that RocksDB uses for L0 files.

	// Sort first by largest sequence number.
	if v := stdcmp.Compare(m.LargestSeqNum, b.LargestSeqNum); v != 0 {
		return v
	}
	// Then by smallest sequence number.
	if v := stdcmp.Compare(m.SmallestSeqNum, b.SmallestSeqNum); v != 0 {
		return v
	}
	// Break ties by file number.
	return stdcmp.Compare(m.FileNum, b.FileNum)
}

func (m *TableMetadata) lessSeqNum(b *TableMetadata) bool {
	return m.cmpSeqNum(b) < 0
}

func (m *TableMetadata) cmpSmallestKey(b *TableMetadata, cmp Compare) int {
	return base.InternalCompare(cmp, m.Smallest, b.Smallest)
}

// KeyRange returns the minimum smallest and maximum largest internalKey for
// all the TableMetadata in iters.
func KeyRange(ucmp Compare, iters ...LevelIterator) (smallest, largest InternalKey) {
	first := true
	for _, iter := range iters {
		for meta := iter.First(); meta != nil; meta = iter.Next() {
			if first {
				first = false
				smallest, largest = meta.Smallest, meta.Largest
				continue
			}
			if base.InternalCompare(ucmp, smallest, meta.Smallest) >= 0 {
				smallest = meta.Smallest
			}
			if base.InternalCompare(ucmp, largest, meta.Largest) <= 0 {
				largest = meta.Largest
			}
		}
	}
	return smallest, largest
}

type bySeqNum []*TableMetadata

func (b bySeqNum) Len() int { return len(b) }
func (b bySeqNum) Less(i, j int) bool {
	return b[i].lessSeqNum(b[j])
}
func (b bySeqNum) Swap(i, j int) { b[i], b[j] = b[j], b[i] }

// SortBySeqNum sorts the specified files by increasing sequence number.
func SortBySeqNum(files []*TableMetadata) {
	sort.Sort(bySeqNum(files))
}

type bySmallest struct {
	files []*TableMetadata
	cmp   Compare
}

func (b bySmallest) Len() int { return len(b.files) }
func (b bySmallest) Less(i, j int) bool {
	return b.files[i].cmpSmallestKey(b.files[j], b.cmp) < 0
}
func (b bySmallest) Swap(i, j int) { b.files[i], b.files[j] = b.files[j], b.files[i] }

// SortBySmallest sorts the specified files by smallest key using the supplied
// comparison function to order user keys.
func SortBySmallest(files []*TableMetadata, cmp Compare) {
	sort.Sort(bySmallest{files, cmp})
}

// NumLevels is the number of levels a Version contains.
const NumLevels = 7

// NewVersion constructs a new Version with the provided files. It requires
// the provided files are already well-ordered. It's intended for testing.
func NewVersion(
	comparer *base.Comparer, flushSplitBytes int64, files [NumLevels][]*TableMetadata,
) *Version {
	v := &Version{
		cmp: comparer,
	}
	for l := range files {
		// NB: We specifically insert `files` into the B-Tree in the order
		// they appear within `files`. Some tests depend on this behavior in
		// order to test consistency checking, etc. Once we've constructed the
		// initial B-Tree, we swap out the btreeCmp for the correct one.
		// TODO(jackson): Adjust or remove the tests and remove this.
		v.Levels[l].tree = makeBTree(comparer.Compare, btreeCmpSpecificOrder(files[l]), files[l])
		v.Levels[l].level = l
		if l == 0 {
			v.Levels[l].tree.bcmp = btreeCmpSeqNum
		} else {
			v.Levels[l].tree.bcmp = btreeCmpSmallestKey(comparer.Compare)
		}
		for _, f := range files[l] {
			v.Levels[l].totalSize += f.Size
		}
	}
	if err := v.InitL0Sublevels(flushSplitBytes); err != nil {
		panic(err)
	}
	return v
}

// TestingNewVersion returns a blank Version, used for tests.
func TestingNewVersion(comparer *base.Comparer) *Version {
	return &Version{
		cmp: comparer,
	}
}

// Version is a collection of table metadata for on-disk tables at various
// levels. In-memory DBs are written to level-0 tables, and compactions
// migrate data from level N to level N+1. The tables map internal keys (which
// are a user key, a delete or set bit, and a sequence number) to user values.
//
// The tables at level 0 are sorted by largest sequence number. Due to file
// ingestion, there may be overlap in the ranges of sequence numbers contain in
// level 0 sstables. In particular, it is valid for one level 0 sstable to have
// the seqnum range [1,100] while an adjacent sstable has the seqnum range
// [50,50]. This occurs when the [50,50] table was ingested and given a global
// seqnum. The ingestion code will have ensured that the [50,50] sstable will
// not have any keys that overlap with the [1,100] in the seqnum range
// [1,49]. The range of internal keys [fileMetadata.smallest,
// fileMetadata.largest] in each level 0 table may overlap.
//
// The tables at any non-0 level are sorted by their internal key range and any
// two tables at the same non-0 level do not overlap.
//
// The internal key ranges of two tables at different levels X and Y may
// overlap, for any X != Y.
//
// Finally, for every internal key in a table at level X, there is no internal
// key in a higher level table that has both the same user key and a higher
// sequence number.
type Version struct {
	refs atomic.Int32

	// The level 0 sstables are organized in a series of sublevels. Similar to
	// the seqnum invariant in normal levels, there is no internal key in a
	// higher level table that has both the same user key and a higher sequence
	// number. Within a sublevel, tables are sorted by their internal key range
	// and any two tables at the same sublevel do not overlap. Unlike the normal
	// levels, sublevel n contains older tables (lower sequence numbers) than
	// sublevel n+1.
	//
	// The L0Sublevels struct is mostly used for compaction picking. As most
	// internal data structures in it are only necessary for compaction picking
	// and not for iterator creation, the reference to L0Sublevels is nil'd
	// after this version becomes the non-newest version, to reduce memory
	// usage.
	//
	// L0Sublevels.Levels contains L0 files ordered by sublevels. All the files
	// in Levels[0] are in L0Sublevels.Levels. L0SublevelFiles is also set to
	// a reference to that slice, as that slice is necessary for iterator
	// creation and needs to outlast L0Sublevels.
	L0Sublevels     *L0Sublevels
	L0SublevelFiles []LevelSlice

	Levels [NumLevels]LevelMetadata

	// RangeKeyLevels holds a subset of the same files as Levels that contain range
	// keys (i.e. fileMeta.HasRangeKeys == true). The memory amplification of this
	// duplication should be minimal, as range keys are expected to be rare.
	RangeKeyLevels [NumLevels]LevelMetadata

	// The callback to invoke when the last reference to a version is
	// removed. Will be called with list.mu held.
	Deleted func(obsolete ObsoleteFiles)

	// Stats holds aggregated stats about the version maintained from
	// version to version.
	Stats struct {
		// MarkedForCompaction records the count of files marked for
		// compaction within the version.
		MarkedForCompaction int
	}

	cmp *base.Comparer

	// The list the version is linked into.
	list *VersionList

	// The next/prev link for the versionList doubly-linked list of versions.
	prev, next *Version
}

// String implements fmt.Stringer, printing the TableMetadata for each level in
// the Version.
func (v *Version) String() string {
	return v.string(false)
}

// DebugString returns an alternative format to String() which includes sequence
// number and kind information for the sstable boundaries.
func (v *Version) DebugString() string {
	return v.string(true)
}

func describeSublevels(format base.FormatKey, verbose bool, sublevels []LevelSlice) string {
	var buf bytes.Buffer
	for sublevel := len(sublevels) - 1; sublevel >= 0; sublevel-- {
		fmt.Fprintf(&buf, "L0.%d:\n", sublevel)
		sublevels[sublevel].Each(func(f *TableMetadata) {
			fmt.Fprintf(&buf, "  %s\n", f.DebugString(format, verbose))
		})
	}
	return buf.String()
}

func (v *Version) string(verbose bool) string {
	var buf bytes.Buffer
	if len(v.L0SublevelFiles) > 0 {
		fmt.Fprintf(&buf, "%s", describeSublevels(v.cmp.FormatKey, verbose, v.L0SublevelFiles))
	}
	for level := 1; level < NumLevels; level++ {
		if v.Levels[level].Empty() {
			continue
		}
		fmt.Fprintf(&buf, "L%d:\n", level)
		iter := v.Levels[level].Iter()
		for f := iter.First(); f != nil; f = iter.Next() {
			fmt.Fprintf(&buf, "  %s\n", f.DebugString(v.cmp.FormatKey, verbose))
		}
	}
	return buf.String()
}

// ParseVersionDebug parses a Version from its DebugString output.
func ParseVersionDebug(comparer *base.Comparer, flushSplitBytes int64, s string) (*Version, error) {
	var files [NumLevels][]*TableMetadata
	level := -1
	for _, l := range strings.Split(s, "\n") {
		if l == "" {
			continue
		}
		p := strparse.MakeParser(debugParserSeparators, l)
		if l, ok := p.TryLevel(); ok {
			level = l
			continue
		}

		if level == -1 {
			return nil, errors.Errorf("version string must start with a level")
		}
		m, err := ParseTableMetadataDebug(l)
		if err != nil {
			return nil, err
		}
		files[level] = append(files[level], m)
	}
	// L0 files are printed from higher sublevel to lower, which means in a
	// partial order that represents newest to oldest. Reverse the order of L0
	// files to ensure we construct the same sublevels.
	slices.Reverse(files[0])
	v := NewVersion(comparer, flushSplitBytes, files)
	if err := v.CheckOrdering(); err != nil {
		return nil, err
	}
	return v, nil
}

// Refs returns the number of references to the version.
func (v *Version) Refs() int32 {
	return v.refs.Load()
}

// Ref increments the version refcount.
func (v *Version) Ref() {
	v.refs.Add(1)
}

// Unref decrements the version refcount. If the last reference to the version
// was removed, the version is removed from the list of versions and the
// Deleted callback is invoked. Requires that the VersionList mutex is NOT
// locked.
func (v *Version) Unref() {
	if v.refs.Add(-1) == 0 {
		l := v.list
		l.mu.Lock()
		l.Remove(v)
		v.Deleted(v.unrefFiles())
		l.mu.Unlock()
	}
}

// UnrefLocked decrements the version refcount. If the last reference to the
// version was removed, the version is removed from the list of versions and
// the Deleted callback is invoked. Requires that the VersionList mutex is
// already locked.
func (v *Version) UnrefLocked() {
	if v.refs.Add(-1) == 0 {
		v.list.Remove(v)
		v.Deleted(v.unrefFiles())
	}
}

func (v *Version) unrefFiles() ObsoleteFiles {
	var obsoleteFiles ObsoleteFiles
	for _, lm := range v.Levels {
		lm.release(&obsoleteFiles)
	}
	for _, lm := range v.RangeKeyLevels {
		lm.release(&obsoleteFiles)
	}
	return obsoleteFiles
}

// ObsoleteFiles holds a set of files that are no longer referenced by any
// referenced Version.
type ObsoleteFiles struct {
	FileBackings []*FileBacking
	BlobFiles    []*BlobFileMetadata
}

// AddBacking appends the provided FileBacking to the list of obsolete files.
func (of *ObsoleteFiles) AddBacking(fb *FileBacking) {
	of.FileBackings = append(of.FileBackings, fb)
}

// AddBlob appends the provided BlobFileMetadata to the list of obsolete files.
func (of *ObsoleteFiles) AddBlob(bm *BlobFileMetadata) {
	of.BlobFiles = append(of.BlobFiles, bm)
}

// Count returns the number of files in the ObsoleteFiles.
func (of *ObsoleteFiles) Count() int {
	return len(of.FileBackings) + len(of.BlobFiles)
}

// Assert that ObsoleteFiles implements the obsoleteFiles interface.
var _ ObsoleteFilesSet = (*ObsoleteFiles)(nil)

// Next returns the next version in the list of versions.
func (v *Version) Next() *Version {
	return v.next
}

// InitL0Sublevels initializes the L0Sublevels
func (v *Version) InitL0Sublevels(flushSplitBytes int64) error {
	var err error
	v.L0Sublevels, err = NewL0Sublevels(&v.Levels[0], v.cmp.Compare, v.cmp.FormatKey, flushSplitBytes)
	if err == nil && v.L0Sublevels != nil {
		v.L0SublevelFiles = v.L0Sublevels.Levels
	}
	return err
}

// CalculateInuseKeyRanges examines table metadata in levels [level, maxLevel]
// within bounds [smallest,largest], returning an ordered slice of key ranges
// that include all keys that exist within levels [level, maxLevel] and within
// [smallest,largest].
func (v *Version) CalculateInuseKeyRanges(
	level, maxLevel int, smallest, largest []byte,
) []base.UserKeyBounds {
	// Use two slices, alternating which one is input and which one is output
	// as we descend the LSM.
	var input, output []base.UserKeyBounds

	// L0 requires special treatment, since sstables within L0 may overlap.
	// We use the L0 Sublevels structure to efficiently calculate the merged
	// in-use key ranges.
	if level == 0 {
		output = v.L0Sublevels.InUseKeyRanges(smallest, largest)
		level++
	}

	// NB: We always treat `largest` as inclusive for simplicity, because
	// there's little consequence to calculating slightly broader in-use key
	// ranges.
	bounds := base.UserKeyBoundsInclusive(smallest, largest)
	for ; level <= maxLevel; level++ {
		overlaps := v.Overlaps(level, bounds)
		iter := overlaps.Iter()

		// We may already have in-use key ranges from higher levels. Iterate
		// through both our accumulated in-use key ranges and this level's
		// files, merging the two.
		//
		// Tables higher within the LSM have broader key spaces. We use this
		// when possible to seek past a level's files that are contained by
		// our current accumulated in-use key ranges. This helps avoid
		// per-sstable work during flushes or compactions in high levels which
		// overlap the majority of the LSM's sstables.
		input, output = output, input
		output = output[:0]

		cmp := v.cmp.Compare
		inputIdx := 0
		var currFile *TableMetadata
		// If we have an accumulated key range and its start is ≤ smallest,
		// we can seek to the accumulated range's end. Otherwise, we need to
		// start at the first overlapping file within the level.
		if len(input) > 0 && cmp(input[0].Start, smallest) <= 0 {
			currFile = seekGT(&iter, cmp, input[0].End)
		} else {
			currFile = iter.First()
		}

		for currFile != nil && inputIdx < len(input) {
			// Invariant: Neither currFile nor input[inputIdx] overlaps any earlier
			// ranges.
			switch {
			case cmp(currFile.Largest.UserKey, input[inputIdx].Start) < 0:
				// File is completely before input range.
				output = append(output, currFile.UserKeyBounds())
				currFile = iter.Next()

			case cmp(input[inputIdx].End.Key, currFile.Smallest.UserKey) < 0:
				// Input range is completely before the next file.
				output = append(output, input[inputIdx])
				inputIdx++

			default:
				// Input range and file range overlap or touch. We will maximally extend
				// the range with more overlapping inputs and files.
				currAccum := currFile.UserKeyBounds()
				if cmp(input[inputIdx].Start, currAccum.Start) < 0 {
					currAccum.Start = input[inputIdx].Start
				}
				currFile = iter.Next()

				// Extend curAccum with any overlapping (or touching) input intervals or
				// files. Note that we will always consume at least input[inputIdx].
				for {
					if inputIdx < len(input) && cmp(input[inputIdx].Start, currAccum.End.Key) <= 0 {
						if currAccum.End.CompareUpperBounds(cmp, input[inputIdx].End) < 0 {
							currAccum.End = input[inputIdx].End
							// Skip over files that are entirely inside this newly extended
							// accumulated range; we expect ranges to be wider in levels that
							// are higher up so this might skip over a non-trivial number of
							// files.
							currFile = seekGT(&iter, cmp, currAccum.End)
						}
						inputIdx++
					} else if currFile != nil && cmp(currFile.Smallest.UserKey, currAccum.End.Key) <= 0 {
						if b := currFile.UserKeyBounds(); currAccum.End.CompareUpperBounds(cmp, b.End) < 0 {
							currAccum.End = b.End
						}
						currFile = iter.Next()
					} else {
						// No overlaps remaining.
						break
					}
				}
				output = append(output, currAccum)
			}
		}
		// If we have either files or input ranges left over, add them to the
		// output.
		output = append(output, input[inputIdx:]...)
		for ; currFile != nil; currFile = iter.Next() {
			output = append(output, currFile.UserKeyBounds())
		}
	}
	return output
}

// seekGT seeks to the first file that ends with a boundary that is after the
// given boundary. Specifically:
//   - if boundary.End is inclusive, the returned file ending boundary is strictly
//     greater than boundary.End.Key
//   - if boundary.End is exclusive, the returned file ending boundary is either
//     greater than boundary.End.Key, or it's inclusive at boundary.End.Key.
func seekGT(iter *LevelIterator, cmp base.Compare, boundary base.UserKeyBoundary) *TableMetadata {
	f := iter.SeekGE(cmp, boundary.Key)
	if f == nil {
		return nil
	}
	// If boundary is inclusive or the file boundary is exclusive we do not
	// tolerate an equal largest key.
	// Note: we know f.Largest.UserKey >= boundary.End.Key so this condition is
	// equivalent to boundary.End.IsUpperBoundForInternalKey(cmp, f.Largest).
	if (boundary.Kind == base.Inclusive || f.Largest.IsExclusiveSentinel()) && cmp(boundary.Key, f.Largest.UserKey) == 0 {
		return iter.Next()
	}
	return f
}

// Contains returns a boolean indicating whether the provided file exists in
// the version at the given level. If level is non-zero then Contains binary
// searches among the files. If level is zero, Contains scans the entire
// level.
func (v *Version) Contains(level int, m *TableMetadata) bool {
	iter := v.Levels[level].Iter()
	if level > 0 {
		overlaps := v.Overlaps(level, m.UserKeyBounds())
		iter = overlaps.Iter()
	}
	for f := iter.First(); f != nil; f = iter.Next() {
		if f == m {
			return true
		}
	}
	return false
}

// Overlaps returns all elements of v.files[level] whose user key range
// intersects the given bounds. If level is non-zero then the user key bounds of
// v.files[level] are assumed to not overlap (although they may touch). If level
// is zero then that assumption cannot be made, and the given bounds are
// expanded to the union of those matching bounds so far and the computation is
// repeated until the bounds stabilize.
// The returned files are a subsequence of the input files, i.e., the ordering
// is not changed.
func (v *Version) Overlaps(level int, bounds base.UserKeyBounds) LevelSlice {
	if level == 0 {
		// Indices that have been selected as overlapping.
		l0 := v.Levels[level]
		l0Iter := l0.Iter()
		selectedIndices := make([]bool, l0.Len())
		numSelected := 0
		var slice LevelSlice
		for {
			restart := false
			for i, meta := 0, l0Iter.First(); meta != nil; i, meta = i+1, l0Iter.Next() {
				selected := selectedIndices[i]
				if selected {
					continue
				}
				if !meta.Overlaps(v.cmp.Compare, &bounds) {
					// meta is completely outside the specified range; skip it.
					continue
				}
				// Overlaps.
				selectedIndices[i] = true
				numSelected++

				// Since this is L0, check if the newly added fileMetadata has expanded
				// the range. We expand the range immediately for files we have
				// remaining to check in this loop. All already checked and unselected
				// files will need to be rechecked via the restart below.
				if v.cmp.Compare(meta.Smallest.UserKey, bounds.Start) < 0 {
					bounds.Start = meta.Smallest.UserKey
					restart = true
				}
				if !bounds.End.IsUpperBoundForInternalKey(v.cmp.Compare, meta.Largest) {
					bounds.End = base.UserKeyExclusiveIf(meta.Largest.UserKey, meta.Largest.IsExclusiveSentinel())
					restart = true
				}
			}

			if !restart {
				// Construct a B-Tree containing only the matching items.
				var tr btree
				tr.bcmp = v.Levels[level].tree.bcmp
				for i, meta := 0, l0Iter.First(); meta != nil; i, meta = i+1, l0Iter.Next() {
					if selectedIndices[i] {
						err := tr.Insert(meta)
						if err != nil {
							panic(err)
						}
					}
				}
				slice = newLevelSlice(tr.Iter())
				// TODO(jackson): Avoid the oddity of constructing and
				// immediately releasing a B-Tree. Make LevelSlice an
				// interface?
				tr.Release(assertNoObsoleteFiles{})
				break
			}
			// Continue looping to retry the files that were not selected.
		}
		return slice
	}

	return v.Levels[level].Slice().Overlaps(v.cmp.Compare, bounds)
}

// IterAllLevelsAndSublevels calls fn with an iterator for each L0 sublevel
// (from top to bottom), then once for each level below L0.
func (v *Version) IterAllLevelsAndSublevels(fn func(it LevelIterator, level Layer)) {
	for sublevel := len(v.L0SublevelFiles) - 1; sublevel >= 0; sublevel-- {
		fn(v.L0SublevelFiles[sublevel].Iter(), L0Sublevel(sublevel))
	}
	for level := 1; level < NumLevels; level++ {
		fn(v.Levels[level].Iter(), Level(level))
	}
}

// CheckOrdering checks that the files are consistent with respect to
// increasing file numbers (for level 0 files) and increasing and non-
// overlapping internal key ranges (for level non-0 files).
func (v *Version) CheckOrdering() error {
	for sublevel := len(v.L0SublevelFiles) - 1; sublevel >= 0; sublevel-- {
		sublevelIter := v.L0SublevelFiles[sublevel].Iter()
		if err := CheckOrdering(v.cmp.Compare, v.cmp.FormatKey, L0Sublevel(sublevel), sublevelIter); err != nil {
			return base.CorruptionErrorf("%s\n%s", err, v.DebugString())
		}
	}

	for level, lm := range v.Levels {
		if err := CheckOrdering(v.cmp.Compare, v.cmp.FormatKey, Level(level), lm.Iter()); err != nil {
			return base.CorruptionErrorf("%s\n%s", err, v.DebugString())
		}
	}
	return nil
}

// VersionList holds a list of versions. The versions are ordered from oldest
// to newest.
type VersionList struct {
	mu   *sync.Mutex
	root Version
}

// Init initializes the version list.
func (l *VersionList) Init(mu *sync.Mutex) {
	l.mu = mu
	l.root.next = &l.root
	l.root.prev = &l.root
}

// Empty returns true if the list is empty, and false otherwise.
func (l *VersionList) Empty() bool {
	return l.root.next == &l.root
}

// Front returns the oldest version in the list. Note that this version is only
// valid if Empty() returns true.
func (l *VersionList) Front() *Version {
	return l.root.next
}

// Back returns the newest version in the list. Note that this version is only
// valid if Empty() returns true.
func (l *VersionList) Back() *Version {
	return l.root.prev
}

// PushBack adds a new version to the back of the list. This new version
// becomes the "newest" version in the list.
func (l *VersionList) PushBack(v *Version) {
	if v.list != nil || v.prev != nil || v.next != nil {
		panic("pebble: version list is inconsistent")
	}
	v.prev = l.root.prev
	v.prev.next = v
	v.next = &l.root
	v.next.prev = v
	v.list = l
	// Let L0Sublevels on the second newest version get GC'd, as it is no longer
	// necessary. See the comment in Version.
	v.prev.L0Sublevels = nil
}

// Remove removes the specified version from the list.
func (l *VersionList) Remove(v *Version) {
	if v == &l.root {
		panic("pebble: cannot remove version list root node")
	}
	if v.list != l {
		panic("pebble: version list is inconsistent")
	}
	v.prev.next = v.next
	v.next.prev = v.prev
	v.next = nil // avoid memory leaks
	v.prev = nil // avoid memory leaks
	v.list = nil // avoid memory leaks
}

// CheckOrdering checks that the files are consistent with respect to
// seqnums (for level 0 files -- see detailed comment below) and increasing and non-
// overlapping internal key ranges (for non-level 0 files).
func CheckOrdering(cmp Compare, format base.FormatKey, level Layer, files LevelIterator) error {
	// The invariants to check for L0 sublevels are the same as the ones to
	// check for all other levels. However, if L0 is not organized into
	// sublevels, or if all L0 files are being passed in, we do the legacy L0
	// checks, defined in the detailed comment below.
	if level == Level(0) {
		// We have 2 kinds of files:
		// - Files with exactly one sequence number: these could be either ingested files
		//   or flushed files. We cannot tell the difference between them based on TableMetadata,
		//   so our consistency checking here uses the weaker checks assuming it is a narrow
		//   flushed file. We cannot error on ingested files having sequence numbers coincident
		//   with flushed files as the seemingly ingested file could just be a flushed file
		//   with just one key in it which is a truncated range tombstone sharing sequence numbers
		//   with other files in the same flush.
		// - Files with multiple sequence numbers: these are necessarily flushed files.
		//
		// Three cases of overlapping sequence numbers:
		// Case 1:
		// An ingested file contained in the sequence numbers of the flushed file -- it must be
		// fully contained (not coincident with either end of the flushed file) since the memtable
		// must have been at [a, b-1] (where b > a) when the ingested file was assigned sequence
		// num b, and the memtable got a subsequent update that was given sequence num b+1, before
		// being flushed.
		//
		// So a sequence [1000, 1000] [1002, 1002] [1000, 2000] is invalid since the first and
		// third file are inconsistent with each other. So comparing adjacent files is insufficient
		// for consistency checking.
		//
		// Visually we have something like
		// x------y x-----------yx-------------y (flushed files where x, y are the endpoints)
		//     y       y  y        y             (y's represent ingested files)
		// And these are ordered in increasing order of y. Note that y's must be unique.
		//
		// Case 2:
		// A flushed file that did not overlap in keys with any file in any level, but does overlap
		// in the file key intervals. This file is placed in L0 since it overlaps in the file
		// key intervals but since it has no overlapping data, it is assigned a sequence number
		// of 0 in RocksDB. We handle this case for compatibility with RocksDB.
		//
		// Case 3:
		// A sequence of flushed files that overlap in sequence numbers with one another,
		// but do not overlap in keys inside the sstables. These files correspond to
		// partitioned flushes or the results of intra-L0 compactions of partitioned
		// flushes.
		//
		// Since these types of SSTables violate most other sequence number
		// overlap invariants, and handling this case is important for compatibility
		// with future versions of pebble, this method relaxes most L0 invariant
		// checks.

		var prev *TableMetadata
		for f := files.First(); f != nil; f, prev = files.Next(), f {
			if prev == nil {
				continue
			}
			// Validate that the sorting is sane.
			if prev.LargestSeqNum == 0 && f.LargestSeqNum == prev.LargestSeqNum {
				// Multiple files satisfying case 2 mentioned above.
			} else if !prev.lessSeqNum(f) {
				return base.CorruptionErrorf("L0 files %s and %s are not properly ordered: <#%d-#%d> vs <#%d-#%d>",
					errors.Safe(prev.FileNum), errors.Safe(f.FileNum),
					errors.Safe(prev.SmallestSeqNum), errors.Safe(prev.LargestSeqNum),
					errors.Safe(f.SmallestSeqNum), errors.Safe(f.LargestSeqNum))
			}
		}
	} else {
		var prev *TableMetadata
		for f := files.First(); f != nil; f, prev = files.Next(), f {
			if err := f.Validate(cmp, format); err != nil {
				return errors.Wrapf(err, "%s ", level)
			}
			if prev != nil {
				if prev.cmpSmallestKey(f, cmp) >= 0 {
					return base.CorruptionErrorf("%s files %s and %s are not properly ordered: [%s-%s] vs [%s-%s]",
						errors.Safe(level), errors.Safe(prev.FileNum), errors.Safe(f.FileNum),
						prev.Smallest.Pretty(format), prev.Largest.Pretty(format),
						f.Smallest.Pretty(format), f.Largest.Pretty(format))
				}

				// In all supported format major version, split user keys are
				// prohibited, so both files cannot contain keys with the same user
				// keys. If the bounds have the same user key, the previous file's
				// boundary must have a InternalKeyTrailer indicating that it's exclusive.
				if v := cmp(prev.Largest.UserKey, f.Smallest.UserKey); v > 0 || (v == 0 && !prev.Largest.IsExclusiveSentinel()) {
					return base.CorruptionErrorf("%s files %s and %s have overlapping ranges: [%s-%s] vs [%s-%s]",
						errors.Safe(level), errors.Safe(prev.FileNum), errors.Safe(f.FileNum),
						prev.Smallest.Pretty(format), prev.Largest.Pretty(format),
						f.Smallest.Pretty(format), f.Largest.Pretty(format))
				}
			}
		}
	}
	return nil
}
