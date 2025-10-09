// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"io"
	"regexp"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/cockroachdb/crlib/fifo"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/humanize"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/pebble/rangekey"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/colblk"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/wal"
	"github.com/cockroachdb/redact"
)

const (
	cacheDefaultSize                        = 8 << 20 // 8 MB
	defaultLevelMultiplier                  = 10
	defaultVirtualTableUnreferencedFraction = 0.3
)

// FilterType exports the base.FilterType type.
type FilterType = base.FilterType

// Exported TableFilter constants.
const (
	TableFilter = base.TableFilter
)

// FilterWriter exports the base.FilterWriter type.
type FilterWriter = base.FilterWriter

// FilterPolicy exports the base.FilterPolicy type.
type FilterPolicy = base.FilterPolicy

var NoFilterPolicy = base.NoFilterPolicy

// KeySchema exports the colblk.KeySchema type.
type KeySchema = colblk.KeySchema

// BlockPropertyCollector exports the sstable.BlockPropertyCollector type.
type BlockPropertyCollector = sstable.BlockPropertyCollector

// BlockPropertyFilter exports the sstable.BlockPropertyFilter type.
type BlockPropertyFilter = base.BlockPropertyFilter

// MaximumSuffixProperty exports the sstable.MaximumSuffixProperty type.
type MaximumSuffixProperty = sstable.MaximumSuffixProperty

// ShortAttributeExtractor exports the base.ShortAttributeExtractor type.
type ShortAttributeExtractor = base.ShortAttributeExtractor

// UserKeyPrefixBound exports the sstable.UserKeyPrefixBound type.
type UserKeyPrefixBound = sstable.UserKeyPrefixBound

// IterKeyType configures which types of keys an iterator should surface.
type IterKeyType int8

// DirLock represents a file lock on a directory. It may be passed to Open through
// Options.Lock to elide lock aquisition during Open.
type DirLock = base.DirLock

// LockDirectory acquires the directory lock in the named directory, preventing
// another process from opening the database. LockDirectory returns a
// handle to the held lock that may be passed to Open, skipping lock acquisition
// during Open.
//
// LockDirectory may be used to expand the critical section protected by the
// database lock to include setup before the call to Open.
func LockDirectory(dirname string, fs vfs.FS) (*DirLock, error) {
	return base.LockDirectory(dirname, fs)
}

const (
	// IterKeyTypePointsOnly configures an iterator to iterate over point keys
	// only.
	IterKeyTypePointsOnly IterKeyType = iota
	// IterKeyTypeRangesOnly configures an iterator to iterate over range keys
	// only.
	IterKeyTypeRangesOnly
	// IterKeyTypePointsAndRanges configures an iterator iterate over both point
	// keys and range keys simultaneously.
	IterKeyTypePointsAndRanges
)

// String implements fmt.Stringer.
func (t IterKeyType) String() string {
	switch t {
	case IterKeyTypePointsOnly:
		return "points-only"
	case IterKeyTypeRangesOnly:
		return "ranges-only"
	case IterKeyTypePointsAndRanges:
		return "points-and-ranges"
	default:
		panic(fmt.Sprintf("unknown key type %d", t))
	}
}

// IterOptions hold the optional per-query parameters for NewIter.
//
// Like Options, a nil *IterOptions is valid and means to use the default
// values.
type IterOptions struct {
	// LowerBound specifies the smallest key (inclusive) that the iterator will
	// return during iteration. If the iterator is seeked or iterated past this
	// boundary the iterator will return Valid()==false. Setting LowerBound
	// effectively truncates the key space visible to the iterator.
	LowerBound []byte
	// UpperBound specifies the largest key (exclusive) that the iterator will
	// return during iteration. If the iterator is seeked or iterated past this
	// boundary the iterator will return Valid()==false. Setting UpperBound
	// effectively truncates the key space visible to the iterator.
	UpperBound []byte
	// SkipPoint may be used to skip over point keys that don't match an
	// arbitrary predicate during iteration. If set, the Iterator invokes
	// SkipPoint for keys encountered. If SkipPoint returns true, the iterator
	// will skip the key without yielding it to the iterator operation in
	// progress.
	//
	// SkipPoint must be a pure function and always return the same result when
	// provided the same arguments. The iterator may call SkipPoint multiple
	// times for the same user key.
	SkipPoint func(userKey []byte) bool
	// PointKeyFilters can be used to avoid scanning tables and blocks in tables
	// when iterating over point keys. This slice represents an intersection
	// across all filters, i.e., all filters must indicate that the block is
	// relevant.
	//
	// Performance note: When len(PointKeyFilters) > 0, the caller should ensure
	// that cap(PointKeyFilters) is at least len(PointKeyFilters)+1. This helps
	// avoid allocations in Pebble internal code that mutates the slice.
	PointKeyFilters []BlockPropertyFilter
	// RangeKeyFilters can be usefd to avoid scanning tables and blocks in tables
	// when iterating over range keys. The same requirements that apply to
	// PointKeyFilters apply here too.
	RangeKeyFilters []BlockPropertyFilter
	// KeyTypes configures which types of keys to iterate over: point keys,
	// range keys, or both.
	KeyTypes IterKeyType
	// RangeKeyMasking can be used to enable automatic masking of point keys by
	// range keys. Range key masking is only supported during combined range key
	// and point key iteration mode (IterKeyTypePointsAndRanges).
	RangeKeyMasking RangeKeyMasking

	// OnlyReadGuaranteedDurable is an advanced option that is only supported by
	// the Reader implemented by DB. When set to true, only the guaranteed to be
	// durable state is visible in the iterator.
	// - This definition is made under the assumption that the FS implementation
	//   is providing a durability guarantee when data is synced.
	// - The visible state represents a consistent point in the history of the
	//   DB.
	// - The implementation is free to choose a conservative definition of what
	//   is guaranteed durable. For simplicity, the current implementation
	//   ignores memtables. A more sophisticated implementation could track the
	//   highest seqnum that is synced to the WAL and published and use that as
	//   the visible seqnum for an iterator. Note that the latter approach is
	//   not strictly better than the former since we can have DBs that are (a)
	//   synced more rarely than memtable flushes, (b) have no WAL. (a) is
	//   likely to be true in a future CockroachDB context where the DB
	//   containing the state machine may be rarely synced.
	// NB: this current implementation relies on the fact that memtables are
	// flushed in seqnum order, and any ingested sstables that happen to have a
	// lower seqnum than a non-flushed memtable don't have any overlapping keys.
	// This is the fundamental level invariant used in other code too, like when
	// merging iterators.
	//
	// Semantically, using this option provides the caller a "snapshot" as of
	// the time the most recent memtable was flushed. An alternate interface
	// would be to add a NewSnapshot variant. Creating a snapshot is heavier
	// weight than creating an iterator, so we have opted to support this
	// iterator option.
	OnlyReadGuaranteedDurable bool
	// UseL6Filters allows the caller to opt into reading filter blocks for L6
	// sstables. Helpful if a lot of SeekPrefixGEs are expected in quick
	// succession, that are also likely to not yield a single key. Filter blocks in
	// L6 can be relatively large, often larger than data blocks, so the benefit of
	// loading them in the cache is minimized if the probability of the key
	// existing is not low or if we just expect a one-time Seek (where loading the
	// data block directly is better).
	UseL6Filters bool
	// Category is used for categorized iterator stats. This should not be
	// changed by calling SetOptions.
	Category block.Category

	// ExemptFromTracking indicates that we should not track the lifetime of the
	// iterator (used to log information about long-lived iterators). Useful for
	// hot paths where we know the iterator will be short-lived.
	ExemptFromTracking bool

	// DebugRangeKeyStack enables additional logging of the range key stack
	// iterator, via keyspan.InjectLogging. Only used for debugging.
	DebugRangeKeyStack bool

	// Internal options.

	logger Logger
	// Layer corresponding to this file. Only passed in if constructed by a
	// levelIter.
	layer manifest.Layer
	// disableLazyCombinedIteration is an internal testing option.
	disableLazyCombinedIteration bool
	// snapshotForHideObsoletePoints is specified for/by levelIter when opening
	// files and is used to decide whether to hide obsolete points. A value of 0
	// implies obsolete points should not be hidden.
	snapshotForHideObsoletePoints base.SeqNum

	// MaximumSuffixProperty is the maximum suffix property for the iterator.
	// This is used to perform the synthetic key optimization.
	MaximumSuffixProperty MaximumSuffixProperty
	// NB: If adding new Options, you must account for them in iterator
	// construction and Iterator.SetOptions.
}

// GetLowerBound returns the LowerBound or nil if the receiver is nil.
func (o *IterOptions) GetLowerBound() []byte {
	if o == nil {
		return nil
	}
	return o.LowerBound
}

// GetUpperBound returns the UpperBound or nil if the receiver is nil.
func (o *IterOptions) GetUpperBound() []byte {
	if o == nil {
		return nil
	}
	return o.UpperBound
}

// GetMaximumSuffixProperty returns the MaximumSuffixProperty.
func (o *IterOptions) GetMaximumSuffixProperty() MaximumSuffixProperty {
	if o == nil {
		return nil
	}
	return o.MaximumSuffixProperty
}

func (o *IterOptions) pointKeys() bool {
	if o == nil {
		return true
	}
	return o.KeyTypes == IterKeyTypePointsOnly || o.KeyTypes == IterKeyTypePointsAndRanges
}

func (o *IterOptions) rangeKeys() bool {
	if o == nil {
		return false
	}
	return o.KeyTypes == IterKeyTypeRangesOnly || o.KeyTypes == IterKeyTypePointsAndRanges
}

func (o *IterOptions) getLogger() Logger {
	if o == nil || o.logger == nil {
		return DefaultLogger
	}
	return o.logger
}

// SpanIterOptions creates a SpanIterOptions from this IterOptions.
func (o *IterOptions) SpanIterOptions() keyspan.SpanIterOptions {
	if o == nil {
		return keyspan.SpanIterOptions{}
	}
	return keyspan.SpanIterOptions{
		RangeKeyFilters: o.RangeKeyFilters,
	}
}

// ScanInternalOptions is similar to IterOptions, meant for use with
// scanInternalIterator.
type ScanInternalOptions struct {
	IterOptions

	Category block.Category

	VisitPointKey     func(key *InternalKey, value LazyValue, iterInfo IteratorLevel) error
	VisitRangeDel     func(start, end []byte, seqNum SeqNum) error
	VisitRangeKey     func(start, end []byte, keys []rangekey.Key) error
	VisitSharedFile   func(sst *SharedSSTMeta) error
	VisitExternalFile func(sst *ExternalFile) error

	// IncludeObsoleteKeys specifies whether keys shadowed by newer internal keys
	// are exposed. If false, only one internal key per user key is exposed.
	IncludeObsoleteKeys bool

	// RateLimitFunc is used to limit the amount of bytes read per second.
	RateLimitFunc func(key *InternalKey, value LazyValue) error
}

// RangeKeyMasking configures automatic hiding of point keys by range keys. A
// non-nil Suffix enables range-key masking. When enabled, range keys with
// suffixes ≥ Suffix behave as masks. All point keys that are contained within a
// masking range key's bounds and have suffixes greater than the range key's
// suffix are automatically skipped.
//
// Specifically, when configured with a RangeKeyMasking.Suffix _s_, and there
// exists a range key with suffix _r_ covering a point key with suffix _p_, and
//
//	_s_ ≤ _r_ < _p_
//
// then the point key is elided.
//
// Range-key masking may only be used when iterating over both point keys and
// range keys with IterKeyTypePointsAndRanges.
type RangeKeyMasking struct {
	// Suffix configures which range keys may mask point keys. Only range keys
	// that are defined at suffixes greater than or equal to Suffix will mask
	// point keys.
	Suffix []byte
	// Filter is an optional field that may be used to improve performance of
	// range-key masking through a block-property filter defined over key
	// suffixes. If non-nil, Filter is called by Pebble to construct a
	// block-property filter mask at iterator creation. The filter is used to
	// skip whole point-key blocks containing point keys with suffixes greater
	// than a covering range-key's suffix.
	//
	// To use this functionality, the caller must create and configure (through
	// Options.BlockPropertyCollectors) a block-property collector that records
	// the maxmimum suffix contained within a block. The caller then must write
	// and provide a BlockPropertyFilterMask implementation on that same
	// property. See the BlockPropertyFilterMask type for more information.
	Filter func() BlockPropertyFilterMask
}

// BlockPropertyFilterMask extends the BlockPropertyFilter interface for use
// with range-key masking. Unlike an ordinary block property filter, a
// BlockPropertyFilterMask's filtering criteria is allowed to change when Pebble
// invokes its SetSuffix method.
//
// When a Pebble iterator steps into a range key's bounds and the range key has
// a suffix greater than or equal to RangeKeyMasking.Suffix, the range key acts
// as a mask. The masking range key hides all point keys that fall within the
// range key's bounds and have suffixes > the range key's suffix. Without a
// filter mask configured, Pebble performs this hiding by stepping through point
// keys and comparing suffixes. If large numbers of point keys are masked, this
// requires Pebble to load, iterate through and discard a large number of
// sstable blocks containing masked point keys.
//
// If a block-property collector and a filter mask are configured, Pebble may
// skip loading some point-key blocks altogether. If a block's keys are known to
// all fall within the bounds of the masking range key and the block was
// annotated by a block-property collector with the maximal suffix, Pebble can
// ask the filter mask to compare the property to the current masking range
// key's suffix. If the mask reports no intersection, the block may be skipped.
//
// If unsuffixed and suffixed keys are written to the database, care must be
// taken to avoid unintentionally masking un-suffixed keys located in the same
// block as suffixed keys. One solution is to interpret unsuffixed keys as
// containing the maximal suffix value, ensuring that blocks containing
// unsuffixed keys are always loaded.
type BlockPropertyFilterMask interface {
	BlockPropertyFilter

	// SetSuffix configures the mask with the suffix of a range key. The filter
	// should return false from Intersects whenever it's provided with a
	// property encoding a block's minimum suffix that's greater (according to
	// Compare) than the provided suffix.
	SetSuffix(suffix []byte) error
}

// WriteOptions hold the optional per-query parameters for Set and Delete
// operations.
//
// Like Options, a nil *WriteOptions is valid and means to use the default
// values.
type WriteOptions struct {
	// Sync is whether to sync writes through the OS buffer cache and down onto
	// the actual disk, if applicable. Setting Sync is required for durability of
	// individual write operations but can result in slower writes.
	//
	// If false, and the process or machine crashes, then a recent write may be
	// lost. This is due to the recently written data being buffered inside the
	// process running Pebble. This differs from the semantics of a write system
	// call in which the data is buffered in the OS buffer cache and would thus
	// survive a process crash.
	//
	// The default value is true.
	Sync bool
}

// Sync specifies the default write options for writes which synchronize to
// disk.
var Sync = &WriteOptions{Sync: true}

// NoSync specifies the default write options for writes which do not
// synchronize to disk.
var NoSync = &WriteOptions{Sync: false}

// GetSync returns the Sync value or true if the receiver is nil.
func (o *WriteOptions) GetSync() bool {
	return o == nil || o.Sync
}

// LevelOptions holds the optional per-level parameters.
type LevelOptions struct {
	// BlockRestartInterval is the number of keys between restart points
	// for delta encoding of keys.
	//
	// The default value is 16 for L0, and the value from the previous level for
	// all other levels.
	BlockRestartInterval int

	// BlockSize is the target uncompressed size in bytes of each table block.
	//
	// The default value is 4096 for L0, and the value from the previous level for
	// all other levels.
	BlockSize int

	// BlockSizeThreshold finishes a block if the block size is larger than the
	// specified percentage of the target block size and adding the next entry
	// would cause the block to be larger than the target block size.
	//
	// The default value is 90 for L0, and the value from the previous level for
	// all other levels.
	BlockSizeThreshold int

	// Compression defines the per-block compression to use.
	//
	// The default value is Snappy for L0, or the function from the previous level
	// for all other levels.
	//
	// ApplyCompressionSettings can be used to initialize this field for all levels.
	Compression func() *sstable.CompressionProfile

	// FilterPolicy defines a filter algorithm (such as a Bloom filter) that can
	// reduce disk reads for Get calls.
	//
	// One such implementation is bloom.FilterPolicy(10) from the pebble/bloom
	// package.
	//
	// The default value for L0 is NoFilterPolicy (no filter), and the value from
	// the previous level for all other levels.
	FilterPolicy FilterPolicy

	// FilterType is a legacy field. The default and only possible value is
	// TableFilter.
	FilterType FilterType

	// IndexBlockSize is the target uncompressed size in bytes of each index
	// block. When the index block size is larger than this target, two-level
	// indexes are automatically enabled. Setting this option to a large value
	// (such as math.MaxInt32) disables the automatic creation of two-level
	// indexes.
	//
	// The default value is the value of BlockSize for L0, or the value from the
	// previous level for all other levels.
	IndexBlockSize int
}

// EnsureL0Defaults ensures that the L0 default values for the options have been
// initialized.
func (o *LevelOptions) EnsureL0Defaults() {
	if o.BlockRestartInterval <= 0 {
		o.BlockRestartInterval = base.DefaultBlockRestartInterval
	}
	if o.BlockSize <= 0 {
		o.BlockSize = base.DefaultBlockSize
	} else if o.BlockSize > sstable.MaximumRestartOffset {
		panic(errors.Errorf("BlockSize %d exceeds MaximumRestartOffset", o.BlockSize))
	}
	if o.BlockSizeThreshold <= 0 {
		o.BlockSizeThreshold = base.DefaultBlockSizeThreshold
	}
	if o.Compression == nil {
		o.Compression = func() *sstable.CompressionProfile { return sstable.SnappyCompression }
	}
	if o.FilterPolicy == nil {
		o.FilterPolicy = NoFilterPolicy
	}
	if o.IndexBlockSize <= 0 {
		o.IndexBlockSize = o.BlockSize
	}
}

// EnsureL1PlusDefaults ensures that the L1+ default values for the options have
// been initialized. Requires the fully initialized options for the level above.
func (o *LevelOptions) EnsureL1PlusDefaults(previousLevel *LevelOptions) {
	if o.BlockRestartInterval <= 0 {
		o.BlockRestartInterval = previousLevel.BlockRestartInterval
	}
	if o.BlockSize <= 0 {
		o.BlockSize = previousLevel.BlockSize
	} else if o.BlockSize > sstable.MaximumRestartOffset {
		panic(errors.Errorf("BlockSize %d exceeds MaximumRestartOffset", o.BlockSize))
	}
	if o.BlockSizeThreshold <= 0 {
		o.BlockSizeThreshold = previousLevel.BlockSizeThreshold
	}
	if o.Compression == nil {
		o.Compression = previousLevel.Compression
	}
	if o.FilterPolicy == nil {
		o.FilterPolicy = previousLevel.FilterPolicy
	}
	if o.IndexBlockSize <= 0 {
		o.IndexBlockSize = previousLevel.IndexBlockSize
	}
}

// Options holds the optional parameters for configuring pebble. These options
// apply to the DB at large; per-query options are defined by the IterOptions
// and WriteOptions types.
type Options struct {
	// Sync sstables periodically in order to smooth out writes to disk. This
	// option does not provide any persistency guarantee, but is used to avoid
	// latency spikes if the OS automatically decides to write out a large chunk
	// of dirty filesystem buffers. This option only controls SSTable syncs; WAL
	// syncs are controlled by WALBytesPerSync.
	//
	// The default value is 512KB.
	BytesPerSync int

	// Cache is used to cache uncompressed blocks from sstables. If it is nil,
	// a block cache of CacheSize will be created for each DB.
	Cache *cache.Cache
	// CacheSize is used when Cache is not set. The default value is 8 MB.
	CacheSize int64

	// LoadBlockSema, if set, is used to limit the number of blocks that can be
	// loaded (i.e. read from the filesystem) in parallel. Each load acquires one
	// unit from the semaphore for the duration of the read.
	LoadBlockSema *fifo.Semaphore

	// Cleaner cleans obsolete files.
	//
	// The default cleaner uses the DeleteCleaner.
	Cleaner Cleaner

	// Local contains option that pertain to files stored on the local filesystem.
	Local struct {
		// ReadaheadConfig is used to retrieve the current readahead mode; it is
		// consulted whenever a read handle is initialized.
		ReadaheadConfig *ReadaheadConfig

		// TODO(radu): move BytesPerSync, LoadBlockSema, Cleaner here.
	}

	// Comparer defines a total ordering over the space of []byte keys: a 'less
	// than' relationship. The same comparison algorithm must be used for reads
	// and writes over the lifetime of the DB.
	//
	// The default value uses the same ordering as bytes.Compare.
	Comparer *Comparer

	// DebugCheck is invoked, if non-nil, whenever a new version is being
	// installed. Typically, this is set to pebble.DebugCheckLevels in tests
	// or tools only, to check invariants over all the data in the database.
	DebugCheck func(*DB) error

	// Disable the write-ahead log (WAL). Disabling the write-ahead log prohibits
	// crash recovery, but can improve performance if crash recovery is not
	// needed (e.g. when only temporary state is being stored in the database).
	//
	// TODO(peter): untested
	DisableWAL bool

	// ErrorIfExists causes an error on Open if the database already exists.
	// The error can be checked with errors.Is(err, ErrDBAlreadyExists).
	//
	// The default value is false.
	ErrorIfExists bool

	// ErrorIfNotExists causes an error on Open if the database does not already
	// exist. The error can be checked with errors.Is(err, ErrDBDoesNotExist).
	//
	// The default value is false which will cause a database to be created if it
	// does not already exist.
	ErrorIfNotExists bool

	// ErrorIfNotPristine causes an error on Open if the database already exists
	// and any operations have been performed on the database. The error can be
	// checked with errors.Is(err, ErrDBNotPristine).
	//
	// Note that a database that contained keys that were all subsequently deleted
	// may or may not trigger the error. Currently, we check if there are any live
	// SSTs or log records to replay.
	ErrorIfNotPristine bool

	// EventListener provides hooks to listening to significant DB events such as
	// flushes, compactions, and table deletion.
	EventListener *EventListener

	// Experimental contains experimental options which are off by default.
	// These options are temporary and will eventually either be deleted, moved
	// out of the experimental group, or made the non-adjustable default. These
	// options may change at any time, so do not rely on them.
	Experimental struct {
		// The threshold of L0 read-amplification at which compaction concurrency
		// is enabled (if CompactionDebtConcurrency was not already exceeded).
		// Every multiple of this value enables another concurrent
		// compaction up to CompactionConcurrencyRange.
		L0CompactionConcurrency int

		// CompactionDebtConcurrency controls the threshold of compaction debt
		// at which additional compaction concurrency slots are added. For every
		// multiple of this value in compaction debt bytes, an additional
		// concurrent compaction is added. This works "on top" of
		// L0CompactionConcurrency, so the higher of the count of compaction
		// concurrency slots as determined by the two options is chosen.
		CompactionDebtConcurrency uint64

		// CompactionGarbageFractionForMaxConcurrency is the fraction of garbage
		// due to DELs and RANGEDELs that causes MaxConcurrentCompactions to be
		// allowed. Concurrent compactions are allowed in a linear manner upto
		// this limit being reached. A value <= 0.0 disables adding concurrency
		// due to garbage.
		CompactionGarbageFractionForMaxConcurrency func() float64

		// UseDeprecatedCompensatedScore is a temporary option to revert the
		// compaction picker to the previous behavior of only considering
		// compaction out of a level if its compensated fill factor divided by
		// the next level's fill factor is >= 1.0. The details of this setting
		// are documented within its use within the compaction picker.
		//
		// The default value is false.
		UseDeprecatedCompensatedScore func() bool

		// IngestSplit, if it returns true, allows for ingest-time splitting of
		// existing sstables into two virtual sstables to allow ingestion sstables to
		// slot into a lower level than they otherwise would have.
		IngestSplit func() bool

		// ReadCompactionRate controls the frequency of read triggered
		// compactions by adjusting `AllowedSeeks` in manifest.TableMetadata:
		//
		// AllowedSeeks = FileSize / ReadCompactionRate
		//
		// From LevelDB:
		// ```
		// We arrange to automatically compact this file after
		// a certain number of seeks. Let's assume:
		//   (1) One seek costs 10ms
		//   (2) Writing or reading 1MB costs 10ms (100MB/s)
		//   (3) A compaction of 1MB does 25MB of IO:
		//         1MB read from this level
		//         10-12MB read from next level (boundaries may be misaligned)
		//         10-12MB written to next level
		// This implies that 25 seeks cost the same as the compaction
		// of 1MB of data.  I.e., one seek costs approximately the
		// same as the compaction of 40KB of data.  We are a little
		// conservative and allow approximately one seek for every 16KB
		// of data before triggering a compaction.
		// ```
		ReadCompactionRate int64

		// ReadSamplingMultiplier is a multiplier for the readSamplingPeriod in
		// iterator.maybeSampleRead() to control the frequency of read sampling
		// to trigger a read triggered compaction. A value of -1 prevents sampling
		// and disables read triggered compactions. The default is 1 << 4. which
		// gets multiplied with a constant of 1 << 16 to yield 1 << 20 (1MB).
		ReadSamplingMultiplier int64

		// NumDeletionsThreshold defines the minimum number of point tombstones
		// that must be present in a single data block for that block to be
		// considered tombstone-dense for the purposes of triggering a
		// tombstone density compaction. Data blocks may also be considered
		// tombstone-dense if they meet the criteria defined by
		// DeletionSizeRatioThreshold below. Tombstone-dense blocks are identified
		// when sstables are written, and so this is effectively an option for
		// sstable writers. The default value is 100.
		NumDeletionsThreshold int

		// DeletionSizeRatioThreshold defines the minimum ratio of the size of
		// point tombstones to the size of a data block that must be reached
		// for that block to be considered tombstone-dense for the purposes of
		// triggering a tombstone density compaction. Data blocks may also be
		// considered tombstone-dense if they meet the criteria defined by
		// NumDeletionsThreshold above. Tombstone-dense blocks are identified
		// when sstables are written, and so this is effectively an option for
		// sstable writers. The default value is 0.5.
		DeletionSizeRatioThreshold float32

		// TombstoneDenseCompactionThreshold is the minimum percent of data
		// blocks in a table that must be tombstone-dense for that table to be
		// eligible for a tombstone density compaction. It should be defined as a
		// ratio out of 1. The default value is 0.10.
		//
		// If multiple tables are eligible for a tombstone density compaction, then
		// tables with a higher percent of tombstone-dense blocks are still
		// prioritized for compaction.
		//
		// A zero or negative value disables tombstone density compactions.
		TombstoneDenseCompactionThreshold float64

		// FileCacheShards is the number of shards per file cache.
		// Reducing the value can reduce the number of idle goroutines per DB
		// instance which can be useful in scenarios with a lot of DB instances
		// and a large number of CPUs, but doing so can lead to higher contention
		// in the file cache and reduced performance.
		//
		// The default value is the number of logical CPUs, which can be
		// limited by runtime.GOMAXPROCS.
		FileCacheShards int

		// ValidateOnIngest schedules validation of sstables after they have
		// been ingested.
		//
		// By default, this value is false.
		ValidateOnIngest bool

		// LevelMultiplier configures the size multiplier used to determine the
		// desired size of each level of the LSM. Defaults to 10.
		LevelMultiplier int

		// MultiLevelCompactionHeuristic determines whether to add an additional
		// level to a conventional two level compaction.
		MultiLevelCompactionHeuristic func() MultiLevelHeuristic

		// EnableValueBlocks is used to decide whether to enable writing
		// TableFormatPebblev3 sstables. This setting is only respected by a
		// specific subset of format major versions: FormatSSTableValueBlocks,
		// FormatFlushableIngest and FormatPrePebblev1MarkedCompacted. In lower
		// format major versions, value blocks are never enabled. In higher
		// format major versions, value blocks are always enabled.
		EnableValueBlocks func() bool

		// ShortAttributeExtractor is used iff EnableValueBlocks() returns true
		// (else ignored). If non-nil, a ShortAttribute can be extracted from the
		// value and stored with the key, when the value is stored elsewhere.
		ShortAttributeExtractor ShortAttributeExtractor

		// DisableIngestAsFlushable disables lazy ingestion of sstables through
		// a WAL write and memtable rotation. Only effectual if the format
		// major version is at least `FormatFlushableIngest`.
		DisableIngestAsFlushable func() bool

		// RemoteStorage enables use of remote storage (e.g. S3) for storing
		// sstables. Setting this option enables use of CreateOnShared option and
		// allows ingestion of external files.
		RemoteStorage remote.StorageFactory

		// If CreateOnShared is non-zero, new sstables are created on remote storage
		// (using CreateOnSharedLocator and with the appropriate
		// CreateOnSharedStrategy). These sstables can be shared between different
		// Pebble instances; the lifecycle of such objects is managed by the
		// remote.Storage constructed by options.RemoteStorage.
		//
		// Can only be used when RemoteStorage is set (and recognizes
		// CreateOnSharedLocator).
		CreateOnShared        remote.CreateOnSharedStrategy
		CreateOnSharedLocator remote.Locator

		// CacheSizeBytesBytes is the size of the on-disk block cache for objects
		// on shared storage in bytes. If it is 0, no cache is used.
		SecondaryCacheSizeBytes int64

		// EnableDeleteOnlyCompactionExcises enables delete-only compactions to also
		// apply delete-only compaction hints on sstables that partially overlap
		// with it. This application happens through an excise, similar to
		// the excise phase of IngestAndExcise.
		EnableDeleteOnlyCompactionExcises func() bool

		// CompactionScheduler, if set, is used to create a scheduler to limit
		// concurrent compactions as well as to pace compactions already chosen. If
		// nil, a default scheduler is created and used.
		CompactionScheduler func() CompactionScheduler

		UserKeyCategories UserKeyCategories

		// ValueSeparationPolicy controls the policy for separating values into
		// external blob files. If nil, value separation defaults to disabled.
		// The value separation policy is ignored if EnableColumnarBlocks() is
		// false.
		ValueSeparationPolicy func() ValueSeparationPolicy

		// SpanPolicyFunc is used to determine the SpanPolicy for a key region.
		SpanPolicyFunc SpanPolicyFunc

		// VirtualTableRewriteUnreferencedFraction configures the minimum fraction of
		// unreferenced data in a backing table required to trigger a virtual table
		// rewrite compaction. This is calculated as the ratio of unreferenced
		// data size to total backing file size. A value of 0.0 triggers
		// rewrites for any amount of unreferenced data. A value of 1.0 disables
		// virtual table rewrite compactions entirely. The default value is 0.30
		// (rewrite when >= 30% of backing data is unreferenced).
		VirtualTableRewriteUnreferencedFraction func() float64

		// IteratorTracking configures periodic logging of iterators held open for
		// too long.
		IteratorTracking struct {
			// PollInterval is the interval at which to log a report of long-lived
			// iterators. If zero, disables iterator tracking.
			//
			// The default value is 0 (disabled).
			PollInterval time.Duration

			// MaxAge is the age above which iterators are considered long-lived. If
			// zero, disables iterator tracking.
			//
			// The default value is 0 (disabled).
			MaxAge time.Duration
		}
	}

	// Filters is a map from filter policy name to filter policy. It is used for
	// debugging tools which may be used on multiple databases configured with
	// different filter policies. It is not necessary to populate this filters
	// map during normal usage of a DB (it will be done automatically by
	// EnsureDefaults).
	Filters map[string]FilterPolicy

	// FlushDelayDeleteRange configures how long the database should wait before
	// forcing a flush of a memtable that contains a range deletion. Disk space
	// cannot be reclaimed until the range deletion is flushed. No automatic
	// flush occurs if zero.
	FlushDelayDeleteRange time.Duration

	// FlushDelayRangeKey configures how long the database should wait before
	// forcing a flush of a memtable that contains a range key. Range keys in
	// the memtable prevent lazy combined iteration, so it's desirable to flush
	// range keys promptly. No automatic flush occurs if zero.
	FlushDelayRangeKey time.Duration

	// FlushSplitBytes denotes the target number of bytes per sublevel in
	// each flush split interval (i.e. range between two flush split keys)
	// in L0 sstables. When set to zero, only a single sstable is generated
	// by each flush. When set to a non-zero value, flushes are split at
	// points to meet L0's TargetFileSize, any grandparent-related overlap
	// options, and at boundary keys of L0 flush split intervals (which are
	// targeted to contain around FlushSplitBytes bytes in each sublevel
	// between pairs of boundary keys). Splitting sstables during flush
	// allows increased compaction flexibility and concurrency when those
	// tables are compacted to lower levels.
	FlushSplitBytes int64

	// FormatMajorVersion sets the format of on-disk files. It is
	// recommended to set the format major version to an explicit
	// version, as the default may change over time.
	//
	// At Open if the existing database is formatted using a later
	// format major version that is known to this version of Pebble,
	// Pebble will continue to use the later format major version. If
	// the existing database's version is unknown, the caller may use
	// FormatMostCompatible and will be able to open the database
	// regardless of its actual version.
	//
	// If the existing database is formatted using a format major
	// version earlier than the one specified, Open will automatically
	// ratchet the database to the specified format major version.
	FormatMajorVersion FormatMajorVersion

	// FS provides the interface for persistent file storage.
	//
	// The default value uses the underlying operating system's file system.
	FS vfs.FS

	// KeySchema is the name of the key schema that should be used when writing
	// new sstables. There must be a key schema with this name defined in
	// KeySchemas. If not set, colblk.DefaultKeySchema is used to construct a
	// default key schema.
	KeySchema string

	// KeySchemas defines the set of known schemas of user keys. When columnar
	// blocks are in use (see FormatColumnarBlocks), the user may specify how a
	// key should be decomposed into columns. Each KeySchema must have a unique
	// name. The schema named by Options.KeySchema is used while writing
	// sstables during flushes and compactions.
	//
	// Multiple KeySchemas may be used over the lifetime of a database. Once a
	// KeySchema is used, it must be provided in KeySchemas in subsequent calls
	// to Open for perpetuity.
	KeySchemas sstable.KeySchemas

	// Lock, if set, must be a database lock acquired through LockDirectory for
	// the same directory passed to Open. If provided, Open will skip locking
	// the directory. Closing the database will not release the lock, and it's
	// the responsibility of the caller to release the lock after closing the
	// database.
	//
	// Open will enforce that the Lock passed locks the same directory passed to
	// Open. Concurrent calls to Open using the same Lock are detected and
	// prohibited.
	Lock *base.DirLock

	// The count of L0 files necessary to trigger an L0 compaction.
	L0CompactionFileThreshold int

	// The amount of L0 read-amplification necessary to trigger an L0 compaction.
	L0CompactionThreshold int

	// Hard limit on L0 read-amplification, computed as the number of L0
	// sublevels. Writes are stopped when this threshold is reached.
	L0StopWritesThreshold int

	// The maximum number of bytes for LBase. The base level is the level which
	// L0 is compacted into. The base level is determined dynamically based on
	// the existing data in the LSM. The maximum number of bytes for other levels
	// is computed dynamically based on the base level's maximum size. When the
	// maximum number of bytes for a level is exceeded, compaction is requested.
	LBaseMaxBytes int64

	// TargetFileSizes contains the target file size for each level, ignoring
	// unpopulated levels. Specifically:
	// - TargetFileSizes[0] is the target file size for L0;
	// - TargetFileSizes[1] is the target file size for Lbase;
	// - TargetFileSizes[2] is the target file size for Lbase+1;
	// and so on.
	//
	// The default value for TargetFileSizes[0] is 2MB.
	// The default value for TargetFileSizes[i] is TargetFileSizes[i-1] * 2.
	TargetFileSizes [manifest.NumLevels]int64

	// Per-level options. Levels[i] contains the options for Li (regardless of
	// what Lbase is).
	Levels [manifest.NumLevels]LevelOptions

	// LoggerAndTracer will be used, if non-nil, else Logger will be used and
	// tracing will be a noop.

	// Logger used to write log messages.
	//
	// The default logger uses the Go standard library log package.
	Logger Logger
	// LoggerAndTracer is used for writing log messages and traces.
	LoggerAndTracer LoggerAndTracer

	// MaxManifestFileSize is the maximum size the MANIFEST file is allowed to
	// become. When the MANIFEST exceeds this size it is rolled over and a new
	// MANIFEST is created.
	MaxManifestFileSize int64

	// MaxOpenFiles is a soft limit on the number of open files that can be
	// used by the DB.
	//
	// The default value is 1000.
	MaxOpenFiles int

	// The size of a MemTable in steady state. The actual MemTable size starts at
	// min(256KB, MemTableSize) and doubles for each subsequent MemTable up to
	// MemTableSize. This reduces the memory pressure caused by MemTables for
	// short lived (test) DB instances. Note that more than one MemTable can be
	// in existence since flushing a MemTable involves creating a new one and
	// writing the contents of the old one in the
	// background. MemTableStopWritesThreshold places a hard limit on the size of
	// the queued MemTables.
	//
	// The default value is 4MB.
	MemTableSize uint64

	// Hard limit on the number of queued of MemTables. Writes are stopped when
	// the sum of the queued memtable sizes exceeds:
	//   MemTableStopWritesThreshold * MemTableSize.
	//
	// This value should be at least 2 or writes will stop whenever a MemTable is
	// being flushed.
	//
	// The default value is 2.
	MemTableStopWritesThreshold int

	// Merger defines the associative merge operation to use for merging values
	// written with {Batch,DB}.Merge.
	//
	// The default merger concatenates values.
	Merger *Merger

	// CompactionConcurrencyRange returns a [lower, upper] range for the number of
	// compactions Pebble runs in parallel (with the caveats below), not including
	// download compactions (which have a separate limit specified by
	// MaxConcurrentDownloads).
	//
	// The lower value is the concurrency allowed under normal circumstances.
	// Pebble can dynamically increase the concurrency based on heuristics (like
	// high read amplification or compaction debt) up to the maximum.
	//
	// The upper value is a rough upper bound since delete-only compactions (a) do
	// not use the CompactionScheduler, and (b) the CompactionScheduler may use
	// other criteria to decide on how many compactions to permit.
	//
	// Elaborating on (b), when the ConcurrencyLimitScheduler is being used, the
	// value returned by DB.GetAllowedWithoutPermission fully controls how many
	// compactions get to run. Other CompactionSchedulers may use additional
	// criteria, like resource availability.
	//
	// Elaborating on (a), we don't use the CompactionScheduler to schedule
	// delete-only compactions since they are expected to be almost free from a
	// CPU and disk usage perspective. Since the CompactionScheduler does not
	// know about their existence, the total running count can exceed this
	// value. For example, consider CompactionConcurrencyRange returns 3, and the
	// current value returned from DB.GetAllowedWithoutPermission is also 3. Say
	// 3 delete-only compactions are also running. Then the
	// ConcurrencyLimitScheduler can also start 3 other compactions, for a total
	// of 6.
	//
	// DB.GetAllowedWithoutPermission returns a value in the interval
	// [lower, upper]. A value > lower is returned:
	//  - when L0 read-amplification passes the L0CompactionConcurrency threshold;
	//  - when compaction debt passes the CompactionDebtConcurrency threshold;
	//  - when there are multiple manual compactions waiting to run.
	//
	// lower and upper must be greater than 0. If lower > upper, then upper is
	// used for both.
	//
	// The default values are 1, 1.
	CompactionConcurrencyRange func() (lower, upper int)

	// MaxConcurrentDownloads specifies the maximum number of download
	// compactions. These are compactions that copy an external file to the local
	// store.
	//
	// This limit is independent of CompactionConcurrencyRange; at any point in
	// time, we may be running CompactionConcurrencyRange non-download compactions
	// and MaxConcurrentDownloads download compactions.
	//
	// MaxConcurrentDownloads() must be greater than 0.
	//
	// The default value is 1.
	MaxConcurrentDownloads func() int

	// DisableAutomaticCompactions dictates whether automatic compactions are
	// scheduled or not. The default is false (enabled). This option is only used
	// externally when running a manual compaction, and internally for tests.
	//
	// Note that this field is only consulted while holding DB.mu, so it it safe
	// for a test to modify it while holding DB.mu.
	DisableAutomaticCompactions bool

	// DisableConsistencyCheck disables the consistency check that is performed on
	// open. Should only be used when a database cannot be opened normally (e.g.
	// some of the tables don't exist / aren't accessible).
	DisableConsistencyCheck bool

	// DisableTableStats dictates whether tables should be loaded asynchronously
	// to compute statistics that inform compaction heuristics. The collection
	// of table stats improves compaction of tombstones, reclaiming disk space
	// more quickly and in some cases reducing write amplification in the
	// presence of tombstones. Disabling table stats may be useful in tests
	// that require determinism as the asynchronicity of table stats collection
	// introduces significant nondeterminism.
	DisableTableStats bool

	// NoSyncOnClose decides whether the Pebble instance will enforce a
	// close-time synchronization (e.g., fdatasync() or sync_file_range())
	// on files it writes to. Setting this to true removes the guarantee for a
	// sync on close. Some implementations can still issue a non-blocking sync.
	NoSyncOnClose bool

	// NumPrevManifest is the number of non-current or older manifests which
	// we want to keep around for debugging purposes. By default, we're going
	// to keep one older manifest.
	NumPrevManifest int

	// ReadOnly indicates that the DB should be opened in read-only mode. Writes
	// to the DB will return an error, background compactions are disabled, and
	// the flush that normally occurs after replaying the WAL at startup is
	// disabled.
	ReadOnly bool

	// FileCache is an initialized FileCache which should be set as an
	// option if the DB needs to be initialized with a pre-existing file cache.
	// If FileCache is nil, then a file cache which is unique to the DB instance
	// is created. FileCache can be shared between db instances by setting it here.
	// The FileCache set here must use the same underlying cache as Options.Cache
	// and pebble will panic otherwise.
	FileCache *FileCache

	// BlockPropertyCollectors is a list of BlockPropertyCollector creation
	// functions. A new BlockPropertyCollector is created for each sstable
	// built and lives for the lifetime of writing that table.
	BlockPropertyCollectors []func() BlockPropertyCollector

	// WALBytesPerSync sets the number of bytes to write to a WAL before calling
	// Sync on it in the background. Just like with BytesPerSync above, this
	// helps smooth out disk write latencies, and avoids cases where the OS
	// writes a lot of buffered data to disk at once. However, this is less
	// necessary with WALs, as many write operations already pass in
	// Sync = true.
	//
	// The default value is 0, i.e. no background syncing. This matches the
	// default behaviour in RocksDB.
	WALBytesPerSync int

	// WALDir specifies the directory to store write-ahead logs (WALs) in. If
	// empty (the default), WALs will be stored in the same directory as sstables
	// (i.e. the directory passed to pebble.Open).
	WALDir string

	// WALDirLock, if set, must be a directory lock acquired through LockDirectory
	// for the same directory set on WALDir passed to Open. If provided, Open will
	// skip locking the directory. Closing the database will not release the lock,
	// and it's the responsibility of the caller to release the lock after closing the
	// database.
	//
	// Open will enforce that the Lock passed locks the same WAL directory passed to
	// Open. Concurrent calls to Open using the same Lock are detected and
	// prohibited.
	WALDirLock *base.DirLock

	// WALFailover may be set to configure Pebble to monitor writes to its
	// write-ahead log and failover to writing write-ahead log entries to a
	// secondary location (eg, a separate physical disk). WALFailover may be
	// used to improve write availability in the presence of transient disk
	// unavailability.
	WALFailover *WALFailoverOptions

	// WALRecoveryDirs is a list of additional directories that should be
	// scanned for the existence of additional write-ahead logs. WALRecoveryDirs
	// is expected to be used when starting Pebble with a new WALDir or a new
	// WALFailover configuration. The directories associated with the previous
	// configuration may still contain WALs that are required for recovery of
	// the current database state.
	//
	// If a previous WAL configuration may have stored WALs elsewhere but there
	// is not a corresponding entry in WALRecoveryDirs, Open will error.
	WALRecoveryDirs []wal.Dir

	// WALMinSyncInterval is the minimum duration between syncs of the WAL. If
	// WAL syncs are requested faster than this interval, they will be
	// artificially delayed. Introducing a small artificial delay (500us) between
	// WAL syncs can allow more operations to arrive and reduce IO operations
	// while having a minimal impact on throughput. This option is supplied as a
	// closure in order to allow the value to be changed dynamically. The default
	// value is 0.
	//
	// TODO(peter): rather than a closure, should there be another mechanism for
	// changing options dynamically?
	WALMinSyncInterval func() time.Duration

	// The controls below manage deletion pacing, which slows down
	// deletions when compactions finish or when readers close and
	// obsolete files must be cleaned up. Rapid deletion of many
	// files simultaneously can increase disk latency on certain
	// SSDs, and this functionality helps protect against that.

	// TargetByteDeletionRate is the rate (in bytes per second) at which sstable file
	// deletions are limited to (under normal circumstances).
	//
	// This value is only a best-effort target; the effective rate can be
	// higher if deletions are falling behind or disk space is running low.
	//
	// A returned value of 0 disables deletion pacing (this is also the default).
	TargetByteDeletionRate func() int

	// FreeSpaceThresholdBytes specifies the minimum amount of free disk space that Pebble
	// attempts to maintain. If free disk space drops below this threshold, deletions
	// are accelerated above TargetByteDeletionRate until the threshold is restored.
	// Default is 16GB.
	FreeSpaceThresholdBytes uint64

	// FreeSpaceTimeframe sets the duration (in seconds) within which Pebble attempts
	// to restore the free disk space back to FreeSpaceThreshold. A lower value means
	// more aggressive deletions. Default is 10s.
	FreeSpaceTimeframe time.Duration

	// ObsoleteBytesMaxRatio specifies the maximum allowed ratio of obsolete files to
	// live files. If this ratio is exceeded, Pebble speeds up deletions above the
	// TargetByteDeletionRate until the ratio is restored. Default is 0.20.
	ObsoleteBytesMaxRatio float64

	// ObsoleteBytesTimeframe sets the duration (in seconds) within which Pebble aims
	// to restore the obsolete-to-live bytes ratio below ObsoleteBytesMaxRatio. A lower
	// value means more aggressive deletions. Default is 300s.
	ObsoleteBytesTimeframe time.Duration

	// EnableSQLRowSpillMetrics specifies whether the Pebble instance will only be used
	// to temporarily persist data spilled to disk for row-oriented SQL query execution.
	EnableSQLRowSpillMetrics bool

	// AllocatorSizeClasses provides a sorted list containing the supported size
	// classes of the underlying memory allocator. This provides hints to the
	// sstable block writer's flushing policy to select block sizes that
	// preemptively reduce internal fragmentation when loaded into the block cache.
	AllocatorSizeClasses []int

	// private options are only used by internal tests or are used internally
	// for facilitating upgrade paths of unconfigurable functionality.
	private struct {
		// disableDeleteOnlyCompactions prevents the scheduling of delete-only
		// compactions that drop sstables wholy covered by range tombstones or
		// range key tombstones.
		disableDeleteOnlyCompactions bool

		// disableElisionOnlyCompactions prevents the scheduling of elision-only
		// compactions that rewrite sstables in place in order to elide obsolete
		// keys.
		disableElisionOnlyCompactions bool

		// disableLazyCombinedIteration is a private option used by the
		// metamorphic tests to test equivalence between lazy-combined iteration
		// and constructing the range-key iterator upfront. It's a private
		// option to avoid littering the public interface with options that we
		// do not want to allow users to actually configure.
		disableLazyCombinedIteration bool

		// testingAlwaysWaitForCleanup is set by some tests to force waiting for
		// obsolete file deletion (to make events deterministic).
		testingAlwaysWaitForCleanup bool

		// testingBeforeIngestApplyFunc when non-nil, is called when ingesting,
		// before calling DB.ingestApply.
		testingBeforeIngestApplyFunc func()

		// timeNow returns the current time. It defaults to time.Now. It's
		// configurable here so that tests can mock the current time.
		timeNow func() time.Time

		// fsCloser holds a closer that should be invoked after a DB using these
		// Options is closed. This is used to automatically stop the
		// long-running goroutine associated with the disk-health-checking FS.
		// See the initialization of FS in EnsureDefaults. Note that care has
		// been taken to ensure that it is still safe to continue using the FS
		// after this closer has been invoked. However, if write operations
		// against the FS are made after the DB is closed, the FS may leak a
		// goroutine indefinitely.
		fsCloser io.Closer
	}
}

// ValueSeparationPolicy controls the policy for separating values into
// external blob files.
type ValueSeparationPolicy struct {
	// Enabled controls whether value separation is enabled.
	Enabled bool
	// MinimumSize imposes a lower bound on the size of values that can be
	// separated into a blob file. Values smaller than this are always written
	// to the sstable (but may still be written to a value block within the
	// sstable).
	//
	// MinimumSize must be > 0.
	MinimumSize int
	// MaxBlobReferenceDepth limits the number of potentially overlapping (in
	// the keyspace) blob files that can be referenced by a single sstable. If a
	// compaction may produce an output sstable referencing more than this many
	// overlapping blob files, the compaction will instead rewrite referenced
	// values into new blob files.
	//
	// MaxBlobReferenceDepth must be > 0.
	MaxBlobReferenceDepth int
	// RewriteMinimumAge specifies how old a blob file must be in order for it
	// to be eligible for a rewrite that reclaims disk space. Lower values
	// reduce space amplification at the cost of write amplification
	RewriteMinimumAge time.Duration
	// GarbageRatioLowPriority is a value in the range [0, 1.0] configuring how
	// aggressively blob files should be written in order to reduce space
	// amplification induced by value separation. As sstable compactions rewrite
	// references to blob files, data may be duplicated.  Older blob files
	// containing the duplicated data may need to remain because other sstables
	// are referencing other values contained in the same file.
	//
	// The DB can rewrite these blob files in place in order to reduce this
	// space amplification, but this incurs write amplification. This option
	// configures how much garbage may accrue before the DB will attempt to
	// rewrite blob files to reduce it if there is no other higher priority
	// compaction work available. A value of 0.20 indicates that once 20% of
	// values in blob files are unreferenced, the DB should attempt to rewrite
	// blob files to reclaim disk space.
	//
	// A value of 1.0 indicates that the DB should never attempt to rewrite blob
	// files.
	GarbageRatioLowPriority float64
	// GarbageRatioHighPriority is a value in the range [0, 1.0] configuring how
	// much garbage must be present before the DB will schedule blob file
	// rewrite compactions at a high priority (including above default
	// compactions necessary to keep up with incoming writes). At most 1 blob file
	// rewrite compaction will be scheduled at a time.
	//
	// See GarbageRatioLowPriority for more details. Must be >=
	// GarbageRatioLowPriority.
	GarbageRatioHighPriority float64
}

// SpanPolicy contains policies that can vary by key range. The zero value is
// the default value.
type SpanPolicy struct {
	// Prefer a faster compression algorithm for the keys in this span.
	//
	// This is useful for keys that are frequently read or written but which don't
	// amount to a significant amount of space.
	PreferFastCompression bool

	// DisableValueSeparationBySuffix disables discriminating KVs depending on
	// suffix.
	//
	// Among a set of keys with the same prefix, Pebble's default heuristics
	// optimize access to the KV with the smallest suffix. This is useful for MVCC
	// keys (where the smallest suffix is the latest version), but should be
	// disabled for keys where the suffix does not correspond to a version.
	DisableValueSeparationBySuffix bool

	// ValueStoragePolicy is a hint used to determine where to store the values
	// for KVs.
	ValueStoragePolicy ValueStoragePolicy
}

// String returns a string representation of the SpanPolicy.
func (p SpanPolicy) String() string {
	var sb strings.Builder
	if p.PreferFastCompression {
		sb.WriteString("fast-compression,")
	}
	if p.DisableValueSeparationBySuffix {
		sb.WriteString("disable-value-separation-by-suffix,")
	}
	switch p.ValueStoragePolicy {
	case ValueStorageLowReadLatency:
		sb.WriteString("low-read-latency,")
	case ValueStorageLatencyTolerant:
		sb.WriteString("latency-tolerant,")
	}
	return strings.TrimSuffix(sb.String(), ",")
}

// ValueStoragePolicy is a hint used to determine where to store the values for
// KVs.
type ValueStoragePolicy uint8

const (
	// ValueStorageDefault is the default value; Pebble will respect global
	// configuration for value blocks and value separation.
	ValueStorageDefault ValueStoragePolicy = iota

	// ValueStorageLowReadLatency indicates Pebble should prefer storing values
	// in-place.
	ValueStorageLowReadLatency

	// ValueStorageLatencyTolerant indicates value retrieval can tolerate
	// additional latency, so Pebble should aggressively prefer storing values
	// separately if it can reduce write amplification.
	//
	// If the global Options' enable value separation, Pebble may choose to
	// separate values under the LatencyTolerant policy even if they do not meet
	// the minimum size threshold of the global Options' ValueSeparationPolicy.
	ValueStorageLatencyTolerant
)

// SpanPolicyFunc is used to determine the SpanPolicy for a key region.
//
// The returned policy is valid from the start key until (and not including) the
// end key.
//
// A flush or compaction will call this function once for the first key to be
// output. If the compaction reaches the end key, the current output sst is
// finished and the function is called again.
//
// The end key can be empty, in which case the policy is valid for the entire
// keyspace after startKey.
type SpanPolicyFunc func(startKey []byte) (policy SpanPolicy, endKey []byte, err error)

// SpanAndPolicy defines a key range and the policy to apply to it.
type SpanAndPolicy struct {
	KeyRange KeyRange
	Policy   SpanPolicy
}

// MakeStaticSpanPolicyFunc returns a SpanPolicyFunc that applies a given policy
// to the given span (and the default policy outside the span). The supplied
// policies must be non-overlapping in key range.
func MakeStaticSpanPolicyFunc(cmp base.Compare, inputPolicies ...SpanAndPolicy) SpanPolicyFunc {
	// Collect all the boundaries of the input policies, sort and deduplicate them.
	uniqueKeys := make([][]byte, 0, 2*len(inputPolicies))
	for i := range inputPolicies {
		uniqueKeys = append(uniqueKeys, inputPolicies[i].KeyRange.Start)
		uniqueKeys = append(uniqueKeys, inputPolicies[i].KeyRange.End)
	}
	slices.SortFunc(uniqueKeys, cmp)
	uniqueKeys = slices.CompactFunc(uniqueKeys, func(a, b []byte) bool { return cmp(a, b) == 0 })

	// Create a list of policies.
	policies := make([]SpanPolicy, len(uniqueKeys)-1)
	for _, p := range inputPolicies {
		idx, _ := slices.BinarySearchFunc(uniqueKeys, p.KeyRange.Start, cmp)
		policies[idx] = p.Policy
	}

	return func(startKey []byte) (_ SpanPolicy, endKey []byte, _ error) {
		// Find the policy that applies to the start key.
		idx, eq := slices.BinarySearchFunc(uniqueKeys, startKey, cmp)
		switch idx {
		case len(uniqueKeys):
			// The start key is after the last policy.
			return SpanPolicy{}, nil, nil
		case len(uniqueKeys) - 1:
			if eq {
				// The start key is exactly the start of the last policy.
				return SpanPolicy{}, nil, nil
			}
		case 0:
			if !eq {
				// The start key is before the first policy.
				return SpanPolicy{}, uniqueKeys[0], nil
			}
		}
		if eq {
			// The start key is exactly the start of this policy.
			return policies[idx], uniqueKeys[idx+1], nil
		}
		// The start key is between two policies.
		return policies[idx-1], uniqueKeys[idx], nil
	}
}

// WALFailoverOptions configures the WAL failover mechanics to use during
// transient write unavailability on the primary WAL volume.
type WALFailoverOptions struct {
	// Secondary indicates the secondary directory and VFS to use in the event a
	// write to the primary WAL stalls. The Lock field may be set during setup to
	// preacquire the lock on the secondary directory.
	Secondary wal.Dir

	// FailoverOptions provides configuration of the thresholds and intervals
	// involved in WAL failover. If any of its fields are left unspecified,
	// reasonable defaults will be used.
	wal.FailoverOptions
}

// ReadaheadConfig controls the use of read-ahead.
type ReadaheadConfig = objstorageprovider.ReadaheadConfig

// JemallocSizeClasses exports sstable.JemallocSizeClasses.
var JemallocSizeClasses = sstable.JemallocSizeClasses

// DebugCheckLevels calls CheckLevels on the provided database.
// It may be set in the DebugCheck field of Options to check
// level invariants whenever a new version is installed.
func DebugCheckLevels(db *DB) error {
	return db.CheckLevels(nil)
}

// DBCompressionSettings contains compression settings for the entire store. It
// defines compression profiles for each LSM level.
type DBCompressionSettings struct {
	Name   string
	Levels [manifest.NumLevels]*block.CompressionProfile
}

// Predefined compression settings.
var (
	DBCompressionNone    = UniformDBCompressionSettings(block.NoCompression)
	DBCompressionFastest = UniformDBCompressionSettings(block.FastestCompression)
	DBCompressionFast    = func() DBCompressionSettings {
		cs := DBCompressionSettings{Name: "Fast"}
		for i := 0; i < manifest.NumLevels-1; i++ {
			cs.Levels[i] = block.FastestCompression
		}
		cs.Levels[manifest.NumLevels-1] = block.FastCompression
		return cs
	}()
	DBCompressionBalanced = func() DBCompressionSettings {
		cs := DBCompressionSettings{Name: "Balanced"}
		for i := 0; i < manifest.NumLevels-2; i++ {
			cs.Levels[i] = block.FastestCompression
		}
		cs.Levels[manifest.NumLevels-2] = block.FastCompression
		cs.Levels[manifest.NumLevels-1] = block.BalancedCompression
		return cs
	}()
	DBCompressionGood = func() DBCompressionSettings {
		cs := DBCompressionSettings{Name: "Good"}
		for i := 0; i < manifest.NumLevels-2; i++ {
			cs.Levels[i] = block.FastestCompression
		}
		cs.Levels[manifest.NumLevels-2] = block.BalancedCompression
		cs.Levels[manifest.NumLevels-1] = block.GoodCompression
		return cs
	}()
)

// UniformDBCompressionSettings returns a DBCompressionSettings which uses the
// same compression profile on all LSM levels.
func UniformDBCompressionSettings(profile *block.CompressionProfile) DBCompressionSettings {
	cs := DBCompressionSettings{Name: profile.Name}
	for i := range cs.Levels {
		cs.Levels[i] = profile
	}
	return cs
}

// ApplyCompressionSettings sets the Compression field in each LevelOptions to
// call the given function and return the compression profile for that level.
func (o *Options) ApplyCompressionSettings(csFn func() DBCompressionSettings) {
	for i := range o.Levels {
		levelIdx := i
		o.Levels[i].Compression = func() *block.CompressionProfile {
			return csFn().Levels[levelIdx]
		}
	}
}

// EnsureDefaults ensures that the default values for all options are set if a
// valid value was not already specified.
func (o *Options) EnsureDefaults() {
	if o.Cache == nil && o.CacheSize == 0 {
		o.CacheSize = cacheDefaultSize
	}
	o.Comparer = o.Comparer.EnsureDefaults()

	if o.BytesPerSync <= 0 {
		o.BytesPerSync = 512 << 10 // 512 KB
	}
	if o.Cleaner == nil {
		o.Cleaner = DeleteCleaner{}
	}

	if o.TargetByteDeletionRate == nil {
		o.TargetByteDeletionRate = func() int { return 0 }
	}

	if o.FreeSpaceThresholdBytes == 0 {
		o.FreeSpaceThresholdBytes = 16 << 30 // 16 GB
	}

	if o.FreeSpaceTimeframe == 0 {
		o.FreeSpaceTimeframe = 10 * time.Second
	}

	if o.ObsoleteBytesMaxRatio == 0 {
		o.ObsoleteBytesMaxRatio = 0.20
	}

	if o.ObsoleteBytesTimeframe == 0 {
		o.ObsoleteBytesTimeframe = 300 * time.Second
	}

	if o.Experimental.DisableIngestAsFlushable == nil {
		o.Experimental.DisableIngestAsFlushable = func() bool { return false }
	}
	if o.Experimental.L0CompactionConcurrency <= 0 {
		o.Experimental.L0CompactionConcurrency = 10
	}
	if o.Experimental.CompactionDebtConcurrency <= 0 {
		o.Experimental.CompactionDebtConcurrency = 1 << 30 // 1 GB
	}
	if o.Experimental.CompactionGarbageFractionForMaxConcurrency == nil {
		// When 40% of the DB is garbage, the compaction concurrency is at the
		// maximum permitted.
		o.Experimental.CompactionGarbageFractionForMaxConcurrency = func() float64 { return 0.4 }
	}
	if o.Experimental.ValueSeparationPolicy == nil {
		o.Experimental.ValueSeparationPolicy = func() ValueSeparationPolicy {
			return ValueSeparationPolicy{Enabled: false}
		}
	}
	if o.KeySchema == "" && len(o.KeySchemas) == 0 {
		ks := colblk.DefaultKeySchema(o.Comparer, 16 /* bundleSize */)
		o.KeySchema = ks.Name
		o.KeySchemas = sstable.MakeKeySchemas(&ks)
	}
	if o.L0CompactionThreshold <= 0 {
		o.L0CompactionThreshold = 4
	}
	if o.L0CompactionFileThreshold <= 0 {
		// Some justification for the default of 500:
		// Why not smaller?:
		// - The default target file size for L0 is 2MB, so 500 files is <= 1GB
		//   of data. At observed compaction speeds of > 20MB/s, L0 can be
		//   cleared of all files in < 1min, so this backlog is not huge.
		// - 500 files is low overhead for instantiating L0 sublevels from
		//   scratch.
		// - Lower values were observed to cause excessive and inefficient
		//   compactions out of L0 in a TPCC import benchmark.
		// Why not larger?:
		// - More than 1min to compact everything out of L0.
		// - CockroachDB's admission control system uses a threshold of 1000
		//   files to start throttling writes to Pebble. Using 500 here gives
		//   us headroom between when Pebble should start compacting L0 and
		//   when the admission control threshold is reached.
		//
		// We can revisit this default in the future based on better
		// experimental understanding.
		//
		// TODO(jackson): Experiment with slightly lower thresholds [or higher
		// admission control thresholds] to see whether a higher L0 score at the
		// threshold (currently 2.0) is necessary for some workloads to avoid
		// starving L0 in favor of lower-level compactions.
		o.L0CompactionFileThreshold = 500
	}
	if o.L0StopWritesThreshold <= 0 {
		o.L0StopWritesThreshold = 12
	}
	if o.LBaseMaxBytes <= 0 {
		o.LBaseMaxBytes = 64 << 20 // 64 MB
	}
	if o.TargetFileSizes[0] <= 0 {
		o.TargetFileSizes[0] = 2 << 20 // 2 MB
	}
	for i := 1; i < len(o.TargetFileSizes); i++ {
		if o.TargetFileSizes[i] <= 0 {
			o.TargetFileSizes[i] = o.TargetFileSizes[i-1] * 2
		}
	}
	o.Levels[0].EnsureL0Defaults()
	for i := 1; i < len(o.Levels); i++ {
		o.Levels[i].EnsureL1PlusDefaults(&o.Levels[i-1])
	}
	if o.Logger == nil {
		o.Logger = DefaultLogger
	}
	if o.EventListener == nil {
		o.EventListener = &EventListener{}
	}
	o.EventListener.EnsureDefaults(o.Logger)
	if o.MaxManifestFileSize == 0 {
		o.MaxManifestFileSize = 128 << 20 // 128 MB
	}
	if o.MaxOpenFiles == 0 {
		o.MaxOpenFiles = 1000
	}
	if o.MemTableSize <= 0 {
		o.MemTableSize = 4 << 20 // 4 MB
	}
	if o.MemTableStopWritesThreshold <= 0 {
		o.MemTableStopWritesThreshold = 2
	}
	if o.Merger == nil {
		o.Merger = DefaultMerger
	}
	if o.CompactionConcurrencyRange == nil {
		o.CompactionConcurrencyRange = func() (int, int) { return 1, 1 }
	}
	if o.MaxConcurrentDownloads == nil {
		o.MaxConcurrentDownloads = func() int { return 1 }
	}
	if o.NumPrevManifest <= 0 {
		o.NumPrevManifest = 1
	}

	if o.FormatMajorVersion == FormatDefault {
		o.FormatMajorVersion = FormatMinSupported
		if o.Experimental.CreateOnShared != remote.CreateOnSharedNone {
			o.FormatMajorVersion = FormatMinForSharedObjects
		}
	}

	if o.FS == nil {
		o.WithFSDefaults()
	}
	if o.FlushSplitBytes <= 0 {
		o.FlushSplitBytes = 2 * o.TargetFileSizes[0]
	}
	if o.WALFailover != nil {
		o.WALFailover.FailoverOptions.EnsureDefaults()
	}
	if o.Experimental.UseDeprecatedCompensatedScore == nil {
		o.Experimental.UseDeprecatedCompensatedScore = func() bool { return false }
	}
	if o.Experimental.LevelMultiplier <= 0 {
		o.Experimental.LevelMultiplier = defaultLevelMultiplier
	}
	if o.Experimental.ReadCompactionRate == 0 {
		o.Experimental.ReadCompactionRate = 16000
	}
	if o.Experimental.ReadSamplingMultiplier == 0 {
		o.Experimental.ReadSamplingMultiplier = 1 << 4
	}
	if o.Experimental.NumDeletionsThreshold == 0 {
		o.Experimental.NumDeletionsThreshold = sstable.DefaultNumDeletionsThreshold
	}
	if o.Experimental.DeletionSizeRatioThreshold == 0 {
		o.Experimental.DeletionSizeRatioThreshold = sstable.DefaultDeletionSizeRatioThreshold
	}
	if o.Experimental.TombstoneDenseCompactionThreshold == 0 {
		o.Experimental.TombstoneDenseCompactionThreshold = 0.10
	}
	if o.Experimental.FileCacheShards <= 0 {
		o.Experimental.FileCacheShards = runtime.GOMAXPROCS(0)
	}
	if o.Experimental.MultiLevelCompactionHeuristic == nil {
		o.Experimental.MultiLevelCompactionHeuristic = OptionWriteAmpHeuristic
	}
	if o.Experimental.SpanPolicyFunc == nil {
		o.Experimental.SpanPolicyFunc = func(startKey []byte) (SpanPolicy, []byte, error) { return SpanPolicy{}, nil, nil }
	}
	if o.Experimental.VirtualTableRewriteUnreferencedFraction == nil {
		o.Experimental.VirtualTableRewriteUnreferencedFraction = func() float64 { return defaultVirtualTableUnreferencedFraction }
	}
	if o.private.timeNow == nil {
		o.private.timeNow = time.Now
	}
	// TODO(jackson): Enable value separation by default once we have confidence
	// in a default policy.

	o.initMaps()
}

// TargetFileSize computes the target file size for the given output level.
func (o *Options) TargetFileSize(outputLevel int, baseLevel int) int64 {
	if outputLevel == 0 {
		return o.TargetFileSizes[0]
	}
	if baseLevel > outputLevel {
		panic(fmt.Sprintf("invalid base level %d (output level %d)", baseLevel, outputLevel))
	}
	return o.TargetFileSizes[outputLevel-baseLevel+1]
}

// DefaultOptions returns a new Options object with the default values set.
func DefaultOptions() *Options {
	o := &Options{}
	o.EnsureDefaults()
	return o
}

// WithFSDefaults configures the Options to wrap the configured filesystem with
// the default virtual file system middleware, like disk-health checking.
func (o *Options) WithFSDefaults() {
	if o.FS == nil {
		o.FS = vfs.Default
	}
	o.FS, o.private.fsCloser = vfs.WithDiskHealthChecks(o.FS, 5*time.Second, nil,
		func(info vfs.DiskSlowInfo) {
			o.EventListener.DiskSlow(info)
		})
}

// AddEventListener adds the provided event listener to the Options, in addition
// to any existing event listener.
func (o *Options) AddEventListener(l EventListener) {
	if o.EventListener != nil {
		l = TeeEventListener(l, *o.EventListener)
	}
	o.EventListener = &l
}

// initMaps initializes the Comparers, Filters, and Mergers maps.
func (o *Options) initMaps() {
	for i := range o.Levels {
		l := &o.Levels[i]
		if l.FilterPolicy != NoFilterPolicy {
			if o.Filters == nil {
				o.Filters = make(map[string]FilterPolicy)
			}
			name := l.FilterPolicy.Name()
			if _, ok := o.Filters[name]; !ok {
				o.Filters[name] = l.FilterPolicy
			}
		}
	}
}

// Clone creates a shallow-copy of the supplied options.
func (o *Options) Clone() *Options {
	if o == nil {
		return &Options{}
	}
	n := *o
	if o.WALFailover != nil {
		c := *o.WALFailover
		n.WALFailover = &c
	}
	return &n
}

func (o *Options) String() string {
	var buf bytes.Buffer

	cacheSize := o.CacheSize
	if o.Cache != nil {
		cacheSize = o.Cache.MaxSize()
	}

	fmt.Fprintf(&buf, "[Version]\n")
	fmt.Fprintf(&buf, "  pebble_version=0.1\n")
	fmt.Fprintf(&buf, "\n")
	fmt.Fprintf(&buf, "[Options]\n")
	fmt.Fprintf(&buf, "  bytes_per_sync=%d\n", o.BytesPerSync)
	fmt.Fprintf(&buf, "  cache_size=%d\n", cacheSize)
	fmt.Fprintf(&buf, "  cleaner=%s\n", o.Cleaner)
	fmt.Fprintf(&buf, "  compaction_debt_concurrency=%d\n", o.Experimental.CompactionDebtConcurrency)
	fmt.Fprintf(&buf, "  compaction_garbage_fraction_for_max_concurrency=%.2f\n",
		o.Experimental.CompactionGarbageFractionForMaxConcurrency())
	fmt.Fprintf(&buf, "  comparer=%s\n", o.Comparer.Name)
	fmt.Fprintf(&buf, "  disable_wal=%t\n", o.DisableWAL)
	if o.Experimental.DisableIngestAsFlushable != nil && o.Experimental.DisableIngestAsFlushable() {
		fmt.Fprintf(&buf, "  disable_ingest_as_flushable=%t\n", true)
	}
	fmt.Fprintf(&buf, "  flush_delay_delete_range=%s\n", o.FlushDelayDeleteRange)
	fmt.Fprintf(&buf, "  flush_delay_range_key=%s\n", o.FlushDelayRangeKey)
	fmt.Fprintf(&buf, "  flush_split_bytes=%d\n", o.FlushSplitBytes)
	fmt.Fprintf(&buf, "  format_major_version=%d\n", o.FormatMajorVersion)
	fmt.Fprintf(&buf, "  key_schema=%s\n", o.KeySchema)
	fmt.Fprintf(&buf, "  l0_compaction_concurrency=%d\n", o.Experimental.L0CompactionConcurrency)
	fmt.Fprintf(&buf, "  l0_compaction_file_threshold=%d\n", o.L0CompactionFileThreshold)
	fmt.Fprintf(&buf, "  l0_compaction_threshold=%d\n", o.L0CompactionThreshold)
	fmt.Fprintf(&buf, "  l0_stop_writes_threshold=%d\n", o.L0StopWritesThreshold)
	fmt.Fprintf(&buf, "  lbase_max_bytes=%d\n", o.LBaseMaxBytes)
	if o.Experimental.LevelMultiplier != defaultLevelMultiplier {
		fmt.Fprintf(&buf, "  level_multiplier=%d\n", o.Experimental.LevelMultiplier)
	}
	lower, upper := o.CompactionConcurrencyRange()
	fmt.Fprintf(&buf, "  concurrent_compactions=%d\n", lower)
	fmt.Fprintf(&buf, "  max_concurrent_compactions=%d\n", upper)
	fmt.Fprintf(&buf, "  max_concurrent_downloads=%d\n", o.MaxConcurrentDownloads())
	fmt.Fprintf(&buf, "  max_manifest_file_size=%d\n", o.MaxManifestFileSize)
	fmt.Fprintf(&buf, "  max_open_files=%d\n", o.MaxOpenFiles)
	fmt.Fprintf(&buf, "  mem_table_size=%d\n", o.MemTableSize)
	fmt.Fprintf(&buf, "  mem_table_stop_writes_threshold=%d\n", o.MemTableStopWritesThreshold)
	fmt.Fprintf(&buf, "  min_deletion_rate=%d\n", o.TargetByteDeletionRate())
	fmt.Fprintf(&buf, "  free_space_threshold_bytes=%d\n", o.FreeSpaceThresholdBytes)
	fmt.Fprintf(&buf, "  free_space_timeframe=%s\n", o.FreeSpaceTimeframe.String())
	fmt.Fprintf(&buf, "  obsolete_bytes_max_ratio=%f\n", o.ObsoleteBytesMaxRatio)
	fmt.Fprintf(&buf, "  obsolete_bytes_timeframe=%s\n", o.ObsoleteBytesTimeframe.String())
	fmt.Fprintf(&buf, "  merger=%s\n", o.Merger.Name)
	if o.Experimental.MultiLevelCompactionHeuristic != nil {
		fmt.Fprintf(&buf, "  multilevel_compaction_heuristic=%s\n", o.Experimental.MultiLevelCompactionHeuristic().String())
	}
	fmt.Fprintf(&buf, "  read_compaction_rate=%d\n", o.Experimental.ReadCompactionRate)
	fmt.Fprintf(&buf, "  read_sampling_multiplier=%d\n", o.Experimental.ReadSamplingMultiplier)
	fmt.Fprintf(&buf, "  num_deletions_threshold=%d\n", o.Experimental.NumDeletionsThreshold)
	fmt.Fprintf(&buf, "  deletion_size_ratio_threshold=%f\n", o.Experimental.DeletionSizeRatioThreshold)
	fmt.Fprintf(&buf, "  tombstone_dense_compaction_threshold=%f\n", o.Experimental.TombstoneDenseCompactionThreshold)
	// We no longer care about strict_wal_tail, but set it to true in case an
	// older version reads the options.
	fmt.Fprintf(&buf, "  strict_wal_tail=%t\n", true)
	fmt.Fprintf(&buf, "  table_cache_shards=%d\n", o.Experimental.FileCacheShards)
	fmt.Fprintf(&buf, "  validate_on_ingest=%t\n", o.Experimental.ValidateOnIngest)
	fmt.Fprintf(&buf, "  wal_dir=%s\n", o.WALDir)
	fmt.Fprintf(&buf, "  wal_bytes_per_sync=%d\n", o.WALBytesPerSync)
	fmt.Fprintf(&buf, "  secondary_cache_size_bytes=%d\n", o.Experimental.SecondaryCacheSizeBytes)
	fmt.Fprintf(&buf, "  create_on_shared=%d\n", o.Experimental.CreateOnShared)

	if o.Experimental.IteratorTracking.PollInterval != 0 {
		fmt.Fprintf(&buf, "  iterator_tracking_poll_interval=%s\n", o.Experimental.IteratorTracking.PollInterval)
	}
	if o.Experimental.IteratorTracking.MaxAge != 0 {
		fmt.Fprintf(&buf, "  iterator_tracking_max_age=%s\n", o.Experimental.IteratorTracking.MaxAge)
	}

	// Private options.
	//
	// These options are only encoded if true, because we do not want them to
	// appear in production serialized Options files, since they're testing-only
	// options. They're only serialized when true, which still ensures that the
	// metamorphic tests may propagate them to subprocesses.
	if o.private.disableDeleteOnlyCompactions {
		fmt.Fprintln(&buf, "  disable_delete_only_compactions=true")
	}
	if o.private.disableElisionOnlyCompactions {
		fmt.Fprintln(&buf, "  disable_elision_only_compactions=true")
	}
	if o.private.disableLazyCombinedIteration {
		fmt.Fprintln(&buf, "  disable_lazy_combined_iteration=true")
	}

	if o.Experimental.ValueSeparationPolicy != nil {
		policy := o.Experimental.ValueSeparationPolicy()
		if policy.Enabled {
			fmt.Fprintln(&buf)
			fmt.Fprintln(&buf, "[Value Separation]")
			fmt.Fprintf(&buf, "  enabled=%t\n", policy.Enabled)
			fmt.Fprintf(&buf, "  minimum_size=%d\n", policy.MinimumSize)
			fmt.Fprintf(&buf, "  max_blob_reference_depth=%d\n", policy.MaxBlobReferenceDepth)
			fmt.Fprintf(&buf, "  rewrite_minimum_age=%s\n", policy.RewriteMinimumAge)
			fmt.Fprintf(&buf, "  garbage_ratio_low_priority=%.2f\n", policy.GarbageRatioLowPriority)
			fmt.Fprintf(&buf, "  garbage_ratio_high_priority=%.2f\n", policy.GarbageRatioHighPriority)
		}
	}

	if o.WALFailover != nil {
		unhealthyThreshold, _ := o.WALFailover.FailoverOptions.UnhealthyOperationLatencyThreshold()
		fmt.Fprintf(&buf, "\n")
		fmt.Fprintf(&buf, "[WAL Failover]\n")
		fmt.Fprintf(&buf, "  secondary_dir=%s\n", o.WALFailover.Secondary.Dirname)
		fmt.Fprintf(&buf, "  primary_dir_probe_interval=%s\n", o.WALFailover.FailoverOptions.PrimaryDirProbeInterval)
		fmt.Fprintf(&buf, "  healthy_probe_latency_threshold=%s\n", o.WALFailover.FailoverOptions.HealthyProbeLatencyThreshold)
		fmt.Fprintf(&buf, "  healthy_interval=%s\n", o.WALFailover.FailoverOptions.HealthyInterval)
		fmt.Fprintf(&buf, "  unhealthy_sampling_interval=%s\n", o.WALFailover.FailoverOptions.UnhealthySamplingInterval)
		fmt.Fprintf(&buf, "  unhealthy_operation_latency_threshold=%s\n", unhealthyThreshold)
		fmt.Fprintf(&buf, "  elevated_write_stall_threshold_lag=%s\n", o.WALFailover.FailoverOptions.ElevatedWriteStallThresholdLag)
	}

	for i := range o.Levels {
		l := &o.Levels[i]
		fmt.Fprintf(&buf, "\n")
		fmt.Fprintf(&buf, "[Level \"%d\"]\n", i)
		fmt.Fprintf(&buf, "  block_restart_interval=%d\n", l.BlockRestartInterval)
		fmt.Fprintf(&buf, "  block_size=%d\n", l.BlockSize)
		fmt.Fprintf(&buf, "  block_size_threshold=%d\n", l.BlockSizeThreshold)
		fmt.Fprintf(&buf, "  compression=%s\n", l.Compression().Name)
		fmt.Fprintf(&buf, "  filter_policy=%s\n", l.FilterPolicy.Name())
		fmt.Fprintf(&buf, "  filter_type=%s\n", l.FilterType)
		fmt.Fprintf(&buf, "  index_block_size=%d\n", l.IndexBlockSize)
		fmt.Fprintf(&buf, "  target_file_size=%d\n", o.TargetFileSizes[i])
	}

	return buf.String()
}

type parseOptionsFuncs struct {
	visitNewSection          func(i, j int, section string) error
	visitKeyValue            func(i, j int, section, key, value string) error
	visitCommentOrWhitespace func(i, j int, whitespace string) error
}

// parseOptions takes options serialized by Options.String() and parses them
// into keys and values. It calls fns.visitNewSection for the beginning of each
// new section, fns.visitKeyValue for each key-value pair, and
// visitCommentOrWhitespace for comments and whitespace between key-value pairs.
func parseOptions(s string, fns parseOptionsFuncs) error {
	var section, mappedSection string
	i := 0
	for i < len(s) {
		rem := s[i:]
		j := strings.IndexByte(rem, '\n')
		if j < 0 {
			j = len(rem)
		} else {
			j += 1 // Include the newline.
		}
		line := strings.TrimSpace(s[i : i+j])
		startOff, endOff := i, i+j
		i += j

		if len(line) == 0 || line[0] == ';' || line[0] == '#' {
			// Skip blank lines and comments.
			if fns.visitCommentOrWhitespace != nil {
				if err := fns.visitCommentOrWhitespace(startOff, endOff, line); err != nil {
					return err
				}
			}
			continue
		}
		n := len(line)
		if line[0] == '[' && line[n-1] == ']' {
			// Parse section.
			section = line[1 : n-1]
			// RocksDB uses a similar (INI-style) syntax for the OPTIONS file, but
			// different section names and keys. The "CFOptions ..." paths are the
			// RocksDB versions which we map to the Pebble paths.
			mappedSection = section
			if section == `CFOptions "default"` {
				mappedSection = "Options"
			}
			if fns.visitNewSection != nil {
				if err := fns.visitNewSection(startOff, endOff, mappedSection); err != nil {
					return err
				}
			}
			continue
		}

		pos := strings.Index(line, "=")
		if pos < 0 {
			const maxLen = 50
			if len(line) > maxLen {
				line = line[:maxLen-3] + "..."
			}
			return base.CorruptionErrorf("invalid key=value syntax: %q", errors.Safe(line))
		}

		key := strings.TrimSpace(line[:pos])
		value := strings.TrimSpace(line[pos+1:])

		if section == `CFOptions "default"` {
			switch key {
			case "comparator":
				key = "comparer"
			case "merge_operator":
				key = "merger"
			}
		}
		if fns.visitKeyValue != nil {
			if err := fns.visitKeyValue(startOff, endOff, mappedSection, key, value); err != nil {
				return err
			}
		}
	}
	return nil
}

// ParseHooks contains callbacks to create options fields which can have
// user-defined implementations.
type ParseHooks struct {
	NewCleaner      func(name string) (Cleaner, error)
	NewComparer     func(name string) (*Comparer, error)
	NewFilterPolicy func(name string) (FilterPolicy, error)
	NewKeySchema    func(name string) (KeySchema, error)
	NewMerger       func(name string) (*Merger, error)
	SkipUnknown     func(name, value string) bool
}

// Parse parses the options from the specified string. Note that certain
// options cannot be parsed into populated fields. For example, comparer and
// merger.
func (o *Options) Parse(s string, hooks *ParseHooks) error {
	var valSepPolicy ValueSeparationPolicy
	var concurrencyLimit struct {
		lower    int
		lowerSet bool
		upper    int
		upperSet bool
	}

	visitKeyValue := func(i, j int, section, key, value string) error {
		// WARNING: DO NOT remove entries from the switches below because doing so
		// causes a key previously written to the OPTIONS file to be considered unknown,
		// a backwards incompatible change. Instead, leave in support for parsing the
		// key but simply don't parse the value.

		parseComparer := func(name string) (*Comparer, error) {
			switch name {
			case DefaultComparer.Name:
				return DefaultComparer, nil
			case testkeys.Comparer.Name:
				return testkeys.Comparer, nil
			default:
				if hooks != nil && hooks.NewComparer != nil {
					return hooks.NewComparer(name)
				}
				return nil, nil
			}
		}

		switch {
		case section == "Version":
			switch key {
			case "pebble_version":
			default:
				if hooks != nil && hooks.SkipUnknown != nil && hooks.SkipUnknown(section+"."+key, value) {
					return nil
				}
				return errors.Errorf("pebble: unknown option: %s.%s",
					errors.Safe(section), errors.Safe(key))
			}
			return nil

		case section == "Options":
			var err error
			switch key {
			case "bytes_per_sync":
				o.BytesPerSync, err = strconv.Atoi(value)
			case "cache_size":
				o.CacheSize, err = strconv.ParseInt(value, 10, 64)
			case "cleaner":
				switch value {
				case "archive":
					o.Cleaner = ArchiveCleaner{}
				case "delete":
					o.Cleaner = DeleteCleaner{}
				default:
					if hooks != nil && hooks.NewCleaner != nil {
						o.Cleaner, err = hooks.NewCleaner(value)
					}
				}
			case "comparer":
				var comparer *Comparer
				comparer, err = parseComparer(value)
				if comparer != nil {
					o.Comparer = comparer
				}
			case "compaction_debt_concurrency":
				o.Experimental.CompactionDebtConcurrency, err = strconv.ParseUint(value, 10, 64)
			case "compaction_garbage_fraction_for_max_concurrency":
				var frac float64
				frac, err = strconv.ParseFloat(value, 64)
				if err == nil {
					o.Experimental.CompactionGarbageFractionForMaxConcurrency =
						func() float64 { return frac }
				}
			case "delete_range_flush_delay":
				// NB: This is a deprecated serialization of the
				// `flush_delay_delete_range`.
				o.FlushDelayDeleteRange, err = time.ParseDuration(value)
			case "disable_delete_only_compactions":
				o.private.disableDeleteOnlyCompactions, err = strconv.ParseBool(value)
			case "disable_elision_only_compactions":
				o.private.disableElisionOnlyCompactions, err = strconv.ParseBool(value)
			case "disable_ingest_as_flushable":
				var v bool
				v, err = strconv.ParseBool(value)
				if err == nil {
					o.Experimental.DisableIngestAsFlushable = func() bool { return v }
				}
			case "disable_lazy_combined_iteration":
				o.private.disableLazyCombinedIteration, err = strconv.ParseBool(value)
			case "disable_wal":
				o.DisableWAL, err = strconv.ParseBool(value)
			case "enable_columnar_blocks":
				// Do nothing; option existed in older versions of pebble.
			case "flush_delay_delete_range":
				o.FlushDelayDeleteRange, err = time.ParseDuration(value)
			case "flush_delay_range_key":
				o.FlushDelayRangeKey, err = time.ParseDuration(value)
			case "flush_split_bytes":
				o.FlushSplitBytes, err = strconv.ParseInt(value, 10, 64)
			case "format_major_version":
				// NB: The version written here may be stale. Open does
				// not use the format major version encoded in the
				// OPTIONS file other than to validate that the encoded
				// version is valid right here.
				var v uint64
				v, err = strconv.ParseUint(value, 10, 64)
				if vers := FormatMajorVersion(v); vers > internalFormatNewest || vers == FormatDefault {
					err = errors.Newf("unsupported format major version %d", o.FormatMajorVersion)
				}
				if err == nil {
					o.FormatMajorVersion = FormatMajorVersion(v)
				}
			case "key_schema":
				o.KeySchema = value
				if o.KeySchemas == nil {
					o.KeySchemas = make(map[string]*KeySchema)
				}
				if _, ok := o.KeySchemas[o.KeySchema]; !ok {
					if strings.HasPrefix(value, "DefaultKeySchema(") && strings.HasSuffix(value, ")") {
						argsStr := strings.TrimSuffix(strings.TrimPrefix(value, "DefaultKeySchema("), ")")
						args := strings.FieldsFunc(argsStr, func(r rune) bool {
							return unicode.IsSpace(r) || r == ','
						})
						var comparer *base.Comparer
						var bundleSize int
						comparer, err = parseComparer(args[0])
						if err == nil {
							bundleSize, err = strconv.Atoi(args[1])
						}
						if err == nil {
							schema := colblk.DefaultKeySchema(comparer, bundleSize)
							o.KeySchema = schema.Name
							o.KeySchemas[o.KeySchema] = &schema
						}
					} else if hooks != nil && hooks.NewKeySchema != nil {
						var schema KeySchema
						schema, err = hooks.NewKeySchema(value)
						if err == nil {
							o.KeySchemas[value] = &schema
						}
					}
				}
			case "l0_compaction_concurrency":
				o.Experimental.L0CompactionConcurrency, err = strconv.Atoi(value)
			case "l0_compaction_file_threshold":
				o.L0CompactionFileThreshold, err = strconv.Atoi(value)
			case "l0_compaction_threshold":
				o.L0CompactionThreshold, err = strconv.Atoi(value)
			case "l0_stop_writes_threshold":
				o.L0StopWritesThreshold, err = strconv.Atoi(value)
			case "l0_sublevel_compactions":
				// Do nothing; option existed in older versions of pebble.
			case "lbase_max_bytes":
				o.LBaseMaxBytes, err = strconv.ParseInt(value, 10, 64)
			case "level_multiplier":
				o.Experimental.LevelMultiplier, err = strconv.Atoi(value)
			case "concurrent_compactions":
				concurrencyLimit.lowerSet = true
				concurrencyLimit.lower, err = strconv.Atoi(value)
			case "max_concurrent_compactions":
				concurrencyLimit.upperSet = true
				concurrencyLimit.upper, err = strconv.Atoi(value)
			case "max_concurrent_downloads":
				var concurrentDownloads int
				concurrentDownloads, err = strconv.Atoi(value)
				if concurrentDownloads <= 0 {
					err = errors.New("max_concurrent_compactions cannot be <= 0")
				} else {
					o.MaxConcurrentDownloads = func() int { return concurrentDownloads }
				}
			case "max_manifest_file_size":
				o.MaxManifestFileSize, err = strconv.ParseInt(value, 10, 64)
			case "max_open_files":
				o.MaxOpenFiles, err = strconv.Atoi(value)
			case "mem_table_size":
				o.MemTableSize, err = strconv.ParseUint(value, 10, 64)
			case "mem_table_stop_writes_threshold":
				o.MemTableStopWritesThreshold, err = strconv.Atoi(value)
			case "min_compaction_rate":
				// Do nothing; option existed in older versions of pebble, and
				// may be meaningful again eventually.
			case "min_deletion_rate":
				var rate int
				rate, err = strconv.Atoi(value)
				if err == nil {
					o.TargetByteDeletionRate = func() int { return rate }
				}
			case "free_space_threshold_bytes":
				o.FreeSpaceThresholdBytes, err = strconv.ParseUint(value, 10, 64)
			case "free_space_timeframe":
				o.FreeSpaceTimeframe, err = time.ParseDuration(value)
			case "obsolete_bytes_max_ratio":
				o.ObsoleteBytesMaxRatio, err = strconv.ParseFloat(value, 64)
			case "obsolete_bytes_timeframe":
				o.ObsoleteBytesTimeframe, err = time.ParseDuration(value)
			case "min_flush_rate":
				// Do nothing; option existed in older versions of pebble, and
				// may be meaningful again eventually.
			case "multilevel_compaction_heuristic":
				switch {
				case value == "none":
					o.Experimental.MultiLevelCompactionHeuristic = OptionNoMultiLevel
				case strings.HasPrefix(value, "wamp"):
					o.Experimental.MultiLevelCompactionHeuristic = OptionWriteAmpHeuristic
					fields := strings.FieldsFunc(strings.TrimPrefix(value, "wamp"), func(r rune) bool {
						return unicode.IsSpace(r) || r == ',' || r == '(' || r == ')'
					})
					if len(fields) != 2 {
						err = errors.Newf("require 2 arguments")
					}
					var h WriteAmpHeuristic
					if err == nil {
						h.AddPropensity, err = strconv.ParseFloat(fields[0], 64)
					}
					if err == nil {
						h.AllowL0, err = strconv.ParseBool(fields[1])
					}

					if err == nil {
						if h.AllowL0 || h.AddPropensity != 0 {
							o.Experimental.MultiLevelCompactionHeuristic = func() MultiLevelHeuristic {
								return &h
							}
						}
					} else {
						err = errors.Wrapf(err, "unexpected wamp heuristic arguments: %s", value)
					}
				default:
					err = errors.Newf("unrecognized multilevel compaction heuristic: %s", value)
				}
			case "point_tombstone_weight":
				// Do nothing; deprecated.
			case "strict_wal_tail":
				var strictWALTail bool
				strictWALTail, err = strconv.ParseBool(value)
				if err == nil && !strictWALTail {
					err = errors.Newf("reading from versions with strict_wal_tail=false no longer supported")
				}
			case "merger":
				switch value {
				case "nullptr":
					o.Merger = nil
				case "pebble.concatenate":
					o.Merger = DefaultMerger
				default:
					if hooks != nil && hooks.NewMerger != nil {
						o.Merger, err = hooks.NewMerger(value)
					}
				}
			case "read_compaction_rate":
				o.Experimental.ReadCompactionRate, err = strconv.ParseInt(value, 10, 64)
			case "read_sampling_multiplier":
				o.Experimental.ReadSamplingMultiplier, err = strconv.ParseInt(value, 10, 64)
			case "num_deletions_threshold":
				o.Experimental.NumDeletionsThreshold, err = strconv.Atoi(value)
			case "deletion_size_ratio_threshold":
				val, parseErr := strconv.ParseFloat(value, 32)
				o.Experimental.DeletionSizeRatioThreshold = float32(val)
				err = parseErr
			case "tombstone_dense_compaction_threshold":
				o.Experimental.TombstoneDenseCompactionThreshold, err = strconv.ParseFloat(value, 64)
			case "table_cache_shards":
				o.Experimental.FileCacheShards, err = strconv.Atoi(value)
			case "table_format":
				switch value {
				case "leveldb":
				case "rocksdbv2":
				default:
					return errors.Errorf("pebble: unknown table format: %q", errors.Safe(value))
				}
			case "table_property_collectors":
				// No longer implemented; ignore.
			case "validate_on_ingest":
				o.Experimental.ValidateOnIngest, err = strconv.ParseBool(value)
			case "wal_dir":
				o.WALDir = value
			case "wal_bytes_per_sync":
				o.WALBytesPerSync, err = strconv.Atoi(value)
			case "max_writer_concurrency":
				// No longer implemented; ignore.
			case "force_writer_parallelism":
				// No longer implemented; ignore.
			case "secondary_cache_size_bytes":
				o.Experimental.SecondaryCacheSizeBytes, err = strconv.ParseInt(value, 10, 64)
			case "create_on_shared":
				var createOnSharedInt int64
				createOnSharedInt, err = strconv.ParseInt(value, 10, 64)
				o.Experimental.CreateOnShared = remote.CreateOnSharedStrategy(createOnSharedInt)
			case "iterator_tracking_poll_interval":
				o.Experimental.IteratorTracking.PollInterval, err = time.ParseDuration(value)
			case "iterator_tracking_max_age":
				o.Experimental.IteratorTracking.MaxAge, err = time.ParseDuration(value)
			default:
				if hooks != nil && hooks.SkipUnknown != nil && hooks.SkipUnknown(section+"."+key, value) {
					return nil
				}
				return errors.Errorf("pebble: unknown option: %s.%s",
					errors.Safe(section), errors.Safe(key))
			}
			return err

		case section == "Value Separation":
			var err error
			switch key {
			case "enabled":
				valSepPolicy.Enabled, err = strconv.ParseBool(value)
			case "minimum_size":
				var minimumSize int
				minimumSize, err = strconv.Atoi(value)
				valSepPolicy.MinimumSize = minimumSize
			case "max_blob_reference_depth":
				valSepPolicy.MaxBlobReferenceDepth, err = strconv.Atoi(value)
			case "rewrite_minimum_age":
				valSepPolicy.RewriteMinimumAge, err = time.ParseDuration(value)
			case "target_garbage_ratio", "garbage_ratio_low_priority":
				// NB: "target_garbage_ratio" is a deprecated name for the same field.
				valSepPolicy.GarbageRatioLowPriority, err = strconv.ParseFloat(value, 64)
			case "garbage_ratio_high_priority":
				valSepPolicy.GarbageRatioHighPriority, err = strconv.ParseFloat(value, 64)
			default:
				if hooks != nil && hooks.SkipUnknown != nil && hooks.SkipUnknown(section+"."+key, value) {
					return nil
				}
				return errors.Errorf("pebble: unknown option: %s.%s", errors.Safe(section), errors.Safe(key))
			}
			return err

		case section == "WAL Failover":
			if o.WALFailover == nil {
				o.WALFailover = new(WALFailoverOptions)
			}
			var err error
			switch key {
			case "secondary_dir":
				o.WALFailover.Secondary = wal.Dir{Dirname: value, FS: vfs.Default}
			case "primary_dir_probe_interval":
				o.WALFailover.PrimaryDirProbeInterval, err = time.ParseDuration(value)
			case "healthy_probe_latency_threshold":
				o.WALFailover.HealthyProbeLatencyThreshold, err = time.ParseDuration(value)
			case "healthy_interval":
				o.WALFailover.HealthyInterval, err = time.ParseDuration(value)
			case "unhealthy_sampling_interval":
				o.WALFailover.UnhealthySamplingInterval, err = time.ParseDuration(value)
			case "unhealthy_operation_latency_threshold":
				var threshold time.Duration
				threshold, err = time.ParseDuration(value)
				o.WALFailover.UnhealthyOperationLatencyThreshold = func() (time.Duration, bool) {
					return threshold, true
				}
			case "elevated_write_stall_threshold_lag":
				o.WALFailover.ElevatedWriteStallThresholdLag, err = time.ParseDuration(value)
			default:
				if hooks != nil && hooks.SkipUnknown != nil && hooks.SkipUnknown(section+"."+key, value) {
					return nil
				}
				return errors.Errorf("pebble: unknown option: %s.%s",
					errors.Safe(section), errors.Safe(key))
			}
			return err

		case strings.HasPrefix(section, "Level "):
			m := regexp.MustCompile(`Level\s*"?(\d+)"?\s*$`).FindStringSubmatch(section)
			if m == nil {
				return errors.Errorf("pebble: unknown section: %q", errors.Safe(section))
			}
			index, err := strconv.Atoi(m[1])
			if err != nil || index < 0 || index >= len(o.Levels) {
				return errors.Errorf("pebble: invalid level index: %q", errors.Safe(m[1]))
			}

			l := &o.Levels[index]

			switch key {
			case "block_restart_interval":
				l.BlockRestartInterval, err = strconv.Atoi(value)
			case "block_size":
				l.BlockSize, err = strconv.Atoi(value)
			case "block_size_threshold":
				l.BlockSizeThreshold, err = strconv.Atoi(value)
			case "compression":
				profile := block.CompressionProfileByName(value)
				if profile == nil {
					return errors.Errorf("pebble: unknown compression: %q", errors.Safe(value))
				}
				l.Compression = func() *sstable.CompressionProfile { return profile }
			case "filter_policy":
				if hooks != nil && hooks.NewFilterPolicy != nil {
					l.FilterPolicy, err = hooks.NewFilterPolicy(value)
				} else {
					l.FilterPolicy = NoFilterPolicy
				}
			case "filter_type":
				switch value {
				case "table":
					l.FilterType = TableFilter
				default:
					return errors.Errorf("pebble: unknown filter type: %q", errors.Safe(value))
				}
			case "index_block_size":
				l.IndexBlockSize, err = strconv.Atoi(value)
			case "target_file_size":
				o.TargetFileSizes[index], err = strconv.ParseInt(value, 10, 64)
			default:
				if hooks != nil && hooks.SkipUnknown != nil && hooks.SkipUnknown(section+"."+key, value) {
					return nil
				}
				return errors.Errorf("pebble: unknown option: %s.%s", errors.Safe(section), errors.Safe(key))
			}
			return err
		}
		if hooks != nil && hooks.SkipUnknown != nil && hooks.SkipUnknown(section+"."+key, value) {
			return nil
		}
		return errors.Errorf("pebble: unknown section %q or key %q", errors.Safe(section), errors.Safe(key))
	}
	err := parseOptions(s, parseOptionsFuncs{
		visitKeyValue: visitKeyValue,
	})
	if err != nil {
		return err
	}
	o.Experimental.ValueSeparationPolicy = func() ValueSeparationPolicy { return valSepPolicy }
	if concurrencyLimit.lowerSet || concurrencyLimit.upperSet {
		if !concurrencyLimit.lowerSet {
			concurrencyLimit.lower = 1
		} else if concurrencyLimit.lower < 1 {
			return errors.New("baseline_concurrent_compactions cannot be <= 0")
		}
		if !concurrencyLimit.upperSet {
			concurrencyLimit.upper = concurrencyLimit.lower
		} else if concurrencyLimit.upper < concurrencyLimit.lower {
			return errors.Newf("max_concurrent_compactions cannot be < %d", concurrencyLimit.lower)
		}
		o.CompactionConcurrencyRange = func() (int, int) {
			return concurrencyLimit.lower, concurrencyLimit.upper
		}
	}
	return nil
}

// ErrMissingWALRecoveryDir is an error returned when a database is attempted to be
// opened without supplying a Options.WALRecoveryDir entry for a directory that
// may contain WALs required to recover a consistent database state.
type ErrMissingWALRecoveryDir struct {
	Dir       string
	ExtraInfo string
}

// Error implements error.
func (e ErrMissingWALRecoveryDir) Error() string {
	return fmt.Sprintf("directory %q may contain relevant WALs but is not in WALRecoveryDirs%s", e.Dir, e.ExtraInfo)
}

// CheckCompatibility verifies the options are compatible with the previous options
// serialized by Options.String(). For example, the Comparer and Merger must be
// the same, or data will not be able to be properly read from the DB.
//
// This function only looks at specific keys and does not error out if the
// options are newer and contain unknown keys.
func (o *Options) CheckCompatibility(storeDir string, previousOptions string) error {
	previousWALDir := ""

	visitKeyValue := func(i, j int, section, key, value string) error {
		switch section + "." + key {
		case "Options.comparer":
			if value != o.Comparer.Name {
				return errors.Errorf("pebble: comparer name from file %q != comparer name from options %q",
					errors.Safe(value), errors.Safe(o.Comparer.Name))
			}
		case "Options.merger":
			// RocksDB allows the merge operator to be unspecified, in which case it
			// shows up as "nullptr".
			if value != "nullptr" && value != o.Merger.Name {
				return errors.Errorf("pebble: merger name from file %q != merger name from options %q",
					errors.Safe(value), errors.Safe(o.Merger.Name))
			}
		case "Options.wal_dir":
			previousWALDir = value
		case "WAL Failover.secondary_dir":
			previousWALSecondaryDir := value
			if err := o.checkWALDir(storeDir, previousWALSecondaryDir, "WALFailover.Secondary changed from previous options"); err != nil {
				return err
			}
		}
		return nil
	}
	if err := parseOptions(previousOptions, parseOptionsFuncs{visitKeyValue: visitKeyValue}); err != nil {
		return err
	}
	if err := o.checkWALDir(storeDir, previousWALDir, "WALDir changed from previous options"); err != nil {
		return err
	}
	return nil
}

// checkWALDir verifies that walDir is among o.WALDir, o.WALFailover.Secondary,
// or o.WALRecoveryDirs. An empty "walDir" maps to the storeDir.
func (o *Options) checkWALDir(storeDir, walDir, errContext string) error {
	walPath := resolveStorePath(storeDir, walDir)
	if walDir == "" {
		walPath = storeDir
	}

	if o.WALDir == "" {
		if walPath == storeDir {
			return nil
		}
	} else {
		if walPath == resolveStorePath(storeDir, o.WALDir) {
			return nil
		}
	}

	if o.WALFailover != nil && walPath == resolveStorePath(storeDir, o.WALFailover.Secondary.Dirname) {
		return nil
	}

	for _, d := range o.WALRecoveryDirs {
		// TODO(radu): should we also check that d.FS is the same as walDir's FS?
		if walPath == resolveStorePath(storeDir, d.Dirname) {
			return nil
		}
	}

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "\n  %s\n", errContext)
	fmt.Fprintf(&buf, "  o.WALDir: %q\n", o.WALDir)
	if o.WALFailover != nil {
		fmt.Fprintf(&buf, "  o.WALFailover.Secondary.Dirname: %q\n", o.WALFailover.Secondary.Dirname)
	}
	fmt.Fprintf(&buf, "  o.WALRecoveryDirs: %d", len(o.WALRecoveryDirs))
	for _, d := range o.WALRecoveryDirs {
		fmt.Fprintf(&buf, "\n    %q", d.Dirname)
	}
	return ErrMissingWALRecoveryDir{Dir: walPath, ExtraInfo: buf.String()}
}

// Validate verifies that the options are mutually consistent. For example,
// L0StopWritesThreshold must be >= L0CompactionThreshold, otherwise a write
// stall would persist indefinitely.
func (o *Options) Validate() error {
	// Note that we can presume Options.EnsureDefaults has been called, so there
	// is no need to check for zero values.

	var buf strings.Builder
	if o.Experimental.L0CompactionConcurrency < 1 {
		fmt.Fprintf(&buf, "L0CompactionConcurrency (%d) must be >= 1\n",
			o.Experimental.L0CompactionConcurrency)
	}
	if o.L0StopWritesThreshold < o.L0CompactionThreshold {
		fmt.Fprintf(&buf, "L0StopWritesThreshold (%d) must be >= L0CompactionThreshold (%d)\n",
			o.L0StopWritesThreshold, o.L0CompactionThreshold)
	}
	if uint64(o.MemTableSize) >= maxMemTableSize {
		fmt.Fprintf(&buf, "MemTableSize (%s) must be < %s\n",
			humanize.Bytes.Uint64(uint64(o.MemTableSize)), humanize.Bytes.Uint64(maxMemTableSize))
	}
	if o.MemTableStopWritesThreshold < 2 {
		fmt.Fprintf(&buf, "MemTableStopWritesThreshold (%d) must be >= 2\n",
			o.MemTableStopWritesThreshold)
	}
	if o.FormatMajorVersion < FormatMinSupported || o.FormatMajorVersion > internalFormatNewest {
		fmt.Fprintf(&buf, "FormatMajorVersion (%d) must be between %d and %d\n",
			o.FormatMajorVersion, FormatMinSupported, internalFormatNewest)
	}
	if o.Experimental.CreateOnShared != remote.CreateOnSharedNone && o.FormatMajorVersion < FormatMinForSharedObjects {
		fmt.Fprintf(&buf, "FormatMajorVersion (%d) when CreateOnShared is set must be at least %d\n",
			o.FormatMajorVersion, FormatMinForSharedObjects)
	}
	if len(o.KeySchemas) > 0 {
		if o.KeySchema == "" {
			fmt.Fprintf(&buf, "KeySchemas is set but KeySchema is not\n")
		}
		if _, ok := o.KeySchemas[o.KeySchema]; !ok {
			fmt.Fprintf(&buf, "KeySchema %q not found in KeySchemas\n", o.KeySchema)
		}
	}
	if policy := o.Experimental.ValueSeparationPolicy(); policy.Enabled {
		if policy.MinimumSize <= 0 {
			fmt.Fprintf(&buf, "ValueSeparationPolicy.MinimumSize (%d) must be > 0\n", policy.MinimumSize)
		}
		if policy.MaxBlobReferenceDepth <= 0 {
			fmt.Fprintf(&buf, "ValueSeparationPolicy.MaxBlobReferenceDepth (%d) must be > 0\n", policy.MaxBlobReferenceDepth)
		}
		if policy.GarbageRatioHighPriority < policy.GarbageRatioLowPriority {
			fmt.Fprintf(&buf, "ValueSeparationPolicy.GarbageRatioHighPriority (%f) must be >= ValueSeparationPolicy.GarbageRatioLowPriority (%f)\n",
				policy.GarbageRatioHighPriority, policy.GarbageRatioLowPriority)
		}
	}

	if buf.Len() == 0 {
		return nil
	}
	return errors.New(buf.String())
}

// MakeReaderOptions constructs sstable.ReaderOptions from the corresponding
// options in the receiver.
func (o *Options) MakeReaderOptions() sstable.ReaderOptions {
	return sstable.ReaderOptions{
		Comparer:   o.Comparer,
		Filters:    o.Filters,
		KeySchemas: o.KeySchemas,
		Merger:     o.Merger,
		ReaderOptions: block.ReaderOptions{
			LoadBlockSema:   o.LoadBlockSema,
			LoggerAndTracer: o.LoggerAndTracer,
		},
	}
}

// MakeWriterOptions constructs sstable.WriterOptions for the specified level
// from the corresponding options in the receiver.
func (o *Options) MakeWriterOptions(level int, format sstable.TableFormat) sstable.WriterOptions {
	writerOpts := sstable.WriterOptions{
		TableFormat:                format,
		Comparer:                   o.Comparer,
		BlockPropertyCollectors:    o.BlockPropertyCollectors,
		AllocatorSizeClasses:       o.AllocatorSizeClasses,
		NumDeletionsThreshold:      o.Experimental.NumDeletionsThreshold,
		DeletionSizeRatioThreshold: o.Experimental.DeletionSizeRatioThreshold,
	}
	if o.Merger != nil {
		writerOpts.MergerName = o.Merger.Name
	}
	if o.KeySchema != "" {
		var ok bool
		writerOpts.KeySchema, ok = o.KeySchemas[o.KeySchema]
		if !ok {
			panic(fmt.Sprintf("invalid schema %q", redact.Safe(o.KeySchema)))
		}
	}
	if format >= sstable.TableFormatPebblev3 {
		writerOpts.ShortAttributeExtractor = o.Experimental.ShortAttributeExtractor
		if format >= sstable.TableFormatPebblev4 && level == numLevels-1 {
			writerOpts.WritingToLowestLevel = true
		}
	}
	levelOpts := o.Levels[level]
	writerOpts.BlockRestartInterval = levelOpts.BlockRestartInterval
	writerOpts.BlockSize = levelOpts.BlockSize
	writerOpts.BlockSizeThreshold = levelOpts.BlockSizeThreshold
	writerOpts.Compression = levelOpts.Compression()
	writerOpts.FilterPolicy = levelOpts.FilterPolicy
	writerOpts.FilterType = levelOpts.FilterType
	writerOpts.IndexBlockSize = levelOpts.IndexBlockSize
	return writerOpts
}

// makeWriterOptions constructs sstable.WriterOptions for the specified level
// using the current DB options and format.
func (d *DB) makeWriterOptions(level int) sstable.WriterOptions {
	return d.opts.MakeWriterOptions(level, d.TableFormat())
}

// makeBlobWriterOptions constructs blob.FileWriterOptions using the current DB
// options and format.
func (d *DB) makeBlobWriterOptions(level int) blob.FileWriterOptions {
	lo := &d.opts.Levels[level]
	return blob.FileWriterOptions{
		Format:       d.BlobFileFormat(),
		Compression:  lo.Compression(),
		ChecksumType: block.ChecksumTypeCRC32c,
		FlushGovernor: block.MakeFlushGovernor(
			lo.BlockSize,
			lo.BlockSizeThreshold,
			base.SizeClassAwareBlockSizeThreshold,
			d.opts.AllocatorSizeClasses,
		),
	}
}

func (o *Options) MakeObjStorageProviderSettings(dirname string) objstorageprovider.Settings {
	s := objstorageprovider.Settings{
		Logger:        o.Logger,
		FS:            o.FS,
		FSDirName:     dirname,
		FSCleaner:     o.Cleaner,
		NoSyncOnClose: o.NoSyncOnClose,
		BytesPerSync:  o.BytesPerSync,
	}
	s.Local.ReadaheadConfig = o.Local.ReadaheadConfig
	s.Remote.StorageFactory = o.Experimental.RemoteStorage
	s.Remote.CreateOnShared = o.Experimental.CreateOnShared
	s.Remote.CreateOnSharedLocator = o.Experimental.CreateOnSharedLocator
	s.Remote.CacheSizeBytes = o.Experimental.SecondaryCacheSizeBytes
	return s
}

// UserKeyCategories describes a partitioning of the user key space. Each
// partition is a category with a name. The categories are used for informative
// purposes only (like pprof labels). Pebble does not treat keys differently
// based on the UserKeyCategories.
//
// The partitions are defined by their upper bounds. The last partition is
// assumed to go until the end of keyspace; its UpperBound is ignored. The rest
// of the partitions are ordered by their UpperBound.
type UserKeyCategories struct {
	categories []UserKeyCategory
	cmp        base.Compare
	// rangeNames[i][j] contains the string referring to the categories in the
	// range [i, j], with j > i.
	rangeNames [][]string
}

// UserKeyCategory describes a partition of the user key space.
//
// User keys >= the previous category's UpperBound and < this category's
// UpperBound are part of this category.
type UserKeyCategory struct {
	Name string
	// UpperBound is the exclusive upper bound of the category. All user keys >= the
	// previous category's UpperBound and < this UpperBound are part of this
	// category.
	UpperBound []byte
}

// MakeUserKeyCategories creates a UserKeyCategories object with the given
// categories. The object is immutable and can be reused across different
// stores.
func MakeUserKeyCategories(cmp base.Compare, categories ...UserKeyCategory) UserKeyCategories {
	n := len(categories)
	if n == 0 {
		return UserKeyCategories{}
	}
	if categories[n-1].UpperBound != nil {
		panic("last category UpperBound must be nil")
	}
	// Verify that the partitions are ordered as expected.
	for i := 1; i < n-1; i++ {
		if cmp(categories[i-1].UpperBound, categories[i].UpperBound) >= 0 {
			panic("invalid UserKeyCategories: key prefixes must be sorted")
		}
	}

	// Precalculate a table of range names to avoid allocations in the
	// categorization path.
	rangeNamesBuf := make([]string, n*n)
	rangeNames := make([][]string, n)
	for i := range rangeNames {
		rangeNames[i] = rangeNamesBuf[:n]
		rangeNamesBuf = rangeNamesBuf[n:]
		for j := i + 1; j < n; j++ {
			rangeNames[i][j] = categories[i].Name + "-" + categories[j].Name
		}
	}
	return UserKeyCategories{
		categories: categories,
		cmp:        cmp,
		rangeNames: rangeNames,
	}
}

// Len returns the number of categories defined.
func (kc *UserKeyCategories) Len() int {
	return len(kc.categories)
}

// CategorizeKey returns the name of the category containing the key.
func (kc *UserKeyCategories) CategorizeKey(userKey []byte) string {
	idx := sort.Search(len(kc.categories)-1, func(i int) bool {
		return kc.cmp(userKey, kc.categories[i].UpperBound) < 0
	})
	return kc.categories[idx].Name
}

// CategorizeKeyRange returns the name of the category containing the key range.
// If the key range spans multiple categories, the result shows the first and
// last category separated by a dash, e.g. `cat1-cat5`.
func (kc *UserKeyCategories) CategorizeKeyRange(startUserKey, endUserKey []byte) string {
	n := len(kc.categories)
	p := sort.Search(n-1, func(i int) bool {
		return kc.cmp(startUserKey, kc.categories[i].UpperBound) < 0
	})
	if p == n-1 || kc.cmp(endUserKey, kc.categories[p].UpperBound) < 0 {
		// Fast path for a single category.
		return kc.categories[p].Name
	}
	// Binary search among the remaining categories.
	q := p + 1 + sort.Search(n-2-p, func(i int) bool {
		return kc.cmp(endUserKey, kc.categories[p+1+i].UpperBound) < 0
	})
	return kc.rangeNames[p][q]
}

const storePathIdentifier = "{store_path}"

// MakeStoreRelativePath takes a path that is relative to the store directory
// and creates a path that can be used for Options.WALDir and wal.Dir.Dirname.
//
// This is used in metamorphic tests, so that the test run directory can be
// copied or moved.
func MakeStoreRelativePath(fs vfs.FS, relativePath string) string {
	if relativePath == "" {
		return storePathIdentifier
	}
	return fs.PathJoin(storePathIdentifier, relativePath)
}

// resolveStorePath is the inverse of MakeStoreRelativePath(). It replaces any
// storePathIdentifier prefix with the store dir.
func resolveStorePath(storeDir, path string) string {
	if remainder, ok := strings.CutPrefix(path, storePathIdentifier); ok {
		return storeDir + remainder
	}
	return path
}
