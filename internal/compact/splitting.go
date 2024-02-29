// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compact

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/sstable"
)

type (
	fileMetadata = manifest.FileMetadata
)

// ShouldSplit indicates whether a compaction should split between output files.
// See the OutputSplitters interface.
type ShouldSplit bool

const (
	// NoSplit may be returned by an OutputSplitter to indicate that it does NOT
	// recommend splitting compaction output sstables between the previous key
	// and the next key.
	NoSplit ShouldSplit = false
	// SplitNow may be returned by an OutputSplitter to indicate that it does
	// recommend splitting compaction output sstables between the previous key
	// and the next key.
	SplitNow ShouldSplit = true
)

// String implements the Stringer interface.
func (s ShouldSplit) String() string {
	if s == NoSplit {
		return "no-split"
	}
	return "split-now"
}

// An OutputSplitter encapsulates logic around switching the output of a
// compaction to a new output file. Additional constraints around switching
// compaction outputs that are specific to that compaction type (eg. flush
// splits) are implemented as separate OutputSplitters that may be composed.
type OutputSplitter interface {
	// ShouldSplitBefore returns whether we should split outputs before the
	// specified "current key". The return value is SplitNow or NoSplit.
	// SplitNow means a split is advised before the specified key, and NoSplit
	// means no split is advised. If ShouldSplitBefore(a) advises a split then
	// ShouldSplitBefore(b) should also advise a split given b >= a, until
	// OnNewOutput is called.
	ShouldSplitBefore(key *base.InternalKey, tw *sstable.Writer) ShouldSplit
	// OnNewOutput updates internal splitter state when the compaction switches
	// to a new sstable, and returns the next limit for the new output which
	// would get used to truncate range tombstones if the compaction iterator
	// runs out of keys. The limit returned MUST be > key according to the
	// compaction's comparator. The specified key is the first key in the new
	// output, or nil if this sstable will only contain range tombstones already
	// in the fragmenter.
	OnNewOutput(key []byte) []byte
}

// CombineSplitters takes a list of OutputSplitters returning an OutputSplitter
// that requests a split whenever any of its child splitters advises a split.
func CombineSplitters(cmp base.Compare, splitters ...OutputSplitter) OutputSplitter {
	return &splitterGroup{
		cmp:       cmp,
		splitters: splitters,
	}
}

// splitterGroup is an OutputSplitter that splits whenever one of its child
// splitters advises a compaction split.
type splitterGroup struct {
	cmp       base.Compare
	splitters []OutputSplitter
}

func (a *splitterGroup) ShouldSplitBefore(
	key *base.InternalKey, tw *sstable.Writer,
) (suggestion ShouldSplit) {
	for _, s := range a.splitters {
		if s.ShouldSplitBefore(key, tw) == SplitNow {
			return SplitNow
		}
	}
	return NoSplit
}

func (a *splitterGroup) OnNewOutput(key []byte) []byte {
	var earliestLimit []byte
	for _, s := range a.splitters {
		limit := s.OnNewOutput(key)
		if limit == nil {
			continue
		}
		if earliestLimit == nil || a.cmp(limit, earliestLimit) < 0 {
			earliestLimit = limit
		}
	}
	return earliestLimit
}

// LimitFuncSplitter returns a new output splitter that splits according to the
// sequence of user keys returned by the provided func. When a new output file
// is begun, the provided func is called with the first key of the new output
// file. If the func returns a non-nil key, a split will be requested before the
// returned key.
func LimitFuncSplitter(f *Frontiers, limitFunc func(userKey []byte) []byte) OutputSplitter {
	lf := &limitFuncSplitter{limitFunc: limitFunc}
	lf.frontier.Init(f, nil, lf.reached)
	return lf
}

type limitFuncSplitter struct {
	frontier  frontier
	limitFunc func(userKey []byte) []byte
	split     ShouldSplit
}

func (lf *limitFuncSplitter) ShouldSplitBefore(
	key *base.InternalKey, tw *sstable.Writer,
) ShouldSplit {
	return lf.split
}

func (lf *limitFuncSplitter) reached(nextKey []byte) []byte {
	lf.split = SplitNow
	return nil
}

func (lf *limitFuncSplitter) OnNewOutput(key []byte) []byte {
	lf.split = NoSplit
	if key != nil {
		// TODO(jackson): For some users, like L0 flush splits, there's no need
		// to binary search over all the flush splits every time. The next split
		// point must be ahead of the previous flush split point.
		limit := lf.limitFunc(key)
		lf.frontier.Update(limit)
		return limit
	}
	lf.frontier.Update(nil)
	return nil
}

// UserKeyChangeSplitter reuturns an output splitter that takes in a child
// splitter and splits when 1) that child splitter has advised a split, and 2)
// the compaction output is at the boundary between two user keys (also the
// boundary between atomic compaction units). Use this splitter to wrap any
// splitters that don't guarantee user key splits (i.e. splitters that make
// their determination in ways other than comparing the current key against a
// limit key.) If a wrapped splitter advises a split, it must continue to advise
// a split until a new output.
func UserKeyChangeSplitter(
	cmp base.Compare, inner OutputSplitter, unsafePrevUserKey func() []byte,
) OutputSplitter {
	return &userKeyChangeSplitter{
		cmp:               cmp,
		splitter:          inner,
		unsafePrevUserKey: unsafePrevUserKey,
	}
}

type userKeyChangeSplitter struct {
	cmp               base.Compare
	splitter          OutputSplitter
	unsafePrevUserKey func() []byte
}

func (u *userKeyChangeSplitter) ShouldSplitBefore(
	key *base.InternalKey, tw *sstable.Writer,
) ShouldSplit {
	// NB: The userKeyChangeSplitter only needs to suffer a key comparison if
	// the wrapped splitter requests a split.
	//
	// We could implement this splitter using Frontiers: When the inner splitter
	// requests a split before key `k`, we'd update a frontier to be
	// ImmediateSuccessor(k). Then on the next key greater than >k, the
	// frontier's `reached` func would be called and we'd return splitNow.
	// This doesn't really save work since duplicate user keys are rare, and it
	// requires us to materialize the ImmediateSuccessor key. It also prevents
	// us from splitting on the same key that the inner splitter requested a
	// split for—instead we need to wait until the next key. The current
	// implementation uses `unsafePrevUserKey` to gain access to the previous
	// key which allows it to immediately respect the inner splitter if
	// possible.
	if split := u.splitter.ShouldSplitBefore(key, tw); split != SplitNow {
		return split
	}
	if u.cmp(key.UserKey, u.unsafePrevUserKey()) > 0 {
		return SplitNow
	}
	return NoSplit
}

func (u *userKeyChangeSplitter) OnNewOutput(key []byte) []byte {
	return u.splitter.OnNewOutput(key)
}

type fileSizeSplitter struct {
	frontier              frontier
	targetFileSize        uint64
	atGrandparentBoundary bool
	boundariesObserved    uint64
	nextGrandparent       *fileMetadata
	grandparents          manifest.LevelIterator
}

// FileSizeSplitter constructs an OutputSplitter that enforces target file
// sizes. This splitter splits to a new output file when the estimated file size
// is 0.5x-2x the target file size. If there are overlapping grandparent files,
// this splitter will attempt to split at a grandparent boundary. For example,
// consider the example where a compaction wrote 'd' to the current output file,
// and the next key has a user key 'g':
//
//	                              previous key   next key
//		                                 |           |
//		                                 |           |
//		                 +---------------|----+   +--|----------+
//		  grandparents:  |       000006  |    |   |  | 000007   |
//		                 +---------------|----+   +--|----------+
//		                 a    b          d    e   f  g       i
//
// Splitting the output file F before 'g' will ensure that the current output
// file F does not overlap the grandparent file 000007. Aligning sstable
// boundaries like this can significantly reduce write amplification, since a
// subsequent compaction of F into the grandparent level will avoid needlessly
// rewriting any keys within 000007 that do not overlap F's bounds. Consider the
// following compaction:
//
//	                       +----------------------+
//		  input            |                      |
//		  level            +----------------------+
//		                              \/
//		           +---------------+       +---------------+
//		  output   |XXXXXXX|       |       |      |XXXXXXXX|
//		  level    +---------------+       +---------------+
//
// The input-level file overlaps two files in the output level, but only
// partially. The beginning of the first output-level file and the end of the
// second output-level file will be rewritten verbatim. This write I/O is
// "wasted" in the sense that no merging is being performed.
//
// To prevent the above waste, this splitter attempts to split output files
// before the start key of grandparent files. It still strives to write output
// files of approximately the target file size, by constraining this splitting
// at grandparent points to apply only if the current output's file size is
// about the right order of magnitude.
//
// Note that, unlike most other splitters, this splitter does not guarantee that
// it will advise splits only at user key change boundaries.
func FileSizeSplitter(
	frontiers *Frontiers, targetFileSize uint64, grandparents manifest.LevelIterator,
) OutputSplitter {
	s := &fileSizeSplitter{targetFileSize: targetFileSize}
	s.nextGrandparent = grandparents.First()
	s.grandparents = grandparents
	if s.nextGrandparent != nil {
		s.frontier.Init(frontiers, s.nextGrandparent.Smallest.UserKey, s.reached)
	}
	return s
}

func (f *fileSizeSplitter) reached(nextKey []byte) []byte {
	f.atGrandparentBoundary = true
	f.boundariesObserved++
	// NB: f.grandparents is a bounded iterator, constrained to the compaction
	// key range.
	f.nextGrandparent = f.grandparents.Next()
	if f.nextGrandparent == nil {
		return nil
	}
	// TODO(jackson): Should we also split before or immediately after
	// grandparents' largest keys? Splitting before the start boundary prevents
	// overlap with the grandparent. Also splitting after the end boundary may
	// increase the probability of move compactions.
	return f.nextGrandparent.Smallest.UserKey
}

func (f *fileSizeSplitter) ShouldSplitBefore(
	key *base.InternalKey, tw *sstable.Writer,
) ShouldSplit {
	atGrandparentBoundary := f.atGrandparentBoundary

	// Clear f.atGrandparentBoundary unconditionally.
	//
	// This is a bit subtle. Even if do decide to split, it's possible that a
	// higher-level splitter will ignore our request (eg, because we're between
	// two internal keys with the same user key). In this case, the next call to
	// shouldSplitBefore will find atGrandparentBoundary=false. This is
	// desirable, because in this case we would've already written the earlier
	// key with the same user key to the output file. The current output file is
	// already doomed to overlap the grandparent whose bound triggered
	// atGrandparentBoundary=true. We should continue on, waiting for the next
	// grandparent boundary.
	f.atGrandparentBoundary = false

	if tw == nil {
		return NoSplit
	}

	estSize := tw.EstimatedSize()
	switch {
	case estSize < f.targetFileSize/2:
		// The estimated file size is less than half the target file size. Don't
		// split it, even if currently aligned with a grandparent file because
		// it's too small.
		return NoSplit
	case estSize >= 2*f.targetFileSize:
		// The estimated file size is double the target file size. Split it even
		// if we were not aligned with a grandparent file boundary to avoid
		// excessively exceeding the target file size.
		return SplitNow
	case !atGrandparentBoundary:
		// Don't split if we're not at a grandparent, except if we've exhausted
		// all the grandparents overlapping this compaction's key range. Then we
		// may want to split purely based on file size.
		if f.nextGrandparent == nil {
			// There are no more grandparents. Optimize for the target file size
			// and split as soon as we hit the target file size.
			if estSize >= f.targetFileSize {
				return SplitNow
			}
		}
		return NoSplit
	default:
		// INVARIANT: atGrandparentBoundary
		// INVARIANT: targetSize/2 < estSize < 2*targetSize
		//
		// The estimated file size is close enough to the target file size that
		// we should consider splitting.
		//
		// Determine whether to split now based on how many grandparent
		// boundaries we have already observed while building this output file.
		// The intuition here is that if the grandparent level is dense in this
		// part of the keyspace, we're likely to continue to have more
		// opportunities to split this file aligned with a grandparent. If this
		// is the first grandparent boundary observed, we split immediately
		// (we're already at ≥50% the target file size). Otherwise, each
		// overlapping grandparent we've observed increases the minimum file
		// size by 5% of the target file size, up to at most 90% of the target
		// file size.
		//
		// TODO(jackson): The particular thresholds are somewhat unprincipled.
		// This is the same heuristic as RocksDB implements. Is there are more
		// principled formulation that can, further reduce w-amp, produce files
		// closer to the target file size, or is more understandable?

		// NB: Subtract 1 from `boundariesObserved` to account for the current
		// boundary we're considering splitting at. `reached` will have
		// incremented it at the same time it set `atGrandparentBoundary`.
		minimumPctOfTargetSize := 50 + 5*min(f.boundariesObserved-1, 8)
		if estSize < (minimumPctOfTargetSize*f.targetFileSize)/100 {
			return NoSplit
		}
		return SplitNow
	}
}

func (f *fileSizeSplitter) OnNewOutput(key []byte) []byte {
	f.boundariesObserved = 0
	return nil
}

// A frontier is used to monitor a compaction's progression across the user
// keyspace.
//
// A frontier hold a user key boundary that it's concerned with in its `key`
// field. If/when the compaction iterator returns an InternalKey with a user key
// _k_ such that k ≥ frontier.key, the compaction iterator invokes the
// frontier's `reached` function, passing _k_ as its argument.
//
// The `reached` function returns a new value to use as the key. If `reached`
// returns nil, the frontier is forgotten and its `reached` method will not be
// invoked again, unless the user calls [Update] to set a new key.
//
// A frontier's key may be updated outside the context of a `reached`
// invocation at any time, through its Update method.
type frontier struct {
	// container points to the containing *Frontiers that was passed to Init
	// when the frontier was initialized.
	container *Frontiers

	// key holds the frontier's current key. If nil, this frontier is inactive
	// and its reached func will not be invoked. The value of this key may only
	// be updated by the `Frontiers` type, or the Update method.
	key []byte

	// reached is invoked to inform a frontier that its key has been reached.
	// It's invoked with the user key that reached the limit. The `key` argument
	// is guaranteed to be ≥ the frontier's key.
	//
	// After reached is invoked, the frontier's key is updated to the return
	// value of `reached`. Note bene, the frontier is permitted to update its
	// key to a user key ≤ the argument `key`.
	//
	// If a frontier is set to key k1, and reached(k2) is invoked (k2 ≥ k1), the
	// frontier will receive reached(k2) calls until it returns nil or a key
	// `k3` such that k2 < k3. This property is useful for Frontiers that use
	// `reached` invocations to drive iteration through collections of keys that
	// may contain multiple keys that are both < k2 and ≥ k1.
	reached func(key []byte) (next []byte)
}

// Init initializes the frontier with the provided key and reached callback.
// The frontier is attached to the provided *Frontiers and the provided reached
// func will be invoked when the *Frontiers is advanced to a key ≥ this
// frontier's key.
func (f *frontier) Init(
	frontiers *Frontiers, initialKey []byte, reached func(key []byte) (next []byte),
) {
	*f = frontier{
		container: frontiers,
		key:       initialKey,
		reached:   reached,
	}
	if initialKey != nil {
		f.container.push(f)
	}
}

// String implements fmt.Stringer.
func (f *frontier) String() string {
	return string(f.key)
}

// Update replaces the existing frontier's key with the provided key. The
// frontier's reached func will be invoked when the new key is reached.
func (f *frontier) Update(key []byte) {
	c := f.container
	prevKeyIsNil := f.key == nil
	f.key = key
	if prevKeyIsNil {
		if key != nil {
			c.push(f)
		}
		return
	}

	// Find the frontier within the heap (it must exist within the heap because
	// f.key was != nil). If the frontier key is now nil, remove it from the
	// heap. Otherwise, fix up its position.
	for i := 0; i < len(c.items); i++ {
		if c.items[i] == f {
			if key != nil {
				c.fix(i)
			} else {
				n := c.len() - 1
				c.swap(i, n)
				c.down(i, n)
				c.items = c.items[:n]
			}
			return
		}
	}
	panic("unreachable")
}

// Frontiers is used to track progression of a task (eg, compaction) across the
// keyspace. Clients that want to be informed when the task advances to a key ≥
// some frontier may register a frontier, providing a callback. The task calls
// `Advance(k)` with each user key encountered, which invokes the `reached` func
// on all tracked Frontiers with `key`s ≤ k.
//
// Internally, Frontiers is implemented as a simple heap.
type Frontiers struct {
	cmp   base.Compare
	items []*frontier
}

// Init initializes a Frontiers for use.
func (f *Frontiers) Init(cmp base.Compare) {
	f.cmp = cmp
}

// String implements fmt.Stringer.
func (f *Frontiers) String() string {
	var buf bytes.Buffer
	for i := 0; i < len(f.items); i++ {
		if i > 0 {
			fmt.Fprint(&buf, ", ")
		}
		fmt.Fprintf(&buf, "%s: %q", f.items[i], f.items[i].key)
	}
	return buf.String()
}

// Advance notifies all member Frontiers with keys ≤ k.
func (f *Frontiers) Advance(k []byte) {
	for len(f.items) > 0 && f.cmp(k, f.items[0].key) >= 0 {
		// This frontier has been reached. Invoke the closure and update with
		// the next frontier.
		f.items[0].key = f.items[0].reached(k)
		if f.items[0].key == nil {
			// This was the final frontier that this user was concerned with.
			// Remove it from the heap.
			f.pop()
		} else {
			// Fix up the heap root.
			f.fix(0)
		}
	}
}

func (f *Frontiers) len() int {
	return len(f.items)
}

func (f *Frontiers) less(i, j int) bool {
	return f.cmp(f.items[i].key, f.items[j].key) < 0
}

func (f *Frontiers) swap(i, j int) {
	f.items[i], f.items[j] = f.items[j], f.items[i]
}

// fix, up and down are copied from the go stdlib.

func (f *Frontiers) fix(i int) {
	if !f.down(i, f.len()) {
		f.up(i)
	}
}

func (f *Frontiers) push(ff *frontier) {
	n := len(f.items)
	f.items = append(f.items, ff)
	f.up(n)
}

func (f *Frontiers) pop() *frontier {
	n := f.len() - 1
	f.swap(0, n)
	f.down(0, n)
	item := f.items[n]
	f.items = f.items[:n]
	return item
}

func (f *Frontiers) up(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !f.less(j, i) {
			break
		}
		f.swap(i, j)
		j = i
	}
}

func (f *Frontiers) down(i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && f.less(j2, j1) {
			j = j2 // = 2*i + 2  // right child
		}
		if !f.less(j, i) {
			break
		}
		f.swap(i, j)
		i = j
	}
	return i > i0
}
