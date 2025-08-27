// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compact

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/internal/invariants"
	"github.com/cockroachdb/pebble/v2/internal/manifest"
)

// ShouldSplit indicates whether a compaction should split between output files.
// See the OutputSplitter interface.
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

// OutputSplitter is used to determine where to split output tables in a
// compaction.
//
// An OutputSplitter is initialized when we start an output file:
//
//	  s := NewOutputSplitter(...)
//	  for nextKey != nil && !s.ShouldSplitBefore(nextKey, ...) {
//	    ...
//	  }
//		splitKey := s.SplitKey()
//
// OutputSplitter enforces a target file size. This splitter splits to a new
// output file when the estimated file size is 0.5x-2x the target file size. If
// there are overlapping grandparent files, this splitter will attempt to split
// at a grandparent boundary. For example, consider the example where a
// compaction wrote 'd' to the current output file, and the next key has a user
// key 'g':
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
//	                 +----------------------+
//	input            |                      |
//	level            +----------------------+
//	                            \/
//	         +---------------+       +---------------+
//	output   |XXXXXXX|       |       |      |XXXXXXXX|
//	level    +---------------+       +---------------+
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
// OutputSplitter guarantees that we never split user keys between files.
//
// The dominant cost of OutputSplitter is one key comparison per
// ShouldSplitBefore call.
type OutputSplitter struct {
	cmp            base.Compare
	startKey       []byte
	limit          []byte
	targetFileSize uint64
	frontier       frontier

	shouldSplitCalled bool

	nextBoundary splitterBoundary
	// reachedBoundary is set when the frontier reaches a boundary and is cleared
	// in the first ShouldSplitBefore call after that.
	reachedBoundary splitterBoundary

	grandparentBoundariesObserved uint64
	grandparentLevel              manifest.LevelIterator

	splitKey []byte
}

type splitterBoundary struct {
	key []byte
	// isGrandparent is true if this boundary corresponds to a grandparent boundary.
	// It is false when the boundary is unset or it is a limit boundary.
	isGrandparent bool
}

// NewOutputSplitter creates a new OutputSplitter. See OutputSplitter for more
// information.
//
// The limitKey must be either nil (no limit) or a key greater than startKey.
//
// NewOutputSplitter registers the splitter with the provided Frontiers.
//
// Note: it is allowed for the startKey to be behind the current frontier, as
// long as the key in the first ShouldSplitBefore call is at the frontier.
func NewOutputSplitter(
	cmp base.Compare,
	startKey []byte,
	limit []byte,
	targetFileSize uint64,
	grandparentLevel manifest.LevelIterator,
	frontiers *Frontiers,
) *OutputSplitter {
	s := &OutputSplitter{
		cmp:              cmp,
		startKey:         slices.Clone(startKey),
		targetFileSize:   targetFileSize,
		grandparentLevel: grandparentLevel,
	}
	if len(limit) > 0 {
		if invariants.Enabled && cmp(startKey, limit) >= 0 {
			panic("limit <= startKey")
		}
		s.limit = slices.Clone(limit)
	}
	// Find the first grandparent that starts at or after startKey.
	grandparent := s.grandparentLevel.SeekGE(cmp, startKey)
	if grandparent != nil && cmp(grandparent.Smallest().UserKey, startKey) <= 0 {
		grandparent = s.grandparentLevel.Next()
	}
	s.setNextBoundary(grandparent)
	if invariants.Enabled && s.nextBoundary.key != nil && s.cmp(s.nextBoundary.key, startKey) <= 0 {
		panic("first boundary is not after startKey")
	}
	// We start using the frontier after the first ShouldSplitBefore call.
	s.frontier.Init(frontiers, nil, s.boundaryReached)
	return s
}

// boundaryReached is the callback registered with Frontiers; it runs whenever
// the frontier advances past the current boundary.
func (s *OutputSplitter) boundaryReached(key []byte) (nextBoundary []byte) {
	// The passed key can be past the next boundary.
	s.reachedBoundary = s.nextBoundary
	if !s.nextBoundary.isGrandparent {
		s.nextBoundary = splitterBoundary{}
		return nil
	}
	s.grandparentBoundariesObserved++
	s.setNextBoundary(s.grandparentLevel.Next())
	// It is possible that the next boundary is already reached; in that case
	// boundaryReached will just fire again immediately.
	return s.nextBoundary.key
}

func (s *OutputSplitter) setNextBoundary(nextGrandparent *manifest.TableMetadata) {
	if nextGrandparent != nil && (s.limit == nil || s.cmp(nextGrandparent.Smallest().UserKey, s.limit) < 0) {
		s.nextBoundary = splitterBoundary{
			key:           nextGrandparent.Smallest().UserKey,
			isGrandparent: true,
		}
	} else {
		s.nextBoundary = splitterBoundary{
			key:           s.limit,
			isGrandparent: false,
		}
	}
}

// ShouldSplitBefore returns whether we should split the output before the next
// key. It is passed the current estimated file size and a function that can be
// used to retrieve the previous user key.
//
// The equalPrevFn function is used to guarantee no split user keys, without
// OutputSplitter copying each key internally. It is not performance sensitive,
// as it is only called once we decide to split.
//
// Once ShouldSplitBefore returns SplitNow, it must not be called again.
// SplitKey() can be used to retrieve the recommended split key.
//
// INVARIANT: nextUserKey must match the current frontier.
func (s *OutputSplitter) ShouldSplitBefore(
	nextUserKey []byte, estimatedFileSize uint64, equalPrevFn func([]byte) bool,
) ShouldSplit {
	if invariants.Enabled && s.splitKey != nil {
		panic("ShouldSplitBefore called after it returned SplitNow")
	}
	if !s.shouldSplitCalled {
		// The boundary could have been advanced to nextUserKey before the splitter
		// was created. So one single time, we advance the boundary manually.
		s.shouldSplitCalled = true
		for s.nextBoundary.key != nil && s.cmp(s.nextBoundary.key, nextUserKey) <= 0 {
			s.boundaryReached(nextUserKey)
		}
		s.frontier.Update(s.nextBoundary.key)
	}

	if invariants.Enabled && s.nextBoundary.key != nil && s.cmp(s.nextBoundary.key, nextUserKey) <= 0 {
		panic("boundary is behind the next key (or startKey was before the boundary)")
	}
	// Note: s.reachedBoundary can be empty.
	reachedBoundary := s.reachedBoundary
	s.reachedBoundary = splitterBoundary{}
	if invariants.Enabled && reachedBoundary.key != nil && s.cmp(reachedBoundary.key, nextUserKey) > 0 {
		panic("reached boundary ahead of the next user key")
	}
	if reachedBoundary.key != nil && !reachedBoundary.isGrandparent {
		// Limit was reached.
		s.splitKey = s.limit
		return SplitNow
	}

	if s.shouldSplitBasedOnSize(estimatedFileSize, reachedBoundary.isGrandparent) == SplitNow {
		// We want to split here based on size, but we cannot split between two keys
		// with the same UserKey.
		//
		// If we are at a grandparent boundary, we know that this key cannot have the
		// same UserKey as the previous key (otherwise, that key would have been the
		// one hitting this boundary).
		if reachedBoundary.isGrandparent {
			s.splitKey = reachedBoundary.key
			return SplitNow
		}

		// When the target file size limit is very small (in tests), we could end up
		// splitting at the first key, which is not allowed.
		if s.cmp(nextUserKey, s.startKey) <= 0 {
			return NoSplit
		}

		// TODO(radu): it would make for a cleaner interface if we didn't rely on a
		// equalPrevFn. We could make a copy of the key here and split at the next
		// user key that is different; the main difficulty is that various tests
		// expect 1 key per output table if the target file size is very small.
		if !equalPrevFn(nextUserKey) {
			s.splitKey = slices.Clone(nextUserKey)
			return SplitNow
		}
	}

	return NoSplit
}

// SplitKey returns the suggested split key - the first key at which the next
// output file should start.
//
// If ShouldSplitBefore never returned SplitNow, then SplitKey returns the limit
// passed to NewOutputSplitter (which can be nil).
//
// Otherwise, it returns a key <= the key passed to the last ShouldSplitBefore
// call and > the key passed to the previous call to ShouldSplitBefore (and >
// than the start key). This key is guaranteed to be larger than the start key.
func (s *OutputSplitter) SplitKey() []byte {
	s.frontier.Update(nil)
	if s.splitKey != nil {
		if invariants.Enabled && s.cmp(s.splitKey, s.startKey) <= 0 {
			panic(fmt.Sprintf("splitKey %q <= startKey %q", s.splitKey, s.startKey))
		}
		return s.splitKey
	}
	return s.limit
}

// shouldSplitBasedOnSize returns whether we should split based on the file size
// and whether we are at a grandparent boundary.
func (s *OutputSplitter) shouldSplitBasedOnSize(
	estSize uint64, atGrandparentBoundary bool,
) ShouldSplit {
	switch {
	case estSize < s.targetFileSize/2:
		// The estimated file size is less than half the target file size. Don't
		// split it, even if currently aligned with a grandparent file because
		// it's too small.
		return NoSplit
	case estSize >= 2*s.targetFileSize:
		// The estimated file size is double the target file size. Split it even
		// if we were not aligned with a grandparent file boundary to avoid
		// excessively exceeding the target file size.
		return SplitNow
	case !atGrandparentBoundary:
		// Don't split if we're not at a grandparent, except if we've exhausted all
		// the grandparents up to the limit. Then we may want to split purely based
		// on file size.
		if !s.nextBoundary.isGrandparent {
			// There are no more grandparents. Optimize for the target file size
			// and split as soon as we hit the target file size.
			if estSize >= s.targetFileSize {
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
		// boundary we're considering splitting at.
		minimumPctOfTargetSize := 50 + 5*min(s.grandparentBoundariesObserved-1, 8)
		if estSize < (minimumPctOfTargetSize*s.targetFileSize)/100 {
			return NoSplit
		}
		return SplitNow
	}
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

	reached frontierReachedFn
}

// frontierReachedFn is invoked to inform a frontier that its key has been
// reached. It's invoked with the user key that reached the limit. The `key`
// argument is guaranteed to be ≥ the frontier's key.
//
// After frontierReachedFn is invoked, the frontier's key is updated to the
// return value of frontierReachedFn. The frontier is permitted to update its
// key to a user key ≤ the argument `key`.
//
// If a frontier is set to key k1, and reached(k2) is invoked (k2 ≥ k1), the
// frontier will receive reached(k2) calls until it returns nil or a key k3 such
// that k2 < k3. This property is useful for Frontiers that use
// frontierReachedFn invocations to drive iteration through collections of keys
// that may contain multiple keys that are both < k2 and ≥ k1.
type frontierReachedFn func(currentFrontier []byte) (next []byte)

// Init initializes the frontier with the provided key and reached callback.
// The frontier is attached to the provided *Frontiers and the provided reached
// func will be invoked when the *Frontiers is advanced to a key ≥ this
// frontier's key.
func (f *frontier) Init(frontiers *Frontiers, initialKey []byte, reached frontierReachedFn) {
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
			// Fix up the heap root. Note that if the key is still smaller than k, the
			// callback will be invoked again in the same loop.
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
