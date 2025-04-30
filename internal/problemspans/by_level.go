// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package problemspans

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/pebble/internal/base"
)

// ByLevel maintains a set of spans (separated by LSM level) with expiration
// times and allows checking for overlap against active (non-expired) spans.
//
// When the spans added to the set are not overlapping, all operations are
// logarithmic.
//
// ByLevel is safe for concurrent use.
type ByLevel struct {
	empty  atomic.Bool
	mu     sync.Mutex
	levels []Set
}

// Init must be called before using the ByLevel.
func (bl *ByLevel) Init(numLevels int, cmp base.Compare) {
	bl.empty.Store(false)
	bl.levels = make([]Set, numLevels)
	for i := range bl.levels {
		bl.levels[i].Init(cmp)
	}
}

// InitForTesting is used by tests which mock the time source.
func (bl *ByLevel) InitForTesting(numLevels int, cmp base.Compare, nowFn func() crtime.Mono) {
	bl.empty.Store(false)
	bl.levels = make([]Set, numLevels)
	for i := range bl.levels {
		bl.levels[i].init(cmp, nowFn)
	}
}

// IsEmpty returns true if there are no problem spans (the "normal" case). It
// can be used in fast paths to avoid checking for specific overlaps.
func (bl *ByLevel) IsEmpty() bool {
	if bl.empty.Load() {
		// Fast path.
		return true
	}
	bl.mu.Lock()
	defer bl.mu.Unlock()
	for i := range bl.levels {
		if !bl.levels[i].IsEmpty() {
			return false
		}
	}
	bl.empty.Store(true)
	return true
}

// Add a span on a specific level. The span automatically expires after the
// given duration.
func (bl *ByLevel) Add(level int, bounds base.UserKeyBounds, expiration time.Duration) {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	bl.empty.Store(false)
	bl.levels[level].Add(bounds, expiration)
}

// Overlaps returns true if any active (non-expired) span on the given level
// overlaps the given bounds.
func (bl *ByLevel) Overlaps(level int, bounds base.UserKeyBounds) bool {
	if bl.empty.Load() {
		// Fast path.
		return false
	}
	bl.mu.Lock()
	defer bl.mu.Unlock()
	return bl.levels[level].Overlaps(bounds)
}

// Excise a span from all levels. Any overlapping active (non-expired) spans are
// split or trimmed accordingly.
func (bl *ByLevel) Excise(bounds base.UserKeyBounds) {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	for i := range bl.levels {
		bl.levels[i].Excise(bounds)
	}
}

// Len returns the number of non-overlapping spans that have not expired. Two
// spans that touch are both counted if they have different expiration times.
func (bl *ByLevel) Len() int {
	if bl.empty.Load() {
		// Fast path.
		return 0
	}
	bl.mu.Lock()
	defer bl.mu.Unlock()
	n := 0
	for i := range bl.levels {
		n += bl.levels[i].Len()
	}
	return n
}

// String prints all active (non-expired) span fragments.
func (bl *ByLevel) String() string {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	var buf strings.Builder

	for i := range bl.levels {
		if !bl.levels[i].IsEmpty() {
			fmt.Fprintf(&buf, "L%d:\n", i)
			for _, line := range crstrings.Lines(bl.levels[i].String()) {
				fmt.Fprintf(&buf, "  %s\n", line)
			}
		}
	}
	if buf.Len() == 0 {
		return "<empty>"
	}
	return buf.String()
}
