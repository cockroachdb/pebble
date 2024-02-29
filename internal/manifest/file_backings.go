// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"golang.org/x/exp/slices"
)

// FileBackings contains information about a set of FileBackings.
type FileBackings struct {
	m map[base.DiskFileNum]*FileBacking
	// totalSize is the sum of the sizes of the fileBackings in m.
	totalSize uint64
}

// Get returns the backing for the given DiskFileNum. If it doesn't exist,
// returns ok = false.
func (b *FileBackings) Get(n base.DiskFileNum) (_ *FileBacking, ok bool) {
	backing, ok := b.m[n]
	return backing, ok
}

// Add a new backing to the set. Another backing for the same DiskFilNum must
// not exist.
func (b *FileBackings) Add(backing *FileBacking) {
	if b.m == nil {
		b.m = make(map[base.DiskFileNum]*FileBacking)
	} else {
		_, ok := b.m[backing.DiskFileNum]
		if ok {
			panic("pebble: trying to add an existing file backing")
		}
	}
	b.m[backing.DiskFileNum] = backing
	b.totalSize += backing.Size
}

// Remove the backing associated with the given DiskFileNum, if it exists.
func (b *FileBackings) Remove(n base.DiskFileNum) {
	backing, ok := b.m[n]
	if ok {
		delete(b.m, n)
		b.totalSize -= backing.Size
	}
}

// Stats returns the number and total size of all the backings.
func (b *FileBackings) Stats() (count int, totalSize uint64) {
	if invariants.Enabled {
		b.Check()
	}
	return len(b.m), b.totalSize
}

// ForEach calls fn on each backing, in an unspecified order.
func (b *FileBackings) ForEach(fn func(backing *FileBacking)) {
	for _, backing := range b.m {
		fn(backing)
	}
}

// DiskFileNums returns disk file nums of all the backing in the set, in sorted
// order.
func (b *FileBackings) DiskFileNums() []base.DiskFileNum {
	res := make([]base.DiskFileNum, 0, len(b.m))
	for n := range b.m {
		res = append(res, n)
	}
	slices.Sort(res)
	return res
}

// Check verifies the internal consistency.
func (b *FileBackings) Check() {
	var totalSize uint64
	for _, backing := range b.m {
		totalSize += backing.Size
	}
	if totalSize != b.totalSize {
		panic("pebble: invalid backing table size accounting")
	}
}
