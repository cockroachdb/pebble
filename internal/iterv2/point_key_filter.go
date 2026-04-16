// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package iterv2

import (
	"context"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/treesteps"
)

// PointKeyFilter wraps an InternalIterator and filters out range deletion and
// range key entries, forwarding only point keys. This is necessary for V2
// iteration because some V1 iterators (e.g. arenaskl iterators for memtables)
// may emit range key boundary markers in the point key stream, but the V2
// merging iterator expects clean point-only input.
type PointKeyFilter struct {
	Iter base.InternalIterator
}

var _ base.InternalIterator = (*PointKeyFilter)(nil)

func isRangeKind(k base.InternalKeyKind) bool {
	return k == base.InternalKeyKindRangeDelete ||
		k == base.InternalKeyKindRangeKeySet ||
		k == base.InternalKeyKindRangeKeyUnset ||
		k == base.InternalKeyKindRangeKeyDelete
}

func (f *PointKeyFilter) skipForward(kv *base.InternalKV) *base.InternalKV {
	for kv != nil && isRangeKind(kv.K.Kind()) {
		kv = f.Iter.Next()
	}
	return kv
}

func (f *PointKeyFilter) skipBackward(kv *base.InternalKV) *base.InternalKV {
	for kv != nil && isRangeKind(kv.K.Kind()) {
		kv = f.Iter.Prev()
	}
	return kv
}

func (f *PointKeyFilter) SeekGE(key []byte, flags base.SeekGEFlags) *base.InternalKV {
	return f.skipForward(f.Iter.SeekGE(key, flags))
}

func (f *PointKeyFilter) SeekPrefixGE(prefix, key []byte, flags base.SeekGEFlags) *base.InternalKV {
	return f.skipForward(f.Iter.SeekPrefixGE(prefix, key, flags))
}

func (f *PointKeyFilter) SeekLT(key []byte, flags base.SeekLTFlags) *base.InternalKV {
	return f.skipBackward(f.Iter.SeekLT(key, flags))
}

func (f *PointKeyFilter) First() *base.InternalKV {
	raw := f.Iter.First()
	return f.skipForward(raw)
}

func (f *PointKeyFilter) Last() *base.InternalKV {
	return f.skipBackward(f.Iter.Last())
}

func (f *PointKeyFilter) Next() *base.InternalKV {
	return f.skipForward(f.Iter.Next())
}

func (f *PointKeyFilter) NextPrefix(succKey []byte) *base.InternalKV {
	return f.skipForward(f.Iter.NextPrefix(succKey))
}

func (f *PointKeyFilter) Prev() *base.InternalKV {
	return f.skipBackward(f.Iter.Prev())
}

func (f *PointKeyFilter) Error() error {
	return f.Iter.Error()
}

func (f *PointKeyFilter) Close() error {
	return f.Iter.Close()
}

func (f *PointKeyFilter) SetBounds(lower, upper []byte) {
	f.Iter.SetBounds(lower, upper)
}

func (f *PointKeyFilter) SetContext(ctx context.Context) {
	f.Iter.SetContext(ctx)
}

func (f *PointKeyFilter) String() string {
	return f.Iter.String()
}

func (f *PointKeyFilter) TreeStepsNode() treesteps.NodeInfo {
	return f.Iter.TreeStepsNode()
}
