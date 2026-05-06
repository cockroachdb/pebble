// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import (
	"bytes"
	"context"
	"io"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/treesteps"
)

// NewDeletableSumValueMerger return a ValueMerger which computes the sum of its
// arguments, but transforms a zero sum into a non-existent entry.
func NewDeletableSumValueMerger(key, value []byte) (ValueMerger, error) {
	m := &deletableSumValueMerger{}
	return m, m.MergeNewer(value)
}

type deletableSumValueMerger struct {
	sum int64
}

func (m *deletableSumValueMerger) parseAndCalculate(value []byte) error {
	v, err := strconv.ParseInt(string(value), 10, 64)
	if err == nil {
		m.sum += v
	}
	return err
}

func (m *deletableSumValueMerger) MergeNewer(value []byte) error {
	return m.parseAndCalculate(value)
}

func (m *deletableSumValueMerger) MergeOlder(value []byte) error {
	return m.parseAndCalculate(value)
}

func (m *deletableSumValueMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	if m.sum == 0 {
		return nil, nil, nil
	}
	return []byte(strconv.FormatInt(m.sum, 10)), nil, nil
}

func (m *deletableSumValueMerger) DeletableFinish(
	includesBase bool,
) ([]byte, bool, io.Closer, error) {
	value, closer, err := m.Finish(includesBase)
	return value, len(value) == 0, closer, err
}

// FakeKVs constructs InternalKVs from the given key strings, in the format
// "key:seq-num". The values are empty.
func FakeKVs(keys ...string) []InternalKV {
	kvs := make([]InternalKV, len(keys))
	for i, k := range keys {
		kvs[i] = InternalKV{K: fakeIkey(k)}
	}
	return kvs
}

func fakeIkey(s string) InternalKey {
	j := strings.Index(s, ":")
	seqNum, err := strconv.Atoi(s[j+1:])
	if err != nil {
		panic(err)
	}
	return MakeInternalKey([]byte(s[:j]), SeqNum(seqNum), InternalKeyKindSet)
}

// NewFakeIter returns an iterator over the given KVs.
func NewFakeIter(cmp *Comparer, kvs []InternalKV) *FakeIter {
	return &FakeIter{
		cmp:   cmp.Compare,
		split: cmp.Split,
		kvs:   kvs,
		index: 0,
	}
}

// FakeIter is an iterator over a fixed set of KVs.
type FakeIter struct {
	cmp   Compare
	split Split
	lower []byte
	upper []byte
	kvs   []InternalKV
	index int
	// prefix, when non-nil, restricts iteration to keys whose split prefix
	// equals prefix. Set by SeekPrefixGE; cleared by any other absolute
	// positioning method (SeekGE, SeekLT, First, Last, SetBounds).
	prefix   []byte
	closeErr error
}

// FakeIter implements the InternalIterator interface.
var _ InternalIterator = (*FakeIter)(nil)

// SetCloseErr causes future calls to Error() and Close() to return this error.
func (f *FakeIter) SetCloseErr(closeErr error) {
	f.closeErr = closeErr
}

func (f *FakeIter) String() string {
	return "fake"
}

// SeekGE is part of the InternalIterator interface.
func (f *FakeIter) SeekGE(key []byte, flags SeekGEFlags) *InternalKV {
	f.prefix = nil
	if flags.TrySeekUsingNext() {
		// Note that f.index could be len(f.kvs) here (iterator exhausted), and that
		// is ok.
		if f.index > 0 && f.cmp(key, f.kvs[f.index-1].K.UserKey) <= 0 {
			panic(errors.AssertionFailedf("invalid use of TrySeekUsingNext"))
		}
	} else {
		f.index = 0
	}
	for ; f.index < len(f.kvs); f.index++ {
		if f.cmp(key, f.key().UserKey) <= 0 {
			if f.upper != nil && f.cmp(f.upper, f.key().UserKey) <= 0 {
				return nil
			}
			return &f.kvs[f.index]
		}
	}
	return nil
}

// SeekPrefixGE is part of the InternalIterator interface.
func (f *FakeIter) SeekPrefixGE(prefix, key []byte, flags SeekGEFlags) *InternalKV {
	// SeekGE clears f.prefix, so we re-apply it after.
	kv := f.SeekGE(key, flags)
	f.prefix = prefix
	if kv != nil && !bytes.Equal(f.split.Prefix(kv.K.UserKey), prefix) {
		return nil
	}
	return kv
}

// SeekLT is part of the InternalIterator interface.
func (f *FakeIter) SeekLT(key []byte, flags SeekLTFlags) *InternalKV {
	f.prefix = nil
	for f.index = len(f.kvs) - 1; f.index >= 0; f.index-- {
		if f.cmp(key, f.key().UserKey) > 0 {
			if f.lower != nil && f.cmp(f.lower, f.key().UserKey) > 0 {
				return nil
			}
			return &f.kvs[f.index]
		}
	}
	return nil
}

// First is part of the InternalIterator interface.
func (f *FakeIter) First() *InternalKV {
	f.prefix = nil
	f.index = -1
	return f.Next()
}

// Last is part of the InternalIterator interface.
func (f *FakeIter) Last() *InternalKV {
	f.prefix = nil
	f.index = len(f.kvs)
	return f.Prev()
}

// Next is part of the InternalIterator interface.
func (f *FakeIter) Next() *InternalKV {
	if f.index == len(f.kvs) {
		return nil
	}
	f.index++
	if f.index == len(f.kvs) {
		return nil
	}
	if f.upper != nil && f.cmp(f.upper, f.key().UserKey) <= 0 {
		return nil
	}
	if f.prefix != nil && !bytes.Equal(f.split.Prefix(f.key().UserKey), f.prefix) {
		return nil
	}
	return &f.kvs[f.index]
}

// Prev is part of the InternalIterator interface.
func (f *FakeIter) Prev() *InternalKV {
	if f.index < 0 {
		return nil
	}
	f.index--
	if f.index < 0 {
		return nil
	}
	if f.lower != nil && f.cmp(f.lower, f.key().UserKey) > 0 {
		return nil
	}
	return &f.kvs[f.index]
}

// NextPrefix is part of the InternalIterator interface.
func (f *FakeIter) NextPrefix(succKey []byte) *InternalKV {
	return f.SeekGE(succKey, SeekGEFlagsNone)
}

// key returns the current Key the iterator is positioned at.
func (f *FakeIter) key() *InternalKey {
	return &f.kvs[f.index].K
}

// Error is part of the InternalIterator interface.
func (f *FakeIter) Error() error {
	return f.closeErr
}

// Close is part of the InternalIterator interface.
func (f *FakeIter) Close() error {
	return f.closeErr
}

// SetBounds is part of the InternalIterator interface.
func (f *FakeIter) SetBounds(lower, upper []byte) {
	f.lower = lower
	f.upper = upper
	f.prefix = nil
}

// SetContext is part of the InternalIterator interface.
func (f *FakeIter) SetContext(_ context.Context) {}

// TreeStepsNode is part of the InternalIterator interface.
func (f *FakeIter) TreeStepsNode() treesteps.NodeInfo {
	return treesteps.NodeInfof(f, "%T(%p)", f, f)
}

// ParseUserKeyBounds parses UserKeyBounds from a string representation of the
// form "[foo, bar]" or "[foo, bar)".
func ParseUserKeyBounds(s string) UserKeyBounds {
	first, last, s := s[0], s[len(s)-1], s[1:len(s)-1]
	start, end, ok := strings.Cut(s, ", ")
	if !ok || first != '[' || (last != ']' && last != ')') {
		panic(errors.AssertionFailedf("invalid bounds %q", s))
	}
	return UserKeyBoundsEndExclusiveIf([]byte(start), []byte(end), last == ')')
}
