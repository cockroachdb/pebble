// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/cockroachdb/pebble/v2/internal/treeprinter"
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
func NewFakeIter(kvs []InternalKV) *FakeIter {
	return &FakeIter{
		kvs:   kvs,
		index: 0,
		valid: len(kvs) > 0,
	}
}

// FakeIter is an iterator over a fixed set of KVs.
type FakeIter struct {
	lower    []byte
	upper    []byte
	kvs      []InternalKV
	index    int
	valid    bool
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
	f.valid = false
	for f.index = 0; f.index < len(f.kvs); f.index++ {
		if DefaultComparer.Compare(key, f.key().UserKey) <= 0 {
			if f.upper != nil && DefaultComparer.Compare(f.upper, f.key().UserKey) <= 0 {
				return nil
			}
			f.valid = true
			return f.KV()
		}
	}
	return nil
}

// SeekPrefixGE is part of the InternalIterator interface.
func (f *FakeIter) SeekPrefixGE(prefix, key []byte, flags SeekGEFlags) *InternalKV {
	return f.SeekGE(key, flags)
}

// SeekLT is part of the InternalIterator interface.
func (f *FakeIter) SeekLT(key []byte, flags SeekLTFlags) *InternalKV {
	f.valid = false
	for f.index = len(f.kvs) - 1; f.index >= 0; f.index-- {
		if DefaultComparer.Compare(key, f.key().UserKey) > 0 {
			if f.lower != nil && DefaultComparer.Compare(f.lower, f.key().UserKey) > 0 {
				return nil
			}
			f.valid = true
			return f.KV()
		}
	}
	return nil
}

// First is part of the InternalIterator interface.
func (f *FakeIter) First() *InternalKV {
	f.valid = false
	f.index = -1
	if kv := f.Next(); kv == nil {
		return nil
	}
	if f.upper != nil && DefaultComparer.Compare(f.upper, f.key().UserKey) <= 0 {
		return nil
	}
	f.valid = true
	return f.KV()
}

// Last is part of the InternalIterator interface.
func (f *FakeIter) Last() *InternalKV {
	f.valid = false
	f.index = len(f.kvs)
	if kv := f.Prev(); kv == nil {
		return nil
	}
	if f.lower != nil && DefaultComparer.Compare(f.lower, f.key().UserKey) > 0 {
		return nil
	}
	f.valid = true
	return f.KV()
}

// Next is part of the InternalIterator interface.
func (f *FakeIter) Next() *InternalKV {
	f.valid = false
	if f.index == len(f.kvs) {
		return nil
	}
	f.index++
	if f.index == len(f.kvs) {
		return nil
	}
	if f.upper != nil && DefaultComparer.Compare(f.upper, f.key().UserKey) <= 0 {
		return nil
	}
	f.valid = true
	return f.KV()
}

// Prev is part of the InternalIterator interface.
func (f *FakeIter) Prev() *InternalKV {
	f.valid = false
	if f.index < 0 {
		return nil
	}
	f.index--
	if f.index < 0 {
		return nil
	}
	if f.lower != nil && DefaultComparer.Compare(f.lower, f.key().UserKey) > 0 {
		return nil
	}
	f.valid = true
	return f.KV()
}

// NextPrefix is part of the InternalIterator interface.
func (f *FakeIter) NextPrefix(succKey []byte) *InternalKV {
	return f.SeekGE(succKey, SeekGEFlagsNone)
}

// key returns the current Key the iterator is positioned at regardless of the
// value of f.valid.
func (f *FakeIter) key() *InternalKey {
	return &f.kvs[f.index].K
}

// KV is part of the InternalIterator interface.
func (f *FakeIter) KV() *InternalKV {
	if f.valid {
		return &f.kvs[f.index]
	}
	// It is invalid to call Key() when Valid() returns false. Rather than
	// returning nil here which would technically be more correct, return a
	// non-nil key which is the behavior of some InternalIterator
	// implementations. This provides better testing of users of
	// InternalIterators.
	if f.index < 0 {
		return &f.kvs[0]
	}
	return &f.kvs[len(f.kvs)-1]
}

// Valid is part of the InternalIterator interface.
func (f *FakeIter) Valid() bool {
	return f.index >= 0 && f.index < len(f.kvs) && f.valid
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
}

// SetContext is part of the InternalIterator interface.
func (f *FakeIter) SetContext(_ context.Context) {}

// DebugTree is part of the InternalIterator interface.
func (f *FakeIter) DebugTree(tp treeprinter.Node) {
	tp.Childf("%T(%p)", f, f)
}

// ParseUserKeyBounds parses UserKeyBounds from a string representation of the
// form "[foo, bar]" or "[foo, bar)".
func ParseUserKeyBounds(s string) UserKeyBounds {
	first, last, s := s[0], s[len(s)-1], s[1:len(s)-1]
	start, end, ok := strings.Cut(s, ", ")
	if !ok || first != '[' || (last != ']' && last != ')') {
		panic(fmt.Sprintf("invalid bounds %q", s))
	}
	return UserKeyBoundsEndExclusiveIf([]byte(start), []byte(end), last == ')')
}
