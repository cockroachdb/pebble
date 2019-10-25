// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

// Merge creates a ValueMerger for the specified key initialized with the value
// of one merge operand.
type Merge func(key, value []byte) ValueMerger

// ValueMerger receives merge operands one by one. The operand received is either
// newer or older than all operands received so far as indicated by the function
// names, `MergeNewer()` and `MergeOlder()`. Once all operands have been received,
// the client will invoke `Finish()` to obtain the final result.
//
// The implementation may choose to merge values into the result immediately upon
// receiving each operand, or buffer operands until Finish() is called. For example,
// buffering may be useful to avoid (de)serializing partial merge results.
//
// The merge operation must be associative. That is, for the values A, B, C:
//
//   Merge(A).MergeOlder(B).MergeOlder(C) == Merge(C).MergeNewer(B).MergeNewer(A)
//
// Examples of merge operators are integer addition, list append, and string
// concatenation.
type ValueMerger interface {
	MergeNewer(value []byte) error
	MergeOlder(value []byte) error
	Finish() []byte
}

// Merger defines an associative merge operation. The merge operation merges
// two or more values for a single key. A merge operation is requested by
// writing a value using {Batch,DB}.Merge(). The value at that key is merged
// with any existing value. It is valid to Set a value at a key and then Merge
// a new value. Similar to non-merged values, a merged value can be deleted by
// either Delete or DeleteRange.
//
// The merge operation is invoked when a merge value is encountered during a
// read, either during a compaction or during iteration.
type Merger struct {
	Merge Merge

	// Name is the name of the merger.
	//
	// Pebble stores the merger name on disk, and opening a database with a
	// different merger from the one it was created with will result in an error.
	Name string
}

type AppendValueMerger struct {
	buf []byte
}

func (a *AppendValueMerger) MergeNewer(value []byte) error {
	a.buf = append(a.buf, value...)
	return nil
}

func (a *AppendValueMerger) MergeOlder(value []byte) error {
	buf := make([]byte, len(a.buf)+len(value))
	copy(buf, value)
	copy(buf[len(value):], a.buf)
	a.buf = buf
	return nil
}

func (a *AppendValueMerger) Finish() []byte {
	return a.buf
}

// DefaultMerger is the default implementation of the Merger interface. It
// concatenates the two values to merge.
var DefaultMerger = &Merger{
	Merge: func(key, value []byte) ValueMerger {
		var res AppendValueMerger
		res.buf = append(res.buf, value...)
		return &res
	},

	Name: "pebble.concatenate",
}
