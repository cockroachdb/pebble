// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

type filterWriter interface {
	addKey(key []byte)
	finishBlock(blockOffset uint64) error
	finish() ([]byte, error)
	metaName() string
	policyName() string
}

type tableFilterReader struct {
	policy FilterPolicy
}

func newTableFilterReader(policy FilterPolicy) *tableFilterReader {
	return &tableFilterReader{
		policy: policy,
	}
}

func (f *tableFilterReader) mayContain(data, key []byte) bool {
	return f.policy.MayContain(TableFilter, data, key)
}

type tableFilterWriter struct {
	policy FilterPolicy
	writer FilterWriter
	// count is the count of the number of keys added to the filter.
	count int
}

func newTableFilterWriter(policy FilterPolicy) *tableFilterWriter {
	return &tableFilterWriter{
		policy: policy,
		writer: policy.NewWriter(TableFilter),
	}
}

func (f *tableFilterWriter) addKey(key []byte) {
	f.count++
	f.writer.AddKey(key)
}

func (f *tableFilterWriter) finishBlock(blockOffset uint64) error {
	// NB: table-level filters have nothing to do when a block is finished.
	return nil
}

func (f *tableFilterWriter) finish() ([]byte, error) {
	if f.count == 0 {
		return nil, nil
	}
	return f.writer.Finish(nil), nil
}

func (f *tableFilterWriter) metaName() string {
	return "fullfilter." + f.policy.Name()
}

func (f *tableFilterWriter) policyName() string {
	return f.policy.Name()
}
