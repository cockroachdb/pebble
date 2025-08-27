// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compressionanalyzer

import (
	"cmp"
	"context"
	"slices"

	"github.com/cockroachdb/pebble/v2/objstorage"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/sstable/block"
	"github.com/cockroachdb/pebble/v2/sstable/block/blockkind"
	"github.com/cockroachdb/tokenbucket"
)

// FileAnalyzer is used to analyze blocks in sstable files.
//
// TODO(radu): add support for blob files.
type FileAnalyzer struct {
	blockAnalyzer *BlockAnalyzer

	readLimiter *tokenbucket.TokenBucket
	sstReadOpts sstable.ReaderOptions
}

func NewFileAnalyzer(
	readLimiter *tokenbucket.TokenBucket, sstReadOpts sstable.ReaderOptions,
) *FileAnalyzer {
	if sstReadOpts.CacheOpts.CacheHandle != nil {
		// We do not support using a cache here (we don't properly populate the
		// block metadata).
		panic("sstReadOpts.CacheOpts.CacheHandle must be nil")
	}
	return &FileAnalyzer{
		blockAnalyzer: NewBlockAnalyzer(),
		readLimiter:   readLimiter,
		sstReadOpts:   sstReadOpts,
	}
}

func (fa *FileAnalyzer) Buckets() *Buckets {
	return fa.blockAnalyzer.Buckets()
}

func (fa *FileAnalyzer) Close() {
	if fa.blockAnalyzer != nil {
		fa.blockAnalyzer.Close()
	}
	*fa = FileAnalyzer{}
}

// SSTable analyzes the blocks in an sstable file and closes the readable (even
// in error cases).
func (fa *FileAnalyzer) SSTable(ctx context.Context, readable objstorage.Readable) error {
	fa.blockAnalyzer.ResetCompressors()
	r, err := sstable.NewReader(ctx, readable, fa.sstReadOpts)
	if err != nil {
		_ = readable.Close()
		return err
	}
	defer func() { _ = r.Close() }()

	// Obtain the layout.
	layout, err := r.Layout()
	if err != nil {
		return err
	}

	rh := readable.NewReadHandle(objstorage.NoReadBefore)
	rh.SetupForCompaction()
	defer func() { _ = rh.Close() }()
	br := r.BlockReader()

	type kindAndHandle struct {
		kind   block.Kind
		handle block.Handle
	}
	var blocks []kindAndHandle
	block := func(kind block.Kind, handle block.Handle) {
		blocks = append(blocks, kindAndHandle{kind: kind, handle: handle})
	}

	for i := range layout.Data {
		block(blockkind.SSTableData, layout.Data[i].Handle)
	}
	for i := range layout.Index {
		block(blockkind.SSTableIndex, layout.Index[i])
	}
	block(blockkind.SSTableIndex, layout.TopIndex)
	for i := range layout.Filter {
		block(blockkind.Filter, layout.Filter[i].Handle)
	}
	block(blockkind.RangeDel, layout.RangeDel)
	block(blockkind.RangeKey, layout.RangeKey)
	for i := range layout.ValueBlock {
		block(blockkind.SSTableValue, layout.ValueBlock[i])
	}
	block(blockkind.Metadata, layout.ValueIndex)

	// Sort blocks by offset (to make effective use of readahead).
	slices.SortFunc(blocks, func(a, b kindAndHandle) int {
		return cmp.Compare(a.handle.Offset, b.handle.Offset)
	})

	for _, b := range blocks {
		if err := fa.sstBlock(ctx, rh, b.kind, b.handle, br); err != nil {
			return err
		}
	}

	return nil
}

func (fa *FileAnalyzer) sstBlock(
	ctx context.Context, rh objstorage.ReadHandle, kind block.Kind, bh block.Handle, r *block.Reader,
) error {
	if bh.Length == 0 {
		return nil
	}
	if fa.readLimiter != nil {
		if err := fa.readLimiter.WaitCtx(ctx, tokenbucket.Tokens(bh.Length)); err != nil {
			return err
		}
	}
	h, err := r.Read(ctx, block.NoReadEnv, rh, bh, kind, func(*block.Metadata, []byte) error { return nil })
	if err != nil {
		return err
	}
	fa.blockAnalyzer.Block(kind, h.BlockData())
	h.Release()
	return nil
}
