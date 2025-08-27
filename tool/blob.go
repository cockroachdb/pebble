// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"context"
	"fmt"
	"io"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/internal/cache"
	"github.com/cockroachdb/pebble/v2/internal/sstableinternal"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/sstable/blob"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/spf13/cobra"
)

// blobT implements the blob introspection tool.
type blobT struct {
	Root   *cobra.Command
	Layout *cobra.Command

	// Configuration and state.
	opts *pebble.Options
}

func newBlob(opts *pebble.Options) *blobT {
	s := &blobT{
		opts: opts,
	}

	s.Root = &cobra.Command{
		Use:   "blob",
		Short: "blob introspection tools",
	}
	s.Layout = &cobra.Command{
		Use:   "layout <blob files>",
		Short: "print the layout of a blob file",
		Long: `
Print the layout for the given blob files.
`,
		Args: cobra.MinimumNArgs(1),
		Run:  s.runLayout,
	}
	s.Root.AddCommand(s.Layout)
	return s
}

func (b *blobT) newReader(
	f vfs.File, cacheHandle *cache.Handle, fn string,
) (*blob.FileReader, error) {
	readable, err := sstable.NewSimpleReadable(f)
	if err != nil {
		return nil, err
	}
	o := b.opts.MakeReaderOptions()
	o.CacheOpts = sstableinternal.CacheOptions{CacheHandle: cacheHandle}
	if cacheHandle != nil {
		_, fileNum, ok := base.ParseFilename(b.opts.FS, fn)
		if ok {
			o.CacheOpts.FileNum = fileNum
		}
	}
	reader, err := blob.NewFileReader(context.TODO(), readable, blob.FileReaderOptions{
		ReaderOptions: o.ReaderOptions,
	})
	if err != nil {
		return nil, errors.CombineErrors(err, readable.Close())
	}
	return reader, nil
}

// foreachBlob opens each blob file specified in the args (if an arg is a
// directory, it is walked for blob files) and calls the given function.
func (b *blobT) foreachBlob(
	stderr io.Writer, args []string, fn func(path string, r *blob.FileReader),
) {
	processFileFn := func(path string, r *blob.FileReader) error {
		fn(path, r)
		return nil
	}
	closeReaderFn := func(r *blob.FileReader) error {
		return r.Close()
	}
	processFiles(stderr, b.opts.FS, args, []string{".blob"}, b.newReader, closeReaderFn, processFileFn)
}

func (b *blobT) runLayout(cmd *cobra.Command, args []string) {
	stdout, stderr := cmd.OutOrStdout(), cmd.OutOrStderr()
	b.foreachBlob(stderr, args, func(path string, r *blob.FileReader) {
		fmt.Fprintf(stdout, "%s\n", path)

		l, err := r.Layout()
		if err != nil {
			fmt.Fprintf(stderr, "%s\n", err)
			return
		}
		fmt.Fprintf(stdout, "%s\n", l)
	})
}
