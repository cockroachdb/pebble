// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestWriter(t *testing.T) {
	var r *Reader
	defer func() {
		if r != nil {
			require.NoError(t, r.Close())
		}
	}()

	datadriven.RunTest(t, "testdata/writer", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "build":
			if r != nil {
				_ = r.Close()
				r = nil
			}
			var meta *WriterMetadata
			var err error
			meta, r, err = runBuildCmd(td, WriterOptions{})
			if err != nil {
				return err.Error()
			}
			return fmt.Sprintf("point:   [%s,%s]\nrange:   [%s,%s]\nseqnums: [%d,%d]\n",
				meta.SmallestPoint, meta.LargestPoint,
				meta.SmallestRange, meta.LargestRange,
				meta.SmallestSeqNum, meta.LargestSeqNum)

		case "build-raw":
			if r != nil {
				_ = r.Close()
				r = nil
			}
			var meta *WriterMetadata
			var err error
			meta, r, err = runBuildRawCmd(td)
			if err != nil {
				return err.Error()
			}
			return fmt.Sprintf("point:   [%s,%s]\nrange:   [%s,%s]\nseqnums: [%d,%d]\n",
				meta.SmallestPoint, meta.LargestPoint,
				meta.SmallestRange, meta.LargestRange,
				meta.SmallestSeqNum, meta.LargestSeqNum)

		case "scan":
			origIter, err := r.NewIter(nil /* lower */, nil /* upper */)
			if err != nil {
				return err.Error()
			}
			iter := newIterAdapter(origIter)
			defer iter.Close()

			var buf bytes.Buffer
			for valid := iter.First(); valid; valid = iter.Next() {
				fmt.Fprintf(&buf, "%s:%s\n", iter.Key(), iter.Value())
			}
			return buf.String()

		case "scan-range-del":
			iter, err := r.NewRawRangeDelIter()
			if err != nil {
				return err.Error()
			}
			if iter == nil {
				return ""
			}
			defer iter.Close()

			var buf bytes.Buffer
			for key, val := iter.First(); key != nil; key, val = iter.Next() {
				fmt.Fprintf(&buf, "%s:%s\n", key, val)
			}
			return buf.String()

		case "layout":
			l, err := r.Layout()
			if err != nil {
				return err.Error()
			}
			var buf bytes.Buffer
			l.Describe(&buf, false, r, nil)
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func spaceSplit(a []byte) int {
	v := bytes.IndexByte(a, ' ')
	if v < 0 {
		return len(a)
	}
	return v
}

func TestReplaceSuffix(t *testing.T) {
	mem := vfs.NewMem()

	// Suffix replacement requires a Split function. Add one that splits a key
	// at the first space.
	comparer := *base.DefaultComparer
	comparer.Split = spaceSplit

	// Write a new file containing a few keys, including a few with the
	// ` world` suffix.
	f, err := mem.Create("prereplacement.sst")
	require.NoError(t, err)
	w := NewWriter(f, WriterOptions{
		Comparer:          &comparer,
		SuffixPlaceholder: []byte(` world`),
	})
	inputKeys := []string{
		`bonjour world`,
		`hello world`,
		`hi world`,
	}
	for _, k := range inputKeys {
		require.NoError(t, w.Add(base.MakeInternalKey([]byte(k), 0, InternalKeyKindSet), nil))
	}
	require.NoError(t, w.Close())

	// The SSTable's SuffixReplacement property should be
	// ` world world`, indicating that the suffix placeholder is
	// ` world` and that the replacement value is not yet known.
	m, err := w.Metadata()
	require.NoError(t, err)
	require.Equal(t, []byte(` world world`), m.Properties.SuffixReplacement)

	// Trying to read the sstable without a replacement value in the sstable
	// property should error. This behavior helps avoid accidental reading of
	// unfinished sstables.
	readOpts := ReaderOptions{Comparer: &comparer}
	f, err = mem.Open("prereplacement.sst")
	require.NoError(t, err)
	_, err = NewReader(f, readOpts)
	require.EqualError(t, err, `pebble: unreplaced suffix`)

	// Perform the replacement.
	f, err = mem.Open("prereplacement.sst")
	require.NoError(t, err)
	sstableBytes, err := ioutil.ReadAll(f)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.NoError(t, ReplaceSuffix(sstableBytes, readOpts, []byte(` world`), []byte(` monde`)))

	// Read the file post-replacement. All the ` world` suffixes should be
	// replaced with ` monde`.
	f = vfs.NewMemFile(sstableBytes)
	r, err := NewReader(f, readOpts)
	require.NoError(t, err)
	defer r.Close()
	iter, err := r.NewIter(nil, nil)
	require.NoError(t, err)
	defer iter.Close()
	var got []string
	for k, _ := iter.First(); k != nil; k, _ = iter.Next() {
		got = append(got, string(k.UserKey))
	}
	want := []string{`bonjour monde`, `hello monde`, `hi monde`}
	require.Equal(t, want, got)
}

func TestReplaceSuffixErrors(t *testing.T) {
	mem := vfs.NewMem()
	writeFile := func(wo WriterOptions, keys ...string) ([]byte, error) {
		filename := fmt.Sprintf("%s.sst", t.Name())
		f, err := mem.Create(filename)
		require.NoError(t, err)
		w := NewWriter(f, wo)
		for _, k := range keys {
			err = w.Add(base.MakeInternalKey([]byte(k), 0, InternalKeyKindSet), nil)
			if err != nil {
				return nil, err
			}
		}
		if err = w.Close(); err != nil {
			return nil, err
		}
		f, err = mem.Open(filename)
		require.NoError(t, err)
		defer f.Close()
		b, err := ioutil.ReadAll(f)
		require.NoError(t, err)
		return b, err
	}

	comparer := *base.DefaultComparer
	comparer.Split = spaceSplit

	t.Run("need split function", func(t *testing.T) {
		_, err := writeFile(WriterOptions{
			Comparer:          base.DefaultComparer,
			SuffixPlaceholder: []byte{0xde, 0xad, 0xbe, 0xef},
		}, "hello world")
		require.EqualError(t, err, `pebble: SuffixPlaceholder requires a comparer Split function`)
	})
	t.Run("all suffixes must match", func(t *testing.T) {
		_, err := writeFile(WriterOptions{
			Comparer:          &comparer,
			SuffixPlaceholder: []byte(" world"),
		}, "hello world", "hi everyone")
		require.EqualError(t, err, `pebble: all keys must contain the suffix placeholder`)
	})
	t.Run("replacement and placeholder must be equal lengths", func(t *testing.T) {
		prereplacementFile, err := writeFile(WriterOptions{
			Comparer:          &comparer,
			SuffixPlaceholder: []byte(" world"),
		}, "hello world")
		require.NoError(t, err)
		err = ReplaceSuffix(prereplacementFile, ReaderOptions{Comparer: &comparer},
			[]byte(" world"),
			[]byte(" universe"))
		require.EqualError(t, err, `pebble: placeholder and replacement suffixes must be equal length`)
	})
	t.Run("sstable must contain suffix replacement property", func(t *testing.T) {
		prereplacementFile, err := writeFile(WriterOptions{
			Comparer:          &comparer,
			SuffixPlaceholder: nil, /* no suffix replacement */
		}, "hello world")
		require.NoError(t, err)
		err = ReplaceSuffix(prereplacementFile, ReaderOptions{Comparer: &comparer},
			[]byte(" world"),
			[]byte(" globe"))
		require.EqualError(t, err, `pebble: sstable does not support suffix replacement`)
	})
	t.Run("ReplaceSuffix placeholder must match property's placeholder", func(t *testing.T) {
		prereplacementFile, err := writeFile(WriterOptions{
			Comparer:          &comparer,
			SuffixPlaceholder: []byte(" world"),
		}, "hello world")
		require.NoError(t, err)
		err = ReplaceSuffix(prereplacementFile, ReaderOptions{Comparer: &comparer},
			[]byte(" globe"),
			[]byte(" monde"))
		require.EqualError(t, err, `pebble: sstable created with a different placeholder suffix`)
	})
}

func TestWriterClearCache(t *testing.T) {
	// Verify that Writer clears the cache of blocks that it writes.
	mem := vfs.NewMem()
	opts := ReaderOptions{Cache: cache.New(64 << 20)}
	defer opts.Cache.Unref()

	writerOpts := WriterOptions{Cache: opts.Cache}
	cacheOpts := &cacheOpts{cacheID: 1, fileNum: 1}
	invalidData := func() *cache.Value {
		invalid := []byte("invalid data")
		v := opts.Cache.Alloc(len(invalid))
		copy(v.Buf(), invalid)
		return v
	}

	build := func(name string) {
		f, err := mem.Create(name)
		require.NoError(t, err)

		w := NewWriter(f, writerOpts, cacheOpts)
		require.NoError(t, w.Set([]byte("hello"), []byte("world")))
		require.NoError(t, w.Close())
	}

	// Build the sstable a first time so that we can determine the locations of
	// all of the blocks.
	build("test")

	f, err := mem.Open("test")
	require.NoError(t, err)

	r, err := NewReader(f, opts)
	require.NoError(t, err)

	layout, err := r.Layout()
	require.NoError(t, err)

	foreachBH := func(layout *Layout, f func(bh BlockHandle)) {
		for _, bh := range layout.Data {
			f(bh)
		}
		for _, bh := range layout.Index {
			f(bh)
		}
		f(layout.TopIndex)
		f(layout.Filter)
		f(layout.RangeDel)
		f(layout.Properties)
		f(layout.MetaIndex)
	}

	// Poison the cache for each of the blocks.
	poison := func(bh BlockHandle) {
		opts.Cache.Set(cacheOpts.cacheID, cacheOpts.fileNum, bh.Offset, invalidData()).Release()
	}
	foreachBH(layout, poison)

	// Build the table a second time. This should clear the cache for the blocks
	// that are written.
	build("test")

	// Verify that the written blocks have been cleared from the cache.
	check := func(bh BlockHandle) {
		h := opts.Cache.Get(cacheOpts.cacheID, cacheOpts.fileNum, bh.Offset)
		if h.Get() != nil {
			t.Fatalf("%d: expected cache to be cleared, but found %q", bh.Offset, h.Get())
		}
	}
	foreachBH(layout, check)

	require.NoError(t, r.Close())
}

type discardFile struct{}

func (f discardFile) Close() error {
	return nil
}

func (f discardFile) Write(p []byte) (int, error) {
	return len(p), nil
}

func (f discardFile) Sync() error {
	return nil
}

func BenchmarkWriter(b *testing.B) {
	keys := make([][]byte, 1e6)
	for i := range keys {
		key := make([]byte, 24)
		binary.BigEndian.PutUint64(key[:8], 123) // 16-byte shared prefix
		binary.BigEndian.PutUint64(key[8:16], 456)
		binary.BigEndian.PutUint64(key[16:], uint64(i))
		keys[i] = key
	}

	benchmarks := []struct {
		name    string
		options WriterOptions
	}{
		{
			name: "Default",
			options: WriterOptions{
				BlockRestartInterval: 16,
				BlockSize:            32 << 10,
				Compression:          SnappyCompression,
				FilterPolicy:         bloom.FilterPolicy(10),
			},
		},
		{
			name: "Zstd",
			options: WriterOptions{
				BlockRestartInterval: 16,
				BlockSize:            32 << 10,
				Compression:          ZstdCompression,
				FilterPolicy:         bloom.FilterPolicy(10),
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				w := NewWriter(discardFile{}, bm.options)

				for j := range keys {
					if err := w.Set(keys[j], keys[j]); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
