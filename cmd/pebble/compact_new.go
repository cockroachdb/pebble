// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/record"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/spf13/cobra"
)

var compactNewConfig struct {
	deleteSourceFiles bool
}

var compactNewCmd = &cobra.Command{
	Use:   "new <data src> <workload dst>",
	Short: "construct a new bench compact workload from a Cockroach archive",
	Args:  cobra.ExactArgs(2),
	RunE:  runCompactNew,
}

func init() {
	compactNewCmd.Flags().BoolVar(&compactNewConfig.deleteSourceFiles,
		"delete", false, "delete source sstables as they're copied")
}

func runCompactNew(cmd *cobra.Command, args []string) error {
	src, workloadDst := args[0], args[1]
	archiveDir := filepath.Join(src, "archive")
	err := os.MkdirAll(workloadDst, os.ModePerm)
	if err != nil {
		return err
	}

	hist, err := replayManifests(src)
	if err != nil {
		return err
	}

	// Rewrite all the flushed and ingested sstables to remove duplicate keys.
	writtenSizes := map[pebble.FileNum]uint64{}
	for _, logItem := range hist {
		if !logItem.newData {
			continue
		}

		// First look in the archive, because that's likely where it is.
		srcPath := base.MakeFilename(vfs.Default, archiveDir, base.FileTypeTable, logItem.num)
		dstPath := base.MakeFilename(vfs.Default, workloadDst, base.FileTypeTable, logItem.num)
		sz, err := flattenSSTable(srcPath, dstPath)
		if os.IsNotExist(err) {
			// Maybe it's still in the data directory.
			srcPath = base.MakeFilename(vfs.Default, src, base.FileTypeTable, logItem.num)
			sz, err = flattenSSTable(srcPath, dstPath)
		}
		if err != nil {
			return errors.Wrap(err, filepath.Base(srcPath))
		}
		verbosef("Rewrote %s to %s.\n", srcPath, dstPath)
		writtenSizes[logItem.num] = sz
		if compactNewConfig.deleteSourceFiles {
			err := os.Remove(srcPath)
			if err != nil {
				return err
			}
		}
	}

	// Write the history as a csv, using our updated table sizes for rewritten tables.
	f, err := os.Create(filepath.Join(workloadDst, "history"))
	if err != nil {
		return err
	}
	for _, li := range hist {
		sz, ok := writtenSizes[li.num]
		if !ok {
			sz = li.size
		}
		addRm := "rm"
		if li.add {
			addRm = "add"
		}
		fmt.Fprintf(f, "%d,%s,%s,%d,%t\n", li.level, li.num, addRm, sz, li.newData)
	}
	return f.Close()
}

type logItem struct {
	level   int
	num     pebble.FileNum
	add     bool
	size    uint64
	newData bool
}

// replayManifests replays all manifests from the archive and the data
// directory in order, returning an in-order history of sstable additions,
// deletions and moves.
func replayManifests(srcPath string) ([]logItem, error) {
	var history []logItem
	metas := make(map[base.FileNum]*manifest.FileMetadata)

	// First look in the archive for deleted manifests.
	archivePath := filepath.Join(srcPath, "archive")
	infos, err := ioutil.ReadDir(archivePath)
	if err != nil {
		return nil, err
	}
	for _, info := range infos {
		typ, _, ok := base.ParseFilename(vfs.Default, info.Name())
		if !ok || typ != base.FileTypeManifest {
			continue
		}
		manifestLog, err := loadManifest(filepath.Join(archivePath, info.Name()), metas)
		if err != nil {
			return nil, err
		}
		history = append(history, manifestLog...)
	}

	// Next look in the data directory for active manifests.
	infos, err = ioutil.ReadDir(srcPath)
	if err != nil {
		return nil, err
	}
	for _, info := range infos {
		typ, _, ok := base.ParseFilename(vfs.Default, info.Name())
		if !ok || typ != base.FileTypeManifest {
			continue
		}
		manifestLog, err := loadManifest(filepath.Join(srcPath, info.Name()), metas)
		if err != nil {
			return nil, err
		}
		history = append(history, manifestLog...)
	}

	return history, nil
}

func loadManifest(path string, metas map[base.FileNum]*manifest.FileMetadata) ([]logItem, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var log []logItem
	rr := record.NewReader(f, 0 /* logNum */)
	for {
		r, err := rr.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		var ve manifest.VersionEdit
		err = ve.Decode(r)
		if err != nil {
			return nil, err
		}
		for _, nf := range ve.NewFiles {
			metas[nf.Meta.FileNum] = nf.Meta
			log = append(log, logItem{
				level: nf.Level,
				num:   nf.Meta.FileNum,
				add:   true,
				size:  nf.Meta.Size,
				// If a version edit doesn't delete files, we assume all its
				// new files must be new data from a flush or an ingest.
				// Only these files will actually be ingested when running the
				// workload.
				newData: len(ve.DeletedFiles) == 0,
			})
		}
		for df := range ve.DeletedFiles {
			log = append(log, logItem{
				level:   df.Level,
				num:     df.FileNum,
				add:     false,
				size:    metas[df.FileNum].Size,
				newData: false,
			})
		}
	}
	return log, nil
}

// flattenSSTable copies an sstable from srcPath to dstPath, zeros its
// keys' sequence numbers and drops duplicate keys in preparation
// for ingestion.
func flattenSSTable(srcPath, dstPath string) (writtenSize uint64, err error) {
	srcFile, err := os.Open(srcPath)
	if err != nil {
		return 0, err
	}
	r, err := sstable.NewReader(srcFile, sstable.ReaderOptions{
		// TODO(jackson): This hardcodes `pebble bench compact` for cockroach workloads.
		// It'd be nice to support archives created with the pebble default
		// comparer and merger too.
		Comparer:   mvccComparer,
		MergerName: "cockroach_merge_operator",
	})
	if err != nil {
		_ = srcFile.Close()
		return 0, err
	}
	defer r.Close()

	dstFile, err := os.Create(dstPath)
	if err != nil {
		return 0, err
	}
	w := sstable.NewWriter(dstFile, sstable.WriterOptions{
		MergerName: r.Properties.MergerName,
		Comparer:   mvccComparer,
	})
	if err != nil {
		_ = dstFile.Close()
		return 0, err
	}

	var dropped uint64
	// Copy points.
	{
		iter, err := r.NewIter(nil, nil)
		if err != nil {
			return 0, err
		}
		var lastUserKey []byte
		for key, value := iter.First(); key != nil; key, value = iter.Next() {
			// Ignore duplicate keys.
			if mvccComparer.Equal(lastUserKey, key.UserKey) {
				dropped++
				continue
			}
			lastUserKey = append(lastUserKey[:0], key.UserKey...)

			key.SetSeqNum(0)
			if err := w.Add(*key, value); err != nil {
				return 0, err
			}
		}
		if err := iter.Close(); err != nil {
			return 0, err
		}
	}

	// Copy range deletions.
	{
		iter, err := r.NewRangeDelIter()
		if err != nil {
			return 0, err
		}
		var lastUserKey []byte
		if iter != nil {
			for key, value := iter.First(); key != nil; key, value = iter.Next() {
				// Ignore duplicate keys.
				if mvccComparer.Equal(lastUserKey, key.UserKey) {
					dropped++
					continue
				}
				lastUserKey = append(lastUserKey[:0], key.UserKey...)

				key.SetSeqNum(0)
				if err := w.Add(*key, value); err != nil {
					return 0, err
				}
			}
			if err := iter.Close(); err != nil {
				return 0, err
			}
		}
	}

	if dropped > 0 {
		verbosef("Dropped %d keys from %s.\n", dropped, srcPath)
	}
	if err = w.Close(); err != nil {
		return 0, err
	}
	meta, err := w.Metadata()
	if err != nil {
		return 0, err
	}
	return meta.Size, nil
}
