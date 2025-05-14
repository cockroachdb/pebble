// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/humanize"
	"github.com/cockroachdb/pebble/internal/lsmview"
	"github.com/cockroachdb/pebble/objstorage"
)

// LSMViewURL returns an URL which shows a diagram of the LSM.
func (d *DB) LSMViewURL() string {
	v := func() *version {
		d.mu.Lock()
		defer d.mu.Unlock()

		v := d.mu.versions.currentVersion()
		v.Ref()
		return v
	}()
	defer v.Unref()

	b := lsmViewBuilder{
		cmp:    d.opts.Comparer.Compare,
		fmtKey: d.opts.Comparer.FormatKey,
	}
	if b.fmtKey == nil {
		b.fmtKey = DefaultComparer.FormatKey
	}
	b.InitLevels(v)
	b.PopulateKeys()
	data := b.Build(d.objProvider, d.newIters)
	url, err := lsmview.GenerateURL(data)
	if err != nil {
		return fmt.Sprintf("error: %s", err)
	}
	return url.String()
}

type lsmViewBuilder struct {
	cmp    base.Compare
	fmtKey base.FormatKey

	levelNames []string
	levels     [][]*tableMetadata

	// The keys that appear as Smallest/Largest, sorted and formatted.
	sortedKeys []string
	// keys[k] is the position of key k in the sortedKeys list.
	keys map[string]int

	// scanTables is set during Build. If we don't have too many tables, we will
	// create iterators and show some of the keys.
	scanTables bool
}

// InitLevels gets the metadata for the tables in the LSM and populates
// levelNames and levels.
func (b *lsmViewBuilder) InitLevels(v *version) {
	var levelNames []string
	var levels [][]*tableMetadata
	for sublevel := len(v.L0SublevelFiles) - 1; sublevel >= 0; sublevel-- {
		var files []*tableMetadata
		for f := range v.L0SublevelFiles[sublevel].All() {
			files = append(files, f)
		}

		levelNames = append(levelNames, fmt.Sprintf("L0.%d", sublevel))
		levels = append(levels, files)
	}
	if len(levels) == 0 {
		levelNames = append(levelNames, "L0")
		levels = append(levels, nil)
	}
	for level := 1; level < len(v.Levels); level++ {
		var files []*tableMetadata
		for f := range v.Levels[level].All() {
			files = append(files, f)
		}
		levelNames = append(levelNames, fmt.Sprintf("L%d", level))
		levels = append(levels, files)
	}
	b.levelNames = levelNames
	b.levels = levels
}

// PopulateKeys initializes the sortedKeys and keys fields.
func (b *lsmViewBuilder) PopulateKeys() {
	// keys[k] will hold the position of k into sortedKeys.
	keys := make(map[string]int)
	for _, l := range b.levels {
		for _, f := range l {
			keys[string(f.Smallest().UserKey)] = -1
			keys[string(f.Largest().UserKey)] = -1
		}
	}

	sortedKeys := make([]string, 0, len(keys))
	for s := range keys {
		sortedKeys = append(sortedKeys, s)
	}
	slices.SortFunc(sortedKeys, func(k1, k2 string) int {
		return b.cmp([]byte(k1), []byte(k2))
	})
	sortedKeys = slices.CompactFunc(sortedKeys, func(k1, k2 string) bool {
		return b.cmp([]byte(k1), []byte(k2)) == 0
	})
	for i, k := range sortedKeys {
		keys[k] = i
	}
	for i := range sortedKeys {
		sortedKeys[i] = fmt.Sprintf("%v", b.fmtKey([]byte(sortedKeys[i])))
	}
	b.sortedKeys = sortedKeys
	b.keys = keys
}

func (b *lsmViewBuilder) Build(
	objProvider objstorage.Provider, newIters tableNewIters,
) lsmview.Data {
	n := 0
	for _, l := range b.levels {
		n += len(l)
	}
	const scanTablesThreshold = 100
	b.scanTables = n <= scanTablesThreshold

	var data lsmview.Data
	data.Keys = b.sortedKeys
	data.Levels = make([]lsmview.Level, len(b.levels))
	for i, files := range b.levels {
		l := &data.Levels[i]
		l.Name = b.levelNames[i]
		l.Tables = make([]lsmview.Table, len(files))
		for j, f := range files {
			t := &l.Tables[j]
			if !f.Virtual {
				t.Label = fmt.Sprintf("%d", f.TableNum)
			} else {
				t.Label = fmt.Sprintf("%d (%d)", f.TableNum, f.FileBacking.DiskFileNum)
			}

			t.Size = f.Size
			t.SmallestKey = b.keys[string(f.Smallest().UserKey)]
			t.LargestKey = b.keys[string(f.Largest().UserKey)]
			t.Details = b.tableDetails(f, objProvider, newIters)
		}
	}
	return data
}

func (b *lsmViewBuilder) tableDetails(
	m *tableMetadata, objProvider objstorage.Provider, newIters tableNewIters,
) []string {
	res := make([]string, 0, 10)
	outf := func(format string, args ...any) {
		res = append(res, fmt.Sprintf(format, args...))
	}

	outf("%s: %s - %s", m.TableNum, m.Smallest().Pretty(b.fmtKey), m.Largest().Pretty(b.fmtKey))
	outf("size: %s", humanize.Bytes.Uint64(m.Size))
	if m.Virtual {
		meta, err := objProvider.Lookup(base.FileTypeTable, m.FileBacking.DiskFileNum)
		var backingInfo string
		switch {
		case err != nil:
			backingInfo = fmt.Sprintf(" (error looking up object: %v)", err)
		case meta.IsShared():
			backingInfo = "shared; "
		case meta.IsExternal():
			backingInfo = "external; "
		}
		outf("virtual; backed by %s (%ssize: %s)", m.FileBacking.DiskFileNum, backingInfo, humanize.Bytes.Uint64(m.FileBacking.Size))
	}
	outf("seqnums: %d - %d", m.SmallestSeqNum, m.LargestSeqNum)
	if m.SyntheticPrefixAndSuffix.HasPrefix() {
		// Note: we are abusing the key formatter by passing just the prefix.
		outf("synthetic prefix: %s", b.fmtKey(m.SyntheticPrefixAndSuffix.Prefix()))
	}
	if m.SyntheticPrefixAndSuffix.HasSuffix() {
		// Note: we are abusing the key formatter by passing just the suffix.
		outf("synthetic suffix: %s", b.fmtKey(m.SyntheticPrefixAndSuffix.Suffix()))
	}
	var iters iterSet
	if b.scanTables {
		var err error
		iters, err = newIters(context.Background(), m, nil /* opts */, internalIterOpts{}, iterPointKeys|iterRangeDeletions|iterRangeKeys)
		if err != nil {
			outf("error opening table: %v", err)
		} else {
			defer func() { _ = iters.CloseAll() }()
		}
	}
	const maxPoints = 14
	const maxRangeDels = 10
	const maxRangeKeys = 10
	if m.HasPointKeys {
		outf("points: %s - %s", m.PointKeyBounds.Smallest().Pretty(b.fmtKey), m.PointKeyBounds.Largest().Pretty(b.fmtKey))
		if b.scanTables {
			n := 0
			if it := iters.point; it != nil {
				for kv := it.First(); kv != nil; kv = it.Next() {
					if n == maxPoints {
						outf("  ...")
						break
					}
					outf("  %s", kv.K.Pretty(b.fmtKey))
					n++
				}
				if err := it.Error(); err != nil {
					outf("  error scanning points: %v", err)
				}
			}
			if n == 0 {
				outf("  no points")
			}

			n = 0
			if it := iters.rangeDeletion; it != nil {
				span, err := it.First()
				for ; span != nil; span, err = it.Next() {
					if n == maxRangeDels {
						outf(" ...")
						break
					}
					seqNums := make([]string, len(span.Keys))
					for i, k := range span.Keys {
						seqNums[i] = fmt.Sprintf("#%d", k.SeqNum())
					}
					outf("  [%s - %s): %s", b.fmtKey(span.Start), b.fmtKey(span.End), strings.Join(seqNums, ","))
					n++
				}
				if err != nil {
					outf("error scanning range dels: %v", err)
				}
			}
			if n == 0 {
				outf("  no range dels")
			}
		}
	}
	if m.HasRangeKeys {
		outf("range keys: %s - %s", m.RangeKeyBounds.Smallest().Pretty(b.fmtKey), m.RangeKeyBounds.Largest().Pretty(b.fmtKey))
		n := 0
		if it := iters.rangeKey; it != nil {
			span, err := it.First()
			for ; span != nil; span, err = it.Next() {
				if n == maxRangeKeys {
					outf(" ...")
					break
				}
				keys := make([]string, len(span.Keys))
				for i, k := range span.Keys {
					keys[i] = k.String()
				}
				outf("  [%s, %s): {%s}", b.fmtKey(span.Start), b.fmtKey(span.End), strings.Join(keys, " "))
				n++
			}
			if err != nil {
				outf("error scanning range keys: %v", err)
			}
		}
		if n == 0 {
			outf("  no range keys")
		}
	}

	return res
}
