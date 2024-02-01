// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package wal

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
)

// A segment represents an individual physical file that makes up a contiguous
// segment of a logical WAL. If a failover occurred during a WAL's lifetime, a
// WAL may be composed of multiple segments.
type segment struct {
	logIndex logIndex
	dir      Dir
}

func (s segment) String() string {
	return fmt.Sprintf("(%s,%s)", s.dir.Dirname, s.logIndex)
}

// A logicalWAL identifies a logical WAL and its consituent segment files.
type logicalWAL struct {
	NumWAL
	// segments contains the list of the consistuent physical segment files that
	// make up the single logical WAL file. segments is ordered by increasing
	// logIndex.
	segments []segment
}

func (w logicalWAL) String() string {
	var sb strings.Builder
	sb.WriteString(base.DiskFileNum(w.NumWAL).String())
	sb.WriteString(": {")
	for i := range w.segments {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(w.segments[i].String())
	}
	sb.WriteString("}")
	return sb.String()
}

// listLogs finds all log files in the provided directories. It returns an
// ordered list of WALs in increasing NumWAL order.
func listLogs(dirs ...Dir) ([]logicalWAL, error) {
	var wals []logicalWAL
	for _, d := range dirs {
		ls, err := d.FS.List(d.Dirname)
		if err != nil {
			return nil, errors.Wrapf(err, "reading %q", d.Dirname)
		}
		for _, name := range ls {
			dfn, li, ok := parseLogFilename(name)
			if !ok {
				continue
			}
			// Have we seen this logical log number yet?
			i, found := slices.BinarySearchFunc(wals, dfn, func(lw logicalWAL, n NumWAL) int {
				return cmp.Compare(lw.NumWAL, n)
			})
			if !found {
				wals = slices.Insert(wals, i, logicalWAL{NumWAL: dfn, segments: make([]segment, 0, 1)})
			}

			// Ensure we haven't seen this log index yet, and find where it
			// slots within this log's segments.
			j, found := slices.BinarySearchFunc(wals[i].segments, li, func(s segment, li logIndex) int {
				return cmp.Compare(s.logIndex, li)
			})
			if found {
				return nil, errors.Errorf("wal: duplicate logIndex=%s for WAL %s in %s and %s",
					li, dfn, d.Dirname, wals[i].segments[j].dir.Dirname)
			}
			wals[i].segments = slices.Insert(wals[i].segments, j, segment{logIndex: li, dir: d})
		}
	}
	return wals, nil
}
