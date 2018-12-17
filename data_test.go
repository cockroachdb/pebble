// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/internal/datadriven"
	"github.com/petermattis/pebble/storage"
)

type iterCmdOpt int

const (
	iterCmdVerboseKey iterCmdOpt = iota
)

func runIterCmd(d *datadriven.TestData, iter *Iterator) string {
	var b bytes.Buffer
	for _, line := range strings.Split(d.Input, "\n") {
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		var valid bool
		switch parts[0] {
		case "seek-ge":
			if len(parts) != 2 {
				return fmt.Sprintf("seek-ge <key>\n")
			}
			valid = iter.SeekGE([]byte(strings.TrimSpace(parts[1])))
		case "seek-lt":
			if len(parts) != 2 {
				return fmt.Sprintf("seek-lt <key>\n")
			}
			valid = iter.SeekLT([]byte(strings.TrimSpace(parts[1])))
		case "first":
			valid = iter.First()
		case "last":
			valid = iter.Last()
		case "next":
			valid = iter.Next()
		case "prev":
			valid = iter.Prev()
		default:
			return fmt.Sprintf("unknown op: %s", parts[0])
		}
		if valid != iter.Valid() {
			fmt.Fprintf(&b, "mismatched valid states: %t vs %t\n", valid, iter.Valid())
		} else if valid {
			fmt.Fprintf(&b, "%s:%s\n", iter.Key(), iter.Value())
		} else if err := iter.Error(); err != nil {
			fmt.Fprintf(&b, "err=%v\n", err)
		} else {
			fmt.Fprintf(&b, ".\n")
		}
	}
	return b.String()
}

func runInternalIterCmd(d *datadriven.TestData, iter internalIterator, opts ...iterCmdOpt) string {
	var verboseKey bool
	for _, opt := range opts {
		if opt == iterCmdVerboseKey {
			verboseKey = true
		}
	}

	var b bytes.Buffer
	for _, line := range strings.Split(d.Input, "\n") {
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		switch parts[0] {
		case "seek-ge":
			if len(parts) != 2 {
				return fmt.Sprintf("seek-ge <key>\n")
			}
			iter.SeekGE([]byte(strings.TrimSpace(parts[1])))
		case "seek-lt":
			if len(parts) != 2 {
				return fmt.Sprintf("seek-lt <key>\n")
			}
			iter.SeekLT([]byte(strings.TrimSpace(parts[1])))
		case "first":
			iter.First()
		case "last":
			iter.Last()
		case "next":
			iter.Next()
		case "prev":
			iter.Prev()
		default:
			return fmt.Sprintf("unknown op: %s", parts[0])
		}
		if iter.Valid() {
			if verboseKey {
				fmt.Fprintf(&b, "%s:%s\n", iter.Key(), iter.Value())
			} else {
				fmt.Fprintf(&b, "%s:%s\n", iter.Key().UserKey, iter.Value())
			}
		} else if err := iter.Error(); err != nil {
			fmt.Fprintf(&b, "err=%v\n", err)
		} else {
			fmt.Fprintf(&b, ".\n")
		}
	}
	return b.String()
}

func runBatchDefineCmd(d *datadriven.TestData, b *Batch) error {
	for _, line := range strings.Split(d.Input, "\n") {
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		if parts[1] == `<nil>` {
			parts[1] = ""
		}
		var err error
		switch parts[0] {
		case "set":
			if len(parts) != 3 {
				return fmt.Errorf("%s expects 2 arguments", parts[0])
			}
			err = b.Set([]byte(parts[1]), []byte(parts[2]), nil)
		case "del":
			if len(parts) != 2 {
				return fmt.Errorf("%s expects 1 argument", parts[0])
			}
			err = b.Delete([]byte(parts[1]), nil)
		case "del-range":
			if len(parts) != 3 {
				return fmt.Errorf("%s expects 2 arguments", parts[0])
			}
			err = b.DeleteRange([]byte(parts[1]), []byte(parts[2]), nil)
		case "merge":
			if len(parts) != 3 {
				return fmt.Errorf("%s expects 2 arguments", parts[0])
			}
			err = b.Merge([]byte(parts[1]), []byte(parts[2]), nil)
		default:
			return fmt.Errorf("unknown op: %s", parts[0])
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func runDBDefineCmd(td *datadriven.TestData) (*DB, error) {
	if td.Input == "" {
		return nil, fmt.Errorf("empty test input")
	}

	fs := storage.NewMem()
	d, err := Open("", &db.Options{
		Storage: fs,
	})
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	var mem *memTable
	ve := &versionEdit{}
	level := -1

	maybeFlush := func() error {
		if level < 0 {
			return nil
		}

		iter := mem.newIter(nil)
		if rangeDelIter := mem.newRangeDelIter(nil); rangeDelIter != nil {
			iter = newMergingIter(d.cmp, iter, rangeDelIter)
		}
		meta, err := d.writeLevel0Table(d.opts.Storage, iter,
			false /* allowRangeTombstoneElision */)
		if err != nil {
			return nil
		}
		ve.newFiles = append(ve.newFiles, newFileEntry{
			level: level,
			meta:  meta,
		})
		level = -1
		return nil
	}

	for _, line := range strings.Split(td.Input, "\n") {
		fields := strings.Fields(line)
		if len(fields) > 0 {
			switch fields[0] {
			case "mem":
				if err := maybeFlush(); err != nil {
					return nil, err
				}
				// Add a memtable layer.
				if !d.mu.mem.mutable.empty() {
					d.mu.mem.mutable = newMemTable(d.opts)
					d.mu.mem.queue = append(d.mu.mem.queue, d.mu.mem.mutable)
				}
				mem = d.mu.mem.mutable
				fields = fields[1:]
			case "L0", "L1", "L2", "L3", "L4", "L5", "L6":
				if err := maybeFlush(); err != nil {
					return nil, err
				}
				var err error
				if level, err = strconv.Atoi(fields[0][1:]); err != nil {
					return nil, err
				}
				fields = fields[1:]
				mem = newMemTable(d.opts)
			}
		}

		for _, data := range fields {
			i := strings.Index(data, ":")
			key := db.ParseInternalKey(data[:i])
			value := []byte(data[i+1:])
			if err := mem.set(key, value); err != nil {
				return nil, err
			}
		}
	}

	if err := maybeFlush(); err != nil {
		return nil, err
	}

	if len(ve.newFiles) > 0 {
		if err := d.mu.versions.logAndApply(ve); err != nil {
			return nil, err
		}
		for i := range ve.newFiles {
			meta := &ve.newFiles[i].meta
			delete(d.mu.compact.pendingOutputs, meta.fileNum)
		}
	}

	return d, nil
}
