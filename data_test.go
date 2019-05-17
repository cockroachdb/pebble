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
	"github.com/petermattis/pebble/vfs"
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

func runCompactCommand(td *datadriven.TestData, d *DB) error {
	if len(td.CmdArgs) > 2 {
		return fmt.Errorf("%s expects at most two arguments", td.Cmd)
	}
	parts := strings.Split(td.CmdArgs[0].Key, "-")
	if len(parts) != 2 {
		return fmt.Errorf("expected <begin>-<end>: %s", td.Input)
	}
	if len(td.CmdArgs) == 2 {
		levelString := td.CmdArgs[1].String()
		iStart := db.MakeInternalKey([]byte(parts[0]), db.InternalKeySeqNumMax, db.InternalKeyKindMax)
		iEnd := db.MakeInternalKey([]byte(parts[1]), 0, 0)
		if levelString[0] != 'L' {
			return fmt.Errorf("expected L<n>: %s", levelString)
		}
		level, err := strconv.Atoi(levelString[1:])
		if err != nil {
			return err
		}
		return d.manualCompact(&manualCompaction{
			done:  make(chan error, 1),
			level: level,
			start: iStart,
			end:   iEnd,
		})
	}
	return d.Compact([]byte(parts[0]), []byte(parts[1]))
}

func runDBDefineCmd(td *datadriven.TestData) (*DB, error) {
	if td.Input == "" {
		return nil, fmt.Errorf("empty test input")
	}

	opts := db.Options{
		FS: vfs.NewMem(),
	}
	var snapshots []uint64
	for _, arg := range td.CmdArgs {
		switch arg.Key {
		case "target-file-sizes":
			opts.Levels = make([]db.LevelOptions, len(arg.Vals))
			for i := range arg.Vals {
				size, err := strconv.ParseInt(arg.Vals[i], 10, 64)
				if err != nil {
					return nil, err
				}
				opts.Levels[i].TargetFileSize = size
			}
		case "snapshots":
			snapshots = make([]uint64, len(arg.Vals))
			for i := range arg.Vals {
				seqNum, err := strconv.ParseUint(arg.Vals[i], 10, 64)
				if err != nil {
					return nil, err
				}
				snapshots[i] = seqNum
				if i > 0 && snapshots[i] < snapshots[i-1] {
					return nil, fmt.Errorf("Snapshots must be in ascending order")
				}
			}
		default:
			return nil, fmt.Errorf("%s: unknown arg: %s", td.Cmd, arg.Key)
		}
	}
	d, err := Open("", &opts)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	d.mu.versions.dynamicBaseLevel = false
	for i := range snapshots {
		s := &Snapshot{db: d}
		s.seqNum = snapshots[i]
		d.mu.snapshots.pushBack(s)
	}
	defer d.mu.Unlock()

	var mem *memTable
	ve := &versionEdit{}
	level := -1

	maybeFlush := func() error {
		if level < 0 {
			return nil
		}

		c := newFlush(d.opts, d.mu.versions.currentVersion(),
			d.mu.versions.picker.baseLevel, []flushable{mem})
		c.disableRangeTombstoneElision = true
		newVE, _, err := d.runCompaction(c)
		if err != nil {
			return nil
		}
		for _, f := range newVE.newFiles {
			ve.newFiles = append(ve.newFiles, newFileEntry{
				level: level,
				meta:  f.meta,
			})
		}
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
					d.updateReadStateLocked()
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
		jobID := d.mu.nextJobID
		d.mu.nextJobID++
		if err := d.mu.versions.logAndApply(jobID, ve, d.dataDir); err != nil {
			return nil, err
		}
		d.updateReadStateLocked()
		for i := range ve.newFiles {
			meta := &ve.newFiles[i].meta
			delete(d.mu.compact.pendingOutputs, meta.fileNum)
		}
	}

	return d, nil
}
