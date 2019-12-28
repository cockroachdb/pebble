// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

type iterCmdOpt int

const (
	iterCmdVerboseKey iterCmdOpt = iota
)

func runGetCmd(td *datadriven.TestData, d *DB) string {
	snap := Snapshot{
		db:     d,
		seqNum: InternalKeySeqNumMax,
	}

	for _, arg := range td.CmdArgs {
		if len(arg.Vals) != 1 {
			return fmt.Sprintf("%s: %s=<value>", td.Cmd, arg.Key)
		}
		switch arg.Key {
		case "seq":
			var err error
			snap.seqNum, err = strconv.ParseUint(arg.Vals[0], 10, 64)
			if err != nil {
				return err.Error()
			}
		default:
			return fmt.Sprintf("%s: unknown arg: %s", td.Cmd, arg.Key)
		}
	}

	var buf bytes.Buffer
	for _, data := range strings.Split(td.Input, "\n") {
		v, err := snap.Get([]byte(data))
		if err != nil {
			fmt.Fprintf(&buf, "%s: %s\n", data, err)
		} else {
			fmt.Fprintf(&buf, "%s:%s\n", data, v)
		}
	}
	return buf.String()
}

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
		case "seek-prefix-ge":
			if len(parts) != 2 {
				return fmt.Sprintf("seek-prefix-ge <key>\n")
			}
			valid = iter.SeekPrefixGE([]byte(strings.TrimSpace(parts[1])))
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
		case "set-bounds":
			if len(parts) <= 1 || len(parts) > 3 {
				return fmt.Sprintf("set-bounds lower=<lower> upper=<upper>\n")
			}
			var lower []byte
			var upper []byte
			for _, part := range parts[1:] {
				arg := strings.Split(strings.TrimSpace(part), "=")
				switch arg[0] {
				case "lower":
					lower = []byte(arg[1])
				case "upper":
					upper = []byte(arg[1])
				default:
					return fmt.Sprintf("set-bounds: unknown arg: %s", arg)
				}
			}
			iter.SetBounds(lower, upper)
			valid = iter.Valid()
		default:
			return fmt.Sprintf("unknown op: %s", parts[0])
		}
		if err := iter.Error(); err != nil {
			fmt.Fprintf(&b, "err=%v\n", err)
		} else if valid != iter.Valid() {
			fmt.Fprintf(&b, "mismatched valid states: %t vs %t\n", valid, iter.Valid())
		} else if valid {
			fmt.Fprintf(&b, "%s:%s\n", iter.Key(), iter.Value())
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
	var prefix []byte
	for _, line := range strings.Split(d.Input, "\n") {
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		var key *InternalKey
		var value []byte
		switch parts[0] {
		case "seek-ge":
			if len(parts) != 2 {
				return fmt.Sprintf("seek-ge <key>\n")
			}
			prefix = nil
			key, value = iter.SeekGE([]byte(strings.TrimSpace(parts[1])))
		case "seek-prefix-ge":
			if len(parts) != 2 {
				return fmt.Sprintf("seek-prefix-ge <key>\n")
			}
			prefix = []byte(strings.TrimSpace(parts[1]))
			key, value = iter.SeekPrefixGE(prefix, prefix /* key */)
		case "seek-lt":
			if len(parts) != 2 {
				return fmt.Sprintf("seek-lt <key>\n")
			}
			prefix = nil
			key, value = iter.SeekLT([]byte(strings.TrimSpace(parts[1])))
		case "first":
			prefix = nil
			key, value = iter.First()
		case "last":
			prefix = nil
			key, value = iter.Last()
		case "next":
			key, value = iter.Next()
		case "prev":
			key, value = iter.Prev()
		case "set-bounds":
			if len(parts) <= 1 || len(parts) > 3 {
				return fmt.Sprintf("set-bounds lower=<lower> upper=<upper>\n")
			}
			var lower []byte
			var upper []byte
			for _, part := range parts[1:] {
				arg := strings.Split(strings.TrimSpace(part), "=")
				switch arg[0] {
				case "lower":
					lower = []byte(arg[1])
				case "upper":
					upper = []byte(arg[1])
				default:
					return fmt.Sprintf("set-bounds: unknown arg: %s", arg)
				}
			}
			iter.SetBounds(lower, upper)
			continue
		default:
			return fmt.Sprintf("unknown op: %s", parts[0])
		}
		if key != nil {
			if verboseKey {
				fmt.Fprintf(&b, "%s:%s\n", key, value)
			} else {
				fmt.Fprintf(&b, "%s:%s\n", key.UserKey, value)
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

func runBuildCmd(td *datadriven.TestData, d *DB, fs vfs.FS) error {
	b := d.NewIndexedBatch()
	if err := runBatchDefineCmd(td, b); err != nil {
		return err
	}

	if len(td.CmdArgs) != 1 {
		return fmt.Errorf("build <path>: argument missing")
	}
	path := td.CmdArgs[0].String()

	f, err := fs.Create(path)
	if err != nil {
		return err
	}
	w := sstable.NewWriter(f, sstable.WriterOptions{})
	iters := []internalIterator{
		b.newInternalIter(nil),
		b.newRangeDelIter(nil),
	}
	for _, iter := range iters {
		if iter == nil {
			continue
		}
		for key, val := iter.First(); key != nil; key, val = iter.Next() {
			tmp := *key
			tmp.SetSeqNum(0)
			if err := w.Add(tmp, val); err != nil {
				return err
			}
		}
		if err := iter.Close(); err != nil {
			return err
		}
	}
	return w.Close()
}

func runCompactCmd(td *datadriven.TestData, d *DB) error {
	if len(td.CmdArgs) > 2 {
		return fmt.Errorf("%s expects at most two arguments", td.Cmd)
	}
	parts := strings.Split(td.CmdArgs[0].Key, "-")
	if len(parts) != 2 {
		return fmt.Errorf("expected <begin>-<end>: %s", td.Input)
	}
	if len(td.CmdArgs) == 2 {
		levelString := td.CmdArgs[1].String()
		iStart := base.MakeInternalKey([]byte(parts[0]), InternalKeySeqNumMax, InternalKeyKindMax)
		iEnd := base.MakeInternalKey([]byte(parts[1]), 0, 0)
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

func runDBDefineCmd(td *datadriven.TestData, opts *Options) (*DB, error) {
	if td.Input == "" {
		return nil, fmt.Errorf("empty test input")
	}

	opts = opts.EnsureDefaults()
	opts.FS = vfs.NewMem()

	var snapshots []uint64
	for _, arg := range td.CmdArgs {
		switch arg.Key {
		case "target-file-sizes":
			opts.Levels = make([]LevelOptions, len(arg.Vals))
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
	d, err := Open("", opts)
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
			d.mu.versions.picker.baseLevel, []flushable{mem}, &d.bytesFlushed)
		c.disableRangeTombstoneElision = true
		newVE, _, err := d.runCompaction(0, c, nilPacer)
		if err != nil {
			return nil
		}
		for _, f := range newVE.NewFiles {
			ve.NewFiles = append(ve.NewFiles, newFileEntry{
				Level: level,
				Meta:  f.Meta,
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
					d.mu.mem.mutable = newMemTable(d.opts, 0 /* size */, nil /* reservation */)
					d.mu.mem.queue = append(d.mu.mem.queue, d.mu.mem.mutable)
					d.updateReadStateLocked(nil)
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
				mem = newMemTable(d.opts, 0 /* size */, nil /* reservation */)
			}
		}

		for _, data := range fields {
			i := strings.Index(data, ":")
			key := base.ParseInternalKey(data[:i])
			value := []byte(data[i+1:])
			if err := mem.set(key, value); err != nil {
				return nil, err
			}
		}
	}

	if err := maybeFlush(); err != nil {
		return nil, err
	}

	if len(ve.NewFiles) > 0 {
		jobID := d.mu.nextJobID
		d.mu.nextJobID++
		d.mu.versions.logLock()
		if err := d.mu.versions.logAndApply(jobID, ve, nil, d.dataDir); err != nil {
			return nil, err
		}
		d.updateReadStateLocked(nil)
	}

	return d, nil
}

func runIngestCmd(td *datadriven.TestData, d *DB, fs vfs.FS) error {
	var paths []string
	for _, arg := range td.CmdArgs {
		paths = append(paths, arg.String())
	}

	if err := d.Ingest(paths); err != nil {
		return err
	}
	for _, path := range paths {
		if err := fs.Remove(path); err != nil {
			return err
		}
	}
	return nil
}

func runLSMCmd(td *datadriven.TestData, d *DB) string {
	d.mu.Lock()
	s := d.mu.versions.currentVersion().DebugString(base.DefaultFormatter)
	d.mu.Unlock()
	return s
}
