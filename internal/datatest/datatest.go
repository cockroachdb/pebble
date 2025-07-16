// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package datatest provides common datadriven test commands for use outside of
// the root Pebble package.
package datatest

import (
	"context"
	"strings"
	"sync"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

// TODO(jackson): Consider a refactoring that can consolidate this package and
// the datadriven commands defined in pebble/data_test.go.

// DefineBatch interprets the provided datadriven command as a sequence of write
// operations, one-per-line, to apply to the provided batch.
func DefineBatch(d *datadriven.TestData, b *pebble.Batch) error {
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
				return errors.Errorf("%s expects 2 arguments", parts[0])
			}
			err = b.Set([]byte(parts[1]), []byte(parts[2]), nil)
		case "del":
			if len(parts) != 2 {
				return errors.Errorf("%s expects 1 argument", parts[0])
			}
			err = b.Delete([]byte(parts[1]), nil)
		case "singledel":
			if len(parts) != 2 {
				return errors.Errorf("%s expects 1 argument", parts[0])
			}
			err = b.SingleDelete([]byte(parts[1]), nil)
		case "del-range":
			if len(parts) != 3 {
				return errors.Errorf("%s expects 2 arguments", parts[0])
			}
			err = b.DeleteRange([]byte(parts[1]), []byte(parts[2]), nil)
		case "merge":
			if len(parts) != 3 {
				return errors.Errorf("%s expects 2 arguments", parts[0])
			}
			err = b.Merge([]byte(parts[1]), []byte(parts[2]), nil)
		case "range-key-set":
			if len(parts) != 5 {
				return errors.Errorf("%s expects 4 arguments", parts[0])
			}
			err = b.RangeKeySet(
				[]byte(parts[1]),
				[]byte(parts[2]),
				[]byte(parts[3]),
				[]byte(parts[4]),
				nil)
		case "range-key-unset":
			if len(parts) != 4 {
				return errors.Errorf("%s expects 3 arguments", parts[0])
			}
			err = b.RangeKeyUnset(
				[]byte(parts[1]),
				[]byte(parts[2]),
				[]byte(parts[3]),
				nil)
		case "range-key-del":
			if len(parts) != 3 {
				return errors.Errorf("%s expects 2 arguments", parts[0])
			}
			err = b.RangeKeyDelete(
				[]byte(parts[1]),
				[]byte(parts[2]),
				nil)
		default:
			return errors.Errorf("unknown op: %s", parts[0])
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// CompactionTracker is a listener that tracks the number of compactions.
type CompactionTracker struct {
	sync.Cond
	count    int
	attached bool
}

// NewCompactionTracker setups the necessary options to keep track of the
// compactions that are in flight.
func NewCompactionTracker(options *pebble.Options) *CompactionTracker {
	ct := CompactionTracker{}
	ct.Cond = sync.Cond{
		L: &sync.Mutex{},
	}
	ct.attached = true
	el := pebble.EventListener{
		CompactionEnd: func(info pebble.CompactionInfo) {
			ct.L.Lock()
			ct.count--
			ct.Broadcast()
			ct.L.Unlock()
		},
		CompactionBegin: func(info pebble.CompactionInfo) {
			ct.L.Lock()
			ct.count++
			ct.Broadcast()
			ct.L.Unlock()
		},
	}

	options.AddEventListener(el)
	return &ct
}

// WaitForInflightCompactionsToEqual waits until compactions meet the specified target.
func (cql *CompactionTracker) WaitForInflightCompactionsToEqual(target int) {
	cql.L.Lock()
	if !cql.attached {
		panic("Cannot wait for compactions if listener has not been attached")
	}
	for cql.count != target {
		cql.Wait()
	}
	cql.L.Unlock()
}

// Below functions are copied from data_test.go from pebble package
func RunBuildSSTCmd(
	input string,
	writerArgs []datadriven.CmdArg,
	path string,
	fs vfs.FS,
	opts ...func(*dataDrivenCmdOptions),
) (sstable.WriterMetadata, error) {
	ddOpts := combineDataDrivenOpts(opts...)

	writerOpts := ddOpts.defaultWriterOpts
	if err := sstable.ParseWriterOptions(&writerOpts, writerArgs...); err != nil {
		return sstable.WriterMetadata{}, err
	}

	f, err := fs.Create(path, vfs.WriteCategoryUnspecified)
	if err != nil {
		return sstable.WriterMetadata{}, err
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), writerOpts)
	if err := sstable.ParseTestSST(w.Raw(), input, nil /* bv */); err != nil {
		return sstable.WriterMetadata{}, err
	}
	if err := w.Close(); err != nil {
		return sstable.WriterMetadata{}, err
	}
	metadata, err := w.Metadata()
	if err != nil {
		return sstable.WriterMetadata{}, err
	}
	return *metadata, nil
}

func combineDataDrivenOpts(opts ...func(*dataDrivenCmdOptions)) dataDrivenCmdOptions {
	combined := dataDrivenCmdOptions{}
	for _, opt := range opts {
		opt(&combined)
	}
	return combined
}

type dataDrivenCmdOptions struct {
	defaultWriterOpts sstable.WriterOptions
}

func WithDefaultWriterOpts(defaultWriterOpts sstable.WriterOptions) func(*dataDrivenCmdOptions) {
	return func(o *dataDrivenCmdOptions) { o.defaultWriterOpts = defaultWriterOpts }
}

func RunIngestAndExciseCmd(td *datadriven.TestData, d *pebble.DB) error {
	paths := make([]string, 0)
	var exciseSpan pebble.KeyRange
	for i := range td.CmdArgs {
		if strings.HasSuffix(td.CmdArgs[i].Key, ".sst") {
			paths = append(paths, td.CmdArgs[i].Key)
		} else if td.CmdArgs[i].Key == "excise" {
			if len(td.CmdArgs[i].Vals) != 1 {
				return errors.New("expected 2 values for excise separated by -, eg. ingest-and-excise foo1 excise=\"start-end\"")
			}
			fields := strings.Split(td.CmdArgs[i].Vals[0], "-")
			if len(fields) != 2 {
				return errors.New("expected 2 values for excise separated by -, eg. ingest-and-excise foo1 excise=\"start-end\"")
			}
			exciseSpan.Start = []byte(fields[0])
			exciseSpan.End = []byte(fields[1])
		}
	}
	if _, err := d.IngestAndExcise(context.Background(), paths, nil /* shared */, nil /* external */, exciseSpan); err != nil {
		return err
	}
	return nil
}
