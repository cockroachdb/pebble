// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/pebble/rangekey"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestScanInternal(t *testing.T) {
	var d *DB
	type scanInternalReader interface {
		ScanInternal(
			ctx context.Context,
			lower, upper []byte, visitPointKey func(key *InternalKey, value LazyValue) error,
			visitRangeDel func(start, end []byte, seqNum uint64) error,
			visitRangeKey func(start, end []byte, keys []rangekey.Key) error,
			visitSharedFile func(sst *SharedSSTMeta) error) error
	}
	batches := map[string]*Batch{}
	snaps := map[string]*Snapshot{}
	efos := map[string]*EventuallyFileOnlySnapshot{}
	parseOpts := func(td *datadriven.TestData) (*Options, error) {
		opts := &Options{
			FS:                 vfs.NewMem(),
			Logger:             testLogger{t: t},
			Comparer:           testkeys.Comparer,
			FormatMajorVersion: FormatRangeKeys,
			BlockPropertyCollectors: []func() BlockPropertyCollector{
				sstable.NewTestKeysBlockPropertyCollector,
			},
		}
		opts.Experimental.RemoteStorage = remote.MakeSimpleFactory(map[remote.Locator]remote.Storage{
			"": remote.NewInMem(),
		})
		opts.Experimental.CreateOnShared = true
		opts.Experimental.CreateOnSharedLocator = ""
		opts.DisableAutomaticCompactions = true
		opts.EnsureDefaults()
		opts.WithFSDefaults()

		for _, cmdArg := range td.CmdArgs {
			switch cmdArg.Key {
			case "format-major-version":
				v, err := strconv.Atoi(cmdArg.Vals[0])
				if err != nil {
					return nil, err
				}
				// Override the DB version.
				opts.FormatMajorVersion = FormatMajorVersion(v)
			case "block-size":
				v, err := strconv.Atoi(cmdArg.Vals[0])
				if err != nil {
					return nil, err
				}
				for i := range opts.Levels {
					opts.Levels[i].BlockSize = v
				}
			case "index-block-size":
				v, err := strconv.Atoi(cmdArg.Vals[0])
				if err != nil {
					return nil, err
				}
				for i := range opts.Levels {
					opts.Levels[i].IndexBlockSize = v
				}
			case "target-file-size":
				v, err := strconv.Atoi(cmdArg.Vals[0])
				if err != nil {
					return nil, err
				}
				for i := range opts.Levels {
					opts.Levels[i].TargetFileSize = int64(v)
				}
			case "bloom-bits-per-key":
				v, err := strconv.Atoi(cmdArg.Vals[0])
				if err != nil {
					return nil, err
				}
				fp := bloom.FilterPolicy(v)
				opts.Filters = map[string]FilterPolicy{fp.Name(): fp}
				for i := range opts.Levels {
					opts.Levels[i].FilterPolicy = fp
				}
			case "merger":
				switch cmdArg.Vals[0] {
				case "appender":
					opts.Merger = base.DefaultMerger
				default:
					return nil, errors.Newf("unrecognized Merger %q\n", cmdArg.Vals[0])
				}
			}
		}
		return opts, nil
	}
	cleanup := func() (err error) {
		for key, batch := range batches {
			err = firstError(err, batch.Close())
			delete(batches, key)
		}
		for key, snap := range snaps {
			err = firstError(err, snap.Close())
			delete(snaps, key)
		}
		for key, es := range efos {
			err = firstError(err, es.Close())
			delete(efos, key)
		}
		if d != nil {
			err = firstError(err, d.Close())
			d = nil
		}
		return err
	}
	defer cleanup()

	datadriven.RunTest(t, "testdata/scan_internal", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			if err := cleanup(); err != nil {
				return err.Error()
			}
			opts, err := parseOpts(td)
			if err != nil {
				return err.Error()
			}
			d, err = runDBDefineCmd(td, opts)
			if err != nil {
				return err.Error()
			}
			return runLSMCmd(td, d)

		case "reset":
			if err := cleanup(); err != nil {
				t.Fatal(err)
				return err.Error()
			}
			opts, err := parseOpts(td)
			if err != nil {
				t.Fatal(err)
				return err.Error()
			}

			d, err = Open("", opts)
			require.NoError(t, err)
			require.NoError(t, d.SetCreatorID(1))
			return ""
		case "snapshot":
			s := d.NewSnapshot()
			var name string
			td.ScanArgs(t, "name", &name)
			snaps[name] = s
			return ""
		case "wait-for-file-only-snapshot":
			if len(td.CmdArgs) != 1 {
				panic("insufficient args for file-only-snapshot command")
			}
			name := td.CmdArgs[0].Key
			es := efos[name]
			if err := es.WaitForFileOnlySnapshot(); err != nil {
				return err.Error()
			}
			return "ok"
		case "file-only-snapshot":
			if len(td.CmdArgs) != 1 {
				panic("insufficient args for file-only-snapshot command")
			}
			name := td.CmdArgs[0].Key
			var keyRanges []KeyRange
			for _, line := range strings.Split(td.Input, "\n") {
				fields := strings.Fields(line)
				if len(fields) != 2 {
					return "expected two fields for file-only snapshot KeyRanges"
				}
				kr := KeyRange{Start: []byte(fields[0]), End: []byte(fields[1])}
				keyRanges = append(keyRanges, kr)
			}

			s := d.NewEventuallyFileOnlySnapshot(keyRanges)
			efos[name] = s
			return "ok"
		case "batch":
			var name string
			td.MaybeScanArgs(t, "name", &name)
			commit := td.HasArg("commit")
			b := d.NewIndexedBatch()
			require.NoError(t, runBatchDefineCmd(td, b))
			var err error
			if commit {
				func() {
					defer func() {
						if r := recover(); r != nil {
							err = errors.New(r.(string))
						}
					}()
					err = b.Commit(nil)
				}()
			} else if name != "" {
				batches[name] = b
			}
			if err != nil {
				return err.Error()
			}
			count := b.Count()
			if commit {
				return fmt.Sprintf("committed %d keys\n", count)
			}
			return fmt.Sprintf("wrote %d keys to batch %q\n", count, name)
		case "compact":
			if err := runCompactCmd(td, d); err != nil {
				return err.Error()
			}
			return runLSMCmd(td, d)
		case "flush":
			err := d.Flush()
			if err != nil {
				return err.Error()
			}
			return ""
		case "lsm":
			return runLSMCmd(td, d)
		case "commit":
			name := pluckStringCmdArg(td, "batch")
			b := batches[name]
			defer b.Close()
			count := b.Count()
			require.NoError(t, d.Apply(b, nil))
			delete(batches, name)
			return fmt.Sprintf("committed %d keys\n", count)
		case "scan-internal":
			var lower, upper []byte
			var reader scanInternalReader = d
			var b strings.Builder
			var fileVisitor func(sst *SharedSSTMeta) error
			for _, arg := range td.CmdArgs {
				switch arg.Key {
				case "lower":
					lower = []byte(arg.Vals[0])
				case "upper":
					upper = []byte(arg.Vals[0])
				case "snapshot":
					name := arg.Vals[0]
					snap, ok := snaps[name]
					if !ok {
						return fmt.Sprintf("no snapshot found for name %s", name)
					}
					reader = snap
				case "file-only-snapshot":
					name := arg.Vals[0]
					efos, ok := efos[name]
					if !ok {
						return fmt.Sprintf("no snapshot found for name %s", name)
					}
					reader = efos
				case "skip-shared":
					fileVisitor = func(sst *SharedSSTMeta) error {
						fmt.Fprintf(&b, "shared file: %s [%s-%s] [point=%s-%s] [range=%s-%s]\n", sst.fileNum, sst.Smallest.String(), sst.Largest.String(), sst.SmallestPointKey.String(), sst.LargestPointKey.String(), sst.SmallestRangeKey.String(), sst.LargestRangeKey.String())
						return nil
					}
				}
			}
			err := reader.ScanInternal(context.TODO(), lower, upper, func(key *InternalKey, value LazyValue) error {
				v := value.InPlaceValue()
				fmt.Fprintf(&b, "%s (%s)\n", key, v)
				return nil
			}, func(start, end []byte, seqNum uint64) error {
				fmt.Fprintf(&b, "%s-%s#%d,RANGEDEL\n", start, end, seqNum)
				return nil
			}, func(start, end []byte, keys []rangekey.Key) error {
				s := keyspan.Span{Start: start, End: end, Keys: keys}
				fmt.Fprintf(&b, "%s\n", s.String())
				return nil
			}, fileVisitor)
			if err != nil {
				return err.Error()
			}
			return b.String()
		default:
			return fmt.Sprintf("unknown command %q", td.Cmd)
		}
	})
}

func TestPointCollapsingIter(t *testing.T) {
	var def string
	datadriven.RunTest(t, "testdata/point_collapsing_iter", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			def = d.Input
			return ""

		case "iter":
			f := &fakeIter{}
			var spans []keyspan.Span
			for _, line := range strings.Split(def, "\n") {
				for _, key := range strings.Fields(line) {
					j := strings.Index(key, ":")
					k := base.ParseInternalKey(key[:j])
					v := []byte(key[j+1:])
					if k.Kind() == InternalKeyKindRangeDelete {
						spans = append(spans, keyspan.Span{
							Start:     k.UserKey,
							End:       v,
							Keys:      []keyspan.Key{{Trailer: k.Trailer}},
							KeysOrder: 0,
						})
						continue
					}
					f.keys = append(f.keys, k)
					f.vals = append(f.vals, v)
				}
			}

			ksIter := keyspan.NewIter(base.DefaultComparer.Compare, spans)
			pcIter := &pointCollapsingIterator{
				comparer: base.DefaultComparer,
				merge:    base.DefaultMerger.Merge,
				seqNum:   math.MaxUint64,
			}
			pcIter.iter.Init(base.DefaultComparer, f, ksIter, nil /* mask */, nil, nil)
			defer pcIter.Close()

			return runInternalIterCmd(t, d, pcIter, iterCmdVerboseKey)

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}
