// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"fmt"
	"io"
	"maps"
	"math"
	"math/rand/v2"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/blobtest"
	"github.com/cockroachdb/pebble/internal/compact"
	"github.com/cockroachdb/pebble/internal/humanize"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/rangekey"
	"github.com/cockroachdb/pebble/internal/sstableinternal"
	"github.com/cockroachdb/pebble/internal/strparse"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
	"github.com/cockroachdb/pebble/wal"
	"github.com/ghemawat/stream"
	"github.com/stretchr/testify/require"
)

func runGetCmd(t testing.TB, td *datadriven.TestData, d *DB) string {
	snap := Snapshot{
		db:     d,
		seqNum: base.SeqNumMax,
	}
	if td.HasArg("seq") {
		var n uint64
		td.ScanArgs(t, "seq", &n)
		snap.seqNum = base.SeqNum(n)
	}

	var buf bytes.Buffer
	for _, data := range strings.Split(td.Input, "\n") {
		v, closer, err := snap.Get([]byte(data))
		if err != nil {
			fmt.Fprintf(&buf, "%s: %s\n", data, err)
		} else {
			fmt.Fprintf(&buf, "%s:%s\n", data, v)
			closer.Close()
		}
	}
	return buf.String()
}

func runIterCmd(d *datadriven.TestData, iter *Iterator, closeIter bool) string {
	if closeIter {
		defer func() {
			if iter != nil {
				iter.Close()
			}
		}()
	}
	var b bytes.Buffer
	for _, line := range strings.Split(d.Input, "\n") {
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		printValidityState := false
		var valid bool
		var validityState IterValidityState
		switch parts[0] {
		case "seek-ge":
			if len(parts) != 2 {
				return "seek-ge <key>\n"
			}
			valid = iter.SeekGE([]byte(parts[1]))
		case "seek-prefix-ge":
			if len(parts) != 2 {
				return "seek-prefix-ge <key>\n"
			}
			valid = iter.SeekPrefixGE([]byte(parts[1]))
		case "seek-lt":
			if len(parts) != 2 {
				return "seek-lt <key>\n"
			}
			valid = iter.SeekLT([]byte(parts[1]))
		case "seek-ge-limit":
			if len(parts) != 3 {
				return "seek-ge-limit <key> <limit>\n"
			}
			validityState = iter.SeekGEWithLimit(
				[]byte(parts[1]), []byte(parts[2]))
			printValidityState = true
		case "seek-lt-limit":
			if len(parts) != 3 {
				return "seek-lt-limit <key> <limit>\n"
			}
			validityState = iter.SeekLTWithLimit(
				[]byte(parts[1]), []byte(parts[2]))
			printValidityState = true
		case "inspect":
			if len(parts) != 2 {
				return "inspect <field>\n"
			}
			field := parts[1]
			switch field {
			case "lastPositioningOp":
				op := "?"
				switch iter.lastPositioningOp {
				case unknownLastPositionOp:
					op = "unknown"
				case seekPrefixGELastPositioningOp:
					op = "seekprefixge"
				case seekGELastPositioningOp:
					op = "seekge"
				case seekLTLastPositioningOp:
					op = "seeklt"
				}
				fmt.Fprintf(&b, "%s=%q\n", field, op)
			case "value.Len()":
				fmt.Fprintf(&b, "%s=%d\n", field, iter.value.Len())
			default:
				return fmt.Sprintf("unrecognized inspect field %q\n", field)
			}
			continue
		case "next-limit":
			if len(parts) != 2 {
				return "next-limit <limit>\n"
			}
			validityState = iter.NextWithLimit([]byte(parts[1]))
			printValidityState = true
		case "internal-next":
			validity, keyKind := iter.internalNext()
			switch validity {
			case internalNextError:
				fmt.Fprintf(&b, "err: %s\n", iter.Error())
			case internalNextExhausted:
				fmt.Fprint(&b, ".\n")
			case internalNextValid:
				fmt.Fprintf(&b, "%s\n", keyKind)
			default:
				panic("unreachable")
			}
			continue
		case "can-deterministically-single-delete":
			ok, err := CanDeterministicallySingleDelete(iter)
			if err != nil {
				fmt.Fprintf(&b, "err: %s\n", err)
			} else {
				fmt.Fprintf(&b, "%t\n", ok)
			}
			continue
		case "prev-limit":
			if len(parts) != 2 {
				return "prev-limit <limit>\n"
			}
			validityState = iter.PrevWithLimit([]byte(parts[1]))
			printValidityState = true
		case "first":
			valid = iter.First()
		case "last":
			valid = iter.Last()
		case "next":
			valid = iter.Next()
		case "next-prefix":
			valid = iter.NextPrefix()
		case "prev":
			valid = iter.Prev()
		case "set-bounds":
			if len(parts) <= 1 || len(parts) > 3 {
				return "set-bounds lower=<lower> upper=<upper>\n"
			}
			var lower []byte
			var upper []byte
			for _, part := range parts[1:] {
				arg := strings.Split(part, "=")
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
		case "set-options":
			opts := iter.opts
			if _, err := parseIterOptions(&opts, &iter.opts, parts[1:]); err != nil {
				return fmt.Sprintf("set-options: %s", err.Error())
			}
			iter.SetOptions(&opts)
			valid = iter.Valid()
		case "stats":
			stats := iter.Stats()
			// The timing is non-deterministic, so set to 0.
			stats.InternalStats.BlockReadDuration = 0
			fmt.Fprintf(&b, "stats: %s\n", stats.String())
			continue
		case "clone":
			var opts CloneOptions
			if len(parts) > 1 {
				var iterOpts IterOptions
				if foundAny, err := parseIterOptions(&iterOpts, &iter.opts, parts[1:]); err != nil {
					return fmt.Sprintf("clone: %s", err.Error())
				} else if foundAny {
					opts.IterOptions = &iterOpts
				}
				for _, part := range parts[1:] {
					if arg := strings.Split(part, "="); len(arg) == 2 && arg[0] == "refresh-batch" {
						var err error
						opts.RefreshBatchView, err = strconv.ParseBool(arg[1])
						if err != nil {
							return fmt.Sprintf("clone: refresh-batch: %s", err.Error())
						}
					}
				}
			}
			clonedIter, err := iter.Clone(opts)
			if err != nil {
				fmt.Fprintf(&b, "error in clone, skipping rest of input: err=%v\n", err)
				return b.String()
			}
			if err = iter.Close(); err != nil {
				fmt.Fprintf(&b, "err=%v\n", err)
			}
			iter = clonedIter
		case "is-using-combined":
			if iter.opts.KeyTypes != IterKeyTypePointsAndRanges {
				fmt.Fprintln(&b, "not configured for combined iteration")
			} else if iter.lazyCombinedIter.combinedIterState.initialized {
				fmt.Fprintln(&b, "using combined (non-lazy) iterator")
			} else {
				fmt.Fprintln(&b, "using lazy iterator")
			}
			continue
		default:
			return fmt.Sprintf("unknown op: %s", parts[0])
		}

		valid = valid || validityState == IterValid
		if valid != iter.Valid() {
			fmt.Fprintf(&b, "mismatched valid states: %t vs %t\n", valid, iter.Valid())
		}
		hasPoint, hasRange := iter.HasPointAndRange()
		hasEither := hasPoint || hasRange
		if hasEither != valid {
			fmt.Fprintf(&b, "mismatched valid/HasPointAndRange states: valid=%t HasPointAndRange=(%t,%t)\n", valid, hasPoint, hasRange)
		}

		if valid {
			validityState = IterValid
		}
		printIterState(&b, iter, validityState, printValidityState)
	}
	return b.String()
}

func parseIterOptions(
	opts *IterOptions, ref *IterOptions, parts []string,
) (foundAny bool, err error) {
	const usageString = "[lower=<lower>] [upper=<upper>] [key-types=point|range|both] [mask-suffix=<suffix>] [mask-filter=<bool>] [only-durable=<bool>] point-filters=reuse|none]\n"
	for _, part := range parts {
		arg := strings.SplitN(part, "=", 2)
		if len(arg) != 2 {
			return false, errors.Newf(usageString)
		}
		switch arg[0] {
		case "point-filters":
			switch arg[1] {
			case "reuse":
				opts.PointKeyFilters = ref.PointKeyFilters
			case "none":
				opts.PointKeyFilters = nil
			default:
				return false, errors.Newf("unknown arg point-filter=%q:\n%s", arg[1], usageString)
			}
		case "lower":
			opts.LowerBound = []byte(arg[1])
		case "upper":
			opts.UpperBound = []byte(arg[1])
		case "key-types":
			switch arg[1] {
			case "point":
				opts.KeyTypes = IterKeyTypePointsOnly
			case "range":
				opts.KeyTypes = IterKeyTypeRangesOnly
			case "both":
				opts.KeyTypes = IterKeyTypePointsAndRanges
			default:
				return false, errors.Newf("unknown key-type %q:\n%s", arg[1], usageString)
			}
		case "mask-suffix":
			opts.RangeKeyMasking.Suffix = []byte(arg[1])
		case "mask-filter":
			opts.RangeKeyMasking.Filter = func() BlockPropertyFilterMask {
				return sstable.NewTestKeysMaskingFilter()
			}
		case "only-durable":
			var err error
			opts.OnlyReadGuaranteedDurable, err = strconv.ParseBool(arg[1])
			if err != nil {
				return false, errors.Newf("cannot parse only-durable=%q: %s", arg[1], err)
			}
		default:
			continue
		}
		foundAny = true
	}
	return foundAny, nil
}

func printIterState(
	b io.Writer, iter *Iterator, validity IterValidityState, printValidityState bool,
) {
	var validityStateStr string
	if printValidityState {
		switch validity {
		case IterExhausted:
			validityStateStr = " exhausted"
		case IterValid:
			validityStateStr = " valid"
		case IterAtLimit:
			validityStateStr = " at-limit"
		}
	}
	if validity == IterValid {
		switch {
		case iter.opts.pointKeys():
			hasPoint, hasRange := iter.HasPointAndRange()
			fmt.Fprintf(b, "%s:%s (", iter.Key(), validityStateStr)
			if hasPoint {
				fmt.Fprintf(b, "%s, ", formatASCIIValue(iter.Value()))
			} else {
				fmt.Fprint(b, "., ")
			}
			if hasRange {
				start, end := iter.RangeBounds()
				fmt.Fprintf(b, "[%s-%s)", formatASCIIKey(start), formatASCIIKey(end))
				writeRangeKeys(b, iter)
			} else {
				fmt.Fprint(b, ".")
			}
			if iter.RangeKeyChanged() {
				fmt.Fprint(b, " UPDATED")
			}
			fmt.Fprint(b, ")")
		default:
			if iter.Valid() {
				hasPoint, hasRange := iter.HasPointAndRange()
				if hasPoint || !hasRange {
					panic(fmt.Sprintf("pebble: unexpected HasPointAndRange (%t, %t)", hasPoint, hasRange))
				}
				start, end := iter.RangeBounds()
				fmt.Fprintf(b, "%s [%s-%s)", iter.Key(), formatASCIIKey(start), formatASCIIKey(end))
				writeRangeKeys(b, iter)
			} else {
				fmt.Fprint(b, ".")
			}
			if iter.RangeKeyChanged() {
				fmt.Fprint(b, " UPDATED")
			}
		}
		fmt.Fprintln(b)
	} else {
		if err := iter.Error(); err != nil {
			fmt.Fprintf(b, "err=%v\n", err)
		} else {
			fmt.Fprintf(b, ".%s\n", validityStateStr)
		}
	}
}

func formatASCIIKey(b []byte) string {
	if bytes.IndexFunc(b, func(r rune) bool { return r < 'A' || r > 'z' }) != -1 {
		// This key is not just ASCII letters. Quote it.
		return fmt.Sprintf("%q", b)
	}
	return string(b)
}

func formatASCIIValue(b []byte) string {
	if len(b) > 1<<10 {
		return fmt.Sprintf("[LARGE VALUE len=%d]", len(b))
	}
	if bytes.IndexFunc(b, func(r rune) bool { return r < '!' || r > 'z' }) != -1 {
		// This key is not just legible ASCII characters. Quote it.
		return fmt.Sprintf("%q", b)
	}
	return string(b)
}

func writeRangeKeys(b io.Writer, iter *Iterator) {
	rangeKeys := iter.RangeKeys()
	for j := 0; j < len(rangeKeys); j++ {
		if j > 0 {
			fmt.Fprint(b, ",")
		}
		fmt.Fprintf(b, " %s=%s", rangeKeys[j].Suffix, formatASCIIValue(rangeKeys[j].Value))
	}
}

func parseValue(s string) []byte {
	if strings.HasPrefix(s, "<rand-bytes=") {
		s = strings.TrimPrefix(s, "<rand-bytes=")
		s = strings.TrimSuffix(s, ">")
		n, err := strconv.Atoi(s)
		if err != nil {
			panic(err)
		}
		b := make([]byte, n)
		rnd := rand.New(rand.NewPCG(0, uint64(n)))
		for i := range b {
			b[i] = byte(rnd.Uint32())
		}
		return b
	}
	return []byte(s)
}

func splitFields(line string, n int) ([]string, error) {
	return splitFieldsRange(line, n, n)
}

func splitFieldsRange(line string, minmum, maximum int) ([]string, error) {
	fields := strings.Fields(line)
	if len(fields) < minmum {
		return nil, errors.Errorf("require at least %d fields, got %d", minmum, len(fields))
	}
	if len(fields) > maximum {
		fields[maximum-1] = strings.Join(fields[maximum-1:], " ")
		fields = fields[:maximum]
	}
	for i := range fields {
		if fields[i] == `<nil>` {
			fields[i] = ""
		}
	}
	return fields, nil
}

func runBatchDefineCmd(d *datadriven.TestData, b *Batch) error {
	for _, line := range crstrings.Lines(d.Input) {
		i := strings.IndexFunc(line, unicode.IsSpace)
		cmd := line
		if i > 0 {
			cmd = line[:i]
		} else if cmd == "" {
			continue
		}

		var parts []string
		var err error
		switch cmd {
		case "set":
			parts, err = splitFields(line, 3)
			if err != nil {
				return err
			}
			err = b.Set([]byte(parts[1]), parseValue(parts[2]), nil)

		case "set-multiple":
			parts, err = splitFields(line, 3)
			if err != nil {
				return err
			}
			n, err := strconv.ParseUint(parts[1], 10, 32)
			if err != nil {
				return err
			}
			for i := uint64(0); i < n; i++ {
				key := fmt.Sprintf("%s-%05d", parts[2], i)
				val := fmt.Sprintf("val-%05d", i)
				if err := b.Set([]byte(key), []byte(val), nil); err != nil {
					return err
				}
			}

		case "del":
			parts, err = splitFields(line, 2)
			if err != nil {
				return err
			}
			err = b.Delete([]byte(parts[1]), nil)
		case "del-sized":
			parts, err = splitFields(line, 3)
			if err != nil {
				return err
			}
			var valSize uint64
			valSize, err = strconv.ParseUint(parts[2], 10, 32)
			if err != nil {
				return err
			}
			err = b.DeleteSized([]byte(parts[1]), uint32(valSize), nil)
		case "singledel":
			parts, err = splitFields(line, 2)
			if err != nil {
				return err
			}
			err = b.SingleDelete([]byte(parts[1]), nil)
		case "del-range":
			parts, err = splitFields(line, 3)
			if err != nil {
				return err
			}
			err = b.DeleteRange([]byte(parts[1]), []byte(parts[2]), nil)
		case "merge":
			parts, err = splitFields(line, 3)
			if err != nil {
				return err
			}
			err = b.Merge([]byte(parts[1]), parseValue(parts[2]), nil)
		case "range-key-set":
			parts, err = splitFieldsRange(line, 4, 5)
			if err != nil {
				return err
			}
			var val []byte
			if len(parts) == 5 {
				val = parseValue(parts[4])
			}
			err = b.RangeKeySet(
				[]byte(parts[1]),
				[]byte(parts[2]),
				[]byte(parts[3]),
				val,
				nil)
		case "range-key-unset":
			parts, err = splitFields(line, 4)
			if err != nil {
				return err
			}
			err = b.RangeKeyUnset(
				[]byte(parts[1]),
				[]byte(parts[2]),
				[]byte(parts[3]),
				nil)
		case "range-key-del":
			parts, err = splitFields(line, 3)
			if err != nil {
				return err
			}
			err = b.RangeKeyDelete(
				[]byte(parts[1]),
				[]byte(parts[2]),
				nil)
		default:
			return errors.Errorf("unknown op: %s", cmd)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func runBuildRemoteCmd(td *datadriven.TestData, d *DB, storage remote.Storage) error {
	if len(td.CmdArgs) < 1 {
		return errors.New("build <path>: argument missing")
	}
	path := td.CmdArgs[0].String()

	// Use TableFormatMax here and downgrade after, if necessary. This ensures
	// that all fields are set.
	writeOpts := d.opts.MakeWriterOptions(0 /* level */, d.TableFormat())
	if rand.IntN(4) == 0 {
		// If block size is not specified (in which case these fields will be
		// overridden by ParseWriterOptions), force two-level indexes some of the
		// time.
		writeOpts.BlockSize = 5
		writeOpts.IndexBlockSize = 5
	}

	// Now parse the arguments again against the real options.
	if err := sstable.ParseWriterOptions(&writeOpts, td.CmdArgs...); err != nil {
		return err
	}

	f, err := storage.CreateObject(path)
	if err != nil {
		return err
	}
	w := sstable.NewWriter(objstorageprovider.NewRemoteWritable(f), writeOpts)
	if err := sstable.ParseTestSST(w.Raw(), td.Input, nil /* bv */); err != nil {
		return err
	}

	return w.Close()
}

type dataDrivenCmdOptions struct {
	blobValues        *blobtest.Values
	defaultWriterOpts sstable.WriterOptions
}

func withBlobValues(bv *blobtest.Values) func(*dataDrivenCmdOptions) {
	return func(o *dataDrivenCmdOptions) { o.blobValues = bv }
}

func withDefaultWriterOpts(defaultWriterOpts sstable.WriterOptions) func(*dataDrivenCmdOptions) {
	return func(o *dataDrivenCmdOptions) { o.defaultWriterOpts = defaultWriterOpts }
}

func combineDataDrivenOpts(opts ...func(*dataDrivenCmdOptions)) dataDrivenCmdOptions {
	combined := dataDrivenCmdOptions{}
	for _, opt := range opts {
		opt(&combined)
	}
	return combined
}

// TODO(radu): remove this in favor of runBuildSSTCmd.
func runBuildCmd(
	td *datadriven.TestData, d *DB, fs vfs.FS, opts ...func(*dataDrivenCmdOptions),
) error {
	ddOpts := combineDataDrivenOpts(opts...)

	b := newIndexedBatch(nil, d.opts.Comparer)
	if err := runBatchDefineCmd(td, b); err != nil {
		return err
	}

	if len(td.CmdArgs) < 1 {
		return errors.New("build <path>: argument missing")
	}
	path := td.CmdArgs[0].String()

	writeOpts := d.opts.MakeWriterOptions(0 /* level */, d.TableFormat())
	if err := sstable.ParseWriterOptions(&writeOpts, td.CmdArgs[1:]...); err != nil {
		return err
	}
	f, err := fs.Create(path, vfs.WriteCategoryUnspecified)
	if err != nil {
		return err
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), writeOpts)
	iter := b.newInternalIter(nil)
	for kv := iter.First(); kv != nil; kv = iter.Next() {
		tmp := kv.K
		tmp.SetSeqNum(0)

		v := kv.InPlaceValue()
		// If the value looks like it's a debug blob handle, parse it and add it
		// to the sstable as a blob handle.
		if blobtest.IsBlobHandle(string(v)) {
			if ddOpts.blobValues == nil {
				return errors.Newf("test not set up to support blob handles")
			}
			handle, _, err := ddOpts.blobValues.ParseInlineHandle(string(v))
			if err != nil {
				return err
			}
			if err := w.Raw().AddWithBlobHandle(tmp, handle, base.ShortAttribute(0), false); err != nil {
				return err
			}
			continue
		}
		// Otherwise add it as an ordinary value.
		if err := w.Raw().Add(tmp, v, false); err != nil {
			return err
		}
	}
	if err := iter.Close(); err != nil {
		return err
	}

	if rdi := b.newRangeDelIter(nil, math.MaxUint64); rdi != nil {
		s, err := rdi.First()
		for ; s != nil && err == nil; s, err = rdi.Next() {
			err = w.DeleteRange(s.Start, s.End)
			if err != nil {
				return err
			}
		}
		if err != nil {
			return err
		}
	}

	if rki := b.newRangeKeyIter(nil, math.MaxUint64); rki != nil {
		s, err := rki.First()
		for ; s != nil; s, err = rki.Next() {
			for _, k := range s.Keys {
				var err error
				switch k.Kind() {
				case base.InternalKeyKindRangeKeySet:
					err = w.RangeKeySet(s.Start, s.End, k.Suffix, k.Value)
				case base.InternalKeyKindRangeKeyUnset:
					err = w.RangeKeyUnset(s.Start, s.End, k.Suffix)
				case base.InternalKeyKindRangeKeyDelete:
					err = w.RangeKeyDelete(s.Start, s.End)
				default:
					panic("not a range key")
				}
				if err != nil {
					return err
				}
			}
		}
		if err != nil {
			return err
		}
	}

	return w.Close()
}

func runBuildSSTCmd(
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

func runCompactCmdAsync(
	td *datadriven.TestData, d *DB, cancellable bool,
) (compactFunc func() error, cancelFunc context.CancelFunc, err error) {
	if len(td.CmdArgs) == 0 {
		return nil, nil, errors.Errorf("%s expects at least one argument", td.Cmd)
	}
	parts := strings.Split(td.CmdArgs[0].Key, "-")
	if len(parts) != 2 {
		return nil, nil, errors.Errorf("expected <begin>-<end>: %s", td.Input)
	}
	parallelize := td.HasArg("parallel")
	ctx := context.Background()
	if cancellable {
		ctx, cancelFunc = context.WithCancel(ctx)
	}
	if len(td.CmdArgs) >= 2 && strings.HasPrefix(td.CmdArgs[1].Key, "L") {
		levelString := td.CmdArgs[1].String()
		iStart := base.MakeInternalKey([]byte(parts[0]), base.SeqNumMax, InternalKeyKindMax)
		iEnd := base.MakeInternalKey([]byte(parts[1]), 0, 0)
		if levelString[0] != 'L' {
			if cancelFunc != nil {
				cancelFunc()
			}
			return nil, nil, errors.Errorf("expected L<n>: %s", levelString)
		}
		level, err := strconv.Atoi(levelString[1:])
		if err != nil {
			if cancelFunc != nil {
				cancelFunc()
			}
			return nil, nil, err
		}
		return func() error {
			return d.manualCompact(ctx, iStart.UserKey, iEnd.UserKey, level, parallelize)
		}, cancelFunc, nil
	}
	return func() error {
		return d.Compact(ctx, []byte(parts[0]), []byte(parts[1]), parallelize)
	}, cancelFunc, nil
}

func runCompactCmd(td *datadriven.TestData, d *DB) error {
	compactFunc, _, err := runCompactCmdAsync(td, d, false)
	if err != nil {
		return err
	}
	return compactFunc()
}

// runDBDefineCmd prepares a database state, returning the opened
// database with the initialized state.
//
// The command accepts input describing memtables and sstables to
// construct. Each new table is indicated by a line containing the
// level of the next table to build (eg, "L6"), or "mem" to build
// a memtable. Each subsequent line contains a new key-value pair.
//
// Point keys and range deletions should be encoded as the
// InternalKey's string representation, as understood by
// ParseInternalKey, followed a colon and the corresponding value.
//
//	b.SET.50:foo
//	c.DEL.20
//
// Range keys may be encoded by prefixing the line with `rangekey:`,
// followed by the keyspan.Span string representation, as understood
// by keyspan.ParseSpan.
//
//	rangekey:b-d:{(#5,RANGEKEYSET,@2,foo)}
//
// # Mechanics
//
// runDBDefineCmd works by simulating a flush for every file written.
// Keys are written to a memtable. When a file is complete, the table
// is flushed to physical files through manually invoking runCompaction.
// The resulting version edit is then manipulated to write the files
// to the indicated level.
//
// Because of it's low-level manipulation, runDBDefineCmd does allow the
// creation of invalid database states. If opts.DebugCheck is set, the
// level checker should detect the invalid state.
func runDBDefineCmd(td *datadriven.TestData, opts *Options) (*DB, error) {
	if opts == nil {
		opts = &Options{}
	}
	opts.EnsureDefaults()
	opts.FS = vfs.NewMem()
	return runDBDefineCmdReuseFS(td, opts)
}

// runDBDefineCmdReuseFS is like runDBDefineCmd, but does not set opts.FS, expecting
// the caller to have set an appropriate FS already.
func runDBDefineCmdReuseFS(td *datadriven.TestData, opts *Options) (*DB, error) {
	// Some tests expect that opts is an in-out parameter in that the changes to
	// opts made here are used later by the caller. But the
	// ConcurrencyLimitScheduler cannot be reused after the DB is closed. So we
	// replace it here.
	scheduler, ok := opts.Experimental.CompactionScheduler.(*ConcurrencyLimitScheduler)
	if ok && scheduler.isUnregisteredForTesting() {
		opts.Experimental.CompactionScheduler =
			NewConcurrencyLimitSchedulerWithNoPeriodicGrantingForTest()
	}
	opts.EnsureDefaults()
	if err := parseDBOptionsArgs(opts, td.CmdArgs); err != nil {
		return nil, err
	}

	var snapshots []base.SeqNum
	var levelMaxBytes map[int]int64
	for _, arg := range td.CmdArgs {
		switch arg.Key {
		case "snapshots":
			snapshots = make([]base.SeqNum, len(arg.Vals))
			for i := range arg.Vals {
				snapshots[i] = base.ParseSeqNum(arg.Vals[i])
				if i > 0 && snapshots[i] < snapshots[i-1] {
					return nil, errors.New("Snapshots must be in ascending order")
				}
			}
		case "level-max-bytes":
			levelMaxBytes = map[int]int64{}
			for i := range arg.Vals {
				j := strings.Index(arg.Vals[i], ":")
				levelStr := strings.TrimSpace(arg.Vals[i][:j])
				level, err := strconv.Atoi(levelStr[1:])
				if err != nil {
					return nil, err
				}
				size, err := strconv.ParseInt(strings.TrimSpace(arg.Vals[i][j+1:]), 10, 64)
				if err != nil {
					return nil, err
				}
				levelMaxBytes[level] = size
			}
		}
	}

	d, err := Open("", opts)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for i := range snapshots {
		s := &Snapshot{db: d}
		s.seqNum = snapshots[i]
		d.mu.snapshots.pushBack(s)
	}
	// Set the level max bytes only right before we exit; the body of this
	// function expects it to be unset.
	defer func() {
		for l, maxBytes := range levelMaxBytes {
			d.mu.versions.picker.(*compactionPickerByScore).levelMaxBytes[l] = maxBytes
		}
	}()
	if td.Input == "" {
		// Empty LSM.
		return d, nil
	}
	d.mu.versions.dynamicBaseLevel = false

	// We use a custom compact.ValueSeparation implementation that wraps
	// preserveBlobReferences. It parses values containing human-readable blob
	// references (as understood by blobtest.Values) and preserves the
	// references.  It also accumulates all the handles so that after writing
	// all the tables, we can construct all the referenced blob files and add
	// them to the final version edit.
	valueSeparator := &defineDBValueSeparator{
		pbr:   &preserveBlobReferences{},
		metas: make(map[base.DiskFileNum]*manifest.BlobFileMetadata),
	}

	var mem *memTable
	var start, end *base.InternalKey
	var blobDepth manifest.BlobReferenceDepth
	ve := &versionEdit{}
	level := -1

	maybeFlush := func() error {
		if level < 0 {
			return nil
		}

		toFlush := flushableList{{
			flushable: mem,
			flushed:   make(chan struct{}),
		}}
		c, err := newFlush(d.opts, d.mu.versions.currentVersion(), d.mu.versions.l0Organizer,
			d.mu.versions.picker.getBaseLevel(), toFlush, time.Now(), d.determineCompactionValueSeparation)
		if err != nil {
			return err
		}
		c.getValueSeparation = func(JobID, *compaction, sstable.TableFormat) compact.ValueSeparation {
			return valueSeparator
		}
		// NB: define allows the test to exactly specify which keys go
		// into which sstables. If the test has a small target file
		// size to test grandparent limits, etc, the maxOutputFileSize
		// can cause splitting /within/ the bounds specified to the
		// test. Ignore the target size here, and split only according
		// to the user-defined boundaries.
		c.maxOutputFileSize = math.MaxUint64

		newVE, _, err := d.runCompaction(0, c)
		if err != nil {
			return err
		}
		largestSeqNum := d.mu.versions.logSeqNum.Load()
		ve.NewBlobFiles = append(ve.NewBlobFiles, newVE.NewBlobFiles...)
		for _, f := range newVE.NewTables {
			if start != nil && end != nil {
				f.Meta.PointKeyBounds.SetInternalKeyBounds(*start, *end)
				f.Meta.ExtendPointKeyBounds(DefaultComparer.Compare, *start, *end)
			} else if start != nil {
				f.Meta.PointKeyBounds.SetSmallest(*start)
				f.Meta.ExtendPointKeyBounds(DefaultComparer.Compare, *start, *start)
			} else if end != nil {
				f.Meta.PointKeyBounds.SetLargest(*end)
				f.Meta.ExtendPointKeyBounds(DefaultComparer.Compare, *end, *end)
			}
			if blobDepth > 0 {
				f.Meta.BlobReferenceDepth = blobDepth
			}
			if largestSeqNum <= f.Meta.LargestSeqNum {
				largestSeqNum = f.Meta.LargestSeqNum + 1
			}
			ve.NewTables = append(ve.NewTables, manifest.NewTableEntry{
				Level: level,
				Meta:  f.Meta,
			})
		}
		// The committed keys were never written to the WAL, so neither
		// the logSeqNum nor the commit pipeline's visibleSeqNum have
		// been ratcheted. Manually ratchet them to the largest sequence
		// number committed to ensure iterators opened from the database
		// correctly observe the committed keys.
		if d.mu.versions.logSeqNum.Load() < largestSeqNum {
			d.mu.versions.logSeqNum.Store(largestSeqNum)
		}
		if d.mu.versions.visibleSeqNum.Load() < largestSeqNum {
			d.mu.versions.visibleSeqNum.Store(largestSeqNum)
		}
		level = -1
		return nil
	}

	// Example, a-c.
	parseMeta := func(s string) (*manifest.TableMetadata, error) {
		parts := strings.Split(s, "-")
		if len(parts) != 2 {
			return nil, errors.Errorf("malformed table spec: %s", s)
		}
		m := (&manifest.TableMetadata{}).ExtendPointKeyBounds(
			opts.Comparer.Compare,
			InternalKey{UserKey: []byte(parts[0])},
			InternalKey{UserKey: []byte(parts[1])},
		)
		m.InitPhysicalBacking()
		return m, nil
	}

	// Example, compact: a-c.
	parseCompaction := func(outputLevel int, s string) (*compaction, error) {
		m, err := parseMeta(s)
		if err != nil {
			return nil, err
		}
		c := &compaction{
			inputs:   []compactionLevel{{}, {level: outputLevel}},
			smallest: m.Smallest(),
			largest:  m.Largest(),
		}
		c.startLevel, c.outputLevel = &c.inputs[0], &c.inputs[1]
		return c, nil
	}

	for _, line := range crstrings.Lines(td.Input) {
		fields := strings.Fields(line)
		if len(fields) > 0 {
			switch fields[0] {
			case "mem":
				if err := maybeFlush(); err != nil {
					return nil, err
				}
				// Add a memtable layer.
				if !d.mu.mem.mutable.empty() {
					d.mu.mem.mutable = newMemTable(memTableOptions{Options: d.opts})
					entry := d.newFlushableEntry(d.mu.mem.mutable, 0, 0)
					entry.readerRefs.Add(1)
					d.mu.mem.queue = append(d.mu.mem.queue, entry)
					d.updateReadStateLocked(nil)
				}
				mem = d.mu.mem.mutable
				start, end = nil, nil
				fields = fields[1:]
				if len(fields) != 0 {
					return nil, errors.Errorf("unexpected excess arguments: %s", strings.Join(fields, " "))
				}
				continue
			case "L0", "L1", "L2", "L3", "L4", "L5", "L6":
				if err := maybeFlush(); err != nil {
					return nil, err
				}
				var err error
				if level, err = strconv.Atoi(fields[0][1:]); err != nil {
					return nil, err
				}
				fields = fields[1:]
				start, end = nil, nil
				blobDepth = 0
				boundFields := 0
				for _, field := range fields {
					toBreak := false
					switch {
					case strings.HasPrefix(field, "start="):
						ikey := base.ParseInternalKey(strings.TrimPrefix(field, "start="))
						start = &ikey
						boundFields++
					case strings.HasPrefix(field, "end="):
						ikey := base.ParseInternalKey(strings.TrimPrefix(field, "end="))
						end = &ikey
						boundFields++
					case strings.HasPrefix(field, "blob-depth="):
						v, err := strconv.Atoi(strings.TrimPrefix(field, "blob-depth="))
						if err != nil {
							return nil, err
						}
						blobDepth = manifest.BlobReferenceDepth(v)
						boundFields++
					default:
						toBreak = true
					}
					if toBreak {
						break
					}
				}
				fields = fields[boundFields:]
				if len(fields) != 0 {
					return nil, errors.Errorf("unexpected excess arguments: %s", strings.Join(fields, " "))
				}
				mem = newMemTable(memTableOptions{Options: d.opts})
				continue
			}
		}

		p := strparse.MakeParser(":{}", line)
		for !p.Done() {
			field := p.Next()
			switch field {
			case "compact":
				// Define in-progress compactions.
				p.Expect(":")
				c, err := parseCompaction(level, p.Remaining())
				if err != nil {
					return nil, err
				}
				d.mu.compact.inProgress[c] = struct{}{}
				continue
			case "rangekey":
				p.Expect(":")
				span := keyspan.ParseSpan(p.Remaining())
				err := rangekey.Encode(span, func(k base.InternalKey, v []byte) error {
					return mem.set(k, v)
				})
				if err != nil {
					return nil, err
				}
				continue
			default:
				key := base.ParseInternalKey(field)
				p.Expect(":")
				var value []byte
				if key.Kind() != base.InternalKeyKindDelete && key.Kind() != base.InternalKeyKindSingleDelete {
					valueStr := p.Next()
					// If the value looks like a blob reference, read until the closing brace.
					if valueStr == "blob" && p.Peek() == "{" {
						tok := p.Next()
						valueStr += " " + tok
						for !p.Done() && tok != "}" {
							tok = p.Next()
							valueStr += " " + tok
						}
					}

					value = []byte(valueStr)
					var randBytes int
					if n, err := fmt.Sscanf(valueStr, "<rand-bytes=%d>", &randBytes); err == nil && n == 1 {
						value = make([]byte, randBytes)
						rnd := rand.New(rand.NewPCG(0, uint64(key.SeqNum())))
						for j := range value {
							value[j] = byte(rnd.Uint32())
						}
					}
				}
				if err := mem.set(key, value); err != nil {
					return nil, err
				}
			}
		}
	}

	if err := maybeFlush(); err != nil {
		return nil, err
	}

	if len(ve.NewTables) > 0 {
		// Collect any blob files created.
		fileStats, err := valueSeparator.bv.WriteFiles(func(fileNum base.DiskFileNum) (objstorage.Writable, error) {
			writable, _, err := d.objProvider.Create(context.Background(), base.FileTypeBlob, fileNum, objstorage.CreateOptions{})
			return writable, err
		}, d.opts.MakeBlobWriterOptions(0))
		if err != nil {
			return nil, err
		}
		for f, stats := range fileStats {
			valueSeparator.metas[f].Size = stats.FileLen
			valueSeparator.metas[f].ValueSize = stats.UncompressedValueBytes
		}

		ve.NewBlobFiles = slices.Collect(maps.Values(valueSeparator.metas))

		jobID := d.newJobIDLocked()
		err = d.mu.versions.UpdateVersionLocked(func() (versionUpdate, error) {
			return versionUpdate{
				VE:                      ve,
				JobID:                   jobID,
				Metrics:                 newFileMetrics(ve.NewTables),
				InProgressCompactionsFn: func() []compactionInfo { return nil },
			}, nil
		})
		if err != nil {
			return nil, err
		}
		d.updateReadStateLocked(nil)
		d.updateTableStatsLocked(ve.NewTables)
	}

	return d, nil
}

func runTableStatsCmd(td *datadriven.TestData, d *DB) string {
	// In the original test file, parse the command args first
	var forceTombstoneDensityRatio float64 = -1.0
	for _, arg := range td.CmdArgs {
		if arg.Key == "force-tombstone-density-ratio" && len(arg.Vals) > 0 {
			ratio, err := strconv.ParseFloat(arg.Vals[0], 64)
			if err != nil {
				return fmt.Sprintf("error parsing force-tombstone-density-ratio: %v", err)
			}
			forceTombstoneDensityRatio = ratio
		}
	}

	u, err := strconv.ParseUint(strings.TrimSpace(td.Input), 10, 64)
	if err != nil {
		return err.Error()
	}
	tableNum := base.TableNum(u)

	d.mu.Lock()
	defer d.mu.Unlock()
	v := d.mu.versions.currentVersion()

	for _, levelMetadata := range v.Levels {
		for f := range levelMetadata.All() {
			if f.TableNum != tableNum {
				continue
			}

			if !f.StatsValid() {
				d.waitTableStats()
			}

			var b bytes.Buffer
			fmt.Fprintf(&b, "num-entries: %d\n", f.Stats.NumEntries)
			fmt.Fprintf(&b, "num-deletions: %d\n", f.Stats.NumDeletions)
			fmt.Fprintf(&b, "num-range-key-sets: %d\n", f.Stats.NumRangeKeySets)
			fmt.Fprintf(&b, "point-deletions-bytes-estimate: %d\n", f.Stats.PointDeletionsBytesEstimate)
			fmt.Fprintf(&b, "range-deletions-bytes-estimate: %d\n", f.Stats.RangeDeletionsBytesEstimate)

			// If requested, override the tombstone density ratio for testing
			if forceTombstoneDensityRatio >= 0 {
				f.Stats.TombstoneDenseBlocksRatio = forceTombstoneDensityRatio
			}
			// Only include the tombstone-dense-blocks-ratio if it was forced or is non-zero
			if forceTombstoneDensityRatio >= 0 || f.Stats.TombstoneDenseBlocksRatio > 0 {
				fmt.Fprintf(&b, "tombstone-dense-blocks-ratio: %0.1f\n", f.Stats.TombstoneDenseBlocksRatio)
			}

			return b.String()
		}
	}
	return "(not found)"
}

func runTableFileSizesCmd(td *datadriven.TestData, d *DB) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return runVersionFileSizes(d.mu.versions.currentVersion())
}

func runVersionFileSizes(v *version) string {
	var buf bytes.Buffer
	for l, levelMetadata := range v.Levels {
		if levelMetadata.Empty() {
			continue
		}
		fmt.Fprintf(&buf, "L%d:\n", l)
		for f := range levelMetadata.All() {
			fmt.Fprintf(&buf, "  %s: %d bytes (%s)", f, f.Size, humanize.Bytes.Uint64(f.Size))
			if f.IsCompacting() {
				fmt.Fprintf(&buf, " (IsCompacting)")
			}
			fmt.Fprintln(&buf)
		}
	}
	return buf.String()
}

// Prints some metadata about some sstable which is currently in the latest
// version.
func runMetadataCommand(t *testing.T, td *datadriven.TestData, d *DB) string {
	var table int
	td.ScanArgs(t, "file", &table)
	var m *manifest.TableMetadata
	d.mu.Lock()
	currVersion := d.mu.versions.currentVersion()
	for _, level := range currVersion.Levels {
		for f := range level.All() {
			if f.TableNum == base.TableNum(table) {
				m = f
				break
			}
		}
	}
	d.mu.Unlock()
	var buf bytes.Buffer
	// Add more metadata as needed.
	fmt.Fprintf(&buf, "size: %d\n", m.Size)
	return buf.String()
}

func runSSTablePropertiesCmd(t *testing.T, td *datadriven.TestData, d *DB) string {
	var file int
	td.ScanArgs(t, "file", &file)

	// See if we can grab the TableMetadata associated with the file. This is
	// needed to easily construct virtual sstable properties.
	var m *manifest.TableMetadata
	d.mu.Lock()
	currVersion := d.mu.versions.currentVersion()
	for _, level := range currVersion.Levels {
		for f := range level.All() {
			if f.TableNum == base.TableNum(uint64(file)) {
				m = f
				break
			}
		}
	}
	d.mu.Unlock()

	// Note that m can be nil here if the sstable exists in the file system, but
	// not in the lsm. If m is nil just assume that file is not virtual.

	backingFileNum := base.DiskFileNum(file)
	if m != nil {
		backingFileNum = m.TableBacking.DiskFileNum
	}
	fileName := base.MakeFilename(base.FileTypeTable, backingFileNum)
	f, err := d.opts.FS.Open(fileName)
	if err != nil {
		return err.Error()
	}
	readable, err := sstable.NewSimpleReadable(f)
	if err != nil {
		return err.Error()
	}
	readerOpts := d.opts.MakeReaderOptions()
	// TODO(bananabrick): cacheOpts is used to set the file number on a Reader,
	// and virtual sstables expect this file number to be set. Split out the
	// opts into fileNum opts, and cache opts.
	readerOpts.CacheOpts = sstableinternal.CacheOptions{
		CacheHandle: d.cacheHandle,
		FileNum:     backingFileNum,
	}
	r, err := sstable.NewReader(context.Background(), readable, readerOpts)
	if err != nil {
		return errors.CombineErrors(err, readable.Close()).Error()
	}
	defer r.Close()

	loadedProps, err := r.ReadPropertiesBlock(context.Background(), nil /* buffer pool */)
	if err != nil {
		return err.Error()
	}
	props := loadedProps.String()
	env := sstable.ReadEnv{}
	if m != nil && m.Virtual {
		env.Virtual = m.VirtualParams
		scaledProps := loadedProps.GetScaledProperties(m.TableBacking.Size, m.Size)
		props = scaledProps.String()
	}
	if len(td.Input) == 0 {
		return props
	}
	var buf bytes.Buffer
	propsSlice := strings.Split(props, "\n")
	for _, requestedProp := range strings.Split(td.Input, "\n") {
		fmt.Fprintf(&buf, "%s:\n", requestedProp)
		for _, prop := range propsSlice {
			if strings.Contains(prop, requestedProp) {
				fmt.Fprintf(&buf, "  %s\n", prop)
			}
		}
	}
	return buf.String()
}

func runLayoutCmd(t *testing.T, td *datadriven.TestData, d *DB) string {
	var filename string
	td.ScanArgs(t, "filename", &filename)
	f, err := d.opts.FS.Open(filename)
	if err != nil {
		return err.Error()
	}
	readable, err := sstable.NewSimpleReadable(f)
	if err != nil {
		return err.Error()
	}
	r, err := sstable.NewReader(context.Background(), readable, d.opts.MakeReaderOptions())
	if err != nil {
		return errors.CombineErrors(err, readable.Close()).Error()
	}
	defer r.Close()
	l, err := r.Layout()
	if err != nil {
		return err.Error()
	}
	return l.Describe(td.HasArg("verbose"), r, nil)
}

func runPopulateCmd(t *testing.T, td *datadriven.TestData, b *Batch) {
	var maxKeyLength, valLength int
	var timestamps []int
	td.ScanArgs(t, "keylen", &maxKeyLength)
	td.MaybeScanArgs(t, "timestamps", &timestamps)
	td.MaybeScanArgs(t, "vallen", &valLength)
	// Default to writing timestamps @1.
	if len(timestamps) == 0 {
		timestamps = append(timestamps, 1)
	}

	ks := testkeys.Alpha(maxKeyLength)
	buf := make([]byte, ks.MaxLen()+testkeys.MaxSuffixLen)
	vbuf := make([]byte, valLength)
	for i := int64(0); i < ks.Count(); i++ {
		for _, ts := range timestamps {
			n := testkeys.WriteKeyAt(buf, ks, i, int64(ts))

			// Default to using the key as the value, but if the user provided
			// the vallen argument, generate a random value of the specified
			// length.
			value := buf[:n]
			if valLength > 0 {
				_, err := crand.Read(vbuf)
				require.NoError(t, err)
				value = vbuf
			}
			require.NoError(t, b.Set(buf[:n], value, nil))
		}
	}
}

// waitTableStatsInitialLoad waits until the statistics for all files that
// existed during Open have been loaded; used in tests.
// The d.mu mutex must be locked while calling this method.
//
// TODO(jackson): Consolidate waitTableStatsInitialLoad and waitTableStats. It
// seems to be possible for loadedInitial to remain indefinitely false in some
// tests that use waitTableStats today.
func (d *DB) waitTableStatsInitialLoad() {
	for !d.mu.tableStats.loadedInitial {
		d.mu.tableStats.cond.Wait()
	}
}

// waitTableStats waits until all new files' statistics have been loaded. It's
// used in tests. The d.mu mutex must be locked while calling this method.
func (d *DB) waitTableStats() {
	for d.mu.tableStats.loading || len(d.mu.tableStats.pending) > 0 {
		d.mu.tableStats.cond.Wait()
	}
}

func runExciseCmd(td *datadriven.TestData, d *DB) error {
	if len(td.CmdArgs) != 2 {
		return errors.New("excise expects two arguments: <start-key> <end-key>")
	}
	exciseSpan := KeyRange{
		Start: []byte(td.CmdArgs[0].String()),
		End:   []byte(td.CmdArgs[1].String()),
	}
	return d.Excise(context.Background(), exciseSpan)
}

func runExciseDryRunCmd(td *datadriven.TestData, d *DB) (*versionEdit, error) {
	ve := &versionEdit{
		DeletedTables: map[manifest.DeletedTableEntry]*manifest.TableMetadata{},
	}
	var exciseSpan KeyRange
	if len(td.CmdArgs) != 2 {
		panic("insufficient args for excise-dryrun command")
	}
	exciseSpan.Start = []byte(td.CmdArgs[0].Key)
	exciseSpan.End = []byte(td.CmdArgs[1].Key)

	d.mu.Lock()
	d.mu.versions.logLock()
	defer func() {
		d.mu.Lock()
		d.mu.versions.logUnlock()
		d.mu.Unlock()
	}()
	d.mu.Unlock()
	current := d.mu.versions.currentVersion()

	exciseBounds := exciseSpan.UserKeyBounds()
	for l, ls := range current.AllLevelsAndSublevels() {
		iter := ls.Iter()
		for m := iter.SeekGE(d.cmp, exciseSpan.Start); m != nil && d.cmp(m.Smallest().UserKey, exciseSpan.End) < 0; m = iter.Next() {
			leftTable, rightTable, err := d.exciseTable(context.Background(), exciseBounds, m, l.Level(), tightExciseBounds)
			if err != nil {
				return nil, errors.Errorf("error when excising %s: %s", m.TableNum, err.Error())
			}
			applyExciseToVersionEdit(ve, m, leftTable, rightTable, l.Level())
		}
	}

	return ve, nil
}

func runIngestAndExciseCmd(td *datadriven.TestData, d *DB) error {
	var exciseSpan KeyRange
	paths := make([]string, 0, len(td.CmdArgs))
	for i, arg := range td.CmdArgs {
		switch td.CmdArgs[i].Key {
		case "excise":
			if len(td.CmdArgs[i].Vals) != 1 {
				return errors.New("expected 2 values for excise separated by -, eg. ingest-and-excise foo1 excise=\"start-end\"")
			}
			fields := strings.Split(td.CmdArgs[i].Vals[0], "-")
			if len(fields) != 2 {
				return errors.New("expected 2 values for excise separated by -, eg. ingest-and-excise foo1 excise=\"start-end\"")
			}
			exciseSpan.Start = []byte(fields[0])
			exciseSpan.End = []byte(fields[1])
		case "no-wait":
			// Handled by callers.
		default:
			paths = append(paths, arg.String())
		}
	}

	if _, err := d.IngestAndExcise(context.Background(), paths, nil /* shared */, nil /* external */, exciseSpan); err != nil {
		return err
	}
	return nil
}

func runIngestCmd(td *datadriven.TestData, d *DB, fs vfs.FS) error {
	paths := make([]string, 0, len(td.CmdArgs))
	for _, arg := range td.CmdArgs {
		if arg.Key == "no-wait" {
			// Handled by callers.
			continue
		}
		paths = append(paths, arg.String())
	}

	if err := d.Ingest(context.Background(), paths); err != nil {
		return err
	}
	return nil
}

func runIngestExternalCmd(
	t testing.TB, td *datadriven.TestData, d *DB, st remote.Storage, locator string,
) error {
	var external []ExternalFile
	for _, line := range strings.Split(td.Input, "\n") {
		usageErr := func(info interface{}) {
			t.Helper()
			td.Fatalf(t, "error parsing %q: %v; "+
				"usage: obj bounds=(smallest,largest) [size=x] [synthetic-prefix=prefix] [synthetic-suffix=suffix] [no-point-keys] [has-range-keys]",
				line, info,
			)
		}
		objName, args, err := datadriven.ParseLine(line)
		if err != nil {
			usageErr(err)
		}
		sz, err := st.Size(objName)
		if err != nil {
			// Tests can attach files that don't exist. Mock a size.
			sz = 1024
		}
		ef := ExternalFile{
			Locator:     remote.Locator(locator),
			ObjName:     objName,
			HasPointKey: true,
			Size:        uint64(sz),
		}
		for _, arg := range args {
			nArgs := func(n int) {
				if len(arg.Vals) != n {
					usageErr(fmt.Sprintf("%s must have %d arguments", arg.Key, n))
				}
			}
			switch arg.Key {
			case "bounds":
				nArgs(2)
				ef.StartKey = []byte(arg.Vals[0])
				ef.EndKey = []byte(arg.Vals[1])
			case "bounds-are-inclusive":
				nArgs(1)
				b, err := strconv.ParseBool(arg.Vals[0])
				if err != nil {
					usageErr(fmt.Sprintf("%s should have boolean argument: %v",
						arg.Key, err))
				}
				ef.EndKeyIsInclusive = b
			case "size":
				nArgs(1)
				arg.Scan(t, 0, &ef.Size)

			case "synthetic-prefix":
				nArgs(1)
				ef.SyntheticPrefix = []byte(arg.Vals[0])

			case "synthetic-suffix":
				nArgs(1)
				ef.SyntheticSuffix = []byte(arg.Vals[0])

			case "no-point-keys":
				ef.HasPointKey = false

			case "has-range-keys":
				ef.HasRangeKey = true

			default:
				usageErr(fmt.Sprintf("unknown argument %v", arg.Key))
			}
		}
		if ef.StartKey == nil {
			usageErr("no bounds specified")
		}

		external = append(external, ef)
	}

	if _, err := d.IngestExternalFiles(context.Background(), external); err != nil {
		return err
	}
	return nil
}

func runLSMCmd(td *datadriven.TestData, d *DB) string {
	return describeLSM(d, td.HasArg("verbose"))
}

func describeLSM(d *DB, verbose bool) string {
	var buf bytes.Buffer
	d.mu.Lock()
	defer d.mu.Unlock()
	if verbose {
		buf.WriteString(d.mu.versions.currentVersion().DebugString())
	} else {
		buf.WriteString(d.mu.versions.currentVersion().String())
	}
	if blobFileMetas := d.mu.versions.blobFiles.Metadatas(); len(blobFileMetas) > 0 {
		buf.WriteString("Blob files:\n")
		for _, meta := range blobFileMetas {
			fmt.Fprintf(&buf, "  %s: %d physical bytes, %d value bytes\n", meta.FileNum, meta.Size, meta.ValueSize)
		}
	}
	return buf.String()
}

func parseDBOptionsArgs(opts *Options, args []datadriven.CmdArg) error {
	for _, cmdArg := range args {
		switch cmdArg.Key {
		case "auto-compactions":
			switch cmdArg.Vals[0] {
			case "off":
				opts.DisableAutomaticCompactions = true
			case "on":
				opts.DisableAutomaticCompactions = false
			default:
				return errors.Errorf("Unrecognized %q arg value: %q", cmdArg.Key, cmdArg.Vals[0])
			}
		case "block-size":
			v, err := strconv.Atoi(cmdArg.Vals[0])
			if err != nil {
				return err
			}
			for i := range opts.Levels {
				opts.Levels[i].BlockSize = v
			}
		case "bloom-bits-per-key":
			v, err := strconv.Atoi(cmdArg.Vals[0])
			if err != nil {
				return err
			}
			fp := bloom.FilterPolicy(v)
			opts.Filters = map[string]FilterPolicy{fp.Name(): fp}
			for i := range opts.Levels {
				opts.Levels[i].FilterPolicy = fp
			}
		case "cache-size":
			if opts.Cache != nil {
				opts.Cache.Unref()
				opts.Cache = nil
			}
			size, err := strconv.ParseInt(cmdArg.Vals[0], 10, 64)
			if err != nil {
				return err
			}
			opts.Cache = NewCache(size)
		case "disable-multi-level":
			opts.Experimental.MultiLevelCompactionHeuristic = NoMultiLevel{}
		case "enable-table-stats":
			enable, err := strconv.ParseBool(cmdArg.Vals[0])
			if err != nil {
				return errors.Errorf("%s: could not parse %q as bool: %s", cmdArg.Key, cmdArg.Vals[0], err)
			}
			opts.DisableTableStats = !enable
		case "format-major-version":
			v, err := strconv.Atoi(cmdArg.Vals[0])
			if err != nil {
				return err
			}
			opts.FormatMajorVersion = FormatMajorVersion(v)
		case "index-block-size":
			v, err := strconv.Atoi(cmdArg.Vals[0])
			if err != nil {
				return err
			}
			for i := range opts.Levels {
				opts.Levels[i].IndexBlockSize = v
			}
		case "inject-errors":
			injs := make([]errorfs.Injector, len(cmdArg.Vals))
			for i := 0; i < len(cmdArg.Vals); i++ {
				inj, err := errorfs.ParseDSL(cmdArg.Vals[i])
				if err != nil {
					return err
				}
				injs[i] = inj
			}
			opts.FS = errorfs.Wrap(opts.FS, errorfs.Any(injs...))
		case "l0-compaction-threshold":
			v, err := strconv.Atoi(cmdArg.Vals[0])
			if err != nil {
				return err
			}
			opts.L0CompactionThreshold = v
		case "lbase-max-bytes":
			lbaseMaxBytes, err := strconv.ParseInt(cmdArg.Vals[0], 10, 64)
			if err != nil {
				return err
			}
			opts.LBaseMaxBytes = lbaseMaxBytes
		case "max-concurrent-compactions":
			maxConcurrentCompactions, err := strconv.ParseInt(cmdArg.Vals[0], 10, 64)
			if err != nil {
				return err
			}
			opts.CompactionConcurrencyRange = func() (int, int) {
				return 1, int(maxConcurrentCompactions)
			}
		case "memtable-size":
			memTableSize, err := strconv.ParseUint(cmdArg.Vals[0], 10, 64)
			if err != nil {
				return err
			}
			opts.MemTableSize = memTableSize
		case "merger":
			switch cmdArg.Vals[0] {
			case "appender":
				opts.Merger = base.DefaultMerger
			default:
				return errors.Newf("unrecognized Merger %q\n", cmdArg.Vals[0])
			}
		case "readonly":
			opts.ReadOnly = true
		case "required-in-place":
			if len(cmdArg.Vals) != 2 {
				return errors.New("required-in-place expects 2 arguments: <start-key> <end-key>")
			}
			span := KeyRange{
				Start: []byte(cmdArg.Vals[0]),
				End:   []byte(cmdArg.Vals[1]),
			}
			policy := SpanPolicy{
				DisableValueSeparationBySuffix: true,
				ValueStoragePolicy:             ValueStorageLowReadLatency,
			}
			opts.Experimental.SpanPolicyFunc = MakeStaticSpanPolicyFunc(opts.Comparer.Compare, span, policy)
		case "target-file-sizes":
			if len(opts.Levels) < len(cmdArg.Vals) {
				opts.Levels = slices.Grow(opts.Levels, len(cmdArg.Vals)-len(opts.Levels))[0:len(cmdArg.Vals)]
			}
			for i := range cmdArg.Vals {
				size, err := strconv.ParseInt(cmdArg.Vals[i], 10, 64)
				if err != nil {
					return err
				}
				opts.Levels[i].TargetFileSize = size
			}
		case "value-separation":
			if len(cmdArg.Vals) != 3 {
				return errors.New("value-separation-policy expects 3 arguments: (enabled, minimum-size, max-blob-reference-depth)")
			}
			var policy ValueSeparationPolicy
			var err error
			policy.Enabled, err = strconv.ParseBool(cmdArg.Vals[0])
			if err != nil {
				return err
			}
			policy.MinimumSize, err = strconv.Atoi(cmdArg.Vals[1])
			if err != nil {
				return err
			}
			policy.MaxBlobReferenceDepth, err = strconv.Atoi(cmdArg.Vals[2])
			if err != nil {
				return err
			}
			opts.Experimental.ValueSeparationPolicy = func() ValueSeparationPolicy {
				return policy
			}
		case "wal-failover":
			if v := cmdArg.Vals[0]; v == "off" || v == "disabled" {
				opts.WALFailover = nil
				continue
			}
			opts.WALFailover = &WALFailoverOptions{
				Secondary: wal.Dir{FS: opts.FS, Dirname: cmdArg.Vals[0]},
			}
			opts.WALFailover.EnsureDefaults()
		}
	}
	return nil
}

func streamFilterBetweenGrep(start, end string) stream.Filter {
	startRegexp, err := regexp.Compile(start)
	if err != nil {
		return stream.FilterFunc(func(stream.Arg) error { return err })
	}
	endRegexp, err := regexp.Compile(end)
	if err != nil {
		return stream.FilterFunc(func(stream.Arg) error { return err })
	}
	var passedStart bool
	return stream.FilterFunc(func(arg stream.Arg) error {
		for s := range arg.In {
			if passedStart {
				if endRegexp.MatchString(s) {
					break
				}
				arg.Out <- s
				continue
			} else {
				passedStart = startRegexp.MatchString(s)
			}
		}
		return nil
	})
}
