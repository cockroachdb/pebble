// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/internal/rangedel"
	"github.com/cockroachdb/pebble/vfs"
)

// modifierFile modifies the slice passed into Write() in place. This is useful
// to uncover any bugs that would otherwise occur in practice when using a
// vfs.File wrapper that, for instance, encrypts in-place.
type modifierFile struct {
	vfs.File
}

func (m modifierFile) Write(p []byte) (n int, err error) {
	n, err = m.File.Write(p)
	for i := range p {
		// Toggle all bits.
		p[i] ^= 0xFF
	}
	return n, err
}

// Flush is a no-op. This is necessary to prevent sstable.Writer from doings its
// own buffering.
func (m modifierFile) Flush() error {
	return nil
}

func runBuildCmd(td *datadriven.TestData, writerOpts WriterOptions) (*WriterMetadata, *Reader, error) {
	mem := vfs.NewMem()
	f0, err := mem.Create("test")
	if err != nil {
		return nil, nil, err
	}

	for _, arg := range td.CmdArgs {
		switch arg.Key {
		case "leveldb":
			if len(arg.Vals) != 0 {
				return nil, nil, errors.Errorf("%s: arg %s expects 0 values", td.Cmd, arg.Key)
			}
			writerOpts.TableFormat = TableFormatLevelDB
		case "block-size":
			if len(arg.Vals) != 1 {
				return nil, nil, errors.Errorf("%s: arg %s expects 1 value", td.Cmd, arg.Key)
			}
			var err error
			writerOpts.BlockSize, err = strconv.Atoi(arg.Vals[0])
			if err != nil {
				return nil, nil, err
			}
		case "index-block-size":
			if len(arg.Vals) != 1 {
				return nil, nil, errors.Errorf("%s: arg %s expects 1 value", td.Cmd, arg.Key)
			}
			var err error
			writerOpts.IndexBlockSize, err = strconv.Atoi(arg.Vals[0])
			if err != nil {
				return nil, nil, err
			}
		case "modifier-file":
			f0 = modifierFile{File: f0}
		default:
			return nil, nil, errors.Errorf("%s: unknown arg %s", td.Cmd, arg.Key)
		}
	}

	w := NewWriter(f0, writerOpts)
	var tombstones []rangedel.Tombstone
	f := rangedel.Fragmenter{
		Cmp: DefaultComparer.Compare,
		Emit: func(fragmented []rangedel.Tombstone) {
			tombstones = append(tombstones, fragmented...)
		},
	}
	for _, data := range strings.Split(td.Input, "\n") {
		j := strings.Index(data, ":")
		key := base.ParseInternalKey(data[:j])
		value := []byte(data[j+1:])
		switch key.Kind() {
		case InternalKeyKindRangeDelete:
			var err error
			func() {
				defer func() {
					if r := recover(); r != nil {
						err = errors.Errorf("%v", r)
					}
				}()
				f.Add(key, value)
			}()
			if err != nil {
				return nil, nil, err
			}
		default:
			if err := w.Add(key, value); err != nil {
				return nil, nil, err
			}

		}
	}
	f.Finish()
	for _, v := range tombstones {
		if err := w.Add(v.Start, v.End); err != nil {
			return nil, nil, err
		}
	}
	if err := w.Close(); err != nil {
		return nil, nil, err
	}
	meta, err := w.Metadata()
	if err != nil {
		return nil, nil, err
	}

	f1, err := mem.Open("test")
	if err != nil {
		return nil, nil, err
	}
	r, err := NewReader(f1, ReaderOptions{})
	if err != nil {
		return nil, nil, err
	}
	return meta, r, nil
}

func runBuildRawCmd(td *datadriven.TestData) (*WriterMetadata, *Reader, error) {
	mem := vfs.NewMem()
	f0, err := mem.Create("test")
	if err != nil {
		return nil, nil, err
	}

	w := NewWriter(f0, WriterOptions{})
	for i := range td.CmdArgs {
		arg := &td.CmdArgs[i]
		if arg.Key == "range-del-v1" {
			w.rangeDelV1Format = true
			break
		}
	}

	for _, data := range strings.Split(td.Input, "\n") {
		j := strings.Index(data, ":")
		key := base.ParseInternalKey(data[:j])
		value := []byte(data[j+1:])
		if err := w.Add(key, value); err != nil {
			return nil, nil, err
		}
	}
	if err := w.Close(); err != nil {
		return nil, nil, err
	}
	meta, err := w.Metadata()
	if err != nil {
		return nil, nil, err
	}

	f1, err := mem.Open("test")
	if err != nil {
		return nil, nil, err
	}
	r, err := NewReader(f1, ReaderOptions{})
	if err != nil {
		return nil, nil, err
	}
	return meta, r, nil
}

func runIterCmd(td *datadriven.TestData, r *Reader) string {
	for _, arg := range td.CmdArgs {
		switch arg.Key {
		case "globalSeqNum":
			if len(arg.Vals) != 1 {
				return fmt.Sprintf("%s: arg %s expects 1 value", td.Cmd, arg.Key)
			}
			v, err := strconv.Atoi(arg.Vals[0])
			if err != nil {
				return err.Error()
			}
			r.Properties.GlobalSeqNum = uint64(v)
		default:
			return fmt.Sprintf("%s: unknown arg: %s", td.Cmd, arg.Key)
		}
	}
	origIter, err := r.NewIter(nil /* lower */, nil /* upper */)
	if err != nil {
		return err.Error()
	}
	iter := newIterAdapter(origIter)
	defer iter.Close()

	var b bytes.Buffer
	var prefix []byte
	for _, line := range strings.Split(td.Input, "\n") {
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		switch parts[0] {
		case "seek-ge":
			if len(parts) != 2 {
				return fmt.Sprintf("seek-ge <key>\n")
			}
			prefix = nil
			iter.SeekGE([]byte(strings.TrimSpace(parts[1])))
		case "seek-prefix-ge":
			if len(parts) != 2 {
				return fmt.Sprintf("seek-prefix-ge <key>\n")
			}
			prefix = []byte(strings.TrimSpace(parts[1]))
			iter.SeekPrefixGE(prefix, prefix /* key */)
		case "seek-lt":
			if len(parts) != 2 {
				return fmt.Sprintf("seek-lt <key>\n")
			}
			prefix = nil
			iter.SeekLT([]byte(strings.TrimSpace(parts[1])))
		case "first":
			prefix = nil
			iter.First()
		case "last":
			prefix = nil
			iter.Last()
		case "next":
			iter.Next()
		case "prev":
			iter.Prev()
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
					if len(lower) == 0 {
						lower = nil
					}
				case "upper":
					upper = []byte(arg[1])
					if len(upper) == 0 {
						upper = nil
					}
				default:
					return fmt.Sprintf("set-bounds: unknown arg: %s", arg)
				}
			}
			iter.SetBounds(lower, upper)
		}
		if iter.Valid() && checkValidPrefix(prefix, iter.Key().UserKey) {
			fmt.Fprintf(&b, "<%s:%d>", iter.Key().UserKey, iter.Key().SeqNum())
		} else if err := iter.Error(); err != nil {
			fmt.Fprintf(&b, "<err=%v>", err)
		} else {
			fmt.Fprintf(&b, ".")
		}
		b.WriteString("\n")
	}
	return b.String()
}
