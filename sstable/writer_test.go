// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/petermattis/pebble/internal/base"
	"github.com/petermattis/pebble/internal/datadriven"
	"github.com/petermattis/pebble/internal/rangedel"
	"github.com/petermattis/pebble/vfs"
)

func TestWriter(t *testing.T) {
	var r *Reader
	datadriven.RunTest(t, "testdata/writer", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "build":
			if r != nil {
				_ = r.Close()
				r = nil
			}

			mem := vfs.NewMem()
			f0, err := mem.Create("test")
			if err != nil {
				return err.Error()
			}

			w := NewWriter(f0, nil, TableOptions{})
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
								err = errors.New(fmt.Sprint(r))
							}
						}()
						f.Add(key, value)
					}()
					if err != nil {
						return err.Error()
					}
				default:
					if err := w.Add(key, value); err != nil {
						return err.Error()
					}
				}
			}
			f.Finish()
			for _, v := range tombstones {
				if err := w.Add(v.Start, v.End); err != nil {
					return err.Error()
				}
			}
			if err := w.Close(); err != nil {
				return err.Error()
			}
			meta, err := w.Metadata()
			if err != nil {
				return err.Error()
			}

			f1, err := mem.Open("test")
			if err != nil {
				return err.Error()
			}
			r, err = NewReader(f1, 0, 0, nil)
			if err != nil {
				return err.Error()
			}
			return fmt.Sprintf("point:   [%s,%s]\nrange:   [%s,%s]\nseqnums: [%d,%d]\n",
				meta.SmallestPoint, meta.LargestPoint,
				meta.SmallestRange, meta.LargestRange,
				meta.SmallestSeqNum, meta.LargestSeqNum)

		case "build-raw":
			if r != nil {
				_ = r.Close()
				r = nil
			}

			mem := vfs.NewMem()
			f0, err := mem.Create("test")
			if err != nil {
				return err.Error()
			}

			w := NewWriter(f0, nil, TableOptions{})
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
					return err.Error()
				}
			}
			if err := w.Close(); err != nil {
				return err.Error()
			}
			meta, err := w.Metadata()
			if err != nil {
				return err.Error()
			}

			f1, err := mem.Open("test")
			if err != nil {
				return err.Error()
			}
			r, err = NewReader(f1, 0, 0, nil)
			if err != nil {
				return err.Error()
			}
			return fmt.Sprintf("point:   [%s,%s]\nrange:   [%s,%s]\nseqnums: [%d,%d]\n",
				meta.SmallestPoint, meta.LargestPoint,
				meta.SmallestRange, meta.LargestRange,
				meta.SmallestSeqNum, meta.LargestSeqNum)

		case "scan":
			iter := iterAdapter{r.NewIter(nil /* lower */, nil /* upper */)}
			defer iter.Close()

			var buf bytes.Buffer
			for valid := iter.First(); valid; valid = iter.Next() {
				fmt.Fprintf(&buf, "%s:%s\n", iter.Key(), iter.Value())
			}
			return buf.String()

		case "scan-range-del":
			iter := r.NewRangeDelIter()
			if iter == nil {
				return ""
			}
			defer iter.Close()

			var buf bytes.Buffer
			for key, val := iter.First(); key != nil; key, val = iter.Next() {
				fmt.Fprintf(&buf, "%s:%s\n", key, val)
			}
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}
