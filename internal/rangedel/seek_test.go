// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangedel

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
)

type iterAdapter struct {
	*Iter
}

func (i *iterAdapter) verify(key *base.InternalKey, val []byte) (*base.InternalKey, []byte) {
	valid := key != nil
	if valid != i.Valid() {
		panic(fmt.Sprintf("inconsistent valid: %t != %t", valid, i.Valid()))
	}
	if valid {
		if base.InternalCompare(bytes.Compare, *key, *i.Key()) != 0 {
			panic(fmt.Sprintf("inconsistent key: %s != %s", *key, i.Key()))
		}
		if !bytes.Equal(val, i.Value()) {
			panic(fmt.Sprintf("inconsistent value: [% x] != [% x]", val, i.Value()))
		}
	}
	return key, val
}

func (i *iterAdapter) SeekGE(key []byte) (*base.InternalKey, []byte) {
	return i.verify(i.Iter.SeekGE(key))
}

func (i *iterAdapter) SeekLT(key []byte) (*base.InternalKey, []byte) {
	return i.verify(i.Iter.SeekLT(key))
}

func (i *iterAdapter) First() (*base.InternalKey, []byte) {
	return i.verify(i.Iter.First())
}

func (i *iterAdapter) Last() (*base.InternalKey, []byte) {
	return i.verify(i.Iter.Last())
}

func (i *iterAdapter) Next() (*base.InternalKey, []byte) {
	return i.verify(i.Iter.Next())
}

func (i *iterAdapter) Prev() (*base.InternalKey, []byte) {
	return i.verify(i.Iter.Prev())
}

func TestSeek(t *testing.T) {
	cmp := base.DefaultComparer.Compare
	fmtKey := base.DefaultComparer.FormatKey
	iter := &iterAdapter{}

	datadriven.RunTest(t, "testdata/seek", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "build":
			tombstones := buildTombstones(t, cmp, fmtKey, d.Input)
			iter.Iter = NewIter(cmp, tombstones)
			return formatTombstones(tombstones)

		case "seek-ge", "seek-le":
			seek := SeekGE
			if d.Cmd == "seek-le" {
				seek = SeekLE
			}

			var buf bytes.Buffer
			for _, line := range strings.Split(d.Input, "\n") {
				parts := strings.Fields(line)
				if len(parts) != 2 {
					return fmt.Sprintf("malformed input: %s", line)
				}
				seq, err := strconv.ParseUint(parts[1], 10, 64)
				if err != nil {
					return err.Error()
				}
				tombstone := seek(cmp, iter, []byte(parts[0]), seq)
				fmt.Fprintf(&buf, "%s", strings.TrimSpace(formatTombstones([]Tombstone{tombstone})))
				fmt.Fprintf(&buf, "\n")
			}
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}
