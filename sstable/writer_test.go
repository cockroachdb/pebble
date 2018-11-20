// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/internal/datadriven"
	"github.com/petermattis/pebble/storage"
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

			fs := storage.NewMem()
			f0, err := fs.Create("test")
			if err != nil {
				return err.Error()
			}

			w := NewWriter(f0, nil, db.LevelOptions{})
			for _, key := range strings.Split(td.Input, "\n") {
				j := strings.Index(key, ":")
				ikey := db.ParseInternalKey(key[:j])
				value := []byte(key[j+1:])
				if err := w.Add(ikey, value); err != nil {
					return err.Error()
				}
			}
			if err := w.Close(); err != nil {
				return err.Error()
			}

			f1, err := fs.Open("test")
			if err != nil {
				return err.Error()
			}
			r = NewReader(f1, 0, nil)
			return ""

		case "scan":
			iter := r.NewIter(nil)
			defer iter.Close()

			var buf bytes.Buffer
			for iter.First(); iter.Valid(); iter.Next() {
				fmt.Fprintf(&buf, "%s:%s\n", iter.Key(), iter.Value())
			}
			return buf.String()

		case "scan-range-del":
			iter := r.NewRangeDelIter(nil)
			if iter == nil {
				return ""
			}
			defer iter.Close()

			var buf bytes.Buffer
			for iter.First(); iter.Valid(); iter.Next() {
				fmt.Fprintf(&buf, "%s:%s\n", iter.Key(), iter.Value())
			}
			return buf.String()

		default:
			t.Fatalf("unknown command: %s", td.Cmd)
		}
		return ""
	})
}
