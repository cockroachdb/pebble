// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sharedobjcat_test

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage/sharedobjcat"
	"github.com/cockroachdb/pebble/vfs"
)

func TestCatalog(t *testing.T) {
	mem := vfs.NewMem()
	var memLog base.InMemLogger

	var cat *sharedobjcat.Catalog
	datadriven.RunTest(t, "testdata/catalog", func(t *testing.T, td *datadriven.TestData) string {
		toInt := func(args ...string) []int {
			t.Helper()
			var res []int
			for _, arg := range args {
				n, err := strconv.Atoi(arg)
				if err != nil {
					td.Fatalf(t, "error parsing arg %s as integer: %v", arg, err)
				}
				res = append(res, int(n))
			}
			return res
		}

		parseAdd := func(args []string) sharedobjcat.SharedObjectMetadata {
			t.Helper()
			if len(args) != 3 {
				td.Fatalf(t, "add <file-num> <creator-id> <creator-file-num>")
			}
			vals := toInt(args...)
			return sharedobjcat.SharedObjectMetadata{
				FileNum:        base.FileNum(vals[0]),
				CreatorID:      uint64(vals[1]),
				CreatorFileNum: base.FileNum(vals[2]),
			}
		}

		parseDel := func(args []string) base.FileNum {
			t.Helper()
			if len(args) != 1 {
				td.Fatalf(t, "delete <file-num>")
			}
			return base.FileNum(toInt(args[0])[0])
		}

		memLog.Reset()
		switch td.Cmd {
		case "open":
			if len(td.CmdArgs) != 1 {
				td.Fatalf(t, "open <dir>")
			}
			dirname := td.CmdArgs[0].String()
			err := mem.MkdirAll(dirname, 0755)
			if err != nil {
				td.Fatalf(t, "%v", err)
			}
			var contents sharedobjcat.CatalogContents
			cat, contents, err = sharedobjcat.Open(vfs.WithLogging(mem, memLog.Infof), dirname)
			if err != nil {
				return err.Error()
			}
			var buf strings.Builder
			for _, meta := range contents {
				fmt.Fprintf(&buf, "%s: %d/%s\n", meta.FileNum, meta.CreatorID, meta.CreatorFileNum)
			}

			return buf.String()

		case "batch":
			var b sharedobjcat.Batch
			for _, cmd := range strings.Split(td.Input, "\n") {
				tokens := strings.Split(cmd, " ")
				if len(tokens) == 0 {
					td.Fatalf(t, "empty batch line")
				}
				switch tokens[0] {
				case "add":
					b.AddObject(parseAdd(tokens[1:]))
				case "delete":
					b.DeleteObject(parseDel(tokens[1:]))
				default:
					td.Fatalf(t, "unknown batch command: %s", tokens[0])
				}
			}
			if err := cat.ApplyBatch(&b); err != nil {
				return fmt.Sprintf("error applying batch: %v", err)
			}
			return memLog.String()

		case "random-batches":
			n := 1
			size := 1000
			for _, arg := range td.CmdArgs {
				if len(arg.Vals) != 1 {
					td.Fatalf(t, "random-batches n=<val> size=<val>")
				}
				val := toInt(arg.Vals[0])[0]
				switch arg.Key {
				case "n":
					n = val
				case "size":
					size = val
				default:
					td.Fatalf(t, "random-batches n=<val> size=<val>")
				}
			}
			var b sharedobjcat.Batch
			for batchIdx := 0; batchIdx < n; batchIdx++ {
				for i := 0; i < size; i++ {
					b.AddObject(sharedobjcat.SharedObjectMetadata{
						FileNum:        base.FileNum(rand.Uint64()),
						CreatorID:      rand.Uint64(),
						CreatorFileNum: base.FileNum(rand.Uint64()),
					})
				}
				if err := cat.ApplyBatch(&b); err != nil {
					td.Fatalf(t, "error applying batch: %v", err)
				}
			}
			return memLog.String()

		case "close":
			if cat == nil {
				return "nil catalog"
			}
			err := cat.Close()
			cat = nil
			if err != nil {
				return fmt.Sprintf("%v", err)
			}
			return memLog.String()

		case "list":
			if len(td.CmdArgs) != 1 {
				td.Fatalf(t, "open <dir>")
			}
			paths, err := mem.List(td.CmdArgs[0].String())
			if err != nil {
				return err.Error()
			}
			sort.Strings(paths)
			return strings.Join(paths, "\n")

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}
