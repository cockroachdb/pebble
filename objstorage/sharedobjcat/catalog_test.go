// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sharedobjcat

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
)

func TestCatalog(t *testing.T) {
	mem := vfs.NewMem()

	var cat *Catalog
	datadriven.RunTest(t, "testdata/catalog", func(t *testing.T, td *datadriven.TestData) string {
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
			var contents CatalogContents
			cat, contents, err = Open(mem, dirname)
			if err != nil {
				return err.Error()
			}
			var nums []int
			for n := range contents {
				nums = append(nums, int(n))
			}
			sort.Ints(nums)
			var buf strings.Builder
			for _, n := range nums {
				n := base.FileNum(n)
				meta := contents[n]
				fmt.Fprintf(&buf, "%s: %d/%s\n", base.FileNum(n), meta.CreatorID, meta.CreatorFileNum)
			}

			return buf.String()

		case "add":
			if len(td.CmdArgs) != 3 {
				td.Fatalf(t, "add <file-num> <creator-id> <creator-file-num>")
			}
			var vals [3]int
			for i := range vals {
				var err error
				vals[i], err = strconv.Atoi(td.CmdArgs[i].String())
				if err != nil {
					td.Fatalf(t, "%v", err)
				}
			}
			cat.AddObject(base.FileNum(vals[0]), SharedObjectMetadata{
				CreatorID:      uint64(vals[1]),
				CreatorFileNum: base.FileNum(vals[2]),
			})
			return ""

		case "delete":
			n, err := strconv.Atoi(td.CmdArgs[0].String())
			if err != nil {
				td.Fatalf(t, "%v", err)
			}
			cat.DeleteObject(base.FileNum(n))
			return ""

		case "close":
			if cat == nil {
				return "nil catalog"
			}
			err := cat.Close()
			cat = nil
			return fmt.Sprintf("%v", err)

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
