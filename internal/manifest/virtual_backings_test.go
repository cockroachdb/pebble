// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/v2/internal/base"
)

func TestVirtualBackings(t *testing.T) {
	bv := MakeVirtualBackings()
	datadriven.RunTest(t, "testdata/virtual_backings", func(t *testing.T, d *datadriven.TestData) (retVal string) {
		var nInt, size uint64
		d.MaybeScanArgs(t, "n", &nInt)
		d.MaybeScanArgs(t, "size", &size)
		n := base.DiskFileNum(nInt)

		defer func() {
			if r := recover(); r != nil {
				retVal = fmt.Sprint(r)
			}
		}()

		switch d.Cmd {
		case "add":
			bv.AddAndRef(&TableBacking{
				DiskFileNum: n,
				Size:        size,
			})

		case "remove":
			bv.Remove(n)

		case "add-table":
			m := &TableMetadata{
				TableBacking: &TableBacking{DiskFileNum: n},
				Size:         size,
				Virtual:      true,
			}
			bv.AddTable(m)

		case "remove-table":
			m := &TableMetadata{
				TableBacking: &TableBacking{DiskFileNum: n},
				Size:         size,
				Virtual:      true,
			}
			bv.RemoveTable(m)

		case "protect":
			bv.Protect(n)

		case "unprotect":
			bv.Unprotect(n)

		default:
			d.Fatalf(t, "unknown command %q", d.Cmd)
		}

		return bv.String()
	})
}
