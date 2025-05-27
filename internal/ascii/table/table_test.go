// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package table

import (
	"fmt"
	"slices"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/ascii"
)

func TestTable(t *testing.T) {
	type Cat struct {
		Name     string
		Age      int
		Cuteness int
	}
	cats := []Cat{
		{Name: "Chicken", Age: 5, Cuteness: 10},
		{Name: "Heart", Age: 4, Cuteness: 10},
		{Name: "Mai", Age: 2, Cuteness: 10},
		{Name: "Poi", Age: 15, Cuteness: 10},
		{Name: "Pigeon", Age: 2, Cuteness: 10},
		{Name: "Sugar", Age: 8, Cuteness: 10},
		{Name: "Yaya", Age: 5, Cuteness: 10},
		{Name: "Yuumi", Age: 5, Cuteness: 10},
	}

	wb := ascii.Make(1, 10)
	datadriven.RunTest(t, "testdata/table", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "cats-autoincrement":
			def := Define[Cat](
				AutoIncrement[Cat]("idx", 3, AlignLeft),
				Div(),
				String("name", 7, AlignRight, func(c Cat) string { return c.Name }),
			)
			wb.Reset(def.CumulativeFieldWidth)
			def.Render(wb.At(0, 0), RenderOptions{}, slices.Values(cats))
			return wb.String()
		case "cats-nodiv":
			def := Define[Cat](
				String("name", 7, AlignLeft, func(c Cat) string { return c.Name }),
				Int("age", 4, AlignRight, func(c Cat) int { return c.Age }),
				Int("cuteness", 8, AlignRight, func(c Cat) int { return c.Cuteness }),
			)
			wb.Reset(def.CumulativeFieldWidth)
			def.Render(wb.At(0, 0), RenderOptions{}, slices.Values(cats))
			return wb.String()
		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}
