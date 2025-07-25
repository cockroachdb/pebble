// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package table_test

import (
	"fmt"
	"slices"

	"github.com/cockroachdb/pebble/internal/ascii"
	"github.com/cockroachdb/pebble/internal/ascii/table"
)

func ExampleDefine() {
	type Cat struct {
		Name     string
		Age      int
		Cuteness int
	}

	tbl := table.Define[Cat](
		table.String("name", 7, table.AlignLeft, func(c Cat) string { return c.Name }),
		table.Div(),
		table.Int("age", 4, table.AlignRight, func(c Cat) int { return c.Age }),
		table.Div(),
		table.Int("cuteness", 8, table.AlignRight, func(c Cat) int { return c.Cuteness }),
	)

	board := ascii.Make(8, 1)
	fmt.Println("Cool cats:")
	tbl.Render(board.At(0, 0), table.RenderOptions{}, slices.Values([]Cat{
		{Name: "Chicken", Age: 5, Cuteness: 10},
		{Name: "Heart", Age: 4, Cuteness: 10},
		{Name: "Mai", Age: 2, Cuteness: 10},
		{Name: "Poi", Age: 15, Cuteness: 10},
		{Name: "Pigeon", Age: 2, Cuteness: 10},
		{Name: "Sugar", Age: 8, Cuteness: 10},
		{Name: "Yaya", Age: 5, Cuteness: 10},
		{Name: "Yuumi", Age: 5, Cuteness: 10},
	}))
	fmt.Println(board.String())
	// Output:
	// Cool cats:
	// name    |  age | cuteness
	// --------+------+---------
	// Chicken |    5 |       10
	// Heart   |    4 |       10
	// Mai     |    2 |       10
	// Poi     |   15 |       10
	// Pigeon  |    2 |       10
	// Sugar   |    8 |       10
	// Yaya    |    5 |       10
	// Yuumi   |    5 |       10
}

func ExampleHorizontally() {
	type Cat struct {
		Name     string
		Age      int
		Cuteness int
	}

	tbl := table.Define[Cat](
		table.String("name", 7, table.AlignRight, func(c Cat) string { return c.Name }),
		table.Div(),
		table.Int("age", 4, table.AlignRight, func(c Cat) int { return c.Age }),
		table.Int("cuteness", 8, table.AlignRight, func(c Cat) int { return c.Cuteness }),
	)

	board := ascii.Make(8, 1)
	fmt.Println("Cool cats:")
	opts := table.RenderOptions{Orientation: table.Horizontally}
	tbl.Render(board.At(0, 0), opts, slices.Values([]Cat{
		{Name: "Chicken", Age: 5, Cuteness: 10},
		{Name: "Heart", Age: 4, Cuteness: 10},
		{Name: "Mai", Age: 2, Cuteness: 10},
		{Name: "Poi", Age: 150000000, Cuteness: 10},
		{Name: "Pigeon", Age: 2, Cuteness: 10},
		{Name: "Sugar", Age: 8, Cuteness: 1000000000},
		{Name: "Yaya", Age: 5, Cuteness: 10},
		{Name: "Yuumi", Age: 5, Cuteness: 10},
		{Name: "Yuumibestcatever", Age: 5, Cuteness: 100},
	}))
	fmt.Println(board.String())
	// Output:
	// Cool cats:
	//     name | Chicken  Heart  Mai        Poi  Pigeon       Sugar  Yaya  Yuumi  Yuumibestcatever
	// ---------+----------------------------------------------------------------------------------
	//      age |       5      4    2  150000000       2           8     5      5                 5
	// cuteness |      10     10   10         10      10  1000000000    10     10               100
}
