// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build ignore

// This program generates the simulation.txt file via:
//
//	go run simulation_gen.go
package main

import (
	"fmt"
	"os"

	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/internal/ascii"
	"github.com/cockroachdb/pebble/internal/ascii/table"
)

func main() {
	const maxBitsPerKey = 20
	const maxProbes = 16

	var fpr [maxBitsPerKey + 1][maxProbes + 1]float64
	var fprStr [maxBitsPerKey + 1][maxProbes + 1]string
	var best [maxBitsPerKey + 1]int

	for bpk := 1; bpk <= maxBitsPerKey; bpk++ {
		for p := 1; p <= bpk && p <= maxProbes; p++ {
			fpr[bpk][p], fprStr[bpk][p] = bloom.SimulateFPR(bpk, p)
			if best[bpk] == 0 || fpr[bpk][p] < fpr[bpk][best[bpk]] {
				best[bpk] = p
			}
		}
	}

	tbl := table.Define[int](
		table.Int("Bits/key", 8, table.AlignRight, func(bpk int) int { return bpk }),
		table.Div(),
		table.Int("Probes", 6, table.AlignCenter, func(bpk int) int { return best[bpk] }),
		table.Div(),
		table.String("FPR", 10, table.AlignCenter, func(bpk int) string { return fprStr[bpk][best[bpk]] }),
	)

	// Render the table.
	board := ascii.Make(100, 100)
	rows := make([]int, maxBitsPerKey)
	for i := range rows {
		rows[i] = i + 1
	}
	cur := board.At(0, 0)
	cur = tbl.Render(cur, table.RenderOptions{}, rows...)
	cur = cur.NewlineReturn()
	cur = cur.WriteString("== Full data ==\n")
	for bpk := 1; bpk <= maxBitsPerKey; bpk++ {
		cur = cur.NewlineReturn()
		cur = cur.Printf("  %d bits per key:\n", bpk)
		for p := 1; p <= bpk && p <= maxProbes; p++ {
			cur = cur.Printf("    %2d probes:  %s\n", p, fprStr[bpk][p])
		}
	}
	fmt.Println(board.String())
	err := os.WriteFile("simulation.txt", []byte(board.String()+"\n"), 0644)
	if err != nil {
		panic(err)
	}
}
