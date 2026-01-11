// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build ignore

// This program generates the simulation.md file via:
//
//	go run simulation_gen.go
package main

import (
	"fmt"
	"os"

	"github.com/cockroachdb/crlib/crhumanize"
	"github.com/cockroachdb/pebble/internal/ascii"
	"github.com/cockroachdb/pebble/sstable/tablefilters/binaryfuse"
	"github.com/cockroachdb/pebble/sstable/tablefilters/internal/testutils"
)

func main() {
	bitVals := []int{4, 8, 12, 16}
	avgSizes := []int{10_000, 100_000, 1_000_000}

	board := ascii.Make(100, 100)
	cur := board.At(0, 0)
	cur = cur.WriteString("# Binary fuse filter simulation results\n\n")

	cur = cur.WriteString("| Bits/fingerprint |  Keys  | Bits/key |            FPR             |\n")
	cur = cur.WriteString("|-----------------:|:------:|:--------:|:--------------------------:|\n")

	for _, b := range bitVals {
		if b > bitVals[0] {
			cur = cur.WriteString("|                  |        |          |                            |\n")
		}
		for _, avgSize := range avgSizes {
			fprMean, fprStdDev, avgBitsPerKey := binaryfuse.SimulateFPR(avgSize, b)
			cur = cur.Printf("| %16d |  %4s  |   %4.1f   | %-26s |\n",
				b, crhumanize.Count(avgSize, crhumanize.Compact),
				avgBitsPerKey,
				testutils.FormatFPRWithStdDev(fprMean, fprStdDev),
			)
		}
	}
	fmt.Println(board.String())
	err := os.WriteFile("simulation.md", []byte(board.String()+"\n"), 0644)
	if err != nil {
		panic(err)
	}
}
