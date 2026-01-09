// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
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

	"github.com/cockroachdb/pebble/internal/ascii"
	"github.com/cockroachdb/pebble/sstable/tablefilters/bloom"
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
		// Pick the smallest number of probes that has FPR within 1% of the optimal.
		for i := 1; i < best[bpk]; i++ {
			if fpr[bpk][i] < fpr[bpk][best[bpk]]*1.01 {
				best[bpk] = i
				break
			}
		}
	}

	board := ascii.Make(100, 100)
	cur := board.At(0, 0)
	cur = cur.WriteString("# Bloom filter simulation results\n\n")

	cur = cur.WriteString("| Bits/key | Probes |            FPR            |\n")
	cur = cur.WriteString("|---------:|:------:|:-------------------------:|\n")
	for bpk := 1; bpk <= maxBitsPerKey; bpk++ {
		p := best[bpk]
		cur = cur.Printf("| %8d |   %2d   | %-25s |\n", bpk, p, fprStr[bpk][p])
	}
	cur = cur.NewlineReturn()

	cur = cur.WriteString("\n## Full data\n")
	cur = cur.WriteString("| Bits/key | Probes |            FPR            |\n")
	cur = cur.WriteString("|---------:|:------:|:-------------------------:|\n")

	for bpk := 1; bpk <= maxBitsPerKey; bpk++ {
		if bpk > 1 {
			cur = cur.WriteString("|          |        |                           |\n")
		}
		for p := 1; p <= bpk && p <= maxProbes; p++ {
			cur = cur.Printf("| %8d |   %2d   | %-25s |\n", bpk, p, fprStr[bpk][p])
		}
	}
	fmt.Println(board.String())
	err := os.WriteFile("simulation.md", []byte(board.String()+"\n"), 0644)
	if err != nil {
		panic(err)
	}
}
