// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package humanize

import (
	"fmt"
	"math"
)

var sizes = []string{"B", "K", "M", "G", "T", "P", "E"}

func logn(n, b float64) float64 {
	return math.Log(n) / math.Log(b)
}

// Uint64 produces a human readable representation of the value.
func Uint64(s uint64) string {
	const base = 1024

	if s < 10 {
		return fmt.Sprintf("%d B", s)
	}
	e := math.Floor(logn(float64(s), base))
	suffix := sizes[int(e)]
	val := math.Floor(float64(s)/math.Pow(base, e)*10+0.5) / 10
	f := "%.0f %s"
	if val < 10 {
		f = "%.1f %s"
	}

	return fmt.Sprintf(f, val, suffix)
}
