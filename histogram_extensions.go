package pebble

import "github.com/HdrHistogram/hdrhistogram-go"

// Subtract returns a hdrhistogram.Histogram produce from subtracting two histograms
// During subtraction values are clamped to zero in each bucket
func Subtract(h *hdrhistogram.Histogram, g *hdrhistogram.Histogram) *hdrhistogram.Histogram {
	snapg, snaph := g.Export(), h.Export()
	for i := range snapg.Counts {
		snaph.Counts[i] = clampToZero(snaph.Counts[i] - snapg.Counts[i])
	}
	return hdrhistogram.Import(snaph)
}

func clampToZero(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}
