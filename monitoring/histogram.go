// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package monitoring

import (
	"fmt"
	"math"
	"strings"
	"sync/atomic"
)

var bucketMapper HistogramBucketMapper

const bucketLen = 109 // len(bucketMapper.bucketValues)

func init() {
	bucketValues := []uint64{1, 2}
	maxVal := float64(math.MaxUint64)
	for bucketVal := float64(bucketValues[len(bucketValues)-1]); bucketVal*1.5 <= maxVal; bucketVal *= 1.5 {
		newVal := bucketVal * 1.5

		// Extracts two most significant digits to make histogram buckets more
		// human-readable. E.g., 172 becomes 170.
		var powerOfTen uint64 = 1
		for newVal/10 > 10 {
			newVal /= 10
			powerOfTen *= 10
		}

		bucketValues = append(bucketValues, uint64(newVal)*powerOfTen)
	}

	bucketMapper = HistogramBucketMapper{
		bucketValues:   bucketValues,
		maxBucketValue: bucketValues[len(bucketValues)-1],
		minBucketValue: bucketValues[0],
	}
}

// HistogramBucketMapper maps index and value.
type HistogramBucketMapper struct {
	bucketValues   []uint64
	maxBucketValue uint64
	minBucketValue uint64
}

// IndexForValue returns array index for value.
func (m HistogramBucketMapper) IndexForValue(value uint64) int {
	if m.maxBucketValue <= value {
		return len(m.bucketValues) - 1
	} else if m.minBucketValue <= value {
		for index, bucketValue := range m.bucketValues {
			if value <= bucketValue {
				return index
			}
		}
	} else {
		return 0
	}

	return 0
}

// BucketLimit returns limit of bucket.
func (m HistogramBucketMapper) BucketLimit(index int) uint64 {
	return m.bucketValues[index]
}

// Histogram measures distribution of a stat across all operations. Most of the histograms
// are for distribution of duration of a DB operation.
type Histogram struct {
	min        uint64
	max        uint64
	num        uint64
	sum        uint64
	sumSquares uint64
	buckets    [bucketLen]uint64
}

// Clear clears histogram.
func (h *Histogram) Clear() {
	atomic.StoreUint64(&h.min, 0)
	atomic.StoreUint64(&h.max, 0)
	atomic.StoreUint64(&h.num, 0)
	atomic.StoreUint64(&h.sum, 0)
	atomic.StoreUint64(&h.sumSquares, 0)
	h.buckets = [bucketLen]uint64{}
}

// Empty return true if histogram empty.
func (h Histogram) Empty() bool {
	return h.Num() == 0
}

// Add adds value to histogram.
func (h *Histogram) Add(value uint64) {
	atomic.AddUint64(&h.buckets[bucketMapper.IndexForValue(value)], 1)

	oldMin := h.Min()
	if value < oldMin {
		atomic.StoreUint64(&h.min, value)
	}

	oldMax := h.Max()
	if oldMax < value {
		atomic.StoreUint64(&h.max, value)
	}

	atomic.AddUint64(&h.num, 1)
	atomic.AddUint64(&h.sum, value)
	atomic.AddUint64(&h.sumSquares, value*value)
}

// Min returns the min value.
func (h Histogram) Min() uint64 {
	return atomic.LoadUint64(&h.min)
}

// Max returns the max value.
func (h Histogram) Max() uint64 {
	return atomic.LoadUint64(&h.max)
}

// Num returns the number of values.
func (h Histogram) Num() uint64 {
	return atomic.LoadUint64(&h.num)
}

// Sum returns the sum of values.
func (h Histogram) Sum() uint64 {
	return atomic.LoadUint64(&h.sum)
}

// SumSquares returns the square of the sum.
func (h Histogram) SumSquares() uint64 {
	return atomic.LoadUint64(&h.sumSquares)
}

// BucketAt returns number in a specific bucket.
func (h Histogram) BucketAt(index int) uint64 {
	return atomic.LoadUint64(&h.buckets[index])
}

// Median returns 50 of Percentile.
func (h Histogram) Median() float64 {
	return h.Percentile(50)
}

// Percentile returns the value below which a given percentage
// of observations in a group of observations falls.
func (h Histogram) Percentile(p float64) float64 {
	threshold := float64(h.Num()) * (p / 100.0)
	var cumulativeSum uint64
	for i := range h.buckets {
		bucketValue := h.BucketAt(i)
		cumulativeSum += bucketValue
		if uint64(threshold) <= cumulativeSum {
			// Scale linearly within this bucket
			var leftPoint uint64
			if i == 0 {
				leftPoint = 0
			} else {
				leftPoint = bucketMapper.BucketLimit(i - 1)
			}
			rightPoint := bucketMapper.BucketLimit(i)
			leftSum := cumulativeSum - bucketValue
			rightSum := cumulativeSum
			var pos float64
			rightLeftDiff := rightSum - leftSum
			if rightLeftDiff != 0 {
				pos = (threshold - float64(leftSum)) / float64(rightLeftDiff)
			}
			r := float64(leftPoint) + (float64(rightPoint-leftPoint) * pos)
			min := float64(h.Min())
			max := float64(h.Max())
			if r < min {
				r = min
			}
			if max < r {
				r = max
			}
			return r
		}
	}
	return float64(h.Max())
}

// Average returns average value.
func (h Histogram) Average() float64 {
	num := h.Num()
	sum := h.Sum()
	if num == 0 {
		return 0
	}

	return float64(sum) / float64(num)
}

// StandardDeviation is a measure of the amount of variation
// or dispersion of a set of values.
func (h Histogram) StandardDeviation() float64 {
	num := h.Num()
	sum := h.Sum()
	sumSquares := h.SumSquares()
	if num == 0 {
		return 0
	}

	doubleVariance := float64(sumSquares*num-sum*sum) / float64(num*num)
	return math.Sqrt(doubleVariance)
}

// Data returns formatted data for share.
func (h Histogram) Data() *HistogramData {
	return &HistogramData{
		Median:            h.Median(),
		Percentile95:      h.Percentile(95),
		Percentile99:      h.Percentile(99),
		Max:               float64(h.Max()),
		Average:           h.Average(),
		StandardDeviation: h.StandardDeviation(),
		Count:             h.Num(),
		Sum:               h.Sum(),
		Min:               float64(h.Min()),
	}
}

func (h Histogram) String() string {
	num := h.Num()
	var str string
	str += fmt.Sprintf("Count: %d Average: %.4f  StdDev: %.2f", num, h.Average(), h.StandardDeviation())
	str += fmt.Sprintln()
	str += fmt.Sprintf("Min: %d  Median: %.4f  Max: %d", h.Min(), h.Median(), h.Max())
	str += fmt.Sprintln()
	str += fmt.Sprintf("Percentiles:")
	str += fmt.Sprintln()
	str += fmt.Sprintf(
		"P50: %.2f P75: %.2f P99: %.2f P99.9: %.2f P99.99: %.2f\n",
		h.Percentile(50),
		h.Percentile(75),
		h.Percentile(99),
		h.Percentile(99.9),
		h.Percentile(99.99))
	str += fmt.Sprintln("------------------------------------------------------")
	if num == 0 {
		return str
	}

	mult := 100 / float64(num)
	var cumulativeSum uint64
	for i := range h.buckets {
		bucketValue := h.BucketAt(i)
		if bucketValue <= 0 {
			continue
		}
		cumulativeSum += bucketValue

		var prefix string
		var preBucketLimit uint64
		if i == 0 {
			prefix = "["
			preBucketLimit = 0
		} else {
			prefix = "("
			preBucketLimit = bucketMapper.BucketLimit(i - 1)
		}

		str += fmt.Sprintf(
			"%s %20d, %20d ] %8d %7.3f%% %7.3f%% ",
			prefix,
			preBucketLimit,
			bucketMapper.BucketLimit(i),
			bucketValue,
			mult*float64(bucketValue),
			mult*float64(cumulativeSum))

		// Add hash marks based on percentage; 20 marks for 100%.
		marks := int(mult*float64(bucketValue)/5 + 0.5)
		str += strings.Repeat("#", marks)
		str += fmt.Sprintln()
	}
	return str
}

// HistogramData is formatted data for share.
type HistogramData struct {
	Median            float64
	Percentile95      float64
	Percentile99      float64
	Average           float64
	StandardDeviation float64
	Max               float64
	Count             uint64
	Sum               uint64
	Min               float64
}
