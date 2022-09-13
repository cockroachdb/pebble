package pebble

import (
	"testing"

	"github.com/HdrHistogram/hdrhistogram-go"
)

const significantValueDigits = 3
const lowestDiscernibleValue = 1
const highestTrackableValue = 1000
const largeValue = 300
const smallValue = 4

func TestSubtractToZeroCounts(t *testing.T) {
	h1 := hdrhistogram.New(lowestDiscernibleValue, highestTrackableValue, significantValueDigits)
	for i := 0; i < 100; i++ {
		handleRecordValue(t, h1, i)
	}

	h1 = Subtract(h1, h1)

	if v, want := h1.ValueAtQuantile(50), int64(0); v != want {
		t.Errorf("Median was %v, but expected %v", v, want)
	}
}

func TestSubtractAfterAdd(t *testing.T) {
	h1 := hdrhistogram.New(lowestDiscernibleValue, 5, significantValueDigits)
	handleRecordValues(t, h1, 1, 1)
	handleRecordValues(t, h1, 2, 2)
	handleRecordValues(t, h1, 3, 3)
	handleRecordValues(t, h1, 4, 2)
	handleRecordValues(t, h1, 5, 1)

	h2 := hdrhistogram.New(lowestDiscernibleValue, 10, significantValueDigits)
	handleRecordValues(t, h2, 5, 1)
	handleRecordValues(t, h2, 6, 2)
	handleRecordValues(t, h2, 7, 3)
	handleRecordValues(t, h2, 8, 10)
	handleRecordValues(t, h2, 9, 1)

	h1Original := hdrhistogram.Import(h1.Export())
	h1.Merge(h2)

	if v, want := h1.ValueAtQuantile(50), int64(7); v != want {
		t.Errorf("Median was %v, but expected %v", v, want)
	}

	h3 := Subtract(h1, h2)
	if !h1Original.Equals(h3) {
		t.Errorf("Expected Histograms to be equal")
	}
}

func TestSubtractToNegativeCounts(t *testing.T) {

	h1 := hdrhistogram.New(lowestDiscernibleValue, highestTrackableValue, significantValueDigits)
	handleRecordValue(t, h1, smallValue)
	handleRecordValue(t, h1, largeValue)
	h2 := hdrhistogram.New(lowestDiscernibleValue, highestTrackableValue, significantValueDigits)
	handleRecordValues(t, h2, smallValue, 2)
	handleRecordValues(t, h2, largeValue, 2)
	h3 := Subtract(h1, h2)
	if total, wanted := h3.TotalCount(), int64(0); total != wanted {
		t.Errorf("Count was %v, but expected %v", total, wanted)
	}
}

func TestSubtractOutsideRanges(t *testing.T) {
	smallHistogram := hdrhistogram.New(lowestDiscernibleValue, highestTrackableValue, significantValueDigits)
	handleRecordValue(t, smallHistogram, smallValue)
	handleRecordValue(t, smallHistogram, largeValue)

	biggerHistogram := hdrhistogram.New(lowestDiscernibleValue, 2*highestTrackableValue, significantValueDigits)
	handleRecordValue(t, biggerHistogram, smallValue)
	handleRecordValue(t, biggerHistogram, largeValue)
	handleRecordValue(t, biggerHistogram, 2*highestTrackableValue)

	h3 := Subtract(smallHistogram, biggerHistogram)
	if h3.TotalCount() != 0 {
		t.Errorf("Failed to subtract")
	}

}

func handleRecordValues(t *testing.T, h *hdrhistogram.Histogram, valueToRecord int, n int) {
	err := h.RecordValues(int64(valueToRecord), int64(n))
	if err != nil {
		t.Fatal(err)
	}
}

func handleRecordValue(t *testing.T, h *hdrhistogram.Histogram, valueToRecord int) {
	handleRecordValues(t, h, valueToRecord, 1)
}
