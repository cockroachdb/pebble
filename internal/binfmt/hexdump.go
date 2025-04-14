package binfmt

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"strconv"
)

// HexDump returns a string representation of the data in a hex dump format.
// The width is the number of bytes per line.
func HexDump(data []byte, width int, includeOffsets bool) string {
	var buf bytes.Buffer
	FHexDump(&buf, data, width, includeOffsets)
	return buf.String()
}

// FHexDump writes a hex dump of the data to w.
func FHexDump(w io.Writer, data []byte, width int, includeOffsets bool) {
	offsetFormatWidth := max(2, int(math.Log(float64(len(data)/2))/math.Log(16))+1)
	offsetFormatStr := "%0" + strconv.Itoa(offsetFormatWidth) + "x"
	for i := 0; i < len(data); i += width {
		if includeOffsets {
			fmt.Fprintf(w, offsetFormatStr+": ", i)
		}
		for j := 0; j < width; j++ {
			if j%4 == 0 {
				fmt.Fprint(w, " ")
			}
			if i+j >= len(data) {
				fmt.Fprintf(w, "  ")
			} else {
				fmt.Fprintf(w, "%02x", data[i+j])
			}
		}

		fmt.Fprint(w, " | ")
		for j := 0; j < width; j++ {
			if i+j >= len(data) {
				break
			}
			if j%4 == 0 {
				fmt.Fprint(w, " ")
			}
			if data[i+j] < 32 || data[i+j] > 126 {
				fmt.Fprint(w, ".")
			} else {
				fmt.Fprintf(w, "%c", data[i+j])
			}
		}
		fmt.Fprintln(w)
	}
}
