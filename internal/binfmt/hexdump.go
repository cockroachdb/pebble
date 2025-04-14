package binfmt

import (
	"bytes"
	"fmt"
	"io"
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
	offsetFormatWidth := len(fmt.Sprintf("%x", max(1, len(data)-1)))
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
