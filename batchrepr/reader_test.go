// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package batchrepr

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"unicode"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/stretchr/testify/require"
)

func TestReader(t *testing.T) {
	datadriven.RunTest(t, "testdata/reader", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "is-empty":
			repr := readRepr(t, td.Input)
			return fmt.Sprint(IsEmpty(repr))

		case "scan":
			repr := readRepr(t, td.Input)
			h, _ := ReadHeader(repr)
			r := Read(repr)
			var out strings.Builder
			fmt.Fprintf(&out, "Header: %s\n", h)
			for {
				kind, ukey, value, ok, err := r.Next()
				if !ok {
					if err != nil {
						fmt.Fprintf(&out, "err: %s\n", err)
					} else {
						fmt.Fprint(&out, "eof")
					}
					break
				}
				fmt.Fprintf(&out, "%s: %q: %q\n", kind, ukey, value)
			}
			return out.String()

		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}

func readRepr(t testing.TB, str string) []byte {
	var reprBuf bytes.Buffer
	for l := range crstrings.LinesSeq(str) {
		// Remove any trailing comments behind #.
		if i := strings.IndexRune(l, '#'); i >= 0 {
			l = l[:i]
		}
		// Strip all whitespace from the line.
		l = strings.Map(func(r rune) rune {
			if unicode.IsSpace(r) {
				return -1
			}
			return r
		}, l)
		b, err := hex.DecodeString(l)
		if err != nil {
			t.Fatal(err)
		}
		reprBuf.Write(b)
	}
	return reprBuf.Bytes()
}

func TestDecodeStr(t *testing.T) {
	t.Run("truncated", func(t *testing.T) {
		// Each of these inputs must not panic and must report ok=false.
		cases := [][]byte{
			nil,
			{},
			{0x01},                                 // length byte says 1, but no payload
			{0x7f},                                 // length byte says 127, but no payload
			{0xff},                                 // partial multi-byte varint
			{0xff, 0xff, 0xff, 0xff},               // longer partial multi-byte varint
			append([]byte{0x05}, []byte("abc")...), // says 5, only 3 bytes
		}
		for i, data := range cases {
			t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
				_, _, ok := DecodeStr(data)
				require.False(t, ok)
			})
		}
	})

	t.Run("valid-fast-path", func(t *testing.T) {
		// Single-byte varint, len(data) <= 128.
		payload := bytes.Repeat([]byte("x"), 127)
		data := append([]byte{0x7f}, payload...)
		require.Equal(t, 128, len(data))
		odata, s, ok := DecodeStr(data)
		require.True(t, ok)
		require.Equal(t, payload, s)
		require.Empty(t, odata)
	})

	t.Run("valid-slow-path", func(t *testing.T) {
		// Force len(data) > 128 with both single- and multi-byte varints.
		for _, strLen := range []int{0, 1, 127, 128, 200, 16384} {
			payload := bytes.Repeat([]byte("y"), strLen)
			var buf [binary.MaxVarintLen32]byte
			n := binary.PutUvarint(buf[:], uint64(strLen))
			data := append([]byte{}, buf[:n]...)
			data = append(data, payload...)
			// Append trailing bytes so len(data) > 128 and we exercise the
			// slow path.
			tail := bytes.Repeat([]byte("z"), 200)
			data = append(data, tail...)
			odata, s, ok := DecodeStr(data)
			require.True(t, ok, "strLen=%d", strLen)
			require.Equal(t, payload, s, "strLen=%d", strLen)
			require.Equal(t, tail, odata, "strLen=%d", strLen)
		}
	})
}

func TestDecodeBlobFileIDs(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		_, ok := DecodeBlobFileIDs(nil)
		require.False(t, ok)
		_, ok = DecodeBlobFileIDs([]byte{})
		require.False(t, ok)
	})

	t.Run("count-too-large", func(t *testing.T) {
		// Varint encoding of a huge count; this would previously panic in
		// make() with "cap out of range".
		var buf [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(buf[:], ^uint64(0))
		_, ok := DecodeBlobFileIDs(buf[:n])
		require.False(t, ok)
	})

	t.Run("count-exceeds-remaining", func(t *testing.T) {
		// Count says 5 but only 2 trailing bytes are present.
		data := []byte{0x05, 0x01, 0x02}
		_, ok := DecodeBlobFileIDs(data)
		require.False(t, ok)
	})

	t.Run("truncated-body", func(t *testing.T) {
		// Count=3, but only one valid varint follows.
		data := []byte{0x03, 0x01}
		_, ok := DecodeBlobFileIDs(data)
		require.False(t, ok)
	})

	t.Run("round-trip", func(t *testing.T) {
		ids := []base.BlobFileID{1, 7, 128, 65535}
		var buf []byte
		var tmp [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(tmp[:], uint64(len(ids)))
		buf = append(buf, tmp[:n]...)
		for _, id := range ids {
			n := binary.PutUvarint(tmp[:], uint64(id))
			buf = append(buf, tmp[:n]...)
		}
		got, ok := DecodeBlobFileIDs(buf)
		require.True(t, ok)
		require.Equal(t, ids, got)
	})
}

func TestReaderNextTruncated(t *testing.T) {
	// Each of these is a corrupt or truncated batch payload (header stripped).
	// Reader.Next must return ok=false with a non-nil error and not panic.
	kindSet := byte(base.InternalKeyKindSet)
	cases := [][]byte{
		{kindSet},                       // kind only, no key
		{kindSet, 0xff},                 // partial varint
		{kindSet, 0x05, 'a', 'b'},       // key length says 5, only 2 bytes
		{kindSet, 0x01, 'k'},            // key ok, missing value
		{kindSet, 0x01, 'k', 0x05, 'v'}, // key ok, value length says 5, only 1 byte
	}
	for i, data := range cases {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			r := Reader(append([]byte(nil), data...))
			_, _, _, ok, err := r.Next()
			require.False(t, ok)
			require.Error(t, err)
		})
	}
}
