// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func TestPrefixBytes(t *testing.T) {
	var out bytes.Buffer
	var pb PrefixBytes
	var builder PrefixBytesBuilder
	var keys int
	var getBuf []byte
	var sizeAtRowCount []uint32

	datadriven.RunTest(t, "testdata/prefix_bytes", func(t *testing.T, td *datadriven.TestData) string {
		out.Reset()
		switch td.Cmd {
		case "init":
			var bundleSize int
			td.ScanArgs(t, "bundle-size", &bundleSize)
			builder.Init(bundleSize)

			keys = 0
			size := builder.Size(keys, 0)
			sizeAtRowCount = append(sizeAtRowCount[:0], size)
			fmt.Fprintf(&out, "Size: %d", size)
			return out.String()
		case "put":
			inputKeys := bytes.Split([]byte(strings.TrimSpace(td.Input)), []byte{'\n'})
			for _, k := range inputKeys {
				keyPrefixLenSharedWithPrev := len(k)
				if builder.nKeys > 0 {
					keyPrefixLenSharedWithPrev = bytesSharedPrefix(builder.PrevKey(), k)
				}
				p := []byte(k)
				builder.Put(p, keyPrefixLenSharedWithPrev)
				keys++
				sizeAtRowCount = append(sizeAtRowCount, builder.Size(keys, 0))
			}
			fmt.Fprint(&out, builder.debugString(0))
			return out.String()
		case "finish":
			var rows int
			td.ScanArgs(t, "rows", &rows)
			buf := make([]byte, sizeAtRowCount[rows])
			offset, desc := builder.Finish(0, rows, 0, buf)
			require.Equal(t, uint32(len(buf)), offset)

			f := binfmt.New(buf)
			prefixBytesToBinFormatter(f, rows, desc.Encoding, nil)
			pb = MakePrefixBytes(rows, buf, 0, desc.Encoding)
			return f.String()
		case "get":
			var indices []int
			td.ScanArgs(t, "indices", &indices)

			getBuf = append(getBuf[:0], pb.SharedPrefix()...)
			l := len(getBuf)
			for _, i := range indices {
				getBuf = append(append(getBuf[:l], pb.RowBundlePrefix(i)...), pb.RowSuffix(i)...)
				fmt.Fprintf(&out, "%s\n", getBuf)
			}
			return out.String()
		case "search":
			for _, l := range strings.Split(strings.TrimSpace(td.Input), "\n") {
				i, eq := pb.Search([]byte(l))
				fmt.Fprintf(&out, "Search(%q) = (%d, %t)\n", l, i, eq)
			}
			return out.String()
		default:
			panic(fmt.Sprintf("unrecognized command %q", td.Cmd))
		}
	})
}

func TestPrefixBytesRandomized(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("Seed: %d", seed)

	runTest := func(t *testing.T, maxKeyCount, maxKeyLen int) {
		rng := rand.New(rand.NewSource(seed))
		randInt := func(lo, hi int) int {
			return lo + rng.Intn(hi-lo)
		}
		minLen := randInt(4, maxKeyLen)
		maxLen := randInt(4, maxKeyLen)
		if maxLen < minLen {
			minLen, maxLen = maxLen, minLen
		}
		blockPrefixLen := randInt(1, minLen+1)
		userKeys := make([][]byte, randInt(1, maxKeyCount))
		// Create the first user key.
		userKeys[0] = make([]byte, randInt(minLen, maxLen+1))
		totalSize := len(userKeys[0])
		for j := range userKeys[0] {
			userKeys[0][j] = byte(randInt(int('a'), int('z')+1))
		}
		// Create the remainder of the user keys, giving them the same block prefix
		// of length [blockPrefixLen].
		for i := 1; i < len(userKeys); i++ {
			userKeys[i] = make([]byte, randInt(minLen, maxLen+1))
			totalSize += len(userKeys[i])
			copy(userKeys[i], userKeys[0][:blockPrefixLen])
			for j := blockPrefixLen; j < len(userKeys[i]); j++ {
				userKeys[i][j] = byte(randInt(int('a'), int('z')+1))
			}
		}
		slices.SortFunc(userKeys, bytes.Compare)

		var pbb PrefixBytesBuilder
		pbb.Init(1 << randInt(1, 4))
		for i := 0; i < len(userKeys); i++ {
			keyPrefixLenSharedWithPrev := 0
			if i > 0 {
				keyPrefixLenSharedWithPrev = bytesSharedPrefix(userKeys[i-1], userKeys[i])
			}
			pbb.Put(userKeys[i], keyPrefixLenSharedWithPrev)
		}

		n := len(userKeys)
		// Sometimes finish all but the last key.
		if rng.Intn(2) == 0 {
			n--
		}

		size := pbb.Size(n, 0)
		buf := make([]byte, size)
		offset, desc := pbb.Finish(0, n, 0, buf)
		if uint32(size) != offset {
			t.Fatalf("bb.Size(...) computed %d, but bb.Finish(...) produced slice of len %d",
				size, offset)
		}
		require.Equal(t, uint32(len(buf)), offset)
		t.Logf("Size: %d; NumUserKeys: %d; Encoding: %s; Aggregate pre-compressed string data: %d",
			size, n, desc.Encoding, totalSize)

		pb := MakePrefixBytes(n, buf, 0, desc.Encoding)

		k := append([]byte(nil), pb.SharedPrefix()...)
		l := len(k)
		for i := 0; i < min(10000, n); i++ {
			j := rand.Intn(n)

			// Ensure that reconstructing the key from the prefix bytes interface
			// produces an identical key.
			k = append(append(k[:l], pb.RowBundlePrefix(j)...), pb.RowSuffix(j)...)
			if !bytes.Equal(k, userKeys[j]) {
				t.Fatalf("Constructed key %q (%q, %q, %q) for index %d; expected %q",
					k, pb.SharedPrefix(), pb.RowBundlePrefix(j), pb.RowSuffix(j), j, userKeys[j])
			}
			// Ensure that searching for the key finds a match.
			idx, equal := pb.Search(userKeys[j])
			require.True(t, equal)
			// NB: idx may be less than j if there are duplicate keys. But if so,
			// the key at index j should still be equal to the key at index idx.
			require.LessOrEqual(t, idx, j)
			require.Equal(t, userKeys[idx], userKeys[j])
		}
	}
	for _, maxKeyCount := range []int{10, 100, 1000, 10000} {
		for _, maxKeyLen := range []int{10, 100} {
			t.Run(fmt.Sprintf("maxKeyCount=%d,maxKeyLen=%d", maxKeyCount, maxKeyLen), func(t *testing.T) {
				runTest(t, maxKeyCount, maxKeyLen)
			})
		}
	}
}

func BenchmarkPrefixBytes(b *testing.B) {
	seed := uint64(205295296)
	rng := rand.New(rand.NewSource(seed))
	randInt := func(lo, hi int) int {
		return lo + rng.Intn(hi-lo)
	}
	minLen := 8
	maxLen := 128
	if maxLen < minLen {
		minLen, maxLen = maxLen, minLen
	}
	blockPrefixLen := 6
	userKeys := make([][]byte, 1000)
	// Create the first user key.
	userKeys[0] = make([]byte, randInt(minLen, maxLen+1))
	for j := range userKeys[0] {
		userKeys[0][j] = byte(randInt(int('a'), int('z')+1))
	}
	// Create the remainder of the user keys, giving them the same block prefix
	// of length [blockPrefixLen].
	for i := 1; i < len(userKeys); i++ {
		userKeys[i] = make([]byte, randInt(minLen, maxLen+1))
		copy(userKeys[i], userKeys[0][:blockPrefixLen])
		for j := blockPrefixLen; j < len(userKeys[i]); j++ {
			userKeys[i][j] = byte(randInt(int('a'), int('z')+1))
		}
	}
	slices.SortFunc(userKeys, bytes.Compare)

	var pbb PrefixBytesBuilder
	pbb.Init(16)
	var buf []byte
	var enc ColumnEncoding
	build := func(n int) ([]byte, ColumnEncoding) {
		pbb.Reset()
		for i := 0; i < n; i++ {
			keyPrefixLenSharedWithPrev := 0
			if i > 0 {
				keyPrefixLenSharedWithPrev = bytesSharedPrefix(userKeys[i-1], userKeys[i])
			}
			pbb.Put(userKeys[i], keyPrefixLenSharedWithPrev)
		}
		size := pbb.Size(n, 0)
		if cap(buf) < int(size) {
			buf = make([]byte, size)
		} else {
			buf = buf[:size]
		}
		_, desc := pbb.Finish(0, n, 0, buf)
		return buf, desc.Encoding
	}

	b.Run("building", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			data, _ := build(len(userKeys))
			fmt.Fprint(io.Discard, data)
		}
	})

	b.Run("iteration", func(b *testing.B) {
		n := len(userKeys)
		buf, enc = build(n)
		pb := MakePrefixBytes(n, buf, 0, enc)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			k := append([]byte(nil), pb.SharedPrefix()...)
			l := len(k)
			for i := 0; i < n; i++ {
				j := rand.Intn(n)
				k = append(append(k[:l], pb.RowBundlePrefix(j)...), pb.RowSuffix(j)...)
				if !bytes.Equal(k, userKeys[j]) {
					b.Fatalf("Constructed key %q (%q, %q, %q) for index %d; expected %q",
						k, pb.SharedPrefix(), pb.RowBundlePrefix(j), pb.RowSuffix(j), j, userKeys[j])
				}
			}
		}
	})
}

func (b *PrefixBytesBuilder) debugString(offset uint32) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Size: %d", b.Size(b.nKeys, offset))
	fmt.Fprintf(&sb, "\nnKeys=%d; bundleSize=%d", b.nKeys, b.bundleSize)
	if b.bundleSize > 0 {
		fmt.Fprintf(&sb, "\nblockPrefixLen=%d; currentBundleLen=%d; currentBundleKeys=%d",
			b.blockPrefixLen, b.currentBundleLen, b.currentBundleKeys)
	}
	fmt.Fprint(&sb, "\nOffsets:")
	for i := 0; i < b.offsets.count; i++ {
		if i%10 == 0 {
			fmt.Fprintf(&sb, "\n  %04d", b.offsets.elems.At(i))
		} else {
			fmt.Fprintf(&sb, "  %04d", b.offsets.elems.At(i))
		}
	}
	fmt.Fprintf(&sb, "\nData (len=%d):\n", len(b.data))
	wrapStr(&sb, string(b.data), 60)
	return sb.String()
}

func wrapStr(w io.Writer, s string, width int) {
	for len(s) > 0 {
		n := min(width, len(s))
		fmt.Fprint(w, s[:n])
		s = s[n:]
		if len(s) > 0 {
			fmt.Fprintln(w)
		}
	}
}