// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/crbytes"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/internal/treeprinter"
	"github.com/stretchr/testify/require"
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
					keyPrefixLenSharedWithPrev = crbytes.CommonPrefix(builder.UnsafeGet(builder.nKeys-1), k)
				}
				p := []byte(k)
				builder.Put(p, keyPrefixLenSharedWithPrev)
				keys++
				sizeAtRowCount = append(sizeAtRowCount, builder.Size(keys, 0))
			}
			fmt.Fprint(&out, builder.debugString(0))
			return out.String()
		case "unsafe-get":
			var indices []int
			td.ScanArgs(t, "i", &indices)
			for _, i := range indices {
				fmt.Fprintf(&out, "UnsafeGet(%d) = %s\n", i, builder.UnsafeGet(i))
			}
			return out.String()
		case "finish":
			var rows int
			td.ScanArgs(t, "rows", &rows)
			// NB: We add 1 to the size because Go pointer rules require all
			// pointers to point into a valid allocation. Without the extra
			// byte, the final offset would point just past the end of the
			// allocation, upsetting -d=checkptr. In actual columnar blocks, the
			// last byte of the block is a versioning byte, so individual
			// columns will never need to point beyond the allocation.
			buf := make([]byte, sizeAtRowCount[rows]+1)
			offset := builder.Finish(0, rows, 0, buf)
			require.Equal(t, uint32(len(buf)-1), offset)

			f := binfmt.New(buf)
			tp := treeprinter.New()
			prefixBytesToBinFormatter(f, tp.Child("prefix-bytes"), rows, nil)
			var endOffset uint32
			pb, endOffset = DecodePrefixBytes(buf, 0, rows)
			require.Equal(t, offset, endOffset)
			require.Equal(t, rows, pb.Rows())
			return tp.String()
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
		case "bundle-prefixes":
			for i := 0; i < pb.BundleCount(); i++ {
				fmt.Fprintf(&out, "%d: %q\n", i, pb.BundlePrefix(i))
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

	var pbb PrefixBytesBuilder
	runTest := func(t *testing.T, maxKeyCount, maxKeyLen, maxAlphaLen int) {
		rng := rand.New(rand.NewPCG(0, seed))
		randInt := func(lo, hi int) int {
			return lo + rng.IntN(hi-lo)
		}
		minLen := randInt(4, maxKeyLen)
		maxLen := randInt(4, maxKeyLen)
		if maxLen < minLen {
			minLen, maxLen = maxLen, minLen
		}
		blockPrefixLen := randInt(1, minLen+1)
		userKeys := make([][]byte, randInt(2, maxKeyCount))
		// Create the first user key.
		userKeys[0] = make([]byte, randInt(minLen, maxLen+1))
		totalSize := len(userKeys[0])
		for j := range userKeys[0] {
			userKeys[0][j] = byte(randInt(int('a'), int('a')+maxAlphaLen))
		}
		// Create the remainder of the user keys, giving them the same block prefix
		// of length [blockPrefixLen].
		for i := 1; i < len(userKeys); i++ {
			userKeys[i] = make([]byte, randInt(minLen, maxLen+1))
			totalSize += len(userKeys[i])
			copy(userKeys[i], userKeys[0][:blockPrefixLen])
			for j := blockPrefixLen; j < len(userKeys[i]); j++ {
				userKeys[i][j] = byte(randInt(int('a'), int('a')+maxAlphaLen))
			}
		}
		slices.SortFunc(userKeys, bytes.Compare)

		bundleShift := randInt(1, 4)
		// Reset rather than Init-ing when possible to exercise the Reset code path.
		if pbb.bundleShift == uint32(bundleShift) {
			pbb.Reset()
		} else {
			pbb.Init(1 << bundleShift)
		}

		for i := 0; i < len(userKeys); i++ {
			keyPrefixLenSharedWithPrev := 0
			if i > 0 {
				keyPrefixLenSharedWithPrev = crbytes.CommonPrefix(userKeys[i-1], userKeys[i])
			}
			pbb.Put(userKeys[i], keyPrefixLenSharedWithPrev)
		}

		n := len(userKeys)
		// Sometimes finish all but the last key.
		if rng.IntN(2) == 0 {
			n--
			t.Logf("Omitting the last key %q; new last key is %q", userKeys[len(userKeys)-1], userKeys[n-1])
		}

		size := pbb.Size(n, 0)
		buf := make([]byte, size+1 /* +1 to avoid checkptr issues */)
		offset := pbb.Finish(0, n, 0, buf)
		if uint32(size) != offset {
			t.Fatalf("bb.Size(...) computed %d, but bb.Finish(...) produced slice of len %d",
				size, offset)
		}
		require.Equal(t, size, offset)
		t.Logf("Size: %d; NumUserKeys: %d (%d); Aggregate pre-compressed string data: %d",
			size, n, len(userKeys), totalSize)

		pb, endOffset := DecodePrefixBytes(buf, 0, n)
		require.Equal(t, offset, endOffset)
		f := binfmt.New(buf)
		tp := treeprinter.New()
		prefixBytesToBinFormatter(f, tp.Child("prefix-bytes"), n, nil)
		t.Log(tp.String())

		k := slices.Clone(pb.SharedPrefix())
		l := len(k)
		perm := rng.Perm(n)
		for _, j := range perm {
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
	for _, maxKeyCount := range []int{10, 25, 50, 100, 1000, 10000} {
		for _, maxKeyLen := range []int{10, 100} {
			for _, maxAlphaLen := range []int{2, 5, 26} {
				t.Run(fmt.Sprintf("maxKeyCount=%d,maxKeyLen=%d,maxAlphaLen=%d", maxKeyCount, maxKeyLen, maxAlphaLen), func(t *testing.T) {
					runTest(t, maxKeyCount, maxKeyLen, maxAlphaLen)
				})
			}
		}
	}
}

func TestPrefixBytesBuilder_UnsafeGet(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("Seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))
	ks := testkeys.Alpha(2)
	var pbb PrefixBytesBuilder
	pbb.Init(8)

	var prevKey []byte
	var prevPrevKey []byte
	for i := 0; i < int(ks.Count()); i++ {
		versionCount := int(rng.Int64N(10))
		k := testkeys.Key(ks, uint64(i))
		for j := 0; j < versionCount; j++ {
			rows := pbb.Rows()
			if rows >= 1 && string(prevKey) != string(pbb.UnsafeGet(rows-1)) {
				t.Fatalf("pbb.UnsafeGet(%d) = %q; expected previous key %q", rows-1, pbb.UnsafeGet(rows-1), prevKey)
			}
			if rows >= 2 && string(prevPrevKey) != string(pbb.UnsafeGet(rows-2)) {
				t.Fatalf("pbb.UnsafeGet(%d) = %q; expected key before previous %q", rows-2, pbb.UnsafeGet(rows-2), prevPrevKey)
			}
			t.Logf("%d: Put(%q)", rows, k)
			pbb.Put(k, crbytes.CommonPrefix(k, prevKey))
			prevPrevKey = prevKey
			prevKey = k
		}
	}
}

func BenchmarkPrefixBytes(b *testing.B) {
	runBenchmark := func(b *testing.B, alphaLen int) {
		seed := uint64(205295296)
		rng := rand.New(rand.NewPCG(0, seed))
		randInt := func(lo, hi int) int {
			return lo + rng.IntN(hi-lo)
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
			userKeys[0][j] = byte(randInt(int('a'), int('a')+alphaLen))
		}
		// Create the remainder of the user keys, giving them the same block prefix
		// of length [blockPrefixLen].
		for i := 1; i < len(userKeys); i++ {
			userKeys[i] = make([]byte, randInt(minLen, maxLen+1))
			copy(userKeys[i], userKeys[0][:blockPrefixLen])
			for j := blockPrefixLen; j < len(userKeys[i]); j++ {
				userKeys[i][j] = byte(randInt(int('a'), int('a')+alphaLen))
			}
		}
		slices.SortFunc(userKeys, bytes.Compare)

		var pbb PrefixBytesBuilder
		pbb.Init(16)
		var buf []byte
		build := func(n int) []byte {
			pbb.Reset()
			for i := 0; i < n; i++ {
				keyPrefixLenSharedWithPrev := 0
				if i > 0 {
					keyPrefixLenSharedWithPrev = crbytes.CommonPrefix(userKeys[i-1], userKeys[i])
				}
				pbb.Put(userKeys[i], keyPrefixLenSharedWithPrev)
			}
			size := pbb.Size(n, 0)
			if cap(buf) < int(size) {
				buf = make([]byte, size)
			} else {
				buf = buf[:size]
			}
			_ = pbb.Finish(0, n, 0, buf)
			return buf
		}

		b.Run("building", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				data := build(len(userKeys))
				fmt.Fprint(io.Discard, data)
			}
		})

		b.Run("iteration", func(b *testing.B) {
			n := len(userKeys)
			buf = build(n)
			pb, _ := DecodePrefixBytes(buf, 0, n)
			b.ResetTimer()
			var pbi PrefixBytesIter
			pbi.Buf = make([]byte, 0, maxLen)
			for i := 0; i < b.N; i++ {
				j := i % n
				if j == 0 {
					pb.SetAt(&pbi, j)
				} else {
					pb.SetNext(&pbi)
				}
				if invariants.Enabled && !bytes.Equal(pbi.Buf, userKeys[j]) {
					b.Fatalf("Constructed key %q (%q, %q, %q) for index %d; expected %q",
						pbi.Buf, pb.SharedPrefix(), pb.RowBundlePrefix(j), pb.RowSuffix(j), j, userKeys[j])
				}
			}
		})
	}
	for _, alphaLen := range []int{2, 5, 26} {
		b.Run(fmt.Sprintf("alphaLen=%d", alphaLen), func(b *testing.B) {
			runBenchmark(b, alphaLen)
		})
	}
}

func (b *PrefixBytesBuilder) debugString(offset uint32) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Size: %d", b.Size(b.nKeys, offset))
	fmt.Fprintf(&sb, "\nnKeys=%d; bundleSize=%d", b.nKeys, b.bundleSize)
	sz := &b.sizings[(b.nKeys+1)%2]
	fmt.Fprintf(&sb, "\nblockPrefixLen=%d; currentBundleLen=%d; currentBundleKeys=%d",
		sz.blockPrefixLen, sz.currentBundleDistinctLen, sz.currentBundleDistinctKeys)
	fmt.Fprint(&sb, "\nOffsets:")
	for i := 0; i < b.offsets.count; i++ {
		if i%10 == 0 {
			fmt.Fprintf(&sb, "\n  %04d", b.offsets.elems[i])
		} else {
			fmt.Fprintf(&sb, "  %04d", b.offsets.elems[i])
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
