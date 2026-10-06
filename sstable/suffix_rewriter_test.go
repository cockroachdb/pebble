package sstable

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/stretchr/testify/require"
)

func TestRewriteSuffixProps(t *testing.T) {
	defer leaktest.AfterTest(t)()

	seed := uint64(time.Now().UnixNano())
	t.Logf("seed %d", seed)

	// This test rewrites a test from suffix @212 to @645. Since the [from] and
	// [to] suffixes are fixed, we also can fix the expected properties for the
	// various test collectors this test uses.
	const keyCount = 1e5
	const rangeKeyCount = 100
	from, to := []byte("@212"), []byte("@645")
	allExpectedProps := map[string][]byte{
		"count":   []byte(strconv.Itoa(keyCount + rangeKeyCount)),
		"parity":  encodeBlockInterval(BlockInterval{1, 2}, []byte{}),
		"log10":   encodeBlockInterval(BlockInterval{3, 4}, []byte{}),
		"onebits": encodeBlockInterval(BlockInterval{4, 5}, []byte{}),
	}

	// Test suffix rewriting from every table format.
	for format := TableFormatPebblev2; format <= TableFormatMax; format++ {
		t.Run(format.String(), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(0, seed))
			// Construct a test sstable.
			wOpts := WriterOptions{
				FilterPolicy:     bloom.FilterPolicy(10),
				Comparer:         testkeys.Comparer,
				KeySchema:        &testkeysSchema,
				TableFormat:      format,
				IsStrictObsolete: format >= TableFormatPebblev3,
			}
			// Pick a random subset of available test collectors.
			var originalCollectors []string
			originalCollectors, wOpts.BlockPropertyCollectors = randomTestCollectors(rng)
			sst := makeTestkeySSTable(t, wOpts, []byte(from), keyCount, rangeKeyCount)

			// Create the rewrite options.
			rwOpts := wOpts
			// NB: Although we set the table format to a random value, the
			// suffix rewriting routine will ignore it and rewrite to the same
			// table format as the original sstable.
			rwOpts.TableFormat = TableFormatPebblev2 + TableFormat(rng.IntN(int(TableFormatMax-TableFormatPebblev2)+1))
			rwOpts.IsStrictObsolete = rwOpts.TableFormat >= TableFormatPebblev3
			// Rewrite with a random subset of the original collectors, in a
			// random order.
			newCollectors := slices.Clone(originalCollectors)
			rng.Shuffle(len(newCollectors), func(i, j int) {
				newCollectors[i], newCollectors[j] = newCollectors[j], newCollectors[i]
			})
			newCollectors = newCollectors[:rng.IntN(len(newCollectors)+1)]
			rwOpts.BlockPropertyCollectors = testCollectorsByNames(newCollectors...)
			expectedProps := make(map[string][]byte)
			for _, collector := range newCollectors {
				expectedProps[collector] = allExpectedProps[collector]
			}

			t.Logf("from format %s, to format %s", format.String(), rwOpts.TableFormat.String())
			t.Logf("from collectors %s to collectors %s",
				strings.Join(originalCollectors, ","), strings.Join(newCollectors, ","))

			// Rewrite the SST using updated options and check the returned props.
			readerOpts := ReaderOptions{
				Comparer:   wOpts.Comparer,
				KeySchemas: KeySchemas{wOpts.KeySchema.Name: wOpts.KeySchema},
				Filters:    map[string]base.FilterPolicy{wOpts.FilterPolicy.Name(): wOpts.FilterPolicy},
			}
			r, err := NewMemReader(sst, readerOpts)
			require.NoError(t, err)
			defer r.Close()

			var sstBytes [2][]byte
			for i, byBlocks := range []bool{false, true} {
				t.Run(fmt.Sprintf("byBlocks=%v", byBlocks), func(t *testing.T) {
					fn := func() {
						rewrittenSST := &objstorage.MemObj{}
						if byBlocks {
							_, rewriteFormat, err := rewriteKeySuffixesInBlocks(
								r, sst, rewrittenSST, rwOpts, from, to, 8)
							require.NoError(t, err)
							// rewriteFormat is equal to the original format, since
							// rwOpts.TableFormat is ignored.
							require.Equal(t, wOpts.TableFormat, rewriteFormat)
						} else {
							_, err := RewriteKeySuffixesViaWriter(r, rewrittenSST, rwOpts, from, to)
							require.NoError(t, err)
						}

						sstBytes[i] = rewrittenSST.Data()
						// Check that a reader on the rewritten STT has the expected props.
						rRewritten, err := NewMemReader(rewrittenSST.Data(), readerOpts)
						require.NoError(t, err)
						defer rRewritten.Close()

						foundValues := make(map[string][]byte)
						for k, v := range rRewritten.Properties.UserProperties {
							if k == "obsolete-key" {
								continue
							}
							require.Contains(t, newCollectors, k)
							require.Equal(t, uint8(slices.Index(newCollectors, k)), v[0], "shortID should match")
							foundValue := []byte(v[1:])
							foundValues[k] = foundValue
							t.Logf("%q => %q", k, foundValues[k])
						}
						require.Equal(t, expectedProps, foundValues)
						require.False(t, rRewritten.Properties.IsStrictObsolete)

						// Compare the block level props from the data blocks in the layout,
						// only if we did not do a rewrite from one format to another. If the
						// format changes, the block boundaries change slightly.
						if !byBlocks && wOpts.TableFormat != rwOpts.TableFormat {
							return
						}
						layout, err := r.Layout()
						require.NoError(t, err)
						newLayout, err := rRewritten.Layout()
						require.NoError(t, err)

						for i := range layout.Data {
							oldProps := make([][]byte, len(wOpts.BlockPropertyCollectors))
							oldDecoder := makeBlockPropertiesDecoder(len(oldProps), layout.Data[i].Props)
							for !oldDecoder.Done() {
								id, val, err := oldDecoder.Next()
								require.NoError(t, err)
								oldProps[id] = val
							}
							newProps := make([][]byte, len(newCollectors))
							newDecoder := makeBlockPropertiesDecoder(len(newProps), newLayout.Data[i].Props)
							for !newDecoder.Done() {
								id, val, err := newDecoder.Next()
								require.NoError(t, err)
								newProps[id] = val
								switch newCollectors[id] {
								case "count":
									require.Equal(t, oldProps[slices.Index(originalCollectors, "count")], val)
								default:
									require.Equal(t, allExpectedProps[newCollectors[id]], val)
								}
							}
						}
					}
					// Perform the rewrite multiple times. This helps ensure
					// idempotence. This helps catch bugs in suffix rewriting
					// that might mangle the in-memory source sstable's buffer
					// by improperly assuming that Write/WriteTo leaves the
					// input buffer unmodified.
					for j := 0; j < 5; j++ {
						fn()
					}
				})
			}
			if wOpts.TableFormat == rwOpts.TableFormat {
				// Both methods of rewriting should produce the same result.
				require.Equal(t, sstBytes[0], sstBytes[1])
			}
		})
	}
}

func makeTestkeySSTable(
	t testing.TB, writerOpts WriterOptions, suffix []byte, keys int, rangeKeys int,
) []byte {
	alphabet := testkeys.Alpha(8)

	const sharedPrefix = `sharedprefixamongallkeys`
	keyBuf := make([]byte, len(sharedPrefix)+alphabet.MaxLen()+testkeys.MaxSuffixLen)
	copy(keyBuf[:0], []byte(sharedPrefix))
	endKeyBuf := make([]byte, len(sharedPrefix)+alphabet.MaxLen())
	copy(endKeyBuf[:0], []byte(sharedPrefix))

	f := &objstorage.MemObj{}
	w := NewWriter(f, writerOpts)
	for i := 0; i < keys; i++ {
		n := testkeys.WriteKey(keyBuf[len(sharedPrefix):], alphabet, int64(i))
		key := append(keyBuf[:len(sharedPrefix)+n], suffix...)
		err := w.Raw().Add(
			base.MakeInternalKey(key, 0, InternalKeyKindSet), key, false)
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < rangeKeys; i++ {
		n := testkeys.WriteKey(keyBuf[len(sharedPrefix):], alphabet, int64(i))
		key := keyBuf[:len(sharedPrefix)+n]

		// 16-byte shared prefix
		n = testkeys.WriteKey(endKeyBuf[len(sharedPrefix):], alphabet, int64(i+1))
		endKey := endKeyBuf[:len(sharedPrefix)+n]
		if err := w.RangeKeySet(key, endKey, suffix, key); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	return f.Data()
}

func BenchmarkRewriteSST(b *testing.B) {
	from, to := []byte("@123"), []byte("@456")
	writerOpts := WriterOptions{
		FilterPolicy: bloom.FilterPolicy(10),
		Comparer:     test4bSuffixComparer,
		TableFormat:  TableFormatPebblev2,
	}

	sizes := []int{100, 10000, 1e6}
	compressions := []block.Compression{block.NoCompression, block.SnappyCompression}

	files := make([][]*Reader, len(compressions))
	sstBytes := make([][][]byte, len(compressions))

	for comp := range compressions {
		files[comp] = make([]*Reader, len(sizes))

		for size := range sizes {
			writerOpts.Compression = compressions[comp]
			sstBytes[comp][size] = makeTestkeySSTable(b, writerOpts, from, sizes[size], 0 /* rangeKeys */)
			r, err := NewMemReader(sstBytes[comp][size], ReaderOptions{
				Comparer: test4bSuffixComparer,
				Filters:  map[string]base.FilterPolicy{writerOpts.FilterPolicy.Name(): writerOpts.FilterPolicy},
			})
			if err != nil {
				b.Fatal(err)
			}
			files[comp][size] = r
		}
	}

	b.ResetTimer()
	for comp := range compressions {
		b.Run(compressions[comp].String(), func(b *testing.B) {
			for sz := range sizes {
				r := files[comp][sz]
				sst := sstBytes[comp][sz]
				b.Run(fmt.Sprintf("keys=%d", sizes[sz]), func(b *testing.B) {
					b.Run("ReaderWriterLoop", func(b *testing.B) {
						b.SetBytes(int64(len(sst)))
						for i := 0; i < b.N; i++ {
							if _, err := RewriteKeySuffixesViaWriter(r, &discardFile{}, writerOpts, from, to); err != nil {
								b.Fatal(err)
							}
						}
					})
					for _, concurrency := range []int{1, 2, 4, 8, 16} {
						b.Run(fmt.Sprintf("RewriteKeySuffixes,concurrency=%d", concurrency), func(b *testing.B) {
							b.SetBytes(int64(len(sst)))
							for i := 0; i < b.N; i++ {
								if _, _, err := rewriteKeySuffixesInBlocks(r, sst, &discardFile{}, writerOpts, []byte("_123"), []byte("_456"), concurrency); err != nil {
									b.Fatal(err)
								}
							}
						})
					}
				})
			}
		})
	}
}

// TestRewriteSuffixesMaxUserKeySize tests the MaxUserKeySize property of tables
// produced by suffix rewriting. The block-based rewriter derives the property
// from the original table's property (resulting in an upper bound), whereas
// RewriteKeySuffixesViaWriter recomputes it exactly.
func TestRewriteSuffixesMaxUserKeySize(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name string
		// points are the point keys, without suffix; each point key gets the
		// `from` suffix.
		points []string
		// rangeKeys are the bounds of the range keys; each range key is a
		// RANGEKEYSET with the `from` suffix.
		rangeKeys [][2]string
		from, to  string
		// sourceLacksProp simulates a source table written by a Pebble version
		// that did not record the property.
		sourceLacksProp bool
		// expectedInBlocks is the expected property of the table produced by
		// RewriteKeySuffixesAndReturnFormat (0 means absent).
		expectedInBlocks uint64
		// expectedExact is the actual size of the largest key after rewriting;
		// this is also the expected property of the table produced by
		// RewriteKeySuffixesViaWriter.
		expectedExact uint64
	}{
		{
			name:             "same-suffix-length",
			points:           []string{"a", "bbbbbbbb", "c"},
			from:             "@1",
			to:               "@2",
			expectedInBlocks: 10,
			expectedExact:    10,
		},
		{
			name:             "longer-suffix",
			points:           []string{"a", "bbbbbbbb", "c"},
			from:             "@1",
			to:               "@12345",
			expectedInBlocks: 14,
			expectedExact:    14,
		},
		{
			name:             "shorter-suffix",
			points:           []string{"a", "bbbbbbbb", "c"},
			from:             "@12345",
			to:               "@1",
			expectedInBlocks: 10,
			expectedExact:    10,
		},
		{
			name:             "point-max-with-range-keys",
			points:           []string{"a", "bbbbbbbbbbbb"},
			rangeKeys:        [][2]string{{"c", "dddd"}},
			from:             "@1",
			to:               "@123",
			expectedInBlocks: 16,
			expectedExact:    16,
		},
		{
			// The original maximum comes from a range key bound, which is not
			// affected by the rewrite; the block-based rewriter can't tell, so
			// it produces an upper bound.
			name:             "range-key-max-longer-suffix",
			points:           []string{"a"},
			rangeKeys:        [][2]string{{"cccccccccc", "d"}},
			from:             "@1",
			to:               "@1234",
			expectedInBlocks: 13,
			expectedExact:    10,
		},
		{
			name:             "range-key-max-shorter-suffix",
			points:           []string{"a"},
			rangeKeys:        [][2]string{{"c", "dddddddddd"}},
			from:             "@1234",
			to:               "@1",
			expectedInBlocks: 10,
			expectedExact:    10,
		},
		{
			// The range keys are re-encoded, so the property is exact.
			name:             "range-keys-only",
			rangeKeys:        [][2]string{{"a", "b"}, {"c", "dddddd"}},
			from:             "@1",
			to:               "@123",
			expectedInBlocks: 6,
			expectedExact:    6,
		},
		{
			// The range keys are re-encoded, so the property is exact even if the
			// source table has a longer bound.
			name:             "range-keys-only-shorter-suffix",
			rangeKeys:        [][2]string{{"a", "b"}},
			from:             "@12345",
			to:               "@1",
			expectedInBlocks: 1,
			expectedExact:    1,
		},
		{
			// The range keys are re-encoded, so the property is computed even if
			// the source table lacked it.
			name:             "range-keys-only-source-lacks-prop",
			rangeKeys:        [][2]string{{"a", "b"}, {"c", "dddddd"}},
			from:             "@1",
			to:               "@123",
			sourceLacksProp:  true,
			expectedInBlocks: 6,
			expectedExact:    6,
		},
		{
			name:             "points-source-lacks-prop",
			points:           []string{"a", "bbbbbbbb", "c"},
			from:             "@1",
			to:               "@123",
			sourceLacksProp:  true,
			expectedInBlocks: 0,
			expectedExact:    12,
		},
		{
			// The point keys are copied at the block level, so if the source
			// lacks the property, the output must lack it too, even though the
			// range keys are re-encoded.
			name:             "points-and-range-keys-source-lacks-prop",
			points:           []string{"a"},
			rangeKeys:        [][2]string{{"cccccccccc", "d"}},
			from:             "@1",
			to:               "@123",
			sourceLacksProp:  true,
			expectedInBlocks: 0,
			expectedExact:    10,
		},
	}

	readerOpts := ReaderOptions{
		Comparer:   testkeys.Comparer,
		KeySchemas: KeySchemas{testkeysSchema.Name: &testkeysSchema},
	}
	// checkOutput verifies the property of the rewritten table.
	checkOutput := func(
		t *testing.T, meta *WriterMetadata, sst []byte, expected, expectedExact uint64,
	) {
		t.Helper()
		r, err := NewMemReader(sst, readerOpts)
		require.NoError(t, err)
		defer func() { require.NoError(t, r.Close()) }()
		// Sanity check expectedExact against the table contents.
		require.Equal(t, expectedExact, maxUserKeySizeFromContents(t, r))
		got, present := readMaxUserKeySizeProp(t, r)
		require.Equal(t, got, meta.Properties.MaxUserKeySize)
		require.Equal(t, expected, got)
		require.Equal(t, expected != 0, present)
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			minFormat := TableFormatPebblev1
			if len(tc.rangeKeys) > 0 {
				minFormat = TableFormatPebblev2
			}
			for tf := minFormat; tf <= TableFormatMax; tf++ {
				t.Run(tf.String(), func(t *testing.T) {
					wOpts := WriterOptions{
						Comparer:    testkeys.Comparer,
						KeySchema:   &testkeysSchema,
						TableFormat: tf,
						// Use small blocks to get multiple data blocks.
						BlockSize:      1,
						IndexBlockSize: 1,
					}
					// Build the source table.
					obj := &objstorage.MemObj{}
					w := NewRawWriter(obj, wOpts)
					for _, p := range tc.points {
						k := base.MakeInternalKey([]byte(p+tc.from), 1, InternalKeyKindSet)
						require.NoError(t, w.Add(k, []byte("val"), false /* forceObsolete */))
					}
					for _, rk := range tc.rangeKeys {
						require.NoError(t, w.EncodeSpan(keyspan.Span{
							Start: []byte(rk[0]),
							End:   []byte(rk[1]),
							Keys: []keyspan.Key{{
								Trailer: base.MakeTrailer(1, base.InternalKeyKindRangeKeySet),
								Suffix:  []byte(tc.from),
								Value:   []byte("val"),
							}},
						}))
					}
					if tc.sourceLacksProp {
						clearMaxUserKeySize(t, w)
					}
					require.NoError(t, w.Close())
					sst := obj.Data()

					r, err := NewMemReader(sst, readerOpts)
					require.NoError(t, err)
					defer func() { require.NoError(t, r.Close()) }()
					srcProp, srcPresent := readMaxUserKeySizeProp(t, r)
					require.Equal(t, !tc.sourceLacksProp, srcPresent)
					if srcPresent {
						require.Equal(t, maxUserKeySizeFromContents(t, r), srcProp)
					}

					from, to := []byte(tc.from), []byte(tc.to)
					for _, concurrency := range []int{1, 3} {
						t.Run(fmt.Sprintf("in-blocks/concurrency=%d", concurrency), func(t *testing.T) {
							out := &objstorage.MemObj{}
							meta, format, err := RewriteKeySuffixesAndReturnFormat(
								sst, readerOpts, out, wOpts, from, to, concurrency)
							require.NoError(t, err)
							require.Equal(t, tf, format)
							checkOutput(t, meta, out.Data(), tc.expectedInBlocks, tc.expectedExact)
						})
					}
					t.Run("via-writer", func(t *testing.T) {
						out := &objstorage.MemObj{}
						meta, err := RewriteKeySuffixesViaWriter(r, out, wOpts, from, to)
						require.NoError(t, err)
						checkOutput(t, meta, out.Data(), tc.expectedExact, tc.expectedExact)
					})
				})
			}
		})
	}
}
