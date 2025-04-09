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
		b.Run(compressions[comp].Family.String(), func(b *testing.B) {
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
