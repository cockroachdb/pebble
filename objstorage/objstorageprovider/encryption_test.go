// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorageprovider

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/stretchr/testify/require"
)

func TestEncryptDecrypt(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	rng := rand.New(rand.NewPCG(seed, seed))
	var key [32]byte
	copy(key[:], testutils.RandBytes(rng, 32))

	t.Run("EncryptFile+DecryptFile", func(t *testing.T) {
		for _, textCopies := range []int{1, 3, 10, 100, 10000} {
			plaintext := bytes.Repeat([]byte("hello world\n"), textCopies)
			t.Run(fmt.Sprintf("copies=%d", textCopies), func(t *testing.T) {
				for _, chunkSize := range []int{1, 7, 64, 1 << 10, 1 << 20} {
					externalEncryptionChunkSize = chunkSize

					t.Run("chunk="+fmt.Sprintf("%d", chunkSize), func(t *testing.T) {
						ciphertext, err := fullEncrypt(t.Context(), plaintext, key)
						require.NoError(t, err)
						require.True(t, ExternalFileAppearsEncrypted(ciphertext), "cipher text should appear encrypted")

						decrypted, err := fullDecrypt(t.Context(), ciphertext, key)
						require.NoError(t, err)
						require.Equal(t, plaintext, decrypted)
					})
				}
			})
		}
	})

	t.Run("helpful error on bad input", func(t *testing.T) {
		_, err := fullDecrypt(t.Context(), []byte("a"), key)
		require.EqualError(t, err, "file does not appear to be encrypted")
	})

	t.Run("ReadAt", func(t *testing.T) {
		rng := rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))

		externalEncryptionChunkSize = 32

		plaintext := testutils.RandBytes(rng, 256)
		plainReadable := plainReadable(t, plaintext)

		ciphertext, err := fullEncrypt(t.Context(), plaintext, key)
		require.NoError(t, err)

		var encryptedReadable objstorage.MemObj
		require.NoError(t, encryptedReadable.Write(ciphertext))
		decryptingReadable, err := NewExternalFileDecryptingReadable(
			t.Context(), &encryptedReadable, key,
		)
		require.NoError(t, err)

		t.Run("start", func(t *testing.T) {
			expected := make([]byte, 24)
			got := make([]byte, len(expected))

			require.NoError(t, plainReadable.ReadAt(t.Context(), expected, 0))
			require.NoError(t, decryptingReadable.ReadAt(t.Context(), got, 0))
			require.Equal(t, expected, got)
		})

		t.Run("spanning", func(t *testing.T) {
			expected := make([]byte, 24)
			got := make([]byte, len(expected))

			require.NoError(t, plainReadable.ReadAt(t.Context(), expected, 30))
			require.NoError(t, decryptingReadable.ReadAt(t.Context(), got, 30))
			require.Equal(t, expected, got)

			expectedEmpty := make([]byte, 0)
			gotEmpty := make([]byte, 0)
			expectedEmptyErr := plainReadable.ReadAt(t.Context(), expectedEmpty, 30)
			gotEmptyErr := decryptingReadable.ReadAt(t.Context(), gotEmpty, 30)

			require.Equal(t, expectedEmptyErr != nil, gotEmptyErr != nil)
			require.Equal(t, expectedEmpty, gotEmpty)
		})

		t.Run("to-end", func(t *testing.T) {
			expected := make([]byte, 24)
			got := make([]byte, len(expected))

			require.NoError(t, plainReadable.ReadAt(t.Context(), expected, 256-24))
			require.NoError(t, decryptingReadable.ReadAt(t.Context(), got, 256-24))
			require.Equal(t, expected, got)
		})

		t.Run("spanning-end", func(t *testing.T) {
			expected := make([]byte, 100)
			got := make([]byte, len(expected))

			require.Error(t, plainReadable.ReadAt(t.Context(), expected, 180))
			require.Error(t, decryptingReadable.ReadAt(t.Context(), got, 180))
			require.Equal(t, expected, got)
		})

		t.Run("after-end", func(t *testing.T) {
			expected := make([]byte, 24)
			got := make([]byte, len(expected))

			require.Error(t, plainReadable.ReadAt(t.Context(), expected, 300))
			require.Error(t, decryptingReadable.ReadAt(t.Context(), got, 300))
			require.Equal(t, expected, got)

			expectedEmpty := make([]byte, 0)
			gotEmpty := make([]byte, 0)
			require.Error(t, plainReadable.ReadAt(t.Context(), expectedEmpty, 300))
			require.Error(t, decryptingReadable.ReadAt(t.Context(), gotEmpty, 300))
			require.Equal(t, expectedEmpty, gotEmpty)
		})
	})

	t.Run("Random", func(t *testing.T) {
		t.Run("DecryptFile", func(t *testing.T) {
			// For some number of randomly chosen chunk-sizes, generate a number
			// of random length plaintexts of random bytes and ensure they each
			// round-trip.
			for i := 0; i < 10; i++ {
				externalEncryptionChunkSize = rng.IntN(1024*24) + 1
				for j := 0; j < 100; j++ {
					plaintext := testutils.RandBytes(rng, rng.IntN(1024*32))
					ciphertext, err := fullEncrypt(t.Context(), plaintext, key)
					require.NoError(t, err)
					decrypted, err := fullDecrypt(t.Context(), ciphertext, key)
					require.NoError(t, err)
					if len(plaintext) == 0 {
						require.Equal(t, len(plaintext), len(decrypted))
					} else {
						require.Equal(t, plaintext, decrypted)
					}
				}
			}
		})

		t.Run("ReadAt", func(t *testing.T) {
			// For each random size of chunk and text, verify random reads.
			const chunkSizes, textSizes, reads = 10, 100, 500

			for i := 0; i < chunkSizes; i++ {
				externalEncryptionChunkSize = rng.IntN(1024*24) + 1
				for j := 0; j < textSizes; j++ {
					plaintext := testutils.RandBytes(rng, rng.IntN(1024*32))
					plainReadable := plainReadable(t, plaintext)

					ciphertext, err := fullEncrypt(t.Context(), plaintext, key)
					require.NoError(t, err)

					var encryptedReadable objstorage.MemObj
					require.NoError(t, encryptedReadable.Write(ciphertext))
					decryptingReadable, err := NewExternalFileDecryptingReadable(t.Context(), &encryptedReadable, key)
					require.NoError(t, err)

					for k := 0; k < reads; k++ {
						start := rng.Int64N(int64(float64(len(plaintext)+1) * 1.1))

						expected := make([]byte, rng.Int64N(int64(len(plaintext)/2+1)))
						got := make([]byte, len(expected))
						expectedErr := plainReadable.ReadAt(t.Context(), expected, start)
						gotErr := decryptingReadable.ReadAt(t.Context(), got, start)

						if expectedErr != nil {
							require.Error(t, gotErr)
						} else {
							require.NoError(t, gotErr)
						}
						require.Equal(t, expected, got)
					}
				}
			}
		})
	})
}

func BenchmarkEncryptDecrypt(b *testing.B) {
	seed := uint64(time.Now().UnixNano())
	rng := rand.New(rand.NewPCG(seed, seed))
	var key [32]byte
	copy(key[:], testutils.RandBytes(rng, 32))

	plaintext1KB := bytes.Repeat([]byte("0123456789abcdef"), 64)
	plaintext100KB := bytes.Repeat(plaintext1KB, 100)
	plaintext1MB := bytes.Repeat(plaintext1KB, 1024)
	plaintext64MB := bytes.Repeat(plaintext1MB, 64)

	chunkSizes := []int{100, 4096, 512 << 10, 1 << 20}
	ciphertext1KB := make([][]byte, len(chunkSizes))
	ciphertext100KB := make([][]byte, len(chunkSizes))
	ciphertext1MB := make([][]byte, len(chunkSizes))
	ciphertext64MB := make([][]byte, len(chunkSizes))
	b.ResetTimer()

	b.Run("EncryptFile", func(b *testing.B) {
		for _, plaintext := range [][]byte{plaintext1KB, plaintext100KB, plaintext1MB, plaintext64MB} {
			b.Run(fmt.Sprintf("%d", len(plaintext)), func(b *testing.B) {
				for _, chunkSize := range chunkSizes {
					externalEncryptionChunkSize = chunkSize
					b.Run(fmt.Sprintf("chunk=%d", chunkSize), func(b *testing.B) {
						for i := 0; i < b.N; i++ {
							_, err := fullEncrypt(b.Context(), plaintext, key)
							if err != nil {
								b.Fatal(err)
							}
						}
						b.SetBytes(int64(len(plaintext)))
					})
				}
			})
		}
	})

	var err error
	for i, chunkSize := range chunkSizes {
		externalEncryptionChunkSize = chunkSize
		ciphertext1KB[i], err = fullEncrypt(b.Context(), plaintext1KB, key)
		require.NoError(b, err)
		ciphertext100KB[i], err = fullEncrypt(b.Context(), plaintext100KB, key)
		require.NoError(b, err)
		ciphertext1MB[i], err = fullEncrypt(b.Context(), plaintext1MB, key)
		require.NoError(b, err)
		ciphertext64MB[i], err = fullEncrypt(b.Context(), plaintext64MB, key)
		require.NoError(b, err)
	}
	b.ResetTimer()

	b.Run("DecryptFile", func(b *testing.B) {
		for _, ciphertextOriginal := range [][][]byte{
			ciphertext1KB, ciphertext100KB, ciphertext1MB, ciphertext64MB,
		} {
			// Decrypt reuses/clobbers the original ciphertext slice.
			b.Run(fmt.Sprintf("%d", len(ciphertextOriginal[0])), func(b *testing.B) {
				for chunkSizeNum, chunkSize := range chunkSizes {
					externalEncryptionChunkSize = chunkSize

					b.Run(fmt.Sprintf("chunk=%d", chunkSize), func(b *testing.B) {
						for i := 0; i < b.N; i++ {
							_, err := fullDecrypt(b.Context(), ciphertextOriginal[chunkSizeNum], key)
							if err != nil {
								b.Fatal(err)
							}
						}
						b.SetBytes(int64(len(ciphertextOriginal[chunkSizeNum])))
					})
				}
			})
		}
	})
}

func fullEncrypt(ctx context.Context, plaintext []byte, key [32]byte) ([]byte, error) {
	var b objstorage.MemObj
	w, err := NewExternalFileEncryptingWritable(ctx, &b, key)
	if err != nil {
		return nil, err
	}

	if err := w.Write(plaintext); err != nil {
		w.Abort()
		return nil, err
	}

	if err := w.Finish(); err != nil {
		return nil, err
	}

	return b.Data(), nil
}

func fullDecrypt(ctx context.Context, ciphertext []byte, key [32]byte) ([]byte, error) {
	var w objstorage.MemObj
	err := w.Write(ciphertext)
	if err != nil {
		return nil, err
	}

	r, err := NewExternalFileDecryptingReadable(ctx, &w, key)
	if err != nil {
		return nil, err
	}

	decrypted := make([]byte, r.Size())
	err = r.ReadAt(ctx, decrypted, 0)
	if err != nil {
		return nil, err
	}
	return decrypted, nil
}

// plainReadable returns a MemObj with bytes loaded in.
// Note that this function will make a copy of bytes as Writable.Write is allowed to mangle
// buffers, so this function ensures the passed bytes are not mangled themselves.
func plainReadable(t *testing.T, bytes []byte) objstorage.Readable {
	b := make([]byte, len(bytes))
	copy(b, bytes)
	var r objstorage.MemObj
	require.NoError(t, r.Write(b))
	return &r
}
