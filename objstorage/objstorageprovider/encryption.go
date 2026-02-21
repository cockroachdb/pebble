// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorageprovider

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	crypto_rand "crypto/rand"
	"encoding/binary"
	"sync"

	"github.com/cockroachdb/pebble/objstorage"
	"github.com/pkg/errors"
)

const (
	externalEncryptionVersion  = 2
	externalEncryptionPreamble = "encrypt"

	externalEncryptionNonceSize  = 12                                                                // GCM standard nonce
	externalEncryptionHeaderSize = len(externalEncryptionPreamble) + 1 + externalEncryptionNonceSize // preamble + version + iv
	externalEncryptionTagSize    = 16                                                                // GCM standard tag
)

var externalEncryptionChunkSize = 64 << 10 // 64kb

type ExternalFileDecryptingReadable struct {
	ciphertext objstorage.Readable

	mu        sync.Mutex
	g         cipher.AEAD
	fileIV    []byte
	ivScratch []byte
	buf       []byte
	chunk     int64
}

func NewExternalFileDecryptingReadable(
	ctx context.Context, ciphertext objstorage.Readable, key [32]byte,
) (objstorage.Readable, error) {
	gcm, err := aesgcm(key[0:])
	if err != nil {
		return nil, err
	}

	header := make([]byte, externalEncryptionHeaderSize)
	readHeaderErr := ciphertext.ReadAt(ctx, header, 0)

	// Verify that the read data does indeed look like an encrypted file and has
	// a encoding version we can decode.
	if !ExternalFileAppearsEncrypted(header) {
		return nil, errors.New("file does not appear to be encrypted")
	}
	if readHeaderErr != nil {
		return nil, errors.Wrap(readHeaderErr, "invalid encryption header")
	}

	version := header[len(externalEncryptionPreamble)]
	if version != externalEncryptionVersion {
		return nil, errors.Errorf("unexpected encryption scheme/config version %d", version)
	}
	iv := header[len(externalEncryptionPreamble)+1:]

	buf := make([]byte, externalEncryptionNonceSize, externalEncryptionChunkSize+externalEncryptionTagSize+externalEncryptionNonceSize)
	ivScratch := buf[:externalEncryptionNonceSize]
	buf = buf[externalEncryptionNonceSize:externalEncryptionNonceSize]
	readable := &ExternalFileDecryptingReadable{
		ciphertext: ciphertext,
		mu:         sync.Mutex{},
		g:          gcm,
		fileIV:     iv,
		ivScratch:  ivScratch,
		buf:        buf,
		chunk:      -1,
	}
	return readable, err
}
func (er *ExternalFileDecryptingReadable) ReadAt(ctx context.Context, p []byte, off int64) error {
	// Ensure that the section we want to read is within bounds.
	if off < 0 || er.Size() < off+int64(len(p)) {
		return errors.New("bad offset")
	}

	er.mu.Lock()
	defer er.mu.Unlock()

	var read int
	for {
		chunk := off / int64(externalEncryptionChunkSize)
		offsetInChunk := off % int64(externalEncryptionChunkSize)

		if err := er.fill(ctx, chunk); err != nil {
			return err
		}

		// Copy from the chunk.
		n := copy(p[read:], er.buf[offsetInChunk:])
		read += n

		// Return if we've fulfilled the request.
		if read == len(p) {
			return nil
		}

		// Move offset by how much we read and go again.
		off += int64(n)
	}
}
func (er *ExternalFileDecryptingReadable) Close() error {
	return er.ciphertext.Close()
}
func (er *ExternalFileDecryptingReadable) Size() int64 {
	size := er.ciphertext.Size()
	size -= int64(externalEncryptionHeaderSize)
	size -= externalEncryptionTagSize * ((size / (int64(externalEncryptionChunkSize) + externalEncryptionTagSize)) + 1)
	return size
}
func (er *ExternalFileDecryptingReadable) NewReadHandle(
	readBeforeSize objstorage.ReadBeforeSize,
) objstorage.ReadHandle {
	handle := objstorage.MakeNoopReadHandle(er)
	return &handle
}

func (er *ExternalFileDecryptingReadable) fill(ctx context.Context, chunk int64) error {
	if chunk == er.chunk {
		return nil // this chunk is already loaded in buf.
	}

	er.chunk = -1 // invalidate the current buffered chunk while we fill it.

	// Load the region of ciphertext that corresponds to chunk.
	ciphertextSize := er.ciphertext.Size()
	ciphertextChunkSize := int64(externalEncryptionChunkSize) + externalEncryptionTagSize
	start := int64(externalEncryptionHeaderSize) + chunk*ciphertextChunkSize
	fillSize := min(cap(er.buf), int(ciphertextSize-start))
	err := er.ciphertext.ReadAt(ctx, er.buf[:fillSize], start)
	if err != nil {
		return err
	}
	er.buf = er.buf[:fillSize]

	buf, err := er.g.Open(er.buf[:0], er.chunkIV(chunk), er.buf, nil)
	if err != nil {
		return errors.Wrap(err, "failed to decrypt — maybe incorrect key")
	}
	er.buf = buf
	er.chunk = chunk
	return err
}
func (er *ExternalFileDecryptingReadable) chunkIV(num int64) []byte {
	er.ivScratch = append(er.ivScratch[:0], er.fileIV...)
	binary.BigEndian.PutUint64(
		er.ivScratch[4:], binary.BigEndian.Uint64(er.ivScratch[4:])+uint64(num),
	)
	return er.ivScratch
}

type ExternalFileEncryptingWritable struct {
	ciphertext objstorage.Writable
	gcm        cipher.AEAD
	iv         []byte

	buf    []byte
	bufPos int
}

func NewExternalFileEncryptingWritable(
	ctx context.Context, ciphertext objstorage.Writable, key [32]byte,
) (objstorage.Writable, error) {
	gcm, err := aesgcm(key[0:])
	if err != nil {
		ciphertext.Abort()
		return nil, err
	}

	header := make([]byte, len(externalEncryptionPreamble)+1+externalEncryptionNonceSize)
	copy(header, externalEncryptionPreamble)
	header[len(externalEncryptionPreamble)] = externalEncryptionVersion

	// Pick a unique IV for this file and write it in the header.
	ivStart := len(externalEncryptionPreamble) + 1
	iv := make([]byte, externalEncryptionNonceSize)
	if _, err := crypto_rand.Read(iv); err != nil {
		ciphertext.Abort()
		return nil, err
	}
	copy(header[ivStart:], iv)

	// Write our header (preamble+version+IV) to the ciphertext sink.
	if err := ciphertext.Write(header); err != nil {
		ciphertext.Abort()
		return nil, err
	}

	writable := &ExternalFileEncryptingWritable{
		gcm: gcm, iv: iv, ciphertext: ciphertext, buf: make([]byte, externalEncryptionChunkSize+externalEncryptionTagSize),
	}
	return writable, nil
}
func (ew *ExternalFileEncryptingWritable) Write(p []byte) error {
	var wrote int
	for wrote < len(p) {
		copied := copy(ew.buf[ew.bufPos:externalEncryptionChunkSize], p[wrote:])
		ew.bufPos += copied
		if ew.bufPos == externalEncryptionChunkSize {
			if err := ew.flush(); err != nil {
				return err
			}
		}
		wrote += copied
	}
	return nil
}
func (ew *ExternalFileEncryptingWritable) Finish() error {
	// Note: there may not be any plaintext left to seal if the chunk we just
	// finished was the end of it, but sealing the (empty) remainder in a final
	// chunk maintains the invariant that a chunked file always ends in a sealed
	// chunk of less than chunk size, thus making tuncation, even along a chunk
	// boundary, detectable.
	if err := ew.flush(); err != nil {
		ew.ciphertext.Abort()
		return err
	}
	return ew.ciphertext.Finish()
}
func (ew *ExternalFileEncryptingWritable) StartMetadataPortion() error {
	if err := ew.flush(); err != nil {
		return err
	}
	return ew.ciphertext.StartMetadataPortion()
}
func (ew *ExternalFileEncryptingWritable) Abort() {
	ew.ciphertext.Abort()
}

func (ew *ExternalFileEncryptingWritable) flush() error {
	ew.buf = ew.gcm.Seal(ew.buf[:0], ew.iv, ew.buf[:ew.bufPos], nil)
	if err := ew.ciphertext.Write(ew.buf); err != nil {
		return err
	}
	binary.BigEndian.PutUint64(ew.iv[4:], binary.BigEndian.Uint64(ew.iv[4:])+1)
	ew.bufPos = 0
	return nil
}

func ExternalFileAppearsEncrypted(text []byte) bool {
	return bytes.HasPrefix(text, []byte(externalEncryptionPreamble))
}

func aesgcm(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
