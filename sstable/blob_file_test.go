// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestBlobFileReaderWriter(t *testing.T) {
	fs := vfs.NewMem()
	dirPath := "/foo/bar"
	fs.MkdirAll(dirPath, 0755)
	fileNum := base.FileNum(42)
	fileName := base.MakeFilepath(fs, dirPath, base.FileTypeBlob, fileNum)
	file, err := fs.Create(fileName)
	require.NoError(t, err)
	w := NewBlobFileWriter(fileNum, file, BlobFileWriterOptions{
		BlockSize:          10,
		BlockSizeThreshold: 20,
		Compression:        SnappyCompression,
		ChecksumType:       ChecksumTypeCRC32c,
	})
	defer w.Close()
	values := []struct {
		value string
		la    string
	}{
		{
			value: "scooby",
			la:    "dooby",
		},
		{
			value: "do",
			la:    "",
		},
		{
			value: "where",
			la:    "are you",
		},
	}
	var handles []BlobValueHandle
	expectedBlobValueSize := 0
	for _, v := range values {
		handle, err := w.AddValue([]byte(v.value), base.LongAttribute(v.la))
		expectedBlobValueSize += len(v.value)
		require.Equal(t, uint64(expectedBlobValueSize), w.BlobValueSize())
		require.NoError(t, err)
		valueLen, handle := decodeLenFromValueHandle(handle)
		require.Equal(t, len(v.value), int(valueLen))
		la, handle := decodeLongAttributeFromValueHandle(handle)
		require.Equal(t, v.la, string(la))
		bvh := decodeRemainingBlobValueHandleForTesting(handle)
		fmt.Printf("%+v\n", bvh)
		bvh.ValueLen = valueLen
		bvh.LongAttribute = append([]byte(nil), la...)
		handles = append(handles, bvh)
	}
	fmt.Printf("estimated size: %d\n", w.EstimatedSize())
	require.NoError(t, w.Flush())
	fileSize := w.FileSize()
	fileInfo, err := fs.Stat(fileName)
	require.NoError(t, err)
	require.Equal(t, fileInfo.Size(), int64(fileSize))

	readerCache := NewBlobFileReaderCache(BlobFileReaderCacheOptions{
		Dirname:       dirPath,
		FS:            fs,
		ReaderOptions: BlobFileReaderOptions{},
		MaxReaders:    2,
	})
	defer readerCache.Close()
	readerInterface, err := readerCache.getBlobFileReader(fileNum)
	require.NoError(t, err)
	fmt.Printf("%+v\n", readerInterface.getValueBlocksIndexHandle())
	require.NoError(t, readerInterface.close())

	var stats base.InternalIteratorStats
	r := newBlobValueReader(readerCache, &stats)
	var buf [blobValueHandleMaxLen + 1]byte
	clonedValues := make([]struct {
		base.LazyValue
		base.LazyFetcher
	}, len(handles))
	for i := range handles {
		buf[0] = byte(valueKindIsBlobValueHandle)
		n := encodeBlobValueHandle(buf[1:], handles[i])
		lv := r.getLazyValueForPrefixAndValueHandle(buf[:n+1])
		la, ok := lv.TryGetLongAttribute()
		require.True(t, ok)
		require.Equal(t, values[i].la, string(la))
		var buffer []byte
		clonedValues[i].LazyValue, buffer = lv.Clone(nil, &clonedValues[i].LazyFetcher)
		require.Equal(t, len(lv.ValueOrHandle)+len(lv.Fetcher.Attribute.LongAttribute), len(buffer))
		val, callerOwned, err := lv.Value(nil)
		require.NoError(t, err)
		require.False(t, callerOwned)
		require.Equal(t, values[i].value, string(val))
	}
	r.close()
	for i, lv := range clonedValues {
		la, ok := lv.TryGetLongAttribute()
		require.True(t, ok)
		require.Equal(t, values[i].la, string(la))

		val, callerOwned, err := lv.Value(nil)
		require.NoError(t, err)
		require.True(t, callerOwned)
		require.Equal(t, values[i].value, string(val))
	}
	var readers []blobFileReaderInterface
	for _, fileNum := range []base.FileNum{50, 51, 52} {
		fileName := base.MakeFilepath(fs, dirPath, base.FileTypeBlob, fileNum)
		file, err := fs.Create(fileName)
		require.NoError(t, err)
		w := NewBlobFileWriter(fileNum, file, BlobFileWriterOptions{
			BlockSize:          10,
			BlockSizeThreshold: 20,
			Compression:        SnappyCompression,
			ChecksumType:       ChecksumTypeCRC32c,
		})
		defer w.Close()
		_, err = w.AddValue([]byte("foo"), nil)
		require.NoError(t, err)
		require.NoError(t, w.Flush())
		readerInterface, err := readerCache.getBlobFileReader(fileNum)
		require.NoError(t, err)
		fmt.Printf("%+v\n", readerInterface.getValueBlocksIndexHandle())
		readers = append(readers, readerInterface)
	}
	for _, reader := range readers {
		reader.close()
	}
}
