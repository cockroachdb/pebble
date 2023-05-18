package objstorageprovider

import (
	"context"
	"runtime"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// TODO(josh): Write a randomized version.
func TestSharedCache(t *testing.T) {
	var log base.InMemLogger
	// TODO(josh): vfs.NewMem() doesn't properly implement OpenReadWrite, in that
	// it errors if files do not exist. I will fix before merging.
	fs := vfs.WithLogging(vfs.Default, func(fmt string, args ...interface{}) {
		log.Infof("<local fs> "+fmt, args...)
	})

	numShards := 2 * runtime.GOMAXPROCS(0)
	// Make the cache large enough that no evictions happen. Else, we cannot
	// assert the number of hits & misses that happen from the test.
	size := shardingBlockSize * int64(numShards) * 10000

	init := func() (*sharedCache, vfs.File) {
		cache, err := openSharedCache(fs, "", 32*1024, size)
		require.NoError(t, err)

		file, err := fs.Create("test")
		require.NoError(t, err)

		return cache, file
	}

	t.Run("small read", func(t *testing.T) {
		cache, file := init()

		size := 10
		wrote := make([]byte, size)
		for i := 0; i < size; i++ {
			wrote[i] = byte(i)
		}
		n, err := file.Write(wrote)
		require.NoError(t, err)
		require.Equal(t, size, n)

		readable, err := newFileReadable(file, fs, "test")
		require.NoError(t, err)

		// Cache miss
		ctx := context.Background()
		got := make([]byte, size)
		err = cache.ReadAt(ctx, 1, got, 0, readable)
		require.NoError(t, err)
		require.Equal(t, wrote, got)
		require.Equal(t, int32(1), cache.misses.Load())

		// Cache hits
		got = make([]byte, size)
		err = cache.ReadAt(ctx, 1, got, 0, readable)
		require.NoError(t, err)
		require.Equal(t, wrote, got)
		require.Equal(t, int32(1), cache.misses.Load())

		got = make([]byte, size-4)
		err = cache.ReadAt(ctx, 1, got, 4, readable)
		require.NoError(t, err)
		require.Equal(t, wrote[4:], got)
		require.Equal(t, int32(1), cache.misses.Load())
	})

	t.Run("small read with write to cache at offset also", func(t *testing.T) {
		cache, file := init()

		size := 10
		wrote := make([]byte, size)
		for i := 0; i < size; i++ {
			wrote[i] = byte(i)
		}
		n, err := file.Write(wrote)
		require.NoError(t, err)
		require.Equal(t, size, n)

		readable, err := newFileReadable(file, fs, "test")
		require.NoError(t, err)

		// Cache miss
		ctx := context.Background()
		got := make([]byte, size-4)
		err = cache.ReadAt(ctx, 5, got, 4, readable)
		require.NoError(t, err)
		require.Equal(t, wrote[4:], got)
		require.Equal(t, int32(1), cache.misses.Load())

		// Cache hit
		got = make([]byte, size-4)
		err = cache.ReadAt(ctx, 5, got, 4, readable)
		require.NoError(t, err)
		require.Equal(t, wrote[4:], got)
		require.Equal(t, int32(1), cache.misses.Load())
	})

	t.Run("read that hits two cache blocks", func(t *testing.T) {
		cache, file := init()

		size := 32*1024 + 5
		wrote := make([]byte, size)
		for i := 0; i < size; i++ {
			wrote[i] = byte(i)
		}
		n, err := file.Write(wrote)
		require.NoError(t, err)
		require.Equal(t, size, n)

		readable, err := newFileReadable(file, fs, "test")
		require.NoError(t, err)

		// Cache miss
		ctx := context.Background()
		got := make([]byte, size)
		err = cache.ReadAt(ctx, 2, got, 0, readable)
		require.NoError(t, err)
		require.Equal(t, wrote, got)
		require.Equal(t, int32(1), cache.misses.Load())

		// Cache hits
		got = make([]byte, size)
		err = cache.ReadAt(ctx, 2, got, 0, readable)
		require.NoError(t, err)
		require.Equal(t, wrote, got)
		require.Equal(t, int32(1), cache.misses.Load())

		got = make([]byte, size-57)
		err = cache.ReadAt(ctx, 2, got, 57, readable)
		require.NoError(t, err)
		require.Equal(t, wrote[57:], got)
		require.Equal(t, int32(1), cache.misses.Load())
	})

	t.Run("read that hits two cache blocks with write to cache at offset also", func(t *testing.T) {
		cache, file := init()

		size := 32*1024 + 5
		wrote := make([]byte, size)
		for i := 0; i < size; i++ {
			wrote[i] = byte(i)
		}
		n, err := file.Write(wrote)
		require.NoError(t, err)
		require.Equal(t, size, n)

		readable, err := newFileReadable(file, fs, "test")
		require.NoError(t, err)

		// Cache miss
		ctx := context.Background()
		got := make([]byte, size-57)
		err = cache.ReadAt(ctx, 8, got, 57, readable)
		require.NoError(t, err)
		require.Equal(t, wrote[57:], got)
		require.Equal(t, int32(1), cache.misses.Load())

		// Cache hit
		got = make([]byte, size-57)
		err = cache.ReadAt(ctx, 8, got, 57, readable)
		require.NoError(t, err)
		require.Equal(t, wrote[57:], got)
		require.Equal(t, int32(1), cache.misses.Load())
	})

	t.Run("read that hits two cache blocks with big offset on read", func(t *testing.T) {
		cache, file := init()

		size := 32*1024 + 5
		wrote := make([]byte, size)
		for i := 0; i < size; i++ {
			wrote[i] = byte(i)
		}
		n, err := file.Write(wrote)
		require.NoError(t, err)
		require.Equal(t, size, n)

		readable, err := newFileReadable(file, fs, "test")
		require.NoError(t, err)

		// Cache miss
		size -= 32 * 1024
		ctx := context.Background()
		got := make([]byte, size)
		err = cache.ReadAt(ctx, 15, got, 32*1024, readable)
		require.NoError(t, err)
		require.Equal(t, wrote[32*1024:], got)
		require.Equal(t, int32(1), cache.misses.Load())

		// Cache hits
		got = make([]byte, size)
		err = cache.ReadAt(ctx, 15, got, 32*1024, readable)
		require.NoError(t, err)
		require.Equal(t, wrote[32*1024:], got)
		require.Equal(t, int32(1), cache.misses.Load())
	})

	t.Run("read that hits two cache shards", func(t *testing.T) {
		cache, file := init()

		size := 1024*1024 + 200
		wrote := make([]byte, size)
		for i := 0; i < size; i++ {
			wrote[i] = byte(i)
		}
		n, err := file.Write(wrote)
		require.NoError(t, err)
		require.Equal(t, size, n)

		readable, err := newFileReadable(file, fs, "test")
		require.NoError(t, err)

		// Cache miss
		ctx := context.Background()
		got := make([]byte, size)
		err = cache.ReadAt(ctx, 3, got, 0, readable)
		require.NoError(t, err)
		require.Equal(t, wrote, got)
		require.Equal(t, int32(1), cache.misses.Load())

		// Cache hits
		got = make([]byte, size)
		err = cache.ReadAt(ctx, 3, got, 0, readable)
		require.NoError(t, err)
		require.Equal(t, wrote, got)
		require.Equal(t, int32(1), cache.misses.Load())

		got = make([]byte, size-57)
		err = cache.ReadAt(ctx, 3, got, 57, readable)
		require.NoError(t, err)
		require.Equal(t, wrote[57:], got)
		require.Equal(t, int32(1), cache.misses.Load())
	})

	t.Run("read that hits two cache shards at offset also", func(t *testing.T) {
		cache, file := init()

		size := 1024*1024 + 200
		wrote := make([]byte, size)
		for i := 0; i < size; i++ {
			wrote[i] = byte(i)
		}
		n, err := file.Write(wrote)
		require.NoError(t, err)
		require.Equal(t, size, n)

		readable, err := newFileReadable(file, fs, "test")
		require.NoError(t, err)

		// Cache miss
		ctx := context.Background()
		got := make([]byte, size-57)
		err = cache.ReadAt(ctx, 11, got, 57, readable)
		require.NoError(t, err)
		require.Equal(t, wrote[57:], got)
		require.Equal(t, int32(1), cache.misses.Load())

		// Cache hit
		got = make([]byte, size-57)
		err = cache.ReadAt(ctx, 11, got, 57, readable)
		require.NoError(t, err)
		require.Equal(t, wrote[57:], got)
		require.Equal(t, int32(1), cache.misses.Load())
	})
}
