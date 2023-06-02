package sharedcache_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider/sharedcache"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func TestSharedCache(t *testing.T) {
	ctx := context.Background()

	numShards := 4
	size := int64(sharedcache.ShardingBlockSize * 32)

	datadriven.Walk(t, "testdata/cache", func(t *testing.T, path string) {
		var log base.InMemLogger
		fs := vfs.WithLogging(vfs.NewMem(), func(fmt string, args ...interface{}) {
			log.Infof("<local fs> "+fmt, args...)
		})

		provider, err := objstorageprovider.Open(objstorageprovider.DefaultSettings(fs, ""))
		require.NoError(t, err)

		cache, err := sharedcache.Open(fs, "", 32*1024, size, numShards, true)
		require.NoError(t, err)
		defer cache.Close()

		var toWrite []byte
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			scanArgs := func(desc string, args ...interface{}) {
				t.Helper()
				if len(d.CmdArgs) != len(args) {
					d.Fatalf(t, "usage: %s %s", d.Cmd, desc)
				}
				for i := range args {
					_, err := fmt.Sscan(d.CmdArgs[i].String(), args[i])
					if err != nil {
						d.Fatalf(t, "%s: error parsing argument '%s'", d.Cmd, d.CmdArgs[i])
					}
				}
			}

			log.Reset()
			switch d.Cmd {
			case "write":
				var size int
				scanArgs("<size>", &size)

				writable, _, err := provider.Create(ctx, base.FileTypeTable, base.FileNum(1).DiskFileNum(), objstorage.CreateOptions{})
				require.NoError(t, err)
				defer writable.Finish()

				// With invariants on, Write will modify its input buffer.
				toWrite = make([]byte, size)
				wrote := make([]byte, size)
				for i := 0; i < size; i++ {
					toWrite[i] = byte(i)
					wrote[i] = byte(i)
				}
				err = writable.Write(wrote)
				// Writing a file is test setup, and it always is expected to succeed, so we assert
				// within the test, rather than returning n and/or err here.
				require.NoError(t, err)

				return ""
			case "read":
				var size int
				var offset int64
				scanArgs("<size> <offset>", &size, &offset)

				readable, err := provider.OpenForReading(ctx, base.FileTypeTable, base.FileNum(1).DiskFileNum(), objstorage.OpenOptions{})
				require.NoError(t, err)
				defer readable.Close()

				got := make([]byte, size)
				err = cache.ReadAt(ctx, 1, got, offset, readable)
				// We always expect cache.ReadAt to succeed.
				require.NoError(t, err)
				// It is easier to assert this condition programmatically, rather than returning
				// got, which may be very large.
				require.Equal(t, toWrite[int(offset):], got)

				// TODO(josh): Not tracing out filesystem activity here, since logging_fs.go
				// doesn't trace calls to ReadAt or WriteAt. We should consider changing this.
				return fmt.Sprintf("misses=%d", cache.Misses.Load())
			case "reload":
				// Cache is persistent, so should be able to reopen & keep getting hits.
				cache, err = sharedcache.Open(fs, "", 32*1024, size, numShards, true)
				require.NoError(t, err)

				return ""
			default:
				d.Fatalf(t, "unknown command %s", d.Cmd)
				return ""
			}
		})
	})
}

func TestSharedCacheRandomized(t *testing.T) {
	ctx := context.Background()
	var log base.InMemLogger

	seed := uint64(time.Now().UnixNano())
	fmt.Printf("seed: %v\n", seed)
	rand.Seed(seed)

	helper := func(blockSize int, numShards int, persistMetadata bool) func(t *testing.T) {
		return func(t *testing.T) {
			fs := vfs.WithLogging(vfs.NewMem(), func(fmt string, args ...interface{}) {
				log.Infof("<local fs> "+fmt, args...)
			})

			provider, err := objstorageprovider.Open(objstorageprovider.DefaultSettings(fs, ""))
			require.NoError(t, err)

			cacheSize := int64(sharedcache.ShardingBlockSize * numShards) // minimum allowed cache size
			if persistMetadata {
				cacheSize = cacheSize * 10 // need more space if metadata is persisted
			}

			cache, err := sharedcache.Open(fs, "", blockSize, cacheSize, numShards, persistMetadata)
			require.NoError(t, err)
			defer cache.Close()

			writable, _, err := provider.Create(ctx, base.FileTypeTable, base.FileNum(1).DiskFileNum(), objstorage.CreateOptions{})
			require.NoError(t, err)
			defer writable.Finish()

			// With invariants on, Write will modify its input buffer.
			size := rand.Int63n(cacheSize)
			toWrite := make([]byte, size)
			wrote := make([]byte, size)
			for i := 0; i < int(size); i++ {
				toWrite[i] = byte(i)
				wrote[i] = byte(i)
			}

			err = writable.Write(wrote)
			require.NoError(t, err)

			readable, err := provider.OpenForReading(ctx, base.FileTypeTable, base.FileNum(1).DiskFileNum(), objstorage.OpenOptions{})
			require.NoError(t, err)
			defer readable.Close()

			const numDistinctReads = 100
			for i := 0; i < numDistinctReads; i++ {
				offset := rand.Int63n(size)

				got := make([]byte, size-offset)
				err = cache.ReadAt(ctx, 1, got, offset, readable)
				require.NoError(t, err)
				require.Equal(t, toWrite[int(offset):], got)

				got = make([]byte, size-offset)
				err = cache.ReadAt(ctx, 1, got, offset, readable)
				require.NoError(t, err)
				require.Equal(t, toWrite[int(offset):], got)
			}

			// Reopen the cache to at least ensure metadata can be read.
			cache, err = sharedcache.Open(fs, "", blockSize, cacheSize, numShards, persistMetadata)
			require.NoError(t, err)
			defer cache.Close()
		}
	}
	t.Run("32 KB, many shards, with no metadata persistence", helper(32*1024, rand.Intn(64)+1, false))
	t.Run("1 MB, many shards, with no metadata persistence", helper(1024*1024, rand.Intn(64)+1, false))
	t.Run("32 KB, one shard, with metadata persistence", helper(32*1024, 1, true))
	t.Run("1 MB, one shard, with metadata persistence", helper(1024*1024, 1, true))
}
