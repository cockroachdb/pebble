package sharedcache_test

import (
	"bytes"
	"context"
	"fmt"
	"sync"
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

	datadriven.Walk(t, "testdata/cache", func(t *testing.T, path string) {
		var log base.InMemLogger
		fs := vfs.WithLogging(vfs.NewMem(), func(fmt string, args ...interface{}) {
			log.Infof("<local fs> "+fmt, args...)
		})

		provider, err := objstorageprovider.Open(objstorageprovider.DefaultSettings(fs, ""))
		require.NoError(t, err)

		var cache *sharedcache.Cache
		defer func() {
			if cache != nil {
				cache.Close()
			}
		}()

		var objData []byte
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
			case "init":
				blockSize := 32 * 1024
				numShards := 32
				size := 32 * sharedcache.ShardingBlockSize()

				d.MaybeScanArgs(t, "block-size", &blockSize)
				d.MaybeScanArgs(t, "num-shards", &numShards)
				d.MaybeScanArgs(t, "size", &size)
				if size%(numShards*sharedcache.ShardingBlockSize()) != 0 {
					d.Fatalf(t, "size (%d) must be a multiple of numShards (%d) * shardingBlockSize(%d)",
						size, numShards, sharedcache.ShardingBlockSize(),
					)
				}
				cache, err = sharedcache.Open(fs, base.DefaultLogger, "", blockSize, int64(size), numShards)
				require.NoError(t, err)
				return fmt.Sprintf("initialized with block-size=%d size=%d num-shards=%d", blockSize, size, numShards)

			case "write":
				var size int
				scanArgs("<size>", &size)

				writable, _, err := provider.Create(ctx, base.FileTypeTable, base.FileNum(1).DiskFileNum(), objstorage.CreateOptions{})
				require.NoError(t, err)
				defer writable.Finish()

				// With invariants on, Write will modify its input buffer.
				objData = make([]byte, size)
				wrote := make([]byte, size)
				for i := 0; i < size; i++ {
					objData[i] = byte(i)
					wrote[i] = byte(i)
				}
				err = writable.Write(wrote)
				// Writing a file is test setup, and it always is expected to succeed, so we assert
				// within the test, rather than returning n and/or err here.
				require.NoError(t, err)

				return ""
			case "read", "read-for-compaction":
				missesBefore := cache.Misses()
				var size int
				var offset int64
				// TODO(radu): swap these arguments (the opposite order is typical).
				scanArgs("<size> <offset>", &size, &offset)

				readable, err := provider.OpenForReading(ctx, base.FileTypeTable, base.FileNum(1).DiskFileNum(), objstorage.OpenOptions{})
				require.NoError(t, err)
				defer readable.Close()

				got := make([]byte, size)
				flags := sharedcache.ReadFlags{
					ReadOnly: d.Cmd == "read-for-compaction",
				}
				err = cache.ReadAt(ctx, base.FileNum(1).DiskFileNum(), got, offset, readable, readable.Size(), flags)
				// We always expect cache.ReadAt to succeed.
				require.NoError(t, err)
				// It is easier to assert this condition programmatically, rather than returning
				// got, which may be very large.
				require.True(t, bytes.Equal(objData[int(offset):int(offset)+size], got), "incorrect data returned")

				// In order to ensure we get a hit on the next read, we must wait for writing to
				// the cache to complete.
				cache.WaitForWritesToComplete()

				// TODO(josh): Not tracing out filesystem activity here, since logging_fs.go
				// doesn't trace calls to ReadAt or WriteAt. We should consider changing this.
				missesAfter := cache.Misses()
				return fmt.Sprintf("misses=%d", missesAfter-missesBefore)
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
	fs := vfs.WithLogging(vfs.NewMem(), func(fmt string, args ...interface{}) {
		log.Infof("<local fs> "+fmt, args...)
	})

	provider, err := objstorageprovider.Open(objstorageprovider.DefaultSettings(fs, ""))
	require.NoError(t, err)

	seed := uint64(time.Now().UnixNano())
	fmt.Printf("seed: %v\n", seed)
	rand.Seed(seed)

	helper := func(
		blockSize int,
		concurrentReads bool) func(t *testing.T) {
		return func(t *testing.T) {
			numShards := rand.Intn(64) + 1
			cacheSize := int64(sharedcache.ShardingBlockSize() * numShards) // minimum allowed cache size

			cache, err := sharedcache.Open(fs, base.DefaultLogger, "", blockSize, cacheSize, numShards)
			require.NoError(t, err)
			defer cache.Close()

			writable, _, err := provider.Create(ctx, base.FileTypeTable, base.FileNum(1).DiskFileNum(), objstorage.CreateOptions{})
			require.NoError(t, err)
			defer writable.Finish()

			// With invariants on, Write will modify its input buffer.
			size := rand.Int63n(cacheSize)
			objData := make([]byte, size)
			wrote := make([]byte, size)
			for i := 0; i < int(size); i++ {
				objData[i] = byte(i)
				wrote[i] = byte(i)
			}

			err = writable.Write(wrote)
			require.NoError(t, err)

			readable, err := provider.OpenForReading(ctx, base.FileTypeTable, base.FileNum(1).DiskFileNum(), objstorage.OpenOptions{})
			require.NoError(t, err)
			defer readable.Close()

			const numDistinctReads = 100
			wg := sync.WaitGroup{}
			for i := 0; i < numDistinctReads; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					offset := rand.Int63n(size)

					got := make([]byte, size-offset)
					err := cache.ReadAt(ctx, base.FileNum(1).DiskFileNum(), got, offset, readable, readable.Size(), sharedcache.ReadFlags{})
					require.NoError(t, err)
					require.Equal(t, objData[int(offset):], got)

					got = make([]byte, size-offset)
					err = cache.ReadAt(ctx, base.FileNum(1).DiskFileNum(), got, offset, readable, readable.Size(), sharedcache.ReadFlags{})
					require.NoError(t, err)
					require.Equal(t, objData[int(offset):], got)
				}()
				// If concurrent reads, only wait 50% of loop iterations on average.
				if concurrentReads && rand.Intn(2) == 0 {
					wg.Wait()
				}
				if !concurrentReads {
					wg.Wait()
				}
			}
			wg.Wait()
		}
	}
	t.Run("32 KB with serial reads", helper(32*1024, false))
	t.Run("1 MB with serial reads", helper(1024*1024, false))
	t.Run("32 KB with concurrent reads", helper(32*1024, true))
	t.Run("1 MB with concurrent reads", helper(1024*1024, true))
}
