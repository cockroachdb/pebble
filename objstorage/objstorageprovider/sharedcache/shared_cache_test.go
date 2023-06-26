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

	const numShards = 32
	const shardingBlockSize = 1024 * 1024
	const size = shardingBlockSize * numShards

	datadriven.Walk(t, "testdata/cache", func(t *testing.T, path string) {
		var log base.InMemLogger
		fs := vfs.WithLogging(vfs.NewMem(), func(fmt string, args ...interface{}) {
			log.Infof("<local fs> "+fmt, args...)
		})

		cache, err := sharedcache.Open(
			fs, base.DefaultLogger, "", 32*1024, shardingBlockSize, size, 32)
		require.NoError(t, err)
		defer cache.Close()

		provider, err := objstorageprovider.Open(objstorageprovider.DefaultSettings(fs, ""))
		require.NoError(t, err)

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
		shardingBlockSize int64) func(t *testing.T) {
		return func(t *testing.T) {
			for _, concurrentReads := range []bool{false, true} {
				t.Run(fmt.Sprintf("concurrentReads=%v", concurrentReads), func(t *testing.T) {
					numShards := rand.Intn(64) + 1
					cacheSize := shardingBlockSize * int64(numShards) // minimum allowed cache size

					cache, err := sharedcache.Open(fs, base.DefaultLogger, "", blockSize, shardingBlockSize, cacheSize, numShards)
					require.NoError(t, err)
					defer cache.Close()

					writable, _, err := provider.Create(ctx, base.FileTypeTable, base.FileNum(1).DiskFileNum(), objstorage.CreateOptions{})
					require.NoError(t, err)

					// With invariants on, Write will modify its input buffer.
					size := rand.Int63n(cacheSize)
					objData := make([]byte, size)
					wrote := make([]byte, size)
					for i := 0; i < int(size); i++ {
						objData[i] = byte(i)
						wrote[i] = byte(i)
					}

					require.NoError(t, writable.Write(wrote))
					require.NoError(t, writable.Finish())

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
				})
			}
		}
	}
	t.Run("32 KB block size", helper(32*1024, 1024*1024))
	t.Run("1 MB block size", helper(1024*1024, 1024*1024))

	for i := 0; i < 5; i++ {
		exp := rand.Intn(11) + 10   // [10, 20]
		randomBlockSize := 1 << exp // [1 KB, 1 MB]

		factor := rand.Intn(10) + 1                                // [1, 10]
		randomShardingBlockSize := int64(randomBlockSize * factor) // [1 KB, 10 MB]

		t.Run("random block and sharding block size", helper(randomBlockSize, randomShardingBlockSize))
	}
}
