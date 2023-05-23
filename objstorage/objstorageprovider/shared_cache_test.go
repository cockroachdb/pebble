package objstorageprovider

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func TestSharedCache(t *testing.T) {
	ctx := context.Background()

	numShards := 32
	size := shardingBlockSize * int64(numShards)

	datadriven.Walk(t, "testdata/cache", func(t *testing.T, path string) {
		var log base.InMemLogger
		fs := vfs.WithLogging(vfs.NewMem(), func(fmt string, args ...interface{}) {
			log.Infof("<local fs> "+fmt, args...)
		})

		cache, err := openSharedCache(fs, "", 32*1024, size, 32)
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

				file, err := fs.Create("test")
				require.NoError(t, err)
				defer file.Close()

				// With invariants on, Write will modify its input buffer.
				toWrite = make([]byte, size)
				wrote := make([]byte, size)
				for i := 0; i < size; i++ {
					toWrite[i] = byte(i)
					wrote[i] = byte(i)
				}
				n, err := file.Write(wrote)
				// Writing a file is test setup, and it always is expected to succeed, so we assert
				// within the test, rather than returning n and/or err here.
				require.NoError(t, err)
				require.Equal(t, size, n)

				return ""
			case "read":
				var size int
				var offset int64
				scanArgs("<size> <offset>", &size, &offset)

				file, err := fs.Open("test")
				require.NoError(t, err)

				readable, err := newFileReadable(file, fs, "test")
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
				return fmt.Sprintf("misses=%d", cache.misses.Load())
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

	seed := uint64(time.Now().UnixNano())
	fmt.Printf("seed: %v\n", seed)
	rand.Seed(seed)

	blockSize := 32 * 1024 // 32 KB
	if rand.Intn(2) == 0 {
		blockSize = 1024 * 1024 // 1 MB
	}
	numShards := rand.Intn(64) + 1
	cacheSize := int64(shardingBlockSize * numShards) // minimum allowed cache size

	cache, err := openSharedCache(fs, "", blockSize, cacheSize, numShards)
	require.NoError(t, err)
	defer cache.Close()

	file, err := fs.Create("test")
	require.NoError(t, err)

	size := rand.Int63n(cacheSize)

	// With invariants on, Write will modify its input buffer.
	toWrite := make([]byte, size)
	wrote := make([]byte, size)
	for i := 0; i < int(size); i++ {
		toWrite[i] = byte(i)
		wrote[i] = byte(i)
	}

	n, err := file.Write(wrote)
	require.NoError(t, err)
	require.Equal(t, size, int64(n))

	readable, err := newFileReadable(file, fs, "test")
	require.NoError(t, err)
	defer readable.Close()

	offset := rand.Int63n(size)

	// Cache miss.
	got := make([]byte, size-offset)
	err = cache.ReadAt(ctx, 1, got, offset, readable)
	require.NoError(t, err)
	require.Equal(t, toWrite[int(offset):], got)

	// Cache hit.
	got = make([]byte, size-offset)
	err = cache.ReadAt(ctx, 1, got, offset, readable)
	require.NoError(t, err)
	require.Equal(t, toWrite[int(offset):], got)
}
