package sharedcache

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func TestMetadataPersistenceRandomized(t *testing.T) {
	var log base.InMemLogger

	seed := uint64(time.Now().UnixNano())
	fmt.Printf("seed: %v\n", seed)
	rand.Seed(seed)

	helper := func(randomizeWhenInit bool) func(t *testing.T) {
		return func(t *testing.T) {
			fs := vfs.WithLogging(vfs.NewMem(), func(fmt string, args ...interface{}) {
				log.Infof("<local fs> "+fmt, args...)
			})

			s := shard{}
			require.NoError(t, s.open(fs, "", 0, 10, 1024, true))

			toWrite := make([]byte, 1024)
			for i := 0; i < int(1024); i++ {
				toWrite[i] = byte(i)
			}

			copyShardState := func(s *shard) (int32, map[metadataMapKey]int64, []*metadataMapKey, []int64) {
				seq := s.mu.seq
				where := make(map[metadataMapKey]int64, len(s.mu.where))
				table := make([]*metadataMapKey, len(s.mu.table))
				free := make([]int64, len(s.mu.free))
				for k, v := range s.mu.where {
					where[k] = v
				}
				copy(table, s.mu.table)
				copy(free, s.mu.free)
				return seq, where, table, free
			}

			const numDistinctSets = 1000
			for i := 0; i < numDistinctSets; i++ {
				require.NoError(t, s.set(base.FileNum(i), toWrite, int64(i*1024)), i)

				if randomizeWhenInit {
					if rand.Intn(2) == 0 { // 50% of time
						continue
					}
				}

				preRestartSeq, preRestartWhere, preRestartTable, preRestartFree := copyShardState(&s)
				s.close()

				// Init from the persistent metadata & ensure in-memory state of re-inited shard
				// matches in-memory state of original shard.
				s = shard{}
				require.NoError(t, s.open(fs, "", 0, 10, 1024, true), i)

				postRestartSeq, postRestartWhere, postRestartTable, postRestartFree := copyShardState(&s)

				require.Equal(t, preRestartSeq, postRestartSeq, i)
				require.Equal(t, preRestartWhere, postRestartWhere, i)
				require.Equal(t, preRestartTable, postRestartTable, i)
				require.Equal(t, preRestartFree, postRestartFree, i)
			}

			// Last write via s.set should be in the where map.
			require.Contains(t, s.mu.where, metadataMapKey{
				filenum:         numDistinctSets - 1,
				logicalBlockInd: numDistinctSets - 1,
			})
			s.close()
		}
	}
	t.Run("open after each write", helper(false))
	t.Run("open after 50% of writes", helper(true))
}
