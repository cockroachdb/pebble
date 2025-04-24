package pebble

import (
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/stretchr/testify/require"
)

func TestMergingIterHeap(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("Using seed %d", seed)
	rng := rand.New(rand.NewPCG(0, uint64(seed)))

	generatedKeys := map[string]struct{}{}
	// Generates unique keys.
	makeKey := func() []byte {
		n := 10 + rng.IntN(20)
		key := make([]byte, n)
		for {
			randStr(key, rng)
			if _, ok := generatedKeys[string(key)]; ok {
				continue
			} else {
				generatedKeys[string(key)] = struct{}{}
				t.Logf("generated key: %s", string(key))
				return key
			}
		}
	}
	levels := make([]*mergingIterLevel, 2+rng.IntN(15))
	for i := range levels {
		levels[i] = &mergingIterLevel{
			index: i,
			iterKV: &base.InternalKV{
				K: InternalKey{
					UserKey: makeKey(),
				},
			},
		}
	}
	heap := mergingIterHeap{
		cmp:     base.DefaultComparer.Compare,
		reverse: rng.IntN(2) != 0,
		items:   slices.Clone(levels),
	}
	checkHeap := func() {
		for _, item := range heap.items {
			require.NotNil(t, levels[item.index].iterKV)
		}
		count := 0
		minIndex := 0
		for i, item := range levels {
			if item.iterKV == nil {
				continue
			}
			if count == 0 {
				minIndex = i
			} else {
				cmp := heap.cmp(item.iterKV.K.UserKey, levels[minIndex].iterKV.K.UserKey)
				if (cmp < 0 && !heap.reverse) || (cmp > 0 && heap.reverse) {
					minIndex = i
				} else if cmp == 0 {
					panic("test generated duplicate keys")
				}
			}
			count++
		}
		if count == 0 {
			require.Equal(t, 0, heap.len())
		} else {
			require.Equal(t, count, heap.len())
			require.Equal(t, levels[minIndex].index, heap.items[0].index)
		}
	}
	heap.init()
	checkHeap()
	for i := 0; i < 50 && heap.len() > 0; i++ {
		t.Logf("heap len: %d", heap.len())
		if rng.IntN(10) == 0 {
			t.Logf("%d: popping heap index %d", i, heap.items[0].index)
			heap.items[0].iterKV = nil
			heap.pop()
		} else {
			t.Logf("%d: fixing heap", i)
			heap.items[0].iterKV.K.UserKey = makeKey()
			heap.fixTop()
		}
		checkHeap()
	}
}
