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
	// Memtable + 5 populated levels = 6 is the baseline. All levels populated +
	// memtable + batch = 9.
	levels := make([]mergingIterHeapItem, 6+rng.IntN(6))
	for i := range levels {
		levels[i].mergingIterLevel = &mergingIterLevel{
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
	for i := 0; i < 400 && heap.len() > 0; i++ {
		t.Logf("heap len: %d", heap.len())
		if rn := rng.IntN(100); rn <= 1 {
			// 1% of the cases, the iterator is exhausted.
			t.Logf("%d: popping heap index %d", i, heap.items[0].index)
			heap.items[0].iterKV = nil
			heap.pop()
		} else if rn <= 16 {
			// 15% of the cases, change the key of the top element. It will stay the
			// top in only 25% of these, so 11.25% will change the top of the heap.
			t.Logf("%d: fixing heap", i)
			heap.items[0].iterKV.K.UserKey = makeKey()
			heap.fixTop()
		} else {
			t.Logf("%d: noop fixing heap", i)
			heap.fixTop()
		}
		checkHeap()
	}
}

// TestMergingIterHeapInit only measures the comparison savings during init,
// with uniform random keys. There is a ~3.7% saving, with the following being
// a representative result: cmp needed=104325 called=100416(frac=0.9625).
func TestMergingIterHeapInit(t *testing.T) {
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
				return key
			}
		}
	}
	for k := 0; k < 10000; k++ {
		// Memtable + 5 populated levels = 6 is the baseline. All levels populated +
		// memtable + batch = 9.
		levels := make([]mergingIterHeapItem, 6+rng.IntN(6))
		for i := range levels {
			levels[i].mergingIterLevel = &mergingIterLevel{
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
	}
}
