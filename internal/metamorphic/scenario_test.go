package metamorphic

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func TestDelToSingleDel(t *testing.T) {
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	g := newGenerator(rng)

	key := g.randValue(4, 12)
	valueFn := func() []byte { return g.randValue(4, 12) }
	s := makeDelToSingleDelSequence(key, valueFn)

	// Track the positions of each operation in the sequence.
	var sets, gets, dels, singleDels []int
	var i int
	for {
		op := s.next(rng)
		if op == nil {
			break
		}
		fmt.Println(op)
		switch op.(type) {
		case *setOp:
			sets = append(sets, i)
		case *getOp:
			gets = append(gets, i)
		case *deleteOp:
			dels = append(dels, i)
		case *singleDeleteOp:
			singleDels = append(singleDels, i)
		}
		i++
	}

	// All ops occurred at least once.
	if sets == nil || gets == nil || dels == nil || singleDels == nil {
		t.Fatalf("expected each operation to occur at least once")
	}

	// Exactly one SINGLEDEL.
	require.Len(t, singleDels, 1)

	// A single SET in between the DELETE and SINGLEDEL.
	idxSingleDel := singleDels[0]
	idxLastDel := dels[len(dels)-1]
	cnt := countOps(sets, func(i int) bool {
		return i > idxLastDel && i < idxSingleDel
	})
	require.Equal(t, 1, cnt)

	// No SETs or DELs after the SINGLEDEL.
	idxLastSet := sets[len(sets)-1]
	require.Less(t, idxLastSet, idxSingleDel)
	require.Less(t, idxLastDel, idxSingleDel)

	// A single GET follows every SET and DEL.
	cnt = countOps(gets, func(i int) bool {
		return i < idxSingleDel && i%2 == 1 /* odd numbered indices */
	})
	require.Equal(t, cnt, len(sets)+len(dels))

	// A fixed number of GETS (10) following the SINGLEDEL.
	cnt = countOps(gets, func(i int) bool {
		return i > idxSingleDel
	})
	require.Equal(t, 10, cnt)
}

// countOps returns the number of operations with indices satisfying the given
// predicate.
func countOps(is []int, predicate func(i int) bool) int {
	var count int
	for _, idx := range is {
		if predicate(idx) {
			count++
		}
	}
	return count
}
