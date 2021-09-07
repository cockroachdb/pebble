package metamorphic

import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func TestSequenceGenerator(t *testing.T) {
	n := 10
	key, value := []byte("foo"), []byte("bar")

	// A simple state machine that alternates states between GET and SET n times
	// before transitioning into a terminal state.
	tMap := transitionMap{
		readerGet: func(rng *rand.Rand) (current op, next opType) {
			current = &getOp{key: key, readerID: makeObjID(dbTag, 0)}
			n--
			if n > 0 {
				next = writerSet
			} else {
				next = stateDone
			}
			return
		},
		writerSet: func(rng *rand.Rand) (current op, next opType) {
			current = &setOp{key: key, value: value, writerID: makeObjID(dbTag, 0)}
			n--
			if n > 0 {
				next = readerGet
			} else {
				next = stateDone
			}
			return
		},
	}

	rng := rand.New(rand.NewSource(0))
	g := newSequenceGenerator(tMap, readerGet)

	// Generate the sequence from the state machine, counting GETs and SETs.
	var nGet, nSet int
	var last opType
	for {
		op := g.next(rng)
		if op == nil {
			break // Terminal state.
		}
		switch op.(type) {
		case *getOp:
			nGet++
			if last > 0 {
				require.Equal(t, writerSet, last)
			}
			last = readerGet
		case *setOp:
			nSet++
			require.Equal(t, readerGet, last)
			last = writerSet
		default:
			t.Fatalf("unknown op type: %s", op)
		}
	}

	require.Equal(t, nGet, 5)
	require.Equal(t, nSet, 5)
}
