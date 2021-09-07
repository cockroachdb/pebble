package metamorphic

import (
	"github.com/cockroachdb/pebble/internal/randvar"
	"golang.org/x/exp/rand"
)

// makeDelToSingleDelSequence returns a sequenceGenerator that will generate
// sequences of the form:
//
//   (SET -> GET)+ -> DELETE -> GET -> SET -> GET -> SINGLEDEL -> GET+
//
// i.e. one or more SETs, followed by a DELETE, followed by a single SET. This
// SET is then removed via a SINGLEDEL. GETs are interspersed between all
// operations. The sequence completes with a fixed number of GETs, to simulate
// fetching the key after it has been single-deleted.
//
// This sequenceGenerator is regression test for cockroachdb/cockroach#69414.
func makeDelToSingleDelSequence(key []byte, valueFn func() []byte) *sequenceGenerator {
	reader, writer := makeObjID(dbTag, 0), makeObjID(dbTag, 0)

	var statePrev opType
	var doneDel, doneSingleDel bool
	var setCount, trailingGetCount int
	return newSequenceGenerator(
		transitionMap{
			writerSet: func(rng *rand.Rand) (current op, next opType) {
				setCount++
				current = &setOp{writerID: writer, key: key, value: valueFn()}
				statePrev = writerSet
				next = readerGet
				return
			},
			writerDelete: func(rng *rand.Rand) (current op, next opType) {
				doneDel = true
				current = &deleteOp{writerID: writer, key: key}
				statePrev = writerDelete
				next = readerGet
				return
			},
			writerSingleDelete: func(rng *rand.Rand) (current op, next opType) {
				doneSingleDel = true
				current = &singleDeleteOp{writerID: writer, key: key}
				statePrev = writerSingleDelete
				next = readerGet
				return
			},
			readerGet: func(rng *rand.Rand) (current op, next opType) {
				switch statePrev {
				case writerSet:
					if doneDel && setCount > 1 {
						// Transition to SINGLEDEL only when we've performed a
						// DEL followed by a single SET.
						next = writerSingleDelete
					} else {
						// Else, bias towards performing more sets.
						r := randvar.NewWeighted(rng, 0.8, 0.2)
						next = []opType{writerSet, writerDelete}[r.Int()]
					}
				case writerDelete:
					// Transition back to SET after performing a DELETE.
					next = writerSet
				case writerSingleDelete, readerGet:
					// Perform a fixed number of trailing GETs to fetch the key
					// after it has been single-deleted.
					if doneSingleDel && trailingGetCount == 10 {
						next = stateDone
						return
					}
					trailingGetCount++
					next = readerGet
				}

				current = &getOp{readerID: reader, key: key}
				statePrev = readerGet
				return
			},
		},
		// Start with a SET.
		writerSet,
	)
}
