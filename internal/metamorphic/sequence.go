package metamorphic

import "golang.org/x/exp/rand"

// transitionFn is a node in the state machine graph. The node is a function
// that generates the op resulting from moving to this node and the next opType
// representing the next state in the state machine.
type transitionFn func(rng *rand.Rand) (current op, next opType)

// transitionMap is a blueprint for generating sequences of ops. It functions as
// a graph representing a state machine. Each node in the graph is a function
// returning the current op and the next opType to transition to.
type transitionMap map[opType]transitionFn

// stateDone is the terminal state for the state machine.
var stateDone opType = -1

// sequenceGenerator generates a sequence of ops from an transitionMap
// corresponding to a specific sequenceGenerator.
type sequenceGenerator struct {
	tMap      transitionMap
	nextState opType
	steps     int
}

// newSequenceGenerator constructs a new sequenceGenerator with the given
// transition map, post-ops, and the initial state of the sequence.
func newSequenceGenerator(m transitionMap, initial opType) *sequenceGenerator {
	return &sequenceGenerator{
		tMap:      m,
		nextState: initial,
	}
}

// next generates the next ops from the transitionMap state machine, and
// transitions the transitionMap to the next state.
func (g *sequenceGenerator) next(rng *rand.Rand) op {
	if g.nextState == stateDone {
		return nil
	}

	// Randomly pick the next state to transition into. Generate the op from
	// this transition and move to the next state.
	var o op
	o, g.nextState = g.tMap[g.nextState](rng)
	g.steps++

	return o
}
