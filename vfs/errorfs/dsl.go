// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package errorfs

import (
	"fmt"
	"go/token"
	"math/rand/v2"
	"path/filepath"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/dsl"
)

// Predicate encodes conditional logic that determines whether to inject an
// error.
type Predicate = dsl.Predicate[Op]

// And returns a predicate that evaluates to true if all of the operands
// evaluate to true.
func And(operands ...Predicate) Predicate {
	return dsl.And[Op](operands...)
}

// Not returns a predicate that evaluates to true if the operand evaluates to false.
func Not(operand Predicate) Predicate {
	return dsl.Not[Op](operand)
}

// PathMatch returns a predicate that returns true if an operation's file path
// matches the provided pattern according to filepath.Match.
func PathMatch(pattern string) Predicate {
	return &pathMatch{pattern: pattern}
}

type pathMatch struct {
	pattern string
}

func (pm *pathMatch) String() string {
	return fmt.Sprintf("(PathMatch %q)", pm.pattern)
}

func (pm *pathMatch) Evaluate(op Op) bool {
	matched, err := filepath.Match(pm.pattern, op.Path)
	if err != nil {
		// Only possible error is ErrBadPattern, indicating an issue with
		// the test itself.
		panic(err)
	}
	return matched
}

var (
	// Reads is a predicate that returns true iff an operation is a read
	// operation.
	Reads Predicate = OpKindIn("Reads", ReadOps)
	// Writes is a predicate that returns true iff an operation is a write
	// operation.
	Writes Predicate = OpKindIn("Writes", WriteOps)
)

// OpKindIn returns a predicate that evaluates to true if an operation's kind is
// in the given set.
func OpKindIn(desc string, kinds OpKinds) Predicate {
	return opKindPred{desc: desc, kinds: kinds}
}

type opKindPred struct {
	desc  string
	kinds OpKinds
}

func (p opKindPred) String() string      { return p.desc }
func (p opKindPred) Evaluate(op Op) bool { return p.kinds.Contains(op.Kind) }

type opFileReadAt struct {
	// offset configures the predicate to evaluate to true only if the
	// operation's offset exactly matches offset.
	offset int64
}

func (o *opFileReadAt) String() string {
	return fmt.Sprintf("(FileReadAt %d)", o.offset)
}

func (o *opFileReadAt) Evaluate(op Op) bool {
	return op.Kind == OpFileReadAt && o.offset == op.Offset
}

// Randomly constructs a new predicate that pseudorandomly evaluates to true
// with probability p using randomness determinstically derived from seed.
//
// The predicate is deterministic with respect to file paths: its behavior for a
// particular file is deterministic regardless of intervening evaluations for
// operations on other files. This can be used to ensure determinism despite
// nondeterministic concurrency if the concurrency is constrained to separate
// files.
func Randomly(p float64, seed int64) Predicate {
	rs := &randomSeed{p: p}
	rs.keyedPrng.init(seed)
	return rs
}

type randomSeed struct {
	// p defines the probability of an error being injected.
	p float64
	keyedPrng
}

func (rs *randomSeed) String() string {
	if rs.rootSeed == 0 {
		return fmt.Sprintf("(Randomly %.2f)", rs.p)
	}
	return fmt.Sprintf("(Randomly %.2f %d)", rs.p, rs.rootSeed)
}

func (rs *randomSeed) Evaluate(op Op) bool {
	var ok bool
	rs.keyedPrng.withKey(op.Path, func(prng *rand.Rand) {
		ok = prng.Float64() < rs.p
	})
	return ok
}

// ParseDSL parses the provided string using the default DSL parser.
func ParseDSL(s string) (Injector, error) {
	return defaultParser.Parse(s)
}

var defaultParser = NewParser()

// NewParser constructs a new parser for an encoding of a lisp-like DSL
// describing error injectors.
//
// Errors:
// - ErrInjected is the only error currently supported by the DSL.
//
// Injectors:
//   - <ERROR>: An error by itself is an injector that injects an error every
//     time.
//   - (<ERROR> <PREDICATE>) is an injector that injects an error only when
//     the operation satisfies the predicate.
//
// Predicates:
//   - Reads is a constant predicate that evalutes to true iff the operation is a
//     read operation (eg, Open, Read, ReadAt, Stat)
//   - Writes is a constant predicate that evaluates to true iff the operation is
//     a write operation (eg, Create, Rename, Write, WriteAt, etc).
//   - (PathMatch <STRING>) is a predicate that evalutes to true iff the
//     operation's file path matches the provided shell pattern.
//   - (OnIndex <INTEGER>) is a predicate that evaluates to true only on the n-th
//     invocation.
//   - (And <PREDICATE> [PREDICATE]...) is a predicate that evaluates to true
//     iff all the provided predicates evaluate to true. And short circuits on
//     the first predicate to evaluate to false.
//   - (Or <PREDICATE> [PREDICATE]...) is a predicate that evaluates to true iff
//     at least one of the provided predicates evaluates to true. Or short
//     circuits on the first predicate to evaluate to true.
//   - (Not <PREDICATE>) is a predicate that evaluates to true iff its provided
//     predicates evaluates to false.
//   - (Randomly <FLOAT> [INTEGER]) is a predicate that pseudorandomly evaluates
//     to true. The probability of evaluating to true is determined by the
//     required float argument (must be ≤1). The optional second parameter is a
//     pseudorandom seed, for adjusting the deterministic randomness.
//   - Operation-specific:
//     (OpFileReadAt <INTEGER>) is a predicate that evaluates to true iff
//     an operation is a file ReadAt call with an offset that's exactly equal.
//
// Example: (ErrInjected (And (PathMatch "*.sst") (OnIndex 5))) is a rule set
// that will inject an error on the 5-th I/O operation involving an sstable.
func NewParser() *Parser {
	p := &Parser{
		predicates: dsl.NewPredicateParser[Op](),
		injectors:  dsl.NewParser[Injector](),
	}
	p.predicates.DefineConstant("Reads", func() dsl.Predicate[Op] { return Reads })
	p.predicates.DefineConstant("Writes", func() dsl.Predicate[Op] { return Writes })
	p.predicates.DefineConstant("OpFileWrite", func() dsl.Predicate[Op] { return OpKindIn("OpFileWrite", MakeOpKinds(OpFileWrite)) })
	p.predicates.DefineFunc("PathMatch",
		func(p *dsl.Parser[dsl.Predicate[Op]], s *dsl.Scanner) dsl.Predicate[Op] {
			pattern := s.ConsumeString()
			s.Consume(token.RPAREN)
			return PathMatch(pattern)
		})
	p.predicates.DefineFunc("OpFileReadAt",
		func(p *dsl.Parser[dsl.Predicate[Op]], s *dsl.Scanner) dsl.Predicate[Op] {
			return parseFileReadAtOp(s)
		})
	p.predicates.DefineFunc("Randomly",
		func(p *dsl.Parser[dsl.Predicate[Op]], s *dsl.Scanner) dsl.Predicate[Op] {
			return parseRandomly(s)
		})
	p.AddError(ErrInjected)
	p.injectors.DefineFunc("RandomLatency",
		func(_ *dsl.Parser[Injector], s *dsl.Scanner) Injector {
			return parseRandomLatency(p, s)
		})
	return p
}

// A Parser parses the error-injecting DSL. It may be extended to include
// additional errors through AddError.
type Parser struct {
	predicates *dsl.Parser[dsl.Predicate[Op]]
	injectors  *dsl.Parser[Injector]
}

// Parse parses the error injection DSL, returning the parsed injector.
func (p *Parser) Parse(s string) (Injector, error) {
	return p.injectors.Parse(s)
}

// AddError defines a new error that may be used within the DSL parsed by
// Parse and will inject the provided error.
func (p *Parser) AddError(le LabelledError) {
	// Define the error both as a constant that unconditionally injects the
	// error, and as a function that injects the error only if the provided
	// predicate evaluates to true.
	p.injectors.DefineConstant(le.Label, func() Injector { return le })
	p.injectors.DefineFunc(le.Label,
		func(_ *dsl.Parser[Injector], s *dsl.Scanner) Injector {
			pred := p.predicates.ParseFromPos(s, s.Scan())
			s.Consume(token.RPAREN)
			return le.If(pred)
		})
}

// LabelledError is an error that also implements Injector, unconditionally
// injecting itself. It implements String() by returning its label. It
// implements Error() by returning its underlying error.
type LabelledError struct {
	error
	Label     string
	predicate Predicate
}

// String implements fmt.Stringer.
func (le LabelledError) String() string {
	if le.predicate == nil {
		return le.Label
	}
	return fmt.Sprintf("(%s %s)", le.Label, le.predicate.String())
}

// MaybeError implements Injector.
func (le LabelledError) MaybeError(op Op) error {
	if le.predicate == nil || le.predicate.Evaluate(op) {
		return errors.WithStack(le)
	}
	return nil
}

// If returns an Injector that returns the receiver error if the provided
// predicate evalutes to true.
func (le LabelledError) If(p Predicate) Injector {
	le.predicate = p
	return le
}

func parseFileReadAtOp(s *dsl.Scanner) *opFileReadAt {
	lit := s.Consume(token.INT).Lit
	off, err := strconv.ParseInt(lit, 10, 64)
	if err != nil {
		panic(err)
	}
	s.Consume(token.RPAREN)
	return &opFileReadAt{offset: off}
}

func parseRandomly(s *dsl.Scanner) Predicate {
	lit := s.Consume(token.FLOAT).Lit
	p, err := strconv.ParseFloat(lit, 64)
	if err != nil {
		panic(err)
	} else if p > 1.0 {
		// NB: It's not possible for p to be less than zero because we don't
		// try to parse the '-' token.
		panic(errors.Newf("errorfs: Randomly proability p must be within p ≤ 1.0"))
	}

	var seed int64
	tok := s.Scan()
	switch tok.Kind {
	case token.RPAREN:
	case token.INT:
		seed, err = strconv.ParseInt(tok.Lit, 10, 64)
		if err != nil {
			panic(err)
		}
		s.Consume(token.RPAREN)
	default:
		panic(errors.Errorf("errorfs: unexpected token %s; expected RPAREN | FLOAT", tok.String()))
	}
	return Randomly(p, seed)
}
