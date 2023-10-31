// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package errorfs

import (
	"encoding/binary"
	"fmt"
	"go/token"
	"hash/maphash"
	"math/rand"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/dsl"
)

// MustParse parses an Injector from the DSL, panicking if parsing fails.
func MustParse(s string) Injector {
	inj, err := Parse(s)
	if err != nil {
		panic(err)
	}
	return inj
}

// MustParsef parses an Injector from the DSL, panicking if parsing fails.
func MustParsef(s string, args ...interface{}) Injector { return MustParse(fmt.Sprintf(s, args...)) }

// Predicate encodes conditional logic that determines whether to inject an
// error.
type Predicate = dsl.Predicate[Op]

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
	Reads dsl.Predicate[Op] = opKindPred{kind: OpIsRead}
	// Writes is a predicate that returns true iff an operation is a write
	// operation.
	Writes Predicate = opKindPred{kind: OpIsWrite}
)

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

type opKindPred struct {
	kind OpReadWrite
}

func (p opKindPred) String() string      { return p.kind.String() }
func (p opKindPred) Evaluate(op Op) bool { return p.kind == op.Kind.ReadOrWrite() }

// OnIndex returns a predicate that returns true on its (n+1)-th invocation.
func OnIndex(index int32) *InjectIndex {
	return &InjectIndex{Index: dsl.OnIndex[Op](index)}
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
	rs := &randomSeed{p: p, rootSeed: seed}
	rs.mu.perFilePrng = make(map[string]*rand.Rand)
	return rs
}

type randomSeed struct {
	// p defines the probability of an error being injected.
	p        float64
	rootSeed int64
	mu       struct {
		sync.Mutex
		h           maphash.Hash
		perFilePrng map[string]*rand.Rand
	}
}

func (rs *randomSeed) String() string {
	if rs.rootSeed == 0 {
		return fmt.Sprintf("(Randomly %.2f)", rs.p)
	}
	return fmt.Sprintf("(Randomly %.2f %d)", rs.p, rs.rootSeed)
}

func (rs *randomSeed) Evaluate(op Op) bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	prng, ok := rs.mu.perFilePrng[op.Path]
	if !ok {
		// This is the first time an operation has been performed on the file at
		// this path. Initialize the per-file prng by computing a deterministic
		// hash of the path.
		rs.mu.h.Reset()
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], uint64(rs.rootSeed))
		if _, err := rs.mu.h.Write(b[:]); err != nil {
			panic(err)
		}
		if _, err := rs.mu.h.WriteString(op.Path); err != nil {
			panic(err)
		}
		seed := rs.mu.h.Sum64()
		prng = rand.New(rand.NewSource(int64(seed)))
		rs.mu.perFilePrng[op.Path] = prng
	}
	return prng.Float64() < rs.p
}

// Parse parses a string encoding a lisp-like DSL describing when errors should
// be injected.
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
//   - <OpKind> : any op kind is a constant predicate that evaluates to true iff
//     the operation is an operation of that kind.
//   - (PathMatch <STRING>) is a predicate that evalutes to true iff the
//     operation's file path matches the provided shell pattern.
//   - (OnIndex <INTEGER>) is a predicate that evaluates to true only on the n-th
//     invocation.
//   - (Not <PREDICATE>) is a predicate that negates another predicate.
//   - (And <PREDICATE> [PREDICATE]...) is a predicate that evaluates to true
//     iff all the provided predicates evaluate to true. And short circuits on
//     the first predicate to evaluate to false.
//   - (Or <PREDICATE> [PREDICATE]...) is a predicate that evaluates to true iff
//     at least one of the provided predicates evaluates to true. Or short
//     circuits on the first predicate to evaluate to true.
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
func Parse(d string) (inj Injector, err error) {
	return dslInjectorParser.Parse(d)
}

// LabelledError is an error that also implements Injector, unconditionally
// injecting itself. It implements String() by returning its label. It
// implements Error() by returning its underlying error.
type LabelledError struct {
	error
	label     string
	predicate Predicate
}

// String implements fmt.Stringer.
func (le LabelledError) String() string {
	if le.predicate == nil {
		return le.label
	}
	return fmt.Sprintf("(%s %s)", le.label, le.predicate.String())
}

// MaybeError implements Injector.
func (le LabelledError) MaybeError(op Op) error {
	if le.predicate == nil || le.predicate.Evaluate(op) {
		return le
	}
	return nil
}

// If returns an Injector that returns the receiver error if the provided
// predicate evalutes to true.
func (le LabelledError) If(p Predicate) LabelledError {
	le.predicate = p
	return le
}

// AddError defines a new error that may be used within the DSL parsed by
// ParseInjectorFromDSL and will inject the provided error.
func AddError(le LabelledError) {
	// Define the error both as a constant that unconditionally injects the
	// error, and as a function that injects the error only if the provided
	// predicate evaluates to true.
	dslInjectorParser.DefineConstant(le.label, func() LabelledError { return le })
	dslInjectorParser.DefineFunc(le.label,
		func(p *dsl.Parser[LabelledError], s *dsl.Scanner) LabelledError {
			pred, err := dslPredicateParser.ParseFromPos(s, s.Scan())
			if err != nil {
				panic(err)
			}
			s.Consume(token.RPAREN)
			return le.If(pred)
		})
}

var (
	dslInjectorParser  *dsl.Parser[LabelledError]     = dsl.NewParser[LabelledError]()
	dslPredicateParser *dsl.Parser[dsl.Predicate[Op]] = dsl.NewPredicateParser[Op]()
)

func init() {
	AddError(ErrInjected)

	dslPredicateParser.DefineConstant("Reads", func() dsl.Predicate[Op] { return Reads })
	dslPredicateParser.DefineConstant("Writes", func() dsl.Predicate[Op] { return Writes })
	for i, name := range opNames {
		opKind := OpKind(i)
		dslPredicateParser.DefineConstant(name, func() dsl.Predicate[Op] {
			// An OpKind implements dsl.Predicate[Op].
			return opKind
		})
	}
	dslPredicateParser.DefineFunc("PathMatch",
		func(p *dsl.Parser[dsl.Predicate[Op]], s *dsl.Scanner) dsl.Predicate[Op] {
			pattern := mustUnquote(s.Consume(token.STRING).Lit)
			s.Consume(token.RPAREN)
			return PathMatch(pattern)
		})
	dslPredicateParser.DefineFunc("OpFileReadAt",
		func(p *dsl.Parser[dsl.Predicate[Op]], s *dsl.Scanner) dsl.Predicate[Op] {
			return parseFileReadAtOp(s)
		})
	dslPredicateParser.DefineFunc("Randomly",
		func(p *dsl.Parser[dsl.Predicate[Op]], s *dsl.Scanner) dsl.Predicate[Op] {
			return parseRandomly(s)
		})
}

func mustUnquote(lit string) string {
	s, err := strconv.Unquote(lit)
	if err != nil {
		panic(errors.Newf("errorfs: unquoting %q: %v", lit, err))
	}
	return s
}

func parseFileReadAtOp(s *dsl.Scanner) *opFileReadAt {
	off, err := strconv.ParseInt(s.Consume(token.INT).Lit, 10, 64)
	if err != nil {
		panic(err)
	}
	s.Consume(token.RPAREN)
	return &opFileReadAt{offset: off}
}

func parseRandomly(s *dsl.Scanner) Predicate {
	p, err := strconv.ParseFloat(s.Consume(token.FLOAT).Lit, 64)
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
		panic(errors.Errorf("errorfs: unexpected token %s; expected RPAREN | FLOAT", tok))
	}
	return Randomly(p, seed)
}
