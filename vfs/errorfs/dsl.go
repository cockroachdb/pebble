// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package errorfs

import (
	"fmt"
	"go/scanner"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

// Predicate encodes conditional logic that determines whether to inject an
// error.
type Predicate interface {
	evaluate(Op) bool
	String() string
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

func (pm *pathMatch) evaluate(op Op) bool {
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
	Reads Predicate = opKindPred{kind: OpIsRead}
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

func (o *opFileReadAt) evaluate(op Op) bool {
	return op.Kind == OpFileReadAt && o.offset == op.Offset
}

type opKindPred struct {
	kind OpReadWrite
}

func (p opKindPred) String() string      { return p.kind.String() }
func (p opKindPred) evaluate(op Op) bool { return p.kind == op.Kind.ReadOrWrite() }

// And returns a predicate that returns true if all its operands return true.
func And(preds ...Predicate) Predicate { return and(preds) }

type and []Predicate

func (a and) String() string {
	var sb strings.Builder
	sb.WriteString("(And")
	for i := 0; i < len(a); i++ {
		sb.WriteRune(' ')
		sb.WriteString(a[i].String())
	}
	sb.WriteRune(')')
	return sb.String()
}

func (a and) evaluate(o Op) bool {
	ok := true
	for _, p := range a {
		ok = ok && p.evaluate(o)
	}
	return ok
}

// Or returns a predicate that returns true if any of its operands return true.
func Or(preds ...Predicate) Predicate { return or(preds) }

type or []Predicate

func (e or) String() string {
	var sb strings.Builder
	sb.WriteString("(Or")
	for i := 0; i < len(e); i++ {
		sb.WriteRune(' ')
		sb.WriteString(e[i].String())
	}
	sb.WriteRune(')')
	return sb.String()
}

func (e or) evaluate(o Op) bool {
	ok := false
	for _, p := range e {
		ok = ok || p.evaluate(o)
	}
	return ok
}

// OnIndex returns a predicate that returns true on its (n+1)-th invocation.
func OnIndex(index int32) *InjectIndex {
	ii := &InjectIndex{}
	ii.index.Store(index)
	return ii
}

// ParseInjectorFromDSL parses a string encoding a lisp-like DSL describing when
// errors should be injected.
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
//   - Operation-specific:
//     (OpFileReadAt <INTEGER>) is a predicate that evaluates to true iff
//     an operation is a file ReadAt call with an offset that's exactly equal.
//
// Example: (ErrInjected (And (PathMatch "*.sst") (OnIndex 5))) is a rule set
// that will inject an error on the 5-th I/O operation involving an sstable.
func ParseInjectorFromDSL(d string) (inj Injector, err error) {
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			err, ok = r.(error)
			if !ok {
				panic(r)
			}
		}
	}()

	fset := token.NewFileSet()
	file := fset.AddFile("", -1, len(d))
	var s scanner.Scanner
	s.Init(file, []byte(strings.TrimSpace(d)), nil /* no error handler */, 0)
	pos, tok, lit := s.Scan()
	inj, err = parseDSLInjectorFromPos(&s, pos, tok, lit)
	if err != nil {
		return nil, err
	}
	pos, tok, lit = s.Scan()
	if tok == token.SEMICOLON {
		pos, tok, lit = s.Scan()
	}
	if tok != token.EOF {
		return nil, errors.Errorf("errorfs: unexpected token %s (%q) at char %v; expected EOF", tok, lit, pos)
	}
	return inj, err
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
	if le.predicate == nil || le.predicate.evaluate(op) {
		return le
	}
	return nil
}

// If returns an Injector that returns the receiver error if the provided
// predicate evalutes to true.
func (le LabelledError) If(p Predicate) Injector {
	le.predicate = p
	return le
}

// AddError defines a new error that may be used within the DSL parsed by
// ParseInjectorFromDSL and will inject the provided error.
func AddError(le LabelledError) {
	dslKnownErrors[le.label] = le
}

var (
	dslPredicateExprs     map[string]func(*scanner.Scanner) Predicate
	dslPredicateConstants map[string]func(*scanner.Scanner) Predicate
	dslKnownErrors        map[string]LabelledError
)

func init() {
	dslKnownErrors = map[string]LabelledError{}
	dslPredicateConstants = map[string]func(*scanner.Scanner) Predicate{
		"Reads":  func(s *scanner.Scanner) Predicate { return Reads },
		"Writes": func(s *scanner.Scanner) Predicate { return Writes },
	}
	// Parsers for predicate exprs of the form `(ident ...)`.
	dslPredicateExprs = map[string]func(*scanner.Scanner) Predicate{
		"PathMatch": func(s *scanner.Scanner) Predicate {
			pattern := mustUnquote(consumeTok(s, token.STRING))
			consumeTok(s, token.RPAREN)
			return PathMatch(pattern)
		},
		"OnIndex": func(s *scanner.Scanner) Predicate {
			i, err := strconv.ParseInt(consumeTok(s, token.INT), 10, 32)
			if err != nil {
				panic(err)
			}
			consumeTok(s, token.RPAREN)
			return OnIndex(int32(i))
		},
		"And": func(s *scanner.Scanner) Predicate {
			return And(parseVariadicPredicate(s)...)
		},
		"Or": func(s *scanner.Scanner) Predicate {
			return Or(parseVariadicPredicate(s)...)
		},
		"OpFileReadAt": func(s *scanner.Scanner) Predicate {
			return parseFileReadAtOp(s)
		},
	}
	AddError(ErrInjected)
}

func parseVariadicPredicate(s *scanner.Scanner) (ret []Predicate) {
	pos, tok, lit := s.Scan()
	for tok == token.LPAREN || tok == token.IDENT {
		pred, err := parseDSLPredicateFromPos(s, pos, tok, lit)
		if err != nil {
			panic(err)
		}
		ret = append(ret, pred)
		pos, tok, lit = s.Scan()
	}
	if tok != token.RPAREN {
		panic(errors.Errorf("errorfs: unexpected token %s (%q) at char %v; expected RPAREN", tok, lit, pos))
	}
	return ret
}

func parseDSLInjectorFromPos(
	s *scanner.Scanner, pos token.Pos, tok token.Token, lit string,
) (Injector, error) {
	switch tok {
	case token.IDENT:
		// It's an injector of the form `ErrInjected`.
		le, ok := dslKnownErrors[lit]
		if !ok {
			return nil, errors.Errorf("errorfs: unknown error %q", lit)
		}
		return le, nil
	case token.LPAREN:
		// Otherwise it's an expression, eg: (ErrInjected (And ...))
		lit = consumeTok(s, token.IDENT)
		le, ok := dslKnownErrors[lit]
		if !ok {
			return nil, errors.Errorf("errorfs: unknown error %q", lit)
		}
		pos, tok, lit := s.Scan()
		pred, err := parseDSLPredicateFromPos(s, pos, tok, lit)
		if err != nil {
			panic(err)
		}
		consumeTok(s, token.RPAREN)
		return le.If(pred), nil
	default:
		return nil, errors.Errorf("errorfs: unexpected token %s (%q) at char %v; expected IDENT or LPAREN", tok, lit, pos)
	}
}

func parseDSLPredicateFromPos(
	s *scanner.Scanner, pos token.Pos, tok token.Token, lit string,
) (Predicate, error) {
	switch tok {
	case token.IDENT:
		// It's a predicate of the form `Reads`.
		p, ok := dslPredicateConstants[lit]
		if !ok {
			return nil, errors.Errorf("errorfs: unknown predicate constant %q", lit)
		}
		return p(s), nil
	case token.LPAREN:
		// Otherwise it's an expression, eg: (OnIndex 1)
		lit = consumeTok(s, token.IDENT)
		p, ok := dslPredicateExprs[lit]
		if !ok {
			return nil, errors.Errorf("errorfs: unknown predicate func %q", lit)
		}
		return p(s), nil
	default:
		return nil, errors.Errorf("errorfs: unexpected token %s (%q) at char %v; expected IDENT or LPAREN", tok, lit, pos)
	}
}

func consumeTok(s *scanner.Scanner, expected token.Token) (lit string) {
	pos, tok, lit := s.Scan()
	if tok != expected {
		panic(errors.Errorf("errorfs: unexpected token %s (%q) at char %v; expected %s", tok, lit, pos, expected))
	}
	return lit
}

func mustUnquote(lit string) string {
	s, err := strconv.Unquote(lit)
	if err != nil {
		panic(errors.Newf("errorfs: unquoting %q: %v", lit, err))
	}
	return s
}

func parseFileReadAtOp(s *scanner.Scanner) *opFileReadAt {
	lit := consumeTok(s, token.INT)
	off, err := strconv.ParseInt(lit, 10, 64)
	if err != nil {
		panic(err)
	}
	consumeTok(s, token.RPAREN)
	return &opFileReadAt{offset: off}
}
