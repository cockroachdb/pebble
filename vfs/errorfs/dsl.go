// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package errorfs

import (
	"fmt"
	"go/scanner"
	"go/token"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

// ParseInjectorFromDSL parses a string encoding a ruleset describing when
// errors should be injected. There are a handful of supported functions and
// primitives:
//   - "ErrInjected" is a constant that injects an ErrInjected every time
//   - "any(injector, [injector]...)" injects the first error injected by the
//     provided injectors, with short circuiting.
//   - "pathMatch(pattern, injector)" wraps an injector, calling into the
//     provided injector only if an operation's file path matches the
//     provided shell pattern.
//   - "onIndex(idx, injector)" wraps an injector, calling into the provided
//     injector on the idx-th time it's invoked.
//   - "reads(injector)" wraps an injector, calling into the provided injector
//     only on read operations (eg, Open, Read, ReadAt, Stat, etc).
//   - "writes(injector)" wraps an injector, calling into the provided injector
//     only on write operations (eg, Create, Rename, Write, WriteAt, etc)
//
// Example: pathMatch("*.sst", onIndex(5, ErrInjected)) is a rule set that will
// inject an error on the 5-th I/O operation involving an sstable.
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
	inj = parseInjectorDSLFunc(&s)
	consumeTok(&s, token.SEMICOLON)
	consumeTok(&s, token.EOF)
	return inj, err
}

// LabelledError is an error that also implements Injector, unconditionally
// injecting itself. It implements String() by returning its label. It
// implements Error() by returning its underlying error.
type LabelledError struct {
	error
	label string
}

// String implements fmt.Stringer.
func (le LabelledError) String() string { return le.label }

// MaybeError implements Injector.
func (le LabelledError) MaybeError(Op) error { return le }

// AddErrorConstant defines a new error constant that may be used within the DSL
// parsed by ParseInjectorFromDSL and will inject the provided error.
func AddErrorConstant(le LabelledError) {
	if _, ok := dslParsers[le.String()]; ok {
		panic(fmt.Sprintf("errorfs: the identifier %s is already defined", le.String()))
	}
	dslParsers[le.String()] = func(*scanner.Scanner) Injector { return le }
}

var dslParsers map[string]func(*scanner.Scanner) Injector

func init() {
	dslParsers = map[string]func(*scanner.Scanner) Injector{
		"Any": func(s *scanner.Scanner) Injector {
			injs := []Injector{parseInjectorDSLFunc(s)}
			pos, tok, lit := s.Scan()
			for tok == token.LPAREN || tok == token.IDENT {
				inj, err := parseInjectorDSLFuncFromPos(s, pos, tok, lit)
				if err != nil {
					panic(err)
				}
				injs = append(injs, inj)
				pos, tok, lit = s.Scan()
			}
			if tok != token.RPAREN {
				panic(errors.Errorf("errorfs: unexpected token %s (%q) at char %v", tok, lit, pos))
			}
			return Any(injs...)
		},
		"PathMatch": func(s *scanner.Scanner) Injector {
			pattern := mustUnquote(consumeTok(s, token.STRING))
			next := parseInjectorDSLFunc(s)
			consumeTok(s, token.RPAREN)
			return PathMatch(pattern, next)
		},
		"OnIndex": func(s *scanner.Scanner) Injector {
			i, err := strconv.ParseInt(consumeTok(s, token.INT), 10, 32)
			if err != nil {
				panic(err)
			}
			next := parseInjectorDSLFunc(s)
			consumeTok(s, token.RPAREN)
			return OnIndex(int32(i), next)
		},
		"Reads": func(s *scanner.Scanner) Injector {
			next := parseInjectorDSLFunc(s)
			consumeTok(s, token.RPAREN)
			return Reads(next)
		},
		"Writes": func(s *scanner.Scanner) Injector {
			next := parseInjectorDSLFunc(s)
			consumeTok(s, token.RPAREN)
			return Writes(next)
		},
	}
	AddErrorConstant(ErrInjected)
}

func parseInjectorDSLFunc(s *scanner.Scanner) Injector {
	pos, tok, lit := s.Scan()
	inj, err := parseInjectorDSLFuncFromPos(s, pos, tok, lit)
	if err != nil {
		panic(err)
	}
	return inj
}

func parseInjectorDSLFuncFromPos(
	s *scanner.Scanner, pos token.Pos, tok token.Token, lit string,
) (Injector, error) {
	if tok == token.LPAREN {
		pos, tok, lit = s.Scan()
	}
	if tok != token.IDENT {
		return nil, errors.Errorf("errorfs: unexpected token %s (%q) at char %v", tok, lit, pos)
	}
	p, ok := dslParsers[lit]
	if !ok {
		return nil, errors.Errorf("errorfs: unknown ident %q", lit)
	}
	return p(s), nil
}

func consumeTok(s *scanner.Scanner, expected token.Token) (lit string) {
	pos, tok, lit := s.Scan()
	if tok != expected {
		panic(errors.Errorf("errorfs: unexpected token %s (%q) at char %v", tok, lit, pos))
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
