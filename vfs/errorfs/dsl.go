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
// injecting itself.
type LabelledError struct {
	error
	label string
}

// String implements fmt.Stringer.
func (le LabelledError) String() string { return le.label }

// MaybeError implements Injector.
func (le LabelledError) MaybeError(Op, string) error { return le }

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
		"any": func(s *scanner.Scanner) Injector {
			var injs []Injector
			consumeTok(s, token.LPAREN)
			injs = append(injs, parseInjectorDSLFunc(s))
			pos, tok, lit := s.Scan()
			for tok == token.COMMA {
				injs = append(injs, parseInjectorDSLFunc(s))
				pos, tok, lit = s.Scan()
			}
			if tok != token.RPAREN {
				panic(errors.Errorf("errorfs: unexpected token %s (%q) at char %v", tok, lit, pos))
			}
			return Any(injs...)
		},
		"pathMatch": func(s *scanner.Scanner) Injector {
			consumeTok(s, token.LPAREN)
			pattern := mustUnquote(consumeTok(s, token.STRING))
			consumeTok(s, token.COMMA)
			next := parseInjectorDSLFunc(s)
			consumeTok(s, token.RPAREN)
			return PathMatch(pattern, next)
		},
		"onIndex": func(s *scanner.Scanner) Injector {
			consumeTok(s, token.LPAREN)
			i, err := strconv.ParseInt(consumeTok(s, token.INT), 10, 32)
			if err != nil {
				panic(err)
			}
			consumeTok(s, token.COMMA)
			next := parseInjectorDSLFunc(s)
			consumeTok(s, token.RPAREN)
			return OnIndex(int32(i), next)
		},
		"reads": func(s *scanner.Scanner) Injector {
			consumeTok(s, token.LPAREN)
			next := parseInjectorDSLFunc(s)
			consumeTok(s, token.RPAREN)
			return Reads(next)
		},
		"writes": func(s *scanner.Scanner) Injector {
			consumeTok(s, token.LPAREN)
			next := parseInjectorDSLFunc(s)
			consumeTok(s, token.RPAREN)
			return Writes(next)
		},
	}
	AddErrorConstant(ErrInjected)
}

func parseInjectorDSLFunc(s *scanner.Scanner) Injector {
	fn := consumeTok(s, token.IDENT)
	p, ok := dslParsers[fn]
	if !ok {
		panic(errors.Errorf("errorfs: unknown ident %q", fn))
	}
	return p(s)
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
