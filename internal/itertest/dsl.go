// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package itertest

import (
	"fmt"
	"go/token"
	"strings"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/dsl"
	"github.com/cockroachdb/pebble/vfs/errorfs"
)

// Predicate encodes conditional logic that yields a boolean.
type Predicate = dsl.Predicate[*ProbeContext]

// NewParser constructs a Probe parser.
func NewParser() *dsl.Parser[Probe] {
	predicateParser := dsl.NewPredicateParser[*ProbeContext]()
	for i, name := range opNames {
		opKind := OpKind(i)
		predicateParser.DefineConstant(name, func() dsl.Predicate[*ProbeContext] {
			// An OpKind implements dsl.Predicate[*ProbeContext].
			return opKind
		})
	}
	predicateParser.DefineFunc("UserKey",
		func(p *dsl.Parser[Predicate], s *dsl.Scanner) dsl.Predicate[*ProbeContext] {
			userKey := s.ConsumeString()
			s.Consume(token.RPAREN)
			return UserKey(userKey)
		})

	probeParser := dsl.NewParser[Probe]()
	probeParser.DefineConstant("ErrInjected", func() Probe { return ErrInjected })
	probeParser.DefineConstant("noop", Noop)
	probeParser.DefineConstant("Nil", Nil)
	probeParser.DefineFunc("If",
		func(p *dsl.Parser[Probe], s *dsl.Scanner) Probe {
			pred := If(
				predicateParser.ParseFromPos(s, s.Scan()),
				probeParser.ParseFromPos(s, s.Scan()),
				probeParser.ParseFromPos(s, s.Scan()),
			)
			s.Consume(token.RPAREN)
			return pred
		})
	probeParser.DefineFunc("ReturnKV",
		func(p *dsl.Parser[Probe], s *dsl.Scanner) Probe {
			ik := base.ParseInternalKey(s.ConsumeString())
			val := []byte(s.ConsumeString())
			s.Consume(token.RPAREN)
			return ReturnKV(&ik, val)
		})
	probeParser.DefineFunc("Log",
		func(p *dsl.Parser[Probe], s *dsl.Scanner) (ret Probe) {
			ret = loggingProbe{prefix: s.ConsumeString()}
			s.Consume(token.RPAREN)
			return ret
		})
	return probeParser
}

// ErrInjected is an error artificially injected for testing.
var ErrInjected = Error("ErrInjected", errorfs.ErrInjected)

// Error returns a Probe that returns the provided error. The name is Name
// returned by String().
func Error(name string, err error) *ErrorProbe {
	return &ErrorProbe{name: name, err: err}
}

// ErrorProbe is a Probe that injects an error.
type ErrorProbe struct {
	name string
	err  error
}

// String implements fmt.Stringer.
func (p *ErrorProbe) String() string {
	return p.name
}

// Error implements error, so that injected error values may be used as probes
// that inject themselves.
func (p *ErrorProbe) Error() error {
	return p.err
}

// Probe implements the Probe interface, replacing the iterator return value
// with an error.
func (p *ErrorProbe) Probe(pctx *ProbeContext) {
	pctx.Op.Return.Err = p.err
	pctx.Op.Return.Key = nil
	pctx.Op.Return.Value = base.LazyValue{}
}

// If a conditional Probe. If its predicate evaluates to true, it probes using
// its Then probe. If its predicate evalutes to false, it probes using its Else
// probe.
func If(pred Predicate, thenProbe, elseProbe Probe) Probe {
	return ifProbe{pred, thenProbe, elseProbe}
}

type ifProbe struct {
	Predicate Predicate
	Then      Probe
	Else      Probe
}

// String implements fmt.Stringer.
func (p ifProbe) String() string { return fmt.Sprintf("(If %s %s %s)", p.Predicate, p.Then, p.Else) }

// Probe implements Probe.
func (p ifProbe) Probe(pctx *ProbeContext) {
	if p.Predicate.Evaluate(pctx) {
		p.Then.Probe(pctx)
	} else {
		p.Else.Probe(pctx)
	}
}

type loggingProbe struct {
	prefix string
}

func (lp loggingProbe) String() string { return fmt.Sprintf("(Log %q)", lp.prefix) }
func (lp loggingProbe) Probe(pctx *ProbeContext) {
	opStr := strings.TrimPrefix(pctx.Kind.String(), "Op")
	fmt.Fprintf(pctx.Log, "%s%s(", lp.prefix, opStr)
	if pctx.SeekKey != nil {
		fmt.Fprintf(pctx.Log, "%q", pctx.SeekKey)
	}
	fmt.Fprint(pctx.Log, ") = ")
	if pctx.Return.Key == nil {
		fmt.Fprint(pctx.Log, "nil")
		if pctx.Return.Err != nil {
			fmt.Fprintf(pctx.Log, " <err=%q>", pctx.Return.Err)
		}
	} else {
		v, _, err := pctx.Return.Value.Value(nil)
		if err != nil {
			panic(err)
		}
		fmt.Fprintf(pctx.Log, "(%s,%q)", pctx.Return.Key, v)
	}
	fmt.Fprintln(pctx.Log)
}

// UserKey implements a predicate that evaluates to true if the returned
// InternalKey holds a specific user key.
type UserKey []byte

// String implements fmt.Stringer.
func (p UserKey) String() string { return fmt.Sprintf("(UserKey %q)", string(p)) }

// Evaluate implements Predicate.
func (p UserKey) Evaluate(pctx *ProbeContext) bool {
	return pctx.Op.Return.Key != nil && pctx.Comparer.Equal(pctx.Op.Return.Key.UserKey, p)
}

// ReturnKV returns a Probe that modifies an operation's return value to the
// provided KV pair.
func ReturnKV(k *base.InternalKey, v []byte) Probe {
	return &returnKV{k, v}
}

type returnKV struct {
	*base.InternalKey
	Value []byte
}

// Probe implements Probe.
func (kv *returnKV) Probe(pctx *ProbeContext) {
	pctx.Op.Return.Key = kv.InternalKey
	pctx.Op.Return.Value = base.MakeInPlaceValue(kv.Value)
}

// Noop returns a Probe that does nothing.
func Noop() Probe { return noop{} }

type noop struct{}

func (noop) String() string           { return "noop" }
func (noop) Probe(pctx *ProbeContext) {}

// Nil returns a Probe that always returns nil.
func Nil() Probe { return returnNil{} }

type returnNil struct{}

func (returnNil) String() string { return "Nil" }
func (returnNil) Probe(pctx *ProbeContext) {
	pctx.Op.Return.Key = nil
	pctx.Op.Return.Value = base.LazyValue{}
}
