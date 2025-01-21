// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"fmt"
	"go/token"
	"io"
	"reflect"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2/internal/dsl"
	"github.com/cockroachdb/pebble/v2/internal/keyspan"
	"github.com/cockroachdb/pebble/v2/internal/treeprinter"
)

// This file contains testing facilities for Spans and FragmentIterators. It's
// a copy of the facilities defined in internal/keyspan/datadriven_test.go.
//
// TODO(jackson): Move keyspan.{Span,Key,FragmentIterator} into internal/base,
// and then move the testing facilities to an independent package, eg
// internal/itertest, where it may be shared by both uses.

// keyspanProbe defines an interface for probes that may inspect or mutate internal
// span iterator behavior.
type keyspanProbe interface {
	// probe inspects, and possibly manipulates, iterator operations' results.
	probe(*keyspanProbeContext)
}

func parseKeyspanProbes(probeDSLs ...string) []keyspanProbe {
	probes := make([]keyspanProbe, len(probeDSLs))
	var err error
	for i := range probeDSLs {
		probes[i], err = probeParser.Parse(probeDSLs[i])
		if err != nil {
			panic(err)
		}
	}
	return probes
}

func attachKeyspanProbes(
	iter keyspan.FragmentIterator, pctx keyspanProbeContext, probes ...keyspanProbe,
) keyspan.FragmentIterator {
	if pctx.log == nil {
		pctx.log = io.Discard
	}
	for i := range probes {
		iter = &probeKeyspanIterator{
			iter:     iter,
			probe:    probes[i],
			probeCtx: pctx,
		}
	}
	return iter
}

// keyspanProbeContext provides the context within which a probe is run. It includes
// information about the iterator operation in progress.
type keyspanProbeContext struct {
	keyspanOp
	log io.Writer
}

type keyspanOp struct {
	Kind    keyspanOpKind
	SeekKey []byte
	Span    *keyspan.Span
	Err     error
}

// errInjected is an error artificially injected for testing.
var errInjected = &errorProbe{name: "ErrInjected", err: errors.New("injected error")}

var probeParser = func() *dsl.Parser[keyspanProbe] {
	valuerParser := dsl.NewParser[valuer]()
	valuerParser.DefineConstant("StartKey", func() valuer { return startKey{} })
	valuerParser.DefineConstant("SeekKey", func() valuer { return seekKey{} })
	valuerParser.DefineFunc("Bytes",
		func(p *dsl.Parser[valuer], s *dsl.Scanner) valuer {
			v := bytesConstant{bytes: []byte(s.ConsumeString())}
			s.Consume(token.RPAREN)
			return v
		})

	predicateParser := dsl.NewPredicateParser[*keyspanProbeContext]()
	predicateParser.DefineFunc("Equal",
		func(p *dsl.Parser[dsl.Predicate[*keyspanProbeContext]], s *dsl.Scanner) dsl.Predicate[*keyspanProbeContext] {
			eq := equal{
				valuerParser.ParseFromPos(s, s.Scan()),
				valuerParser.ParseFromPos(s, s.Scan()),
			}
			s.Consume(token.RPAREN)
			return eq
		})
	for i, name := range opNames {
		opKind := keyspanOpKind(i)
		predicateParser.DefineConstant(name, func() dsl.Predicate[*keyspanProbeContext] {
			// An OpKind implements dsl.Predicate[*probeContext].
			return opKind
		})
	}
	probeParser := dsl.NewParser[keyspanProbe]()
	probeParser.DefineConstant("ErrInjected", func() keyspanProbe { return errInjected })
	probeParser.DefineConstant("noop", func() keyspanProbe { return noop{} })
	probeParser.DefineFunc("If",
		func(p *dsl.Parser[keyspanProbe], s *dsl.Scanner) keyspanProbe {
			probe := ifProbe{
				predicateParser.ParseFromPos(s, s.Scan()),
				probeParser.ParseFromPos(s, s.Scan()),
				probeParser.ParseFromPos(s, s.Scan()),
			}
			s.Consume(token.RPAREN)
			return probe
		})
	probeParser.DefineFunc("Return",
		func(p *dsl.Parser[keyspanProbe], s *dsl.Scanner) (ret keyspanProbe) {
			switch tok := s.Scan(); tok.Kind {
			case token.STRING:
				str, err := strconv.Unquote(tok.Lit)
				if err != nil {
					panic(err)
				}
				span := keyspan.ParseSpan(str)
				ret = returnSpan{s: &span}
			case token.IDENT:
				switch tok.Lit {
				case "nil":
					ret = returnSpan{s: nil}
				default:
					panic(errors.Newf("unrecognized return value %q", tok.Lit))
				}
			}
			s.Consume(token.RPAREN)
			return ret
		})
	probeParser.DefineFunc("Log",
		func(p *dsl.Parser[keyspanProbe], s *dsl.Scanner) (ret keyspanProbe) {
			ret = loggingProbe{prefix: s.ConsumeString()}
			s.Consume(token.RPAREN)
			return ret
		})
	return probeParser
}()

// probe implementations

type errorProbe struct {
	name string
	err  error
}

func (p *errorProbe) String() string { return p.name }
func (p *errorProbe) Error() error   { return p.err }
func (p *errorProbe) probe(pctx *keyspanProbeContext) {
	pctx.keyspanOp.Err = p.err
	pctx.keyspanOp.Span = nil
}

// ifProbe is a conditional probe. If its predicate evaluates to true, it probes
// using its Then probe. If its predicate evalutes to false, it probes using its
// Else probe.
type ifProbe struct {
	Predicate dsl.Predicate[*keyspanProbeContext]
	Then      keyspanProbe
	Else      keyspanProbe
}

func (p ifProbe) String() string { return fmt.Sprintf("(If %s %s %s)", p.Predicate, p.Then, p.Else) }
func (p ifProbe) probe(pctx *keyspanProbeContext) {
	if p.Predicate.Evaluate(pctx) {
		p.Then.probe(pctx)
	} else {
		p.Else.probe(pctx)
	}
}

type returnSpan struct {
	s *keyspan.Span
}

func (p returnSpan) String() string {
	if p.s == nil {
		return "(Return nil)"
	}
	return fmt.Sprintf("(Return %q)", p.s.String())
}

func (p returnSpan) probe(pctx *keyspanProbeContext) {
	pctx.keyspanOp.Span = p.s
	pctx.keyspanOp.Err = nil
}

type noop struct{}

func (noop) String() string                  { return "Noop" }
func (noop) probe(pctx *keyspanProbeContext) {}

type loggingProbe struct {
	prefix string
}

func (lp loggingProbe) String() string { return fmt.Sprintf("(Log %q)", lp.prefix) }
func (lp loggingProbe) probe(pctx *keyspanProbeContext) {
	opStr := strings.TrimPrefix(pctx.keyspanOp.Kind.String(), "Op")
	fmt.Fprintf(pctx.log, "%s%s(", lp.prefix, opStr)
	if pctx.keyspanOp.SeekKey != nil {
		fmt.Fprintf(pctx.log, "%q", pctx.keyspanOp.SeekKey)
	}
	fmt.Fprint(pctx.log, ") = ")
	if pctx.keyspanOp.Span == nil {
		fmt.Fprint(pctx.log, "nil")
		if pctx.keyspanOp.Err != nil {
			fmt.Fprintf(pctx.log, " <err=%q>", pctx.keyspanOp.Err)
		}
	} else {
		fmt.Fprint(pctx.log, pctx.keyspanOp.Span.String())
	}
	fmt.Fprintln(pctx.log)
}

// dsl.Predicate[*probeContext] implementations.

type equal struct {
	a, b valuer
}

func (e equal) String() string { return fmt.Sprintf("(Equal %s %s)", e.a, e.b) }
func (e equal) Evaluate(pctx *keyspanProbeContext) bool {
	return reflect.DeepEqual(e.a.value(pctx), e.b.value(pctx))
}

// keyspanOpKind indicates the type of iterator operation being performed.
type keyspanOpKind int8

const (
	opSpanSeekGE keyspanOpKind = iota
	opSpanSeekLT
	opSpanFirst
	opSpanLast
	opSpanNext
	opSpanPrev
	opSpanClose
	numKeyspanOpKinds
)

func (o keyspanOpKind) String() string                          { return opNames[o] }
func (o keyspanOpKind) Evaluate(pctx *keyspanProbeContext) bool { return pctx.keyspanOp.Kind == o }

var opNames = [numKeyspanOpKinds]string{
	opSpanSeekGE: "opSpanSeekGE",
	opSpanSeekLT: "opSpanSeekLT",
	opSpanFirst:  "opSpanFirst",
	opSpanLast:   "opSpanLast",
	opSpanNext:   "opSpanNext",
	opSpanPrev:   "opSpanPrev",
	opSpanClose:  "opSpanClose",
}

// valuer implementations

type valuer interface {
	fmt.Stringer
	value(pctx *keyspanProbeContext) any
}

type bytesConstant struct {
	bytes []byte
}

func (b bytesConstant) String() string                      { return fmt.Sprintf("%q", string(b.bytes)) }
func (b bytesConstant) value(pctx *keyspanProbeContext) any { return b.bytes }

type startKey struct{}

func (s startKey) String() string { return "StartKey" }
func (s startKey) value(pctx *keyspanProbeContext) any {
	if pctx.keyspanOp.Span == nil {
		return nil
	}
	return pctx.keyspanOp.Span.Start
}

type seekKey struct{}

func (s seekKey) String() string { return "SeekKey" }
func (s seekKey) value(pctx *keyspanProbeContext) any {
	if pctx.keyspanOp.SeekKey == nil {
		return nil
	}
	return pctx.keyspanOp.SeekKey
}

type probeKeyspanIterator struct {
	iter     keyspan.FragmentIterator
	probe    keyspanProbe
	probeCtx keyspanProbeContext
}

// Assert that probeIterator implements the fragment iterator interface.
var _ keyspan.FragmentIterator = (*probeKeyspanIterator)(nil)

func (p *probeKeyspanIterator) handleOp(preProbeOp keyspanOp) (*keyspan.Span, error) {
	p.probeCtx.keyspanOp = preProbeOp
	p.probe.probe(&p.probeCtx)
	return p.probeCtx.keyspanOp.Span, p.probeCtx.keyspanOp.Err
}

func (p *probeKeyspanIterator) SeekGE(key []byte) (*keyspan.Span, error) {
	op := keyspanOp{
		Kind:    opSpanSeekGE,
		SeekKey: key,
	}
	if p.iter != nil {
		op.Span, op.Err = p.iter.SeekGE(key)
	}
	return p.handleOp(op)
}

func (p *probeKeyspanIterator) SeekLT(key []byte) (*keyspan.Span, error) {
	op := keyspanOp{
		Kind:    opSpanSeekLT,
		SeekKey: key,
	}
	if p.iter != nil {
		op.Span, op.Err = p.iter.SeekLT(key)
	}
	return p.handleOp(op)
}

func (p *probeKeyspanIterator) First() (*keyspan.Span, error) {
	op := keyspanOp{Kind: opSpanFirst}
	if p.iter != nil {
		op.Span, op.Err = p.iter.First()
	}
	return p.handleOp(op)
}

func (p *probeKeyspanIterator) Last() (*keyspan.Span, error) {
	op := keyspanOp{Kind: opSpanLast}
	if p.iter != nil {
		op.Span, op.Err = p.iter.Last()
	}
	return p.handleOp(op)
}

func (p *probeKeyspanIterator) Next() (*keyspan.Span, error) {
	op := keyspanOp{Kind: opSpanNext}
	if p.iter != nil {
		op.Span, op.Err = p.iter.Next()
	}
	return p.handleOp(op)
}

func (p *probeKeyspanIterator) Prev() (*keyspan.Span, error) {
	op := keyspanOp{Kind: opSpanPrev}
	if p.iter != nil {
		op.Span, op.Err = p.iter.Prev()
	}
	return p.handleOp(op)
}

func (p *probeKeyspanIterator) WrapChildren(wrap keyspan.WrapFn) {
	p.iter = wrap(p.iter)
}

// DebugTree is part of the FragmentIterator interface.
func (p *probeKeyspanIterator) DebugTree(tp treeprinter.Node) {
	n := tp.Childf("%T(%p)", p, p)
	if p.iter != nil {
		p.iter.DebugTree(n)
	}
}

// SetContext is part of the FragmentIterator interface.
func (p *probeKeyspanIterator) SetContext(ctx context.Context) {
	p.iter.SetContext(ctx)
}

func (p *probeKeyspanIterator) Close() {
	op := keyspanOp{Kind: opSpanClose}
	if p.iter != nil {
		p.iter.Close()
	}
	_, _ = p.handleOp(op)
}
