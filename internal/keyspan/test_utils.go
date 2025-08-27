// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"bytes"
	"context"
	"fmt"
	"go/token"
	"io"
	"reflect"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2/internal/dsl"
	"github.com/cockroachdb/pebble/v2/internal/treeprinter"
)

// This file contains testing facilities for Spans and FragmentIterators. It's
// defined here so that it may be used by the keyspan package to test its
// various FragmentIterator implementations.
//
// TODO(jackson): Move keyspan.{Span,Key,FragmentIterator} into internal/base,
// and then move the testing facilities to an independent package, eg
// internal/itertest. Alternatively, make all tests that use it use the
// keyspan_test package, which can then import a separate itertest package.

// probe defines an interface for probes that may inspect or mutate internal
// span iterator behavior.
type probe interface {
	// probe inspects, and possibly manipulates, iterator operations' results.
	probe(*probeContext)
}

func parseProbes(probeDSLs ...string) []probe {
	probes := make([]probe, len(probeDSLs))
	var err error
	for i := range probeDSLs {
		probes[i], err = probeParser.Parse(probeDSLs[i])
		if err != nil {
			panic(err)
		}
	}
	return probes
}

func attachProbes(iter FragmentIterator, pctx probeContext, probes ...probe) FragmentIterator {
	if pctx.log == nil {
		pctx.log = io.Discard
	}
	for i := range probes {
		iter = &probeIterator{
			iter:     iter,
			probe:    probes[i],
			probeCtx: pctx,
		}
	}
	return iter
}

// ParseAndAttachProbes parses DSL probes and attaches them to an iterator.
func ParseAndAttachProbes(
	iter FragmentIterator, log io.Writer, probeDSLs ...string,
) FragmentIterator {
	pctx := probeContext{log: log}
	return attachProbes(iter, pctx, parseProbes(probeDSLs...)...)
}

// probeContext provides the context within which a probe is run. It includes
// information about the iterator operation in progress.
type probeContext struct {
	op
	log io.Writer
}

type op struct {
	Kind    OpKind
	SeekKey []byte
	Span    *Span
	Err     error
}

// ErrInjected is an error artificially injected for testing.
var ErrInjected = &errorProbe{name: "ErrInjected", err: errors.New("injected error")}

var probeParser = func() *dsl.Parser[probe] {
	valuerParser := dsl.NewParser[valuer]()
	valuerParser.DefineConstant("StartKey", func() valuer { return startKey{} })
	valuerParser.DefineFunc("Bytes",
		func(p *dsl.Parser[valuer], s *dsl.Scanner) valuer {
			v := bytesConstant{bytes: []byte(s.ConsumeString())}
			s.Consume(token.RPAREN)
			return v
		})

	predicateParser := dsl.NewPredicateParser[*probeContext]()
	predicateParser.DefineFunc("Equal",
		func(p *dsl.Parser[dsl.Predicate[*probeContext]], s *dsl.Scanner) dsl.Predicate[*probeContext] {
			eq := equal{
				valuerParser.ParseFromPos(s, s.Scan()),
				valuerParser.ParseFromPos(s, s.Scan()),
			}
			s.Consume(token.RPAREN)
			return eq
		})
	for i, name := range opNames {
		opKind := OpKind(i)
		predicateParser.DefineConstant(name, func() dsl.Predicate[*probeContext] {
			// An OpKind implements dsl.Predicate[*probeContext].
			return opKind
		})
	}
	probeParser := dsl.NewParser[probe]()
	probeParser.DefineConstant("ErrInjected", func() probe { return ErrInjected })
	probeParser.DefineConstant("noop", func() probe { return noop{} })
	probeParser.DefineFunc("If",
		func(p *dsl.Parser[probe], s *dsl.Scanner) probe {
			probe := ifProbe{
				predicateParser.ParseFromPos(s, s.Scan()),
				probeParser.ParseFromPos(s, s.Scan()),
				probeParser.ParseFromPos(s, s.Scan()),
			}
			s.Consume(token.RPAREN)
			return probe
		})
	probeParser.DefineFunc("Return",
		func(p *dsl.Parser[probe], s *dsl.Scanner) (ret probe) {
			switch tok := s.Scan(); tok.Kind {
			case token.STRING:
				str, err := strconv.Unquote(tok.Lit)
				if err != nil {
					panic(err)
				}
				span := ParseSpan(str)
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
		func(p *dsl.Parser[probe], s *dsl.Scanner) (ret probe) {
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
func (p *errorProbe) probe(pctx *probeContext) {
	pctx.op.Err = p.err
	pctx.op.Span = nil
}

// ifProbe is a conditional probe. If its predicate evaluates to true, it probes
// using its Then probe. If its predicate evalutes to false, it probes using its
// Else probe.
type ifProbe struct {
	Predicate dsl.Predicate[*probeContext]
	Then      probe
	Else      probe
}

func (p ifProbe) String() string { return fmt.Sprintf("(If %s %s %s)", p.Predicate, p.Then, p.Else) }
func (p ifProbe) probe(pctx *probeContext) {
	if p.Predicate.Evaluate(pctx) {
		p.Then.probe(pctx)
	} else {
		p.Else.probe(pctx)
	}
}

type returnSpan struct {
	s *Span
}

func (p returnSpan) String() string {
	if p.s == nil {
		return "(Return nil)"
	}
	return fmt.Sprintf("(Return %q)", p.s.String())
}

func (p returnSpan) probe(pctx *probeContext) {
	pctx.op.Span = p.s
	pctx.op.Err = nil
}

type noop struct{}

func (noop) String() string           { return "Noop" }
func (noop) probe(pctx *probeContext) {}

type loggingProbe struct {
	prefix string
}

func (lp loggingProbe) String() string { return fmt.Sprintf("(Log %q)", lp.prefix) }
func (lp loggingProbe) probe(pctx *probeContext) {
	opStr := strings.TrimPrefix(pctx.op.Kind.String(), "Op")
	fmt.Fprintf(pctx.log, "%s%s(", lp.prefix, opStr)
	if pctx.op.SeekKey != nil {
		fmt.Fprintf(pctx.log, "%q", pctx.op.SeekKey)
	}
	fmt.Fprint(pctx.log, ") = ")
	if pctx.op.Span == nil {
		fmt.Fprint(pctx.log, "nil")
		if pctx.op.Err != nil {
			fmt.Fprintf(pctx.log, " <err=%q>", pctx.op.Err)
		}
	} else {
		fmt.Fprint(pctx.log, pctx.op.Span.String())
	}
	fmt.Fprintln(pctx.log)
}

// dsl.Predicate[*probeContext] implementations.

type equal struct {
	a, b valuer
}

func (e equal) String() string { return fmt.Sprintf("(Equal %s %s)", e.a, e.b) }
func (e equal) Evaluate(pctx *probeContext) bool {
	return reflect.DeepEqual(e.a.value(pctx), e.b.value(pctx))
}

// OpKind indicates the type of iterator operation being performed.
type OpKind int8

// OpKind values.
const (
	OpSeekGE OpKind = iota
	OpSeekLT
	OpFirst
	OpLast
	OpNext
	OpPrev
	OpClose
	numOpKinds
)

func (o OpKind) String() string { return opNames[o] }

// Evaluate implements dsl.Predicate.
func (o OpKind) Evaluate(pctx *probeContext) bool { return pctx.op.Kind == o }

var opNames = [numOpKinds]string{
	OpSeekGE: "OpSeekGE",
	OpSeekLT: "OpSeekLT",
	OpFirst:  "OpFirst",
	OpLast:   "OpLast",
	OpNext:   "OpNext",
	OpPrev:   "OpPrev",
	OpClose:  "OpClose",
}

// valuer implementations

type valuer interface {
	fmt.Stringer
	value(pctx *probeContext) any
}

type bytesConstant struct {
	bytes []byte
}

func (b bytesConstant) String() string               { return fmt.Sprintf("%q", string(b.bytes)) }
func (b bytesConstant) value(pctx *probeContext) any { return b.bytes }

type startKey struct{}

func (s startKey) String() string { return "StartKey" }
func (s startKey) value(pctx *probeContext) any {
	if pctx.op.Span == nil {
		return nil
	}
	return pctx.op.Span.Start
}

type probeIterator struct {
	iter     FragmentIterator
	probe    probe
	probeCtx probeContext
}

// Assert that probeIterator implements the fragment iterator interface.
var _ FragmentIterator = (*probeIterator)(nil)

func (p *probeIterator) handleOp(preProbeOp op) (*Span, error) {
	p.probeCtx.op = preProbeOp
	p.probe.probe(&p.probeCtx)
	return p.probeCtx.op.Span, p.probeCtx.op.Err
}

func (p *probeIterator) SeekGE(key []byte) (*Span, error) {
	op := op{
		Kind:    OpSeekGE,
		SeekKey: key,
	}
	if p.iter != nil {
		op.Span, op.Err = p.iter.SeekGE(key)
	}
	return p.handleOp(op)
}

func (p *probeIterator) SeekLT(key []byte) (*Span, error) {
	op := op{
		Kind:    OpSeekLT,
		SeekKey: key,
	}
	if p.iter != nil {
		op.Span, op.Err = p.iter.SeekLT(key)
	}
	return p.handleOp(op)
}

func (p *probeIterator) First() (*Span, error) {
	op := op{Kind: OpFirst}
	if p.iter != nil {
		op.Span, op.Err = p.iter.First()
	}
	return p.handleOp(op)
}

func (p *probeIterator) Last() (*Span, error) {
	op := op{Kind: OpLast}
	if p.iter != nil {
		op.Span, op.Err = p.iter.Last()
	}
	return p.handleOp(op)
}

func (p *probeIterator) Next() (*Span, error) {
	op := op{Kind: OpNext}
	if p.iter != nil {
		op.Span, op.Err = p.iter.Next()
	}
	return p.handleOp(op)
}

func (p *probeIterator) Prev() (*Span, error) {
	op := op{Kind: OpPrev}
	if p.iter != nil {
		op.Span, op.Err = p.iter.Prev()
	}
	return p.handleOp(op)
}

// SetContext is part of the FragmentIterator interface.
func (p *probeIterator) SetContext(ctx context.Context) {
	p.iter.SetContext(ctx)
}

func (p *probeIterator) Close() {
	op := op{Kind: OpClose}
	if p.iter != nil {
		p.iter.Close()
	}
	_, _ = p.handleOp(op)
}

func (p *probeIterator) WrapChildren(wrap WrapFn) {
	p.iter = wrap(p.iter)
}

// DebugTree is part of the FragmentIterator interface.
func (p *probeIterator) DebugTree(tp treeprinter.Node) {
	n := tp.Childf("%T(%p)", p, p)
	if p.iter != nil {
		p.iter.DebugTree(n)
	}
}

// RunIterCmd evaluates a datadriven command controlling an internal
// keyspan.FragmentIterator, writing the results of the iterator operations to
// the provided writer.
func RunIterCmd(tdInput string, iter FragmentIterator, w io.Writer) {
	lines := strings.Split(strings.TrimSpace(tdInput), "\n")
	for i, line := range lines {
		if i > 0 {
			fmt.Fprintln(w)
		}
		line = strings.TrimSpace(line)
		i := strings.IndexByte(line, '#')
		iterCmd := line
		if i > 0 {
			iterCmd = string(line[:i])
		}
		runIterOp(w, iter, iterCmd)
	}
}

var iterDelim = map[rune]bool{',': true, ' ': true, '(': true, ')': true, '"': true}

func runIterOp(w io.Writer, it FragmentIterator, op string) {
	fields := strings.FieldsFunc(op, func(r rune) bool { return iterDelim[r] })
	var s *Span
	var err error
	switch strings.ToLower(fields[0]) {
	case "first":
		s, err = it.First()
	case "last":
		s, err = it.Last()
	case "seekge", "seek-ge":
		if len(fields) == 1 {
			panic(fmt.Sprintf("unable to parse iter op %q", op))
		}
		s, err = it.SeekGE([]byte(fields[1]))
	case "seeklt", "seek-lt":
		if len(fields) == 1 {
			panic(fmt.Sprintf("unable to parse iter op %q", op))
		}
		s, err = it.SeekLT([]byte(fields[1]))
	case "next":
		s, err = it.Next()
	case "prev":
		s, err = it.Prev()
	default:
		panic(fmt.Sprintf("unrecognized iter op %q", fields[0]))
	}
	switch {
	case err != nil:
		fmt.Fprintf(w, "<nil> err=<%s>", err)
	case s == nil:
		fmt.Fprint(w, "<nil>")
	default:
		fmt.Fprint(w, s)
	}
}

// RunFragmentIteratorCmd runs a command on an iterator; intended for testing.
func RunFragmentIteratorCmd(iter FragmentIterator, input string, extraInfo func() string) string {
	var b bytes.Buffer
	for _, line := range strings.Split(input, "\n") {
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		var span *Span
		var err error
		switch parts[0] {
		case "seek-ge":
			if len(parts) != 2 {
				return "seek-ge <key>\n"
			}
			span, err = iter.SeekGE([]byte(strings.TrimSpace(parts[1])))
		case "seek-lt":
			if len(parts) != 2 {
				return "seek-lt <key>\n"
			}
			span, err = iter.SeekLT([]byte(strings.TrimSpace(parts[1])))
		case "first":
			span, err = iter.First()
		case "last":
			span, err = iter.Last()
		case "next":
			span, err = iter.Next()
		case "prev":
			span, err = iter.Prev()
		default:
			return fmt.Sprintf("unknown op: %s", parts[0])
		}
		switch {
		case err != nil:
			fmt.Fprintf(&b, "err=%v\n", err)
		case span == nil:
			fmt.Fprintf(&b, ".\n")
		default:
			fmt.Fprintf(&b, "%s", span)
			if extraInfo != nil {
				fmt.Fprintf(&b, " (%s)", extraInfo())
			}
			b.WriteByte('\n')
		}
	}
	return b.String()
}

// NewInvalidatingIter wraps a FragmentIterator; spans surfaced by the inner
// iterator are copied to buffers that are zeroed by subsequent iterator
// positioning calls. This is intended to help surface bugs in improper lifetime
// expectations of Spans.
func NewInvalidatingIter(iter FragmentIterator) FragmentIterator {
	return &invalidatingIter{
		iter: iter,
	}
}

type invalidatingIter struct {
	iter FragmentIterator
	bufs [][]byte
	keys []Key
	span Span
}

// invalidatingIter implements FragmentIterator.
var _ FragmentIterator = (*invalidatingIter)(nil)

func (i *invalidatingIter) invalidate(s *Span, err error) (*Span, error) {
	// Mangle the entirety of the byte bufs and the keys slice.
	for j := range i.bufs {
		for k := range i.bufs[j] {
			i.bufs[j][k] = 0xff
		}
		i.bufs[j] = nil
	}
	for j := range i.keys {
		i.keys[j] = Key{}
	}
	if s == nil {
		return nil, err
	}

	// Copy all of the span's slices into slices owned by the invalidating iter
	// that we can invalidate on a subsequent positioning method.
	i.bufs = i.bufs[:0]
	i.keys = i.keys[:0]
	i.span = Span{
		Start:     i.saveBytes(s.Start),
		End:       i.saveBytes(s.End),
		KeysOrder: s.KeysOrder,
	}
	for j := range s.Keys {
		i.keys = append(i.keys, Key{
			Trailer: s.Keys[j].Trailer,
			Suffix:  i.saveBytes(s.Keys[j].Suffix),
			Value:   i.saveBytes(s.Keys[j].Value),
		})
	}
	i.span.Keys = i.keys
	return &i.span, err
}

func (i *invalidatingIter) saveBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	saved := append([]byte(nil), b...)
	i.bufs = append(i.bufs, saved)
	return saved
}

func (i *invalidatingIter) SeekGE(key []byte) (*Span, error) { return i.invalidate(i.iter.SeekGE(key)) }
func (i *invalidatingIter) SeekLT(key []byte) (*Span, error) { return i.invalidate(i.iter.SeekLT(key)) }
func (i *invalidatingIter) First() (*Span, error)            { return i.invalidate(i.iter.First()) }
func (i *invalidatingIter) Last() (*Span, error)             { return i.invalidate(i.iter.Last()) }
func (i *invalidatingIter) Next() (*Span, error)             { return i.invalidate(i.iter.Next()) }
func (i *invalidatingIter) Prev() (*Span, error)             { return i.invalidate(i.iter.Prev()) }

// SetContext is part of the FragmentIterator interface.
func (i *invalidatingIter) SetContext(ctx context.Context) {
	i.iter.SetContext(ctx)
}

func (i *invalidatingIter) Close() {
	_, _ = i.invalidate(nil, nil)
	i.iter.Close()
}

func (i *invalidatingIter) WrapChildren(wrap WrapFn) {
	i.iter = wrap(i.iter)
}

// DebugTree is part of the FragmentIterator interface.
func (i *invalidatingIter) DebugTree(tp treeprinter.Node) {
	n := tp.Childf("%T(%p)", i, i)
	if i.iter != nil {
		i.iter.DebugTree(n)
	}
}
