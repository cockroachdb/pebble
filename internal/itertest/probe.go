// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package itertest

import (
	"context"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/dsl"
)

// OpKind indicates the type of iterator operation being performed.
type OpKind int8

const (
	// OpSeekGE indicates a SeekGE internal iterator operation.
	OpSeekGE OpKind = iota
	// OpSeekPrefixGE indicates a SeekPrefixGE internal iterator operation.
	OpSeekPrefixGE
	// OpSeekLT indicates a SeekLT internal iterator operation.
	OpSeekLT
	// OpFirst indicates a First internal iterator operation.
	OpFirst
	// OpLast indicates a Last internal iterator operation.
	OpLast
	// OpNext indicates a Next internal iterator operation.
	OpNext
	// OpNextPrefix indicates a NextPrefix internal iterator operation.
	OpNextPrefix
	// OpPrev indicates a Prev internal iterator operation.
	OpPrev
	// OpClose indicates a Close internal iterator operation.
	OpClose
	numOpKinds
)

var opNames = [numOpKinds]string{
	OpSeekGE:       "OpSeekGE",
	OpSeekPrefixGE: "OpSeekPrefixGE",
	OpSeekLT:       "OpSeekLT",
	OpFirst:        "OpFirst",
	OpLast:         "OpLast",
	OpNext:         "OpNext",
	OpNextPrefix:   "OpNextPrefix",
	OpPrev:         "OpPrev",
	OpClose:        "OpClose",
}

// OpKind implements Predicate.
var _ Predicate = OpKind(0)

// String imlements fmt.Stringer.
func (o OpKind) String() string { return opNames[o] }

// Evaluate implements Predicate.
func (o OpKind) Evaluate(pctx *ProbeContext) bool { return pctx.Op.Kind == o }

// Op describes an individual iterator operation being performed.
type Op struct {
	Kind    OpKind
	SeekKey []byte
	Key     *base.InternalKey
	Value   base.LazyValue
	Err     error
}

// Probe defines an interface for probes that may inspect or mutate internal
// iterator behavior.
type Probe interface {
	// Probe inspects, and possibly manipulates, iterator operations' results.
	Probe(*ProbeContext)
}

// ProbeContext provides the context within which a Probe is run. It includes
// information about the iterator operation in progress.
type ProbeContext struct {
	Op
	ProbeState
}

// ProbeState holds state additional to the context of the operation that's
// accessible to probes.
type ProbeState struct {
	*base.Comparer
	Log io.Writer
}

// Attach takes an iterator, an initial state and a probe, returning an iterator
// that will invoke the provided Probe on all internal iterator operations.
func Attach(
	iter base.InternalIterator, initialState ProbeState, probes ...Probe,
) base.InternalIterator {
	for i := range probes {
		iter = &probeIterator{
			iter:  iter,
			probe: probes[i],
			probeCtx: ProbeContext{
				ProbeState: initialState,
			},
		}
	}
	return iter
}

// MustParseProbes parses each DSL string as a separate probe, returning a slice
// of parsed probes. Panics if any of the probes fail to parse.
func MustParseProbes(parser *dsl.Parser[Probe], probeDSLs ...string) []Probe {
	probes := make([]Probe, len(probeDSLs))
	var err error
	for i := range probeDSLs {
		probes[i], err = parser.Parse(probeDSLs[i])
		if err != nil {
			panic(err)
		}
	}
	return probes
}

type probeIterator struct {
	iter     base.InternalIterator
	err      error
	probe    Probe
	probeCtx ProbeContext
}

// Assert that errIterator implements the internal iterator interface.
var _ base.InternalIterator = (*probeIterator)(nil)

func (p *probeIterator) handleOp(preProbeOp Op) (*base.InternalKey, base.LazyValue) {
	p.probeCtx.Op = preProbeOp
	if preProbeOp.Key == nil && p.iter != nil {
		p.probeCtx.Op.Err = p.iter.Error()
	}

	p.probe.Probe(&p.probeCtx)
	p.err = p.probeCtx.Op.Err
	return p.probeCtx.Op.Key, p.probeCtx.Op.Value
}

func (p *probeIterator) SeekGE(
	key []byte, flags base.SeekGEFlags,
) (*base.InternalKey, base.LazyValue) {
	op := Op{
		Kind:    OpSeekGE,
		SeekKey: key,
	}
	if p.iter != nil {
		op.Key, op.Value = p.iter.SeekGE(key, flags)
	}
	return p.handleOp(op)
}

func (p *probeIterator) SeekPrefixGE(
	prefix, key []byte, flags base.SeekGEFlags,
) (*base.InternalKey, base.LazyValue) {
	op := Op{
		Kind:    OpSeekPrefixGE,
		SeekKey: key,
	}
	if p.iter != nil {
		op.Key, op.Value = p.iter.SeekPrefixGE(prefix, key, flags)
	}
	return p.handleOp(op)
}

func (p *probeIterator) SeekLT(
	key []byte, flags base.SeekLTFlags,
) (*base.InternalKey, base.LazyValue) {
	op := Op{
		Kind:    OpSeekLT,
		SeekKey: key,
	}
	if p.iter != nil {
		op.Key, op.Value = p.iter.SeekLT(key, flags)
	}
	return p.handleOp(op)
}

func (p *probeIterator) First() (*base.InternalKey, base.LazyValue) {
	op := Op{Kind: OpFirst}
	if p.iter != nil {
		op.Key, op.Value = p.iter.First()
	}
	return p.handleOp(op)
}

func (p *probeIterator) Last() (*base.InternalKey, base.LazyValue) {
	op := Op{Kind: OpLast}
	if p.iter != nil {
		op.Key, op.Value = p.iter.Last()
	}
	return p.handleOp(op)
}

func (p *probeIterator) Next() (*base.InternalKey, base.LazyValue) {
	op := Op{Kind: OpNext}
	if p.iter != nil {
		op.Key, op.Value = p.iter.Next()
	}
	return p.handleOp(op)
}

func (p *probeIterator) NextPrefix(succKey []byte) (*base.InternalKey, base.LazyValue) {
	op := Op{Kind: OpNextPrefix, SeekKey: succKey}
	if p.iter != nil {
		op.Key, op.Value = p.iter.NextPrefix(succKey)
	}
	return p.handleOp(op)
}

func (p *probeIterator) Prev() (*base.InternalKey, base.LazyValue) {
	op := Op{Kind: OpPrev}
	if p.iter != nil {
		op.Key, op.Value = p.iter.Prev()
	}
	return p.handleOp(op)
}

func (p *probeIterator) Error() error {
	return p.err
}

func (p *probeIterator) Close() error {
	op := Op{Kind: OpClose}
	if p.iter != nil {
		op.Err = p.iter.Close()
	}

	p.probeCtx.Op = op
	p.probe.Probe(&p.probeCtx)
	p.err = p.probeCtx.Op.Err
	return p.err
}

func (p *probeIterator) SetBounds(lower, upper []byte) {
	if p.iter != nil {
		p.iter.SetBounds(lower, upper)
	}
}

func (p *probeIterator) SetContext(ctx context.Context) {
	if p.iter != nil {
		p.iter.SetContext(ctx)
	}
}

func (p *probeIterator) String() string {
	if p.iter != nil {
		return fmt.Sprintf("probeIterator(%q)", p.iter.String())
	}
	return "probeIterator(nil)"
}
