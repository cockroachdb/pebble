// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package itertest

import (
	"context"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/internal/dsl"
	"github.com/cockroachdb/pebble/v2/internal/treeprinter"
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
	// Return is initialized with the return result of the underlying iterator.
	// Probes may mutate them.
	Return struct {
		KV  *base.InternalKV
		Err error
	}
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
	probe    Probe
	probeCtx ProbeContext
}

// Assert that errIterator implements the internal iterator interface.
var _ base.InternalIterator = (*probeIterator)(nil)

// handleOp takes an Op representing the iterator operation performed, and the
// underlying iterator's return value. It populates `Return.Err` and invokes the
// probe.
func (p *probeIterator) handleOp(preProbeOp Op) *base.InternalKV {
	p.probeCtx.Op = preProbeOp
	if preProbeOp.Return.KV == nil && p.iter != nil {
		p.probeCtx.Op.Return.Err = p.iter.Error()
	}

	p.probe.Probe(&p.probeCtx)
	return p.probeCtx.Op.Return.KV
}

func (p *probeIterator) SeekGE(key []byte, flags base.SeekGEFlags) *base.InternalKV {
	op := Op{
		Kind:    OpSeekGE,
		SeekKey: key,
	}
	if p.iter != nil {
		op.Return.KV = p.iter.SeekGE(key, flags)
	}
	return p.handleOp(op)
}

func (p *probeIterator) SeekPrefixGE(prefix, key []byte, flags base.SeekGEFlags) *base.InternalKV {
	op := Op{
		Kind:    OpSeekPrefixGE,
		SeekKey: key,
	}
	if p.iter != nil {
		op.Return.KV = p.iter.SeekPrefixGE(prefix, key, flags)
	}
	return p.handleOp(op)
}

func (p *probeIterator) SeekLT(key []byte, flags base.SeekLTFlags) *base.InternalKV {
	op := Op{
		Kind:    OpSeekLT,
		SeekKey: key,
	}
	if p.iter != nil {
		op.Return.KV = p.iter.SeekLT(key, flags)
	}
	return p.handleOp(op)
}

func (p *probeIterator) First() *base.InternalKV {
	op := Op{Kind: OpFirst}
	if p.iter != nil {
		op.Return.KV = p.iter.First()
	}
	return p.handleOp(op)
}

func (p *probeIterator) Last() *base.InternalKV {
	op := Op{Kind: OpLast}
	if p.iter != nil {
		op.Return.KV = p.iter.Last()
	}
	return p.handleOp(op)
}

func (p *probeIterator) Next() *base.InternalKV {
	op := Op{Kind: OpNext}
	if p.iter != nil {
		op.Return.KV = p.iter.Next()
	}
	return p.handleOp(op)
}

func (p *probeIterator) NextPrefix(succKey []byte) *base.InternalKV {
	op := Op{Kind: OpNextPrefix, SeekKey: succKey}
	if p.iter != nil {
		op.Return.KV = p.iter.NextPrefix(succKey)
	}
	return p.handleOp(op)
}

func (p *probeIterator) Prev() *base.InternalKV {
	op := Op{Kind: OpPrev}
	if p.iter != nil {
		op.Return.KV = p.iter.Prev()
	}
	return p.handleOp(op)
}

func (p *probeIterator) Error() error {
	return p.probeCtx.Op.Return.Err
}

func (p *probeIterator) Close() error {
	op := Op{Kind: OpClose}
	if p.iter != nil {
		op.Return.Err = p.iter.Close()
	}

	// NB: Can't use handleOp because a close returns its error value directly
	// (and does not return a KV pair). We don't want to call iter.Error()
	// again, but rather use the error directly returned by iter.Close().
	p.probeCtx.Op = op
	p.probe.Probe(&p.probeCtx)
	return p.Error()
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

// DebugTree is part of the InternalIterator interface.
func (p *probeIterator) DebugTree(tp treeprinter.Node) {
	n := tp.Childf("%T(%p)", p, p)
	if p.iter != nil {
		p.iter.DebugTree(n)
	}
}

func (p *probeIterator) String() string {
	if p.iter != nil {
		return fmt.Sprintf("probeIterator(%q)", p.iter.String())
	}
	return "probeIterator(nil)"
}
