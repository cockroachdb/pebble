// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"fmt"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/treeprinter"
)

// WrapFn is the prototype for a function that wraps a FragmentIterator.
type WrapFn func(in FragmentIterator) FragmentIterator

// InjectLogging wraps all iterators in a stack with logging iterators,
// producing log messages showing each operation and its result.
func InjectLogging(iter FragmentIterator, logger base.Logger) FragmentIterator {
	// All iterators in the stack will use the same logging state.
	state := &loggingState{
		log: logger,
	}
	var wrap WrapFn
	wrap = func(in FragmentIterator) FragmentIterator {
		if in == nil {
			return nil
		}
		// Recursively wrap all descendants.
		in.WrapChildren(wrap)
		return newLoggingIter(state, in)
	}
	return wrap(iter)
}

func newLoggingIter(state *loggingState, iter FragmentIterator) FragmentIterator {
	return &loggingIter{
		iter:    iter,
		state:   state,
		context: fmt.Sprintf("%T(%p):", iter, iter),
	}
}

// loggingIter is a pass-through FragmentIterator wrapper which performs checks
// on what the wrapped iterator returns.
type loggingIter struct {
	iter    FragmentIterator
	state   *loggingState
	context string
}

// loggingState is shared by all iterators in a stack.
type loggingState struct {
	node treeprinter.Node
	log  base.Logger
}

func (i *loggingIter) opStartf(format string, args ...any) func(results ...any) {
	savedNode := i.state.node

	n := i.state.node
	topLevelOp := false
	if n == (treeprinter.Node{}) {
		n = treeprinter.New()
		topLevelOp = true
	}
	op := fmt.Sprintf(format, args...)

	child := n.Childf("%s %s", i.context, op)
	i.state.node = child

	return func(results ...any) {
		if len(results) > 0 {
			child.Childf("%s", fmt.Sprint(results...))
		}
		if topLevelOp {
			for _, row := range n.FormattedRows() {
				i.state.log.Infof("%s\n", row)
			}
		}
		i.state.node = savedNode
	}
}

var _ FragmentIterator = (*loggingIter)(nil)

// SeekGE implements FragmentIterator.
func (i *loggingIter) SeekGE(key []byte) (*Span, error) {
	opEnd := i.opStartf("SeekGE(%q)", key)
	span, err := i.iter.SeekGE(key)
	opEnd(span, err)
	return span, err
}

// SeekLT implements FragmentIterator.
func (i *loggingIter) SeekLT(key []byte) (*Span, error) {
	opEnd := i.opStartf("SeekLT(%q)", key)
	span, err := i.iter.SeekLT(key)
	opEnd(span, err)
	return span, err
}

// First implements FragmentIterator.
func (i *loggingIter) First() (*Span, error) {
	opEnd := i.opStartf("First()")
	span, err := i.iter.First()
	opEnd(span, err)
	return span, err
}

// Last implements FragmentIterator.
func (i *loggingIter) Last() (*Span, error) {
	opEnd := i.opStartf("Last()")
	span, err := i.iter.Last()
	opEnd(span, err)
	return span, err
}

// Next implements FragmentIterator.
func (i *loggingIter) Next() (*Span, error) {
	opEnd := i.opStartf("Next()")
	span, err := i.iter.Next()
	opEnd(span, err)
	return span, err
}

// Prev implements FragmentIterator.
func (i *loggingIter) Prev() (*Span, error) {
	opEnd := i.opStartf("Prev()")
	span, err := i.iter.Prev()
	opEnd(span, err)
	return span, err
}

// Close implements FragmentIterator.
func (i *loggingIter) Close() error {
	opEnd := i.opStartf("Close()")
	err := i.iter.Close()
	if err != nil {
		opEnd(err)
	} else {
		opEnd()
	}
	return err
}

// WrapChildren implements FragmentIterator.
func (i *loggingIter) WrapChildren(wrap WrapFn) {
	i.iter = wrap(i.iter)
}
