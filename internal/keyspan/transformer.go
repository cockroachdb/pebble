// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import "github.com/cockroachdb/pebble/internal/base"

// Transformer defines a transformation to be applied to a Span.
type Transformer interface {
	// Transform takes a Span as input and writes the transformed Span to the
	// provided output *Span pointer. The output Span's Keys slice may be reused
	// by Transform to reduce allocations.
	Transform(cmp base.Compare, in Span, out *Span) error
}

// The TransformerFunc type is an adapter to allow the use of ordinary functions
// as Transformers. If f is a function with the appropriate signature,
// TransformerFunc(f) is a Transformer that calls f.
type TransformerFunc func(base.Compare, Span, *Span) error

// Transform calls f(cmp, in, out).
func (tf TransformerFunc) Transform(cmp base.Compare, in Span, out *Span) error {
	return tf(cmp, in, out)
}

// NoopTransform is a Transformer that performs no mutations.
var NoopTransform Transformer = TransformerFunc(func(_ base.Compare, s Span, dst *Span) error {
	dst.Start, dst.End = s.Start, s.End
	dst.Keys = append(dst.Keys[:0], s.Keys...)
	return nil
})

// VisibleTransform filters keys that are invisible at the provided snapshot
// sequence number.
func VisibleTransform(snapshot uint64) Transformer {
	return TransformerFunc(func(_ base.Compare, s Span, dst *Span) error {
		dst.Start, dst.End = s.Start, s.End
		dst.Keys = dst.Keys[:0]
		for _, k := range s.Keys {
			// NB: The InternalKeySeqNumMax value is used for the batch snapshot
			// because a batch's visible span keys are filtered when they're
			// fragmented. There's no requirement to enforce visibility at
			// iteration time.
			if base.Visible(k.SeqNum(), snapshot, base.InternalKeySeqNumMax) {
				dst.Keys = append(dst.Keys, k)
			}
		}
		return nil
	})
}

// TransformerIter is a FragmentIterator that applies a Transformer on all
// returned keys. Used for when a caller needs to apply a transformer on an
// iterator but does not otherwise need the mergingiter's merging ability.
type TransformerIter struct {
	FragmentIterator

	// Transformer is applied on every Span returned by this iterator.
	Transformer Transformer
	// Comparer in use for this keyspace.
	Compare base.Compare

	span Span
}

func (t *TransformerIter) applyTransform(span *Span) (*Span, error) {
	if span == nil {
		return nil, nil
	}
	t.span = Span{
		Start: t.span.Start[:0],
		End:   t.span.End[:0],
		Keys:  t.span.Keys[:0],
	}
	if err := t.Transformer.Transform(t.Compare, *span, &t.span); err != nil {
		return nil, err
	}
	return &t.span, nil
}

// SeekGE implements the FragmentIterator interface.
func (t *TransformerIter) SeekGE(key []byte) (*Span, error) {
	span, err := t.FragmentIterator.SeekGE(key)
	if err != nil {
		return nil, err
	}
	return t.applyTransform(span)
}

// SeekLT implements the FragmentIterator interface.
func (t *TransformerIter) SeekLT(key []byte) (*Span, error) {
	span, err := t.FragmentIterator.SeekLT(key)
	if err != nil {
		return nil, err
	}
	return t.applyTransform(span)
}

// First implements the FragmentIterator interface.
func (t *TransformerIter) First() (*Span, error) {
	span, err := t.FragmentIterator.First()
	if err != nil {
		return nil, err
	}
	return t.applyTransform(span)
}

// Last implements the FragmentIterator interface.
func (t *TransformerIter) Last() (*Span, error) {
	span, err := t.FragmentIterator.Last()
	if err != nil {
		return nil, err
	}
	return t.applyTransform(span)
}

// Next implements the FragmentIterator interface.
func (t *TransformerIter) Next() (*Span, error) {
	span, err := t.FragmentIterator.Next()
	if err != nil {
		return nil, err
	}
	return t.applyTransform(span)
}

// Prev implements the FragmentIterator interface.
func (t *TransformerIter) Prev() (*Span, error) {
	span, err := t.FragmentIterator.Prev()
	if err != nil {
		return nil, err
	}
	return t.applyTransform(span)
}

// Close implements the FragmentIterator interface.
func (t *TransformerIter) Close() {
	t.FragmentIterator.Close()
}
