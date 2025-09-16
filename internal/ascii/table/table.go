// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package table

import (
	"fmt"
	"math"
	"strconv"

	"github.com/cockroachdb/crlib/crhumanize"
	"github.com/cockroachdb/pebble/internal/ascii"
	"golang.org/x/exp/constraints"
)

// Define defines a new table layout with the given fields.
//
// Example:
//
//	wb := ascii.Make(10, 10)
//	type Cat struct {
//		Name     string
//		Age      int
//		Cuteness int
//	}
//	cats := []Cat{
//		{Name: "Mai", Age: 2, Cuteness: 10},
//		{Name: "Yuumi", Age: 5, Cuteness: 10},
//	}
//
//	def := Define[Cat](
//		String("name", 7, AlignLeft, func(c Cat) string { return c.Name }),
//		Int("age", 4, AlignRight, func(c Cat) int { return c.Age }),
//		Int("cuteness", 8, AlignRight, func(c Cat) int { return c.Cuteness }),
//	)
//
//	wb.Reset(10)
//	def.Render(wb.At(0, 0), RenderOptions{}, cats)
//
// Output of wb.String():
//
//	name    agecuteness
//	-------------------
//	Mai       2      10
//	Yuumi     5      10
func Define[T any](fields ...Element) Layout[T] {
	for i := range fields {
		if f, ok := fields[i].(Field[T]); ok {
			if h := f.header(); len(h) > f.width() {
				panic(fmt.Sprintf("header %q is too long for column %d", h, i))
			}
		}
	}
	return Layout[T]{
		fields: fields,
	}
}

// A Layout defines the layout of a table.
type Layout[T any] struct {
	fields []Element
}

// HorizontalDividers is a set of row indices before which a horizontal divider
// is placed. If nil, the defult is to place a divider before the first row
// (i.e. HorizontalDividers{0}).
type HorizontalDividers map[int]struct{}

func MakeHorizontalDividers(rowIdx ...int) HorizontalDividers {
	hd := make(HorizontalDividers)
	for _, i := range rowIdx {
		hd[i] = struct{}{}
	}
	return hd
}

func (hd HorizontalDividers) Contains(rowIdx int) bool {
	if hd == nil {
		// Special case the nil value.
		return rowIdx == 0
	}
	_, ok := hd[rowIdx]
	return ok
}

// RenderOptions specifies the options for rendering a table.
type RenderOptions struct {
	HorizontalDividers HorizontalDividers
}

// Render renders the given iterator of rows of a table into the given cursor,
// returning the modified cursor.
func (d *Layout[T]) Render(start ascii.Cursor, opts RenderOptions, rows ...T) ascii.Cursor {
	cur := start

	tuples := rows
	vals := make([]string, len(tuples))
	for fieldIdx, c := range d.fields {
		if fieldIdx > 0 {
			// Each column is separated by a space from the previous column or
			// separator.
			rowCur := cur
			for rowIdx := range tuples {
				if opts.HorizontalDividers.Contains(rowIdx) {
					rowCur = rowCur.Down(1)
					rowCur.WriteString("-")
				}
				rowCur = rowCur.Down(1)
			}
			cur = cur.Right(1)
		}
		if _, ok := c.(divider); ok {
			rowCur := cur
			rowCur.WriteString("|")
			for rowIdx := range tuples {
				if opts.HorizontalDividers.Contains(rowIdx) {
					rowCur = rowCur.Down(1)
					rowCur.WriteString("+")
				}
				rowCur = rowCur.Down(1)
				rowCur.WriteString("|")
			}
			cur = cur.Right(1)
			continue
		}
		f := c.(Field[T])
		for i, t := range tuples {
			vals[i] = f.renderValue(i, t)
		}

		width := c.width()
		// If one of the values exceeds the column width, widen the column as
		// necessary.
		for i := range vals {
			width = max(width, len(vals[i]))
		}
		header := f.header()
		align := f.align()
		pad(cur, width, align, header)
		rowCur := cur
		for rowIdx := range vals {
			if opts.HorizontalDividers.Contains(rowIdx) {
				rowCur = rowCur.Down(1)
				rowCur.RepeatByte(width, '-')
			}
			rowCur = rowCur.Down(1)
			pad(rowCur, width, align, vals[rowIdx])
		}
		cur = cur.Right(width)
	}
	rowCur := start.Down(len(vals) + 1)
	for rowIdx := range vals {
		if opts.HorizontalDividers.Contains(rowIdx) {
			rowCur = rowCur.Down(1)
		}
	}
	return rowCur
}

// Element is the base interface, common to all table elements.
type Element interface {
	width() int
	align() Align
}

// Field is an Element that depends on the tuple value for rendering.
type Field[T any] interface {
	Element
	header() string
	renderValue(tupleIndex int, tuple T) string
}

// Div creates a divider field used to visually separate regions of the table.
func Div() Element {
	return divider{}
}

type divider struct{}

var (
	_ Element = (*divider)(nil)

	// TODO(jackson): The staticcheck tool doesn't recognize that these are used to
	// satisfy the Field interface. Why not?
	_ = divider.width
	_ = divider.align
)

func (d divider) width() int   { return 1 }
func (d divider) align() Align { return AlignLeft }

func Literal[T any](s string) Field[T] {
	return literal[T](s)
}

type literal[T any] string

var (
	_ Field[any] = (*literal[any])(nil)

	// TODO(jackson): The staticcheck tool doesn't recognize that these are used to
	// satisfy the Field interface. Why not?
	_ = literal[any].header
	_ = literal[any].width
	_ = literal[any].align
	_ = literal[any].renderValue
)

func (l literal[T]) header() string { return " " }
func (l literal[T]) width() int     { return len(l) }
func (l literal[T]) align() Align   { return AlignLeft }
func (l literal[T]) renderValue(tupleIndex int, tuple T) string {
	return string(l)
}

const (
	AlignLeft Align = iota
	AlignRight
	AlignCenter
)

type Align uint8

// pad writes the given string to the cursor, padding it to the given width
// (according to the alignment).
func pad(cur ascii.Cursor, toWidth int, align Align, s string) ascii.Cursor {
	if len(s) >= toWidth {
		return cur.WriteString(s)
	}
	padding := 0
	switch align {
	case AlignRight:
		padding = toWidth - len(s)
	case AlignCenter:
		padding = (toWidth - len(s)) / 2
	}
	cur.Right(padding).WriteString(s)
	return cur.Right(toWidth)
}

func String[T any](header string, width int, align Align, fn func(r T) string) Field[T] {
	return makeFuncField(header, width, align, func(tupleIndex int, r T) string {
		return fn(r)
	})
}

func Int[T any](header string, width int, align Align, fn func(r T) int) Field[T] {
	return makeFuncField(header, width, align, func(tupleIndex int, tuple T) string {
		return strconv.Itoa(fn(tuple))
	})
}

func Int64[T any](header string, width int, align Align, fn func(r T) int64) Field[T] {
	return makeFuncField(header, width, align, func(tupleIndex int, tuple T) string {
		return strconv.FormatInt(fn(tuple), 10)
	})
}

func AutoIncrement[T any](header string, width int, align Align) Field[T] {
	return makeFuncField(header, width, align, func(tupleIndex int, tuple T) string {
		return strconv.Itoa(tupleIndex)
	})
}

func Count[T any, N constraints.Integer](
	header string, width int, align Align, fn func(r T) N,
) Field[T] {
	return makeFuncField(header, width, align, func(tupleIndex int, tuple T) string {
		return string(crhumanize.Count(fn(tuple), crhumanize.Compact, crhumanize.OmitI))
	})
}

func Bytes[T any, N constraints.Integer](
	header string, width int, align Align, fn func(r T) N,
) Field[T] {
	return makeFuncField(header, width, align, func(tupleIndex int, tuple T) string {
		return string(crhumanize.Bytes(fn(tuple), crhumanize.Compact, crhumanize.OmitI))
	})
}

func Float[T any](header string, width int, align Align, fn func(r T) float64) Field[T] {
	return makeFuncField(header, width, align, func(tupleIndex int, tuple T) string {
		return humanizeFloat(fn(tuple), width)
	})
}

func makeFuncField[T any](
	header string, width int, align Align, toStringFn func(tupleIndex int, tuple T) string,
) Field[T] {
	return &funcField[T]{
		headerValue: header,
		widthValue:  width,
		alignValue:  align,
		toStringFn:  toStringFn,
	}
}

type funcField[T any] struct {
	headerValue string
	widthValue  int
	alignValue  Align
	toStringFn  func(tupleIndex int, tuple T) string
}

var (
	_ Field[any] = (*funcField[any])(nil)

	// TODO(jackson): The staticcheck tool doesn't recognize that these are used to
	// satisfy the Field interface. Why not?
	_ = (&funcField[any]{}).header
	_ = (&funcField[any]{}).width
	_ = (&funcField[any]{}).align
	_ = (&funcField[any]{}).renderValue
)

func (c *funcField[T]) header() string { return c.headerValue }
func (c *funcField[T]) width() int     { return c.widthValue }
func (c *funcField[T]) align() Align   { return c.alignValue }
func (c *funcField[T]) renderValue(tupleIndex int, tuple T) string {
	return c.toStringFn(tupleIndex, tuple)
}

// humanizeFloat formats a float64 value as a string. It shows up to two
// decimals, depending on the target length. NaN is shown as "-".
func humanizeFloat(v float64, targetLength int) string {
	if math.IsNaN(v) {
		return "-"
	}
	// We treat 0 specially. Values near zero will show up as 0.00.
	if v == 0 {
		return "0"
	}
	res := fmt.Sprintf("%.2f", v)
	if len(res) <= targetLength {
		return res
	}
	if len(res) == targetLength+1 {
		return fmt.Sprintf("%.1f", v)
	}
	return fmt.Sprintf("%.0f", v)
}
