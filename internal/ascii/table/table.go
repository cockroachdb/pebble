// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package table

import (
	"fmt"
	"iter"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/cockroachdb/crlib/crhumanize"
	"github.com/cockroachdb/pebble/internal/ascii"
	"golang.org/x/exp/constraints"
)

// Define defines a new table layout with the given fields.
//
// Example:
//
//	wb := ascii.Make(1, 10)
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
//	wb.Reset(def.CumulativeFieldWidth)
//	def.Render(wb.At(0, 0), RenderOptions{}, cats)
//
// Output of wb.String():
//
//	name    agecuteness
//	-------------------
//	Mai       2      10
//	Yuumi     5      10
func Define[T any](fields ...Element) Layout[T] {
	maxFieldWidth := 0
	for i := range len(fields) {
		maxFieldWidth = max(maxFieldWidth, fields[i].width())
	}

	cumulativeFieldWidth := 0
	for i := range len(fields) {
		w := fields[i].width()
		h := fields[i].header(Vertically, maxFieldWidth)
		if len(h) > w {
			panic(fmt.Sprintf("header %q is too long for column %d", h, i))
		}
		cumulativeFieldWidth += w
	}
	return Layout[T]{
		CumulativeFieldWidth: cumulativeFieldWidth,
		MaxFieldWidth:        maxFieldWidth,
		fields:               fields,
	}
}

// A Layout defines the layout of a table.
type Layout[T any] struct {
	CumulativeFieldWidth int
	MaxFieldWidth        int
	fields               []Element
}

// RenderOptions specifies the options for rendering a table.
type RenderOptions struct {
	Orientation Orientation
}

// Render renders the given iterator of rows of a table into the given cursor,
// returning the modified cursor.
func (d *Layout[T]) Render(start ascii.Cursor, opts RenderOptions, rows iter.Seq[T]) ascii.Cursor {
	cur := start
	tuples := slices.Collect(rows)

	if opts.Orientation == Vertically {
		vals := make([]string, len(tuples))
		for fieldIdx, c := range d.fields {
			if fieldIdx > 0 {
				cur.Offset(1, 0).WriteString("-")
				// Each column is separated by a space from the previous column or
				// separator.
				cur = cur.Offset(0, 1)
			}
			if _, ok := c.(divider); ok {
				cur.Offset(0, 0).WriteString("|")
				cur.Offset(1, 0).WriteString("+")
				for i := range tuples {
					cur.Offset(2+i, 0).WriteString("|")
				}
				cur = cur.Offset(0, 1)
				continue
			}
			for i, t := range tuples {
				vals[i] = c.(Field[T]).renderValue(i, t)
			}

			width := c.width()
			// If one of the values exceeds the column width, widen the column as
			// necessary.
			for i := range vals {
				width = max(width, len(vals[i]))
			}
			header := c.header(Vertically, width)
			align := c.align()
			padding := width - len(header)
			cur.Offset(0, 0).WriteString(
				align.maybePadding(AlignRight, padding) + header + align.maybePadding(AlignLeft, padding),
			)
			cur.Offset(1, 0).WriteString(strings.Repeat("-", width))

			for i := range vals {
				ctx := RenderContext[T]{
					Orientation:   Vertically,
					Pos:           cur.Offset(2+i, 0),
					MaxFieldWidth: d.MaxFieldWidth,
				}
				spec := widthStr(width, c.align()) + "s"
				ctx.PaddedPos(width).Printf(spec, vals[i])
			}
			cur = cur.Offset(0, width)
		}
		return start.Offset(2+len(tuples), 0)
	}

	for i := range d.fields {
		cur.Offset(i, 0).WriteString(d.fields[i].header(Horizontally, d.MaxFieldWidth))
		if _, ok := d.fields[i].(divider); ok {
			cur.Offset(i, d.MaxFieldWidth).WriteString("-+-")
		} else {
			cur.Offset(i, d.MaxFieldWidth).WriteString(" | ")
		}
	}
	tupleIndex := 0
	c := d.MaxFieldWidth + 3
	for t := range rows {
		for i := range d.fields {
			if div, ok := d.fields[i].(divider); ok {
				div.renderStatic(Horizontally, d.MaxFieldWidth, cur.Offset(i, c))
			} else {
				ctx := RenderContext[T]{
					Orientation:   Horizontally,
					Pos:           cur.Offset(i, c),
					MaxFieldWidth: d.MaxFieldWidth,
				}
				f := d.fields[i].(Field[T])
				width := f.width()
				spec := widthStr(width, f.align()) + "s"
				ctx.PaddedPos(width).Printf(spec, f.renderValue(tupleIndex, t))
			}
		}
		tupleIndex++
		c += d.MaxFieldWidth
	}
	return cur.Offset(len(d.fields), c)
}

// A RenderContext provides the context for rendering a table.
type RenderContext[T any] struct {
	Orientation   Orientation
	Pos           ascii.Cursor
	MaxFieldWidth int
}

func (c *RenderContext[T]) PaddedPos(width int) ascii.Cursor {
	if c.Orientation == Vertically {
		return c.Pos
	}
	// Horizontally, we need to pad the width to the max field width.
	return c.Pos.Offset(0, c.MaxFieldWidth-width)
}

// Element is the base interface, common to all table elements.
type Element interface {
	header(o Orientation, maxWidth int) string
	width() int
	align() Align
}

// StaticElement is an Element that doesn't depend on the tuple value for
// rendering.
type StaticElement interface {
	Element
	renderStatic(o Orientation, maxWidth int, pos ascii.Cursor)
}

// Field is an Element that depends on the tuple value for rendering.
type Field[T any] interface {
	Element
	renderValue(tupleIndex int, tuple T) string
}

// Div creates a divider field used to visually separate regions of the table.
func Div() StaticElement {
	return divider{}
}

type divider struct{}

var (
	_ StaticElement = (*divider)(nil)

	// TODO(jackson): The staticcheck tool doesn't recognize that these are used to
	// satisfy the Field interface. Why not?
	_ = divider.header
	_ = divider.width
	_ = divider.align
	_ = divider.renderStatic
)

func (d divider) header(o Orientation, maxWidth int) string {
	if o == Horizontally {
		return strings.Repeat("-", maxWidth)
	}
	return " | "
}
func (d divider) width() int   { return 3 }
func (d divider) align() Align { return AlignLeft }
func (d divider) renderStatic(o Orientation, maxWidth int, pos ascii.Cursor) {
	if o == Horizontally {
		pos.RepeatByte(maxWidth, '-')
	} else {
		pos.WriteString(" | ")
	}
}

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

func (l literal[T]) header(o Orientation, maxWidth int) string { return " " }
func (l literal[T]) width() int                                { return len(l) }
func (l literal[T]) align() Align                              { return AlignLeft }
func (l literal[T]) renderValue(tupleIndex int, tuple T) string {
	return string(l)
}

const (
	AlignLeft Align = iota
	AlignRight
)

type Align uint8

func (a Align) maybePadding(ifAlign Align, width int) string {
	if a == ifAlign {
		return strings.Repeat(" ", width)
	}
	return ""
}

const (
	Vertically Orientation = iota
	Horizontally
)

// Orientation specifies the orientation of the table. The default orientation
// is vertical.
type Orientation uint8

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

func (c *funcField[T]) header(o Orientation, maxWidth int) string { return c.headerValue }
func (c *funcField[T]) width() int                                { return c.widthValue }
func (c *funcField[T]) align() Align                              { return c.alignValue }
func (c *funcField[T]) renderValue(tupleIndex int, tuple T) string {
	return c.toStringFn(tupleIndex, tuple)
}

func widthStr(width int, align Align) string {
	if align == AlignLeft {
		return "%-" + strconv.Itoa(width)
	}
	return "%" + strconv.Itoa(width)
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
