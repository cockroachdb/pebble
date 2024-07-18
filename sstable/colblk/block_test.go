// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
	"golang.org/x/exp/rand"
)

type ColumnSpec struct {
	DataType
	// AllowAbsence is used to determine whether a zero value should be
	// represented using the "presence" encoding.
	AllowAbsence bool
	BundleSize   int // Only used for DataTypePrefixBytes
}

func (c ColumnSpec) String() string {
	var sb strings.Builder
	sb.WriteString(c.DataType.String())
	if c.AllowAbsence {
		sb.WriteString("+allowAbsence")
	}
	if c.BundleSize > 0 {
		fmt.Fprintf(&sb, "+bundleSize=%d", c.BundleSize)
	}
	return sb.String()
}

func (c ColumnSpec) Writer() ColumnWriter {
	var w ColumnWriter
	switch c.DataType {
	case DataTypeBool:
		w = &BitmapBuilder{}
	case DataTypeUint8:
		b := &UintBuilder[uint8]{}
		b.Init()
		w = b
	case DataTypeUint16:
		b := &UintBuilder[uint16]{}
		b.Init()
		w = b
	case DataTypeUint32:
		b := &UintBuilder[uint32]{}
		b.Init()
		w = b
	case DataTypeUint64:
		b := &UintBuilder[uint64]{}
		b.Init()
		w = b
	case DataTypeBytes:
		b := &RawBytesBuilder{}
		b.Reset()
		w = b
	case DataTypePrefixBytes:
		b := &PrefixBytesBuilder{}
		b.Init(c.BundleSize)
		w = b
	default:
		panic(fmt.Sprintf("unsupported data type: %s", c.DataType))
	}
	if c.AllowAbsence {
		return &DefaultAbsentWriter[ColumnWriter]{Writer: w}
	}
	return w
}

func (c ColumnSpec) WriteValue(w ColumnWriter, row int, value string) {
	if c.AllowAbsence {
		if value == "." {
			// Do nothing; the value is absent.
			return
		}
		// The value is present.
		w, row = w.(*DefaultAbsentWriter[ColumnWriter]).Present(row)
	}
	if value == "." {
		panic(fmt.Sprintf("value in row %d is absent, but column is not zero-as-absent: %s", row, c))
	}

	switch c.DataType {
	case DataTypeBool:
		bb := w.(*BitmapBuilder)
		v, err := strconv.ParseBool(value)
		panicIfErr(c, value, err)
		bb.Set(row, v)
	case DataTypeUint8:
		b := w.(*UintBuilder[uint8])
		v, err := strconv.ParseUint(value, 10, 8)
		panicIfErr(c, value, err)
		b.Set(row, uint8(v))
	case DataTypeUint16:
		b := w.(*UintBuilder[uint16])
		v, err := strconv.ParseUint(value, 10, 16)
		panicIfErr(c, value, err)
		b.Set(row, uint16(v))
	case DataTypeUint32:
		b := w.(*UintBuilder[uint32])
		v, err := strconv.ParseUint(value, 10, 32)
		panicIfErr(c, value, err)
		b.Set(row, uint32(v))
	case DataTypeUint64:
		b := w.(*UintBuilder[uint64])
		v, err := strconv.ParseUint(value, 10, 64)
		panicIfErr(c, value, err)
		b.Set(row, v)
	case DataTypeBytes:
		b := w.(*RawBytesBuilder)
		b.Put([]byte(value))
	case DataTypePrefixBytes:
		b := w.(*PrefixBytesBuilder)
		bv := []byte(value)
		b.Put(bv, bytesSharedPrefix(bv, b.LastKey()))
	default:
		panic(fmt.Sprintf("unsupported data type: %s", c.DataType))
	}
}

func decodeColumnSpec(s string) ColumnSpec {
	fields := strings.FieldsFunc(s, func(r rune) bool { return r == '+' })
	spec := ColumnSpec{DataType: dataTypeFromName(fields[0])}
	for _, f := range fields[1:] {
		switch {
		case f == "allowAbsence":
			spec.AllowAbsence = true
		case strings.HasPrefix(f, "bundleSize="):
			n, err := strconv.Atoi(strings.TrimPrefix(f, "bundleSize="))
			if err != nil {
				panic(err)
			}
			spec.BundleSize = n
		default:
			panic(fmt.Sprintf("unknown field: %s", f))
		}
	}
	return spec
}

func panicIfErr(spec ColumnSpec, stringValue string, err error) {
	if err != nil {
		panic(fmt.Sprintf("unable to decode %q as value for col of spec %s: %s", stringValue, spec, err))
	}
}

func TestBlockWriter(t *testing.T) {
	var buf bytes.Buffer
	var rows int
	var schema []ColumnSpec
	var colWriters []ColumnWriter
	datadriven.RunTest(t, "testdata/block_writer", func(t *testing.T, td *datadriven.TestData) string {
		buf.Reset()
		switch td.Cmd {
		case "init":
			schemaArg, ok := td.Arg("schema")
			if !ok {
				return "schema argument missing"
			}
			schema = make([]ColumnSpec, len(schemaArg.Vals))
			colWriters = make([]ColumnWriter, len(schema))
			for i, v := range schemaArg.Vals {
				schema[i] = decodeColumnSpec(v)
				colWriters[i] = schema[i].Writer()
			}
			rows = 0
			return ""
		case "write":
			lines := strings.Split(td.Input, "\n")
			lineFields := make([][]string, len(lines))
			for i, line := range lines {
				lineFields[i] = strings.Fields(line)
			}
			for r := range lineFields {
				for c := range lineFields[r] {
					spec := schema[c]
					spec.WriteValue(colWriters[c], rows, lineFields[r][c])
				}
				rows++
			}
			return ""
		case "finish":
			for col := range colWriters {
				if !schema[col].AllowAbsence {
					colWriters[col] = AllPresentWriter[ColumnWriter]{Writer: colWriters[col]}
				}
			}
			block := FinishBlock(int(rows), colWriters)
			r := ReadBlock(block, 0)
			return r.FormattedString()
		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func dataTypeFromName(name string) DataType {
	for dt, n := range dataTypeName {
		if n == name {
			return DataType(dt)
		}
	}
	return DataTypeInvalid
}

// randBlock generates a random block of n rows with the provided schema. It
// returns the serialized raw block and a []interface{} slice containing the
// generated data. The type of each element of the slice is dependent on the
// corresponding column's type.
func randBlock(rng *rand.Rand, rows int, schema []ColumnSpec) ([]byte, []interface{}) {
	data := make([]interface{}, len(schema))
	for col := range data {
		switch schema[col].DataType {
		case DataTypeBool:
			v := make([]bool, rows)
			for row := 0; row < rows; row++ {
				v[row] = (rng.Int31() % 2) == 0
			}
			data[col] = v
		case DataTypeUint8:
			v := make([]uint8, rows)
			for row := 0; row < rows; row++ {
				v[row] = uint8(rng.Uint32())
			}
			data[col] = v
		case DataTypeUint16:
			v := make([]uint16, rows)
			for row := 0; row < rows; row++ {
				v[row] = uint16(rng.Uint32())
			}
			data[col] = v
		case DataTypeUint32:
			v := make([]uint32, rows)
			for row := 0; row < rows; row++ {
				v[row] = rng.Uint32()
			}
			data[col] = v
		case DataTypeUint64:
			v := make([]uint64, rows)
			for row := 0; row < rows; row++ {
				v[row] = rng.Uint64()
			}
			data[col] = v
		case DataTypeBytes:
			v := make([][]byte, rows)
			for row := 0; row < rows; row++ {
				v[row] = make([]byte, rng.Intn(20))
				rng.Read(v[row])
			}
			data[col] = v
		case DataTypePrefixBytes:
			v := make([][]byte, rows)
			minLen := 1
			if schema[col].AllowAbsence {
				minLen = 0
			}
			for row := 0; row < rows; row++ {
				v[row] = make([]byte, rng.Intn(20)+minLen)
				rng.Read(v[row])
			}
			// PrefixBytes are required to be lexicographically sorted.
			slices.SortFunc(v, bytes.Compare)
			data[col] = v

		}
	}
	buf := buildBlock(schema, rows, data)
	return buf, data
}

func buildBlock(schema []ColumnSpec, rows int, data []interface{}) []byte {
	cw := make([]ColumnWriter, len(schema))
	for col := range schema {
		switch schema[col].DataType {
		case DataTypeBool:
			if schema[col].AllowAbsence {
				panic("unimplemented")
			}
			var bb BitmapBuilder
			bb.Reset()
			for row, v := range data[col].([]bool) {
				bb.Set(row, v)
			}
			cw[col] = AllPresentWriter[*BitmapBuilder]{&bb}
		case DataTypeUint8:
			cw[col] = writeUintCol(schema[col], data[col].([]uint8))
		case DataTypeUint16:
			cw[col] = writeUintCol(schema[col], data[col].([]uint16))
		case DataTypeUint32:
			cw[col] = writeUintCol(schema[col], data[col].([]uint32))
		case DataTypeUint64:
			cw[col] = writeUintCol(schema[col], data[col].([]uint64))
		case DataTypeBytes:
			var rb RawBytesBuilder
			rb.Reset()
			if schema[col].AllowAbsence {
				b := &DefaultAbsentWriter[*RawBytesBuilder]{Writer: &rb}
				for row, v := range data[col].([][]byte) {
					if len(v) == 0 {
						// Skip as absent.
						continue
					}
					w, _ := b.Present(row)
					w.Put(v)
				}
				cw[col] = b
			} else {
				for _, v := range data[col].([][]byte) {
					rb.Put(v)
				}
				cw[col] = AllPresentWriter[*RawBytesBuilder]{&rb}
			}
		case DataTypePrefixBytes:
			var pbb PrefixBytesBuilder
			pbb.Init(schema[col].BundleSize)
			colData := data[col].([][]byte)
			if schema[col].AllowAbsence {
				b := &DefaultAbsentWriter[*PrefixBytesBuilder]{Writer: &pbb}
				for row, v := range colData {
					if len(v) == 0 {
						// Skip as absent.
						continue
					}
					w, _ := b.Present(row)
					w.Put(v, bytesSharedPrefix(pbb.LastKey(), v))
				}
				cw[col] = b
			} else {
				for _, v := range colData {
					pbb.Put(v, bytesSharedPrefix(pbb.LastKey(), v))
				}
				cw[col] = AllPresentWriter[*PrefixBytesBuilder]{&pbb}
			}
		}
	}
	return FinishBlock(rows, cw)
}

func writeUintCol[U Uint](spec ColumnSpec, vals []U) ColumnWriter {
	var b UintBuilder[U]
	b.Init()
	if spec.AllowAbsence {
		// TODO(jackson): Increase probability of absent values on uint columns.
		w := &DefaultAbsentWriter[*UintBuilder[U]]{Writer: &b}
		for row, v := range vals {
			if v != 0 {
				w, j := w.Present(row)
				w.Set(j, v)
			}
		}
		return w
	}
	for row, v := range vals {
		b.Set(row, v)
	}
	return AllPresentWriter[*UintBuilder[U]]{Writer: &b}
}

func testRandomBlock(t *testing.T, rng *rand.Rand, rows int, schema []ColumnSpec) {
	var sb strings.Builder
	for i := range schema {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(schema[i].String())
	}

	t.Run(sb.String(), func(t *testing.T) {
		block, data := randBlock(rng, rows, schema)
		r := ReadBlock(block, 0)
		if uint32(r.header.Columns) != uint32(len(schema)) {
			t.Fatalf("expected %d columns, but found %d\n", len(schema), r.header.Columns)
		}
		if r.Rows() != rows {
			t.Fatalf("expected %d rows, but found %d\n", rows, r.Rows())
		}
		for col := range schema {
			if schema[col].DataType != r.DataType(col) {
				t.Fatalf("schema mismatch: %s != %s\n", schema[col], r.DataType(col))
			}
		}

		for col := range data {
			spec := schema[col]
			var got interface{}
			switch spec.DataType {
			case DataTypeBool:
				got = Clone(r.Bitmap(col), rows)
			case DataTypeUint8:
				got = Clone(r.Uint8s(col), rows)
			case DataTypeUint16:
				got = Clone(r.Uint16s(col), rows)
			case DataTypeUint32:
				got = Clone(r.Uint32s(col), rows)
			case DataTypeUint64:
				got = Clone(r.Uint64s(col), rows)
			case DataTypeBytes:
				got = Clone(r.RawBytes(col), rows)
			case DataTypePrefixBytes:
				got = Clone(r.PrefixBytes(col), rows)
			}
			if !reflect.DeepEqual(data[col], got) {
				t.Fatalf("%d: %s: expected\n%+v\ngot\n%+v\n% x",
					col, schema[col], data[col], got, r.data)
			}
		}
	})
}

func TestBlockWriterRandomized(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("Seed: %d", seed)
	// Make a new rng for each test so that we have determinism when running
	// subtests versus the entire test.
	makeRng := func() *rand.Rand { return rand.New(rand.NewSource(seed)) }
	rng := makeRng()
	randInt := func(lo, hi int) int {
		return lo + rng.Intn(hi-lo)
	}
	testRandomBlock(t, makeRng(), randInt(1, 100), []ColumnSpec{{DataType: DataTypeBool}})
	testRandomBlock(t, makeRng(), randInt(1, 100), []ColumnSpec{{DataType: DataTypeUint8}})
	testRandomBlock(t, makeRng(), randInt(1, 100), []ColumnSpec{{DataType: DataTypeUint8, AllowAbsence: true}})
	testRandomBlock(t, makeRng(), randInt(1, 100), []ColumnSpec{{DataType: DataTypeUint16}})
	testRandomBlock(t, makeRng(), randInt(1, 100), []ColumnSpec{{DataType: DataTypeUint16, AllowAbsence: true}})
	testRandomBlock(t, makeRng(), randInt(1, 100), []ColumnSpec{{DataType: DataTypeUint32}})
	testRandomBlock(t, makeRng(), randInt(1, 100), []ColumnSpec{{DataType: DataTypeUint32, AllowAbsence: true}})
	testRandomBlock(t, makeRng(), randInt(1, 100), []ColumnSpec{{DataType: DataTypeUint64}})
	testRandomBlock(t, makeRng(), randInt(1, 100), []ColumnSpec{{DataType: DataTypeUint64, AllowAbsence: true}})
	testRandomBlock(t, makeRng(), randInt(1, 100), []ColumnSpec{{DataType: DataTypeBytes}})
	testRandomBlock(t, makeRng(), randInt(1, 100), []ColumnSpec{{DataType: DataTypeBytes, AllowAbsence: true}})
	testRandomBlock(t, makeRng(), randInt(1, 100), []ColumnSpec{
		{
			DataType:   DataTypePrefixBytes,
			BundleSize: 1 << randInt(0, 6),
		},
	})
	testRandomBlock(t, makeRng(), randInt(1, 100), []ColumnSpec{
		{
			DataType:     DataTypePrefixBytes,
			AllowAbsence: true,
			BundleSize:   1 << randInt(0, 6),
		},
	})

	for i := 0; i < 100; i++ {
		schema := make([]ColumnSpec, 2+rng.Intn(8))
		for j := range schema {
			schema[j].DataType = DataType(randInt(1, int(dataTypesCount)))
			if schema[j].DataType == DataTypePrefixBytes {
				schema[j].BundleSize = 1 << randInt(0, 6)
			}
			switch schema[j].DataType {
			case DataTypeBytes, DataTypeUint8, DataTypeUint16, DataTypeUint32, DataTypeUint64,
				DataTypePrefixBytes:
				schema[j].AllowAbsence = rng.Intn(2) == 0
			}
		}
		testRandomBlock(t, makeRng(), randInt(1, 100), schema)
	}
}
