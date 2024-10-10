// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/crbytes"
	"github.com/cockroachdb/datadriven"
)

type testColumnSpec struct {
	DataType
	IntRange   intRange // Only used for DataTypeUint
	BundleSize int      // Only used for DataTypePrefixBytes
}

type intRange struct {
	Min, Max         uint64
	ExpectedEncoding UintEncoding
}

func (ir intRange) Rand(rng *rand.Rand) uint64 {
	v := rng.Uint64()
	if ir.Min == 0 && ir.Max == math.MaxUint64 {
		return v
	}
	return ir.Min + v%(ir.Max-ir.Min+1)
}

var interestingIntRanges = []intRange{
	// zero
	{Min: 0, Max: 0, ExpectedEncoding: makeUintEncoding(0, false)},
	// const
	{Min: 1, Max: 1, ExpectedEncoding: makeUintEncoding(0, true)},
	{Min: math.MaxUint32, Max: math.MaxUint32, ExpectedEncoding: makeUintEncoding(0, true)},
	{Min: math.MaxUint64, Max: math.MaxUint64, ExpectedEncoding: makeUintEncoding(0, true)},
	// 1b
	{Min: 10, Max: 200, ExpectedEncoding: makeUintEncoding(1, false)},
	{Min: 0, Max: math.MaxUint8, ExpectedEncoding: makeUintEncoding(1, false)},
	// 1b,delta
	{Min: 100, Max: 300, ExpectedEncoding: makeUintEncoding(1, true)},
	{Min: math.MaxUint32 + 100, Max: math.MaxUint32 + 300, ExpectedEncoding: makeUintEncoding(1, true)},
	{Min: math.MaxUint64 - 1, Max: math.MaxUint64, ExpectedEncoding: makeUintEncoding(1, true)},
	// 2b
	{Min: 10, Max: 20_000, ExpectedEncoding: makeUintEncoding(2, false)},
	{Min: 0, Max: math.MaxUint8 + 1, ExpectedEncoding: makeUintEncoding(2, false)},
	{Min: 0, Max: math.MaxUint16, ExpectedEncoding: makeUintEncoding(2, false)},
	// 2b,delta
	{Min: 20_000, Max: 80_000, ExpectedEncoding: makeUintEncoding(2, true)},
	{Min: math.MaxUint32, Max: math.MaxUint32 + 50_000, ExpectedEncoding: makeUintEncoding(2, true)},
	// 4b
	{Min: 10, Max: 20_000_000, ExpectedEncoding: makeUintEncoding(4, false)},
	{Min: 0, Max: math.MaxUint16 + 1, ExpectedEncoding: makeUintEncoding(4, false)},
	{Min: 0, Max: math.MaxUint32, ExpectedEncoding: makeUintEncoding(4, false)},
	// 4b,delta
	{Min: 100_000, Max: math.MaxUint32 + 10, ExpectedEncoding: makeUintEncoding(4, true)},
	{Min: math.MaxUint32, Max: math.MaxUint32 + 20_000_000, ExpectedEncoding: makeUintEncoding(4, true)},
	// 8b
	{Min: 10, Max: math.MaxUint32 + 100, ExpectedEncoding: makeUintEncoding(8, false)},
	{Min: 0, Max: math.MaxUint32 + 1, ExpectedEncoding: makeUintEncoding(8, false)},
	{Min: 0, Max: math.MaxUint64, ExpectedEncoding: makeUintEncoding(8, false)},
	{Min: math.MaxUint64 - math.MaxUint32 - 1, Max: math.MaxUint64, ExpectedEncoding: makeUintEncoding(8, false)},
}

func TestBlockWriter(t *testing.T) {
	panicIfErr := func(dataType DataType, stringValue string, err error) {
		if err != nil {
			panic(fmt.Sprintf("unable to decode %q as value for data type %s: %s", stringValue, dataType, err))
		}
	}
	var buf bytes.Buffer
	var rows int
	var colDataTypes []DataType
	var colWriters []ColumnWriter
	datadriven.RunTest(t, "testdata/block_writer", func(t *testing.T, td *datadriven.TestData) string {
		buf.Reset()
		switch td.Cmd {
		case "init":
			colTypeNames, ok := td.Arg("schema")
			if !ok {
				return "schema argument missing"
			}
			colDataTypes = make([]DataType, len(colTypeNames.Vals))
			colWriters = make([]ColumnWriter, len(colDataTypes))
			for i, v := range colTypeNames.Vals {
				colDataTypes[i] = dataTypeFromName(v)
				switch colDataTypes[i] {
				case DataTypeBool:
					colWriters[i] = &BitmapBuilder{}
				case DataTypeUint:
					b := &UintBuilder{}
					b.Init()
					colWriters[i] = b
				case DataTypeBytes:
					bb := &RawBytesBuilder{}
					bb.Reset()
					colWriters[i] = bb
				case DataTypePrefixBytes:
					panic("unimplemented")
				default:
					panic(fmt.Sprintf("unsupported data type: %s", v))
				}
				colWriters[i].Reset()
			}
			rows = 0
			return ""
		case "write":
			lines := strings.Split(td.Input, "\n")
			lineFields := make([][]string, len(lines))
			for i, line := range lines {
				lineFields[i] = strings.Fields(line)
			}
			rows += len(lineFields)
			for c, dataType := range colDataTypes {
				switch dataType {
				case DataTypeBool:
					bb := colWriters[c].(*BitmapBuilder)
					for r := range lineFields {
						v, err := strconv.ParseBool(lineFields[r][c])
						panicIfErr(dataType, lineFields[r][c], err)
						if v {
							bb.Set(r)
						}
					}
				case DataTypeUint:
					b := colWriters[c].(*UintBuilder)
					for r := range lineFields {
						v, err := strconv.ParseUint(lineFields[r][c], 10, 64)
						panicIfErr(dataType, lineFields[r][c], err)
						b.Set(r, v)
					}
				case DataTypeBytes:
					b := colWriters[c].(*RawBytesBuilder)
					for r := range lineFields {
						b.Put([]byte(lineFields[r][c]))
					}
				case DataTypePrefixBytes:
					panic("unimplemented")
				default:
					panic(fmt.Sprintf("unsupported data type: %s", dataType))
				}
			}
			return ""
		case "finish":
			block := FinishBlock(int(rows), colWriters)
			d := DecodeBlock(block, 0)
			return d.FormattedString()
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
func randBlock(rng *rand.Rand, rows int, schema []testColumnSpec) ([]byte, []interface{}) {
	data := make([]interface{}, len(schema))
	for col := range data {
		switch schema[col].DataType {
		case DataTypeBool:
			v := make([]bool, rows)
			for row := 0; row < rows; row++ {
				v[row] = (rng.Int32() % 2) == 0
			}
			data[col] = v
		case DataTypeUint:
			v := make([]uint64, rows)
			for row := 0; row < rows; row++ {
				v[row] = schema[col].IntRange.Rand(rng)
			}
			data[col] = v
		case DataTypeBytes:
			v := make([][]byte, rows)
			for row := 0; row < rows; row++ {
				v[row] = make([]byte, rng.IntN(20))
				for j := range v[row] {
					v[row][j] = byte(rng.Uint32())
				}
			}
			data[col] = v
		case DataTypePrefixBytes:
			v := make([][]byte, rows)
			for row := 0; row < rows; row++ {
				v[row] = make([]byte, rng.IntN(20)+1)
				for j := range v[row] {
					v[row][j] = byte(rng.Uint32())
				}
			}
			// PrefixBytes are required to be lexicographically sorted.
			slices.SortFunc(v, bytes.Compare)
			data[col] = v

		}
	}
	buf := buildBlock(schema, rows, data)
	return buf, data
}

func buildBlock(schema []testColumnSpec, rows int, data []interface{}) []byte {
	cw := make([]ColumnWriter, len(schema))
	for col := range schema {
		switch schema[col].DataType {
		case DataTypeBool:
			var bb BitmapBuilder
			bb.Reset()
			for row, v := range data[col].([]bool) {
				if v {
					bb.Set(row)
				}
			}
			cw[col] = &bb
		case DataTypeUint:
			var b UintBuilder
			b.Init()
			for row, v := range data[col].([]uint64) {
				b.Set(row, v)
			}
			cw[col] = &b
		case DataTypeBytes:
			var b RawBytesBuilder
			b.Reset()
			for _, v := range data[col].([][]byte) {
				b.Put(v)
			}
			cw[col] = &b
		case DataTypePrefixBytes:
			var pbb PrefixBytesBuilder
			pbb.Init(schema[col].BundleSize)
			colData := data[col].([][]byte)
			for r, v := range colData {
				sharedPrefix := 0
				if r > 0 {
					sharedPrefix = crbytes.CommonPrefix(colData[r-1], v)
				}
				pbb.Put(v, sharedPrefix)
			}
			cw[col] = &pbb
		}
	}
	return FinishBlock(rows, cw)
}

func testRandomBlock(t *testing.T, rng *rand.Rand, rows int, schema []testColumnSpec) {
	var sb strings.Builder
	for i := range schema {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(schema[i].String())
	}

	t.Run(sb.String(), func(t *testing.T) {
		block, data := randBlock(rng, rows, schema)
		d := DecodeBlock(block, 0)
		if uint32(d.header.Columns) != uint32(len(schema)) {
			t.Fatalf("expected %d columns, but found %d\n", len(schema), d.header.Columns)
		}
		if d.header.Rows != uint32(rows) {
			t.Fatalf("expected %d rows, but found %d\n", rows, d.header.Rows)
		}
		for col := range schema {
			if schema[col].DataType != d.DataType(col) {
				t.Fatalf("schema mismatch: %s != %s\n", schema[col], d.DataType(col))
			}
		}

		for col := range data {
			spec := schema[col]
			var got interface{}
			switch spec.DataType {
			case DataTypeBool:
				got = Clone(d.Bitmap(col), rows)
			case DataTypeUint:
				got = Clone(d.Uints(col), rows)
			case DataTypeBytes:
				got = Clone(d.RawBytes(col), rows)
			case DataTypePrefixBytes:
				got = Clone(d.PrefixBytes(col), rows)
			}
			if !reflect.DeepEqual(data[col], got) {
				t.Fatalf("%d: %s: expected\n%+v\ngot\n%+v\n% x",
					col, schema[col], data[col], got, d.data)
			}
		}
	})
}

func TestBlockWriterRandomized(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("Seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))
	randInt := func(lo, hi int) int {
		return lo + rng.IntN(hi-lo)
	}
	testRandomBlock(t, rng, randInt(1, 100), []testColumnSpec{{DataType: DataTypeBool}})
	for _, r := range interestingIntRanges {
		testRandomBlock(t, rng, randInt(1, 100), []testColumnSpec{{DataType: DataTypeUint, IntRange: r}})
	}
	testRandomBlock(t, rng, randInt(1, 100), []testColumnSpec{{DataType: DataTypeBytes}})
	testRandomBlock(t, rng, randInt(1, 100), []testColumnSpec{{DataType: DataTypePrefixBytes, BundleSize: 1 << randInt(0, 6)}})

	for i := 0; i < 100; i++ {
		schema := make([]testColumnSpec, 2+rng.IntN(8))
		for j := range schema {
			schema[j].DataType = DataType(randInt(1, int(dataTypesCount)))
			if schema[j].DataType == DataTypePrefixBytes {
				schema[j].BundleSize = 1 << randInt(0, 6)
			}
		}
		testRandomBlock(t, rng, randInt(1, 100), schema)
	}
}
