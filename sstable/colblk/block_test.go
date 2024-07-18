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
	BundleSize int // Only used for DataTypePrefixBytes
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
				case DataTypeUint8:
					b := &UintBuilder[uint8]{}
					b.Init()
					colWriters[i] = b
				case DataTypeUint16:
					b := &UintBuilder[uint16]{}
					b.Init()
					colWriters[i] = b
				case DataTypeUint32:
					b := &UintBuilder[uint32]{}
					b.Init()
					colWriters[i] = b
				case DataTypeUint64:
					b := &UintBuilder[uint64]{}
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
						bb.Set(r, v)
					}
				case DataTypeUint8:
					b := colWriters[c].(*UintBuilder[uint8])
					for r := range lineFields {
						v, err := strconv.ParseUint(lineFields[r][c], 10, 8)
						panicIfErr(dataType, lineFields[r][c], err)
						b.Set(r, uint8(v))
					}
				case DataTypeUint16:
					b := colWriters[c].(*UintBuilder[uint16])
					for r := range lineFields {
						v, err := strconv.ParseUint(lineFields[r][c], 10, 16)
						panicIfErr(dataType, lineFields[r][c], err)
						b.Set(r, uint16(v))
					}
				case DataTypeUint32:
					b := colWriters[c].(*UintBuilder[uint32])
					for r := range lineFields {
						v, err := strconv.ParseUint(lineFields[r][c], 10, 32)
						panicIfErr(dataType, lineFields[r][c], err)
						b.Set(r, uint32(v))
					}
				case DataTypeUint64:
					b := colWriters[c].(*UintBuilder[uint64])
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
			for row := 0; row < rows; row++ {
				v[row] = make([]byte, rng.Intn(20)+1)
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
			var bb BitmapBuilder
			bb.Reset()
			for row, v := range data[col].([]bool) {
				bb.Set(row, v)
			}
			cw[col] = &bb
		case DataTypeUint8:
			var b UintBuilder[uint8]
			b.Init()
			for row, v := range data[col].([]uint8) {
				b.Set(row, v)
			}
			cw[col] = &b
		case DataTypeUint16:
			var b UintBuilder[uint16]
			b.Init()
			for row, v := range data[col].([]uint16) {
				b.Set(row, v)
			}
			cw[col] = &b
		case DataTypeUint32:
			var b UintBuilder[uint32]
			b.Init()
			for row, v := range data[col].([]uint32) {
				b.Set(row, v)
			}
			cw[col] = &b
		case DataTypeUint64:
			var b UintBuilder[uint64]
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
					sharedPrefix = bytesSharedPrefix(colData[r-1], v)
				}
				pbb.Put(v, sharedPrefix)
			}
			cw[col] = &pbb
		}
	}
	return FinishBlock(rows, cw)
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
		if r.header.Rows != uint32(rows) {
			t.Fatalf("expected %d rows, but found %d\n", rows, r.header.Rows)
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
				b := r.Bitmap(col)
				vals := make([]bool, r.header.Rows)
				for i := range vals {
					vals[i] = b.At(i)
				}
				got = vals
			case DataTypeUint8:
				got = r.Uint8s(col).Clone(rows)
			case DataTypeUint16:
				got = r.Uint16s(col).Clone(rows)
			case DataTypeUint32:
				got = r.Uint32s(col).Clone(rows)
			case DataTypeUint64:
				got = r.Uint64s(col).Clone(rows)
			case DataTypeBytes:
				vals2 := make([][]byte, rows)
				vals := r.RawBytes(col)
				for i := range vals2 {
					vals2[i] = vals.At(i)
				}
				got = vals2
			case DataTypePrefixBytes:
				vals2 := make([][]byte, rows)
				vals := r.PrefixBytes(col)
				for i := range vals2 {
					vals2[i] = slices.Concat(vals.SharedPrefix(), vals.RowBundlePrefix(i), vals.RowSuffix(i))
				}
				got = vals2
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
	rng := rand.New(rand.NewSource(seed))
	randInt := func(lo, hi int) int {
		return lo + rng.Intn(hi-lo)
	}
	testRandomBlock(t, rng, randInt(1, 100), []ColumnSpec{{DataType: DataTypeBool}})
	testRandomBlock(t, rng, randInt(1, 100), []ColumnSpec{{DataType: DataTypeUint8}})
	testRandomBlock(t, rng, randInt(1, 100), []ColumnSpec{{DataType: DataTypeUint16}})
	testRandomBlock(t, rng, randInt(1, 100), []ColumnSpec{{DataType: DataTypeUint32}})
	testRandomBlock(t, rng, randInt(1, 100), []ColumnSpec{{DataType: DataTypeUint64}})
	testRandomBlock(t, rng, randInt(1, 100), []ColumnSpec{{DataType: DataTypeBytes}})
	testRandomBlock(t, rng, randInt(1, 100), []ColumnSpec{{DataType: DataTypePrefixBytes, BundleSize: 1 << randInt(2, 6)}})

	for i := 0; i < 100; i++ {
		schema := make([]ColumnSpec, 2+rng.Intn(8))
		for j := range schema {
			schema[j].DataType = DataType(randInt(1, int(dataTypesCount)))
			if schema[j].DataType == DataTypePrefixBytes {
				schema[j].BundleSize = 1 << randInt(2, 6)
			}
		}
		testRandomBlock(t, rng, randInt(1, 100), schema)
	}
}
