package ptable

import (
	"math/rand"
	"reflect"
	"testing"
	"time"
	"unsafe"
)

func randBlock(rng *rand.Rand, rows int, schema []ColumnType) ([]byte, []interface{}) {
	data := make([]interface{}, len(schema))
	for col := range data {
		switch schema[col] {
		case ColumnTypeBool:
			var v Bitmap
			for row := 0; row < rows; row++ {
				v = v.set(row, rng.Int31n(2) == 0)
			}
			data[col] = v
		case ColumnTypeInt8:
			v := make([]int8, rows)
			for row := 0; row < rows; row++ {
				v[row] = int8(rng.Int31n(1 << 8))
			}
			data[col] = v
		case ColumnTypeInt16:
			v := make([]int16, rows)
			for row := 0; row < rows; row++ {
				v[row] = int16(rng.Int31n(1 << 16))
			}
			data[col] = v
		case ColumnTypeInt32:
			v := make([]int32, rows)
			for row := 0; row < rows; row++ {
				v[row] = rng.Int31()
			}
			data[col] = v
		case ColumnTypeInt64:
			v := make([]int64, rows)
			for row := 0; row < rows; row++ {
				v[row] = rng.Int63()
			}
			data[col] = v
		case ColumnTypeFloat32:
			v := make([]float32, rows)
			for row := 0; row < rows; row++ {
				v[row] = rng.Float32()
			}
			data[col] = v
		case ColumnTypeFloat64:
			v := make([]float64, rows)
			for row := 0; row < rows; row++ {
				v[row] = rng.Float64()
			}
			data[col] = v
		case ColumnTypeBytes:
			v := make([][]byte, rows)
			for row := 0; row < rows; row++ {
				v[row] = make([]byte, rng.Intn(20))
				rng.Read(v[row])
			}
			data[col] = v
		}
	}

	var w blockWriter
	w.SetSchema(schema)

	for row := 0; row < rows; row++ {
		for col := 0; col < len(schema); col++ {
			switch schema[col] {
			case ColumnTypeBool:
				w.PutBool(col, data[col].(Bitmap).Get(row))
			case ColumnTypeInt8:
				w.PutInt8(col, data[col].([]int8)[row])
			case ColumnTypeInt16:
				w.PutInt16(col, data[col].([]int16)[row])
			case ColumnTypeInt32:
				w.PutInt32(col, data[col].([]int32)[row])
			case ColumnTypeInt64:
				w.PutInt64(col, data[col].([]int64)[row])
			case ColumnTypeFloat32:
				w.PutFloat32(col, data[col].([]float32)[row])
			case ColumnTypeFloat64:
				w.PutFloat64(col, data[col].([]float64)[row])
			case ColumnTypeBytes:
				w.PutBytes(col, data[col].([][]byte)[row])
			}
		}
	}

	return w.Finish(), data
}

func testSchema(t *testing.T, rng *rand.Rand, rows int, schema []ColumnType) {
	// TODO(peter): fix this wart. The writer is otherwise unused except for the
	// w.String() call.
	var w blockWriter
	w.SetSchema(schema)
	t.Run(w.String(), func(t *testing.T) {
		block, data := randBlock(rng, rows, schema)

		r := newReader(block)
		if r.cols != int32(len(schema)) {
			t.Fatalf("expected %d columns, but found %d\n", len(schema), r.cols)
		}
		if r.rows != int32(rows) {
			t.Fatalf("expected %d rows, but found %d\n", rows, r.rows)
		}
		for col := range schema {
			if schema[col] != r.Vec(col).Type {
				t.Fatalf("schema mismatch: %s != %s\n", schema[col], r.Vec(col).Type)
			}
		}

		for col := range data {
			var got interface{}
			switch schema[col] {
			case ColumnTypeBool:
				got = r.Vec(col).Bool()
			case ColumnTypeInt8:
				got = r.Vec(col).Int8()
			case ColumnTypeInt16:
				got = r.Vec(col).Int16()
				if v := uintptr(unsafe.Pointer(&(got.([]int16)[0]))); v%2 != 0 {
					t.Fatalf("expected 2-byte alignment, but found %x\n", v)
				}
			case ColumnTypeInt32:
				got = r.Vec(col).Int32()
				if v := uintptr(unsafe.Pointer(&(got.([]int32)[0]))); v%4 != 0 {
					t.Fatalf("expected 2-byte alignment, but found %x\n", v)
				}
			case ColumnTypeInt64:
				got = r.Vec(col).Int64()
				if v := uintptr(unsafe.Pointer(&(got.([]int64)[0]))); v%8 != 0 {
					t.Fatalf("expected 2-byte alignment, but found %x\n", v)
				}
			case ColumnTypeFloat32:
				got = r.Vec(col).Float32()
				if v := uintptr(unsafe.Pointer(&(got.([]float32)[0]))); v%4 != 0 {
					t.Fatalf("expected 2-byte alignment, but found %x\n", v)
				}
			case ColumnTypeFloat64:
				got = r.Vec(col).Float64()
				if v := uintptr(unsafe.Pointer(&(got.([]float64)[0]))); v%8 != 0 {
					t.Fatalf("expected 2-byte alignment, but found %x\n", v)
				}
			case ColumnTypeBytes:
				vals := r.Vec(col).Bytes()
				vals2 := make([][]byte, r.rows)
				for i := range vals2 {
					vals2[i] = vals.At(i)
				}
				got = vals2
			}
			if !reflect.DeepEqual(data[col], got) {
				t.Fatalf("expected\n%+v\ngot\n%+v\n% x", data[col], got, r.Data())
			}
		}
	})
}

func TestBlockWriter(t *testing.T) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	randInt := func(lo, hi int) int {
		return lo + rng.Intn(hi-lo)
	}
	testSchema(t, rng, randInt(1, 100), []ColumnType{ColumnTypeBool})
	testSchema(t, rng, randInt(1, 100), []ColumnType{ColumnTypeInt8})
	testSchema(t, rng, randInt(1, 100), []ColumnType{ColumnTypeInt16})
	testSchema(t, rng, randInt(1, 100), []ColumnType{ColumnTypeInt32})
	testSchema(t, rng, randInt(1, 100), []ColumnType{ColumnTypeInt64})
	testSchema(t, rng, randInt(1, 100), []ColumnType{ColumnTypeFloat32})
	testSchema(t, rng, randInt(1, 100), []ColumnType{ColumnTypeFloat64})
	testSchema(t, rng, randInt(1, 100), []ColumnType{ColumnTypeBytes})

	for i := 0; i < 100; i++ {
		schema := make([]ColumnType, 2+rng.Intn(8))
		for j := range schema {
			schema[j] = ColumnType(1 + rng.Intn(ColumnTypeBytes))
		}
		testSchema(t, rng, randInt(1, 100), schema)
	}
}

func BenchmarkBlockReader(b *testing.B) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	blocks := make([][]byte, 128)
	for i := range blocks {
		blocks[i], _ = randBlock(rng, 4096, []ColumnType{ColumnTypeInt64})
	}

	b.ResetTimer()
	var sum int64
	for i, k := 0, 0; i < b.N; i += k {
		r := newReader(blocks[rng.Intn(len(blocks))])
		vals := r.Vec(0).Int64()

		k = len(vals)
		if k > b.N-i {
			k = b.N - i
		}
		for j := 0; j < k; j++ {
			sum += vals[j]
		}
	}
}
