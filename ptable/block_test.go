package ptable

import (
	"math/rand"
	"reflect"
	"testing"
	"time"
	"unsafe"
)

func randBlock(rng *rand.Rand, rows int, schema []columnDef) ([]byte, []interface{}) {
	data := make([]interface{}, len(schema))
	for col := range data {
		switch schema[col].Type {
		case columnTypeBool:
			var v bitmap
			for row := 0; row < rows; row++ {
				v = v.set(row, rng.Int31n(2) == 0)
			}
			data[col] = v
		case columnTypeInt8:
			v := make([]int8, rows)
			for row := 0; row < rows; row++ {
				v[row] = int8(rng.Int31n(1 << 8))
			}
			data[col] = v
		case columnTypeInt16:
			v := make([]int16, rows)
			for row := 0; row < rows; row++ {
				v[row] = int16(rng.Int31n(1 << 16))
			}
			data[col] = v
		case columnTypeInt32:
			v := make([]int32, rows)
			for row := 0; row < rows; row++ {
				v[row] = rng.Int31()
			}
			data[col] = v
		case columnTypeInt64:
			v := make([]int64, rows)
			for row := 0; row < rows; row++ {
				v[row] = rng.Int63()
			}
			data[col] = v
		case columnTypeFloat32:
			v := make([]float32, rows)
			for row := 0; row < rows; row++ {
				v[row] = rng.Float32()
			}
			data[col] = v
		case columnTypeFloat64:
			v := make([]float64, rows)
			for row := 0; row < rows; row++ {
				v[row] = rng.Float64()
			}
			data[col] = v
		case columnTypeBytes:
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
			switch schema[col].Type {
			case columnTypeBool:
				w.PutBool(col, data[col].(bitmap).get(row))
			case columnTypeInt8:
				w.PutInt8(col, data[col].([]int8)[row])
			case columnTypeInt16:
				w.PutInt16(col, data[col].([]int16)[row])
			case columnTypeInt32:
				w.PutInt32(col, data[col].([]int32)[row])
			case columnTypeInt64:
				w.PutInt64(col, data[col].([]int64)[row])
			case columnTypeFloat32:
				w.PutFloat32(col, data[col].([]float32)[row])
			case columnTypeFloat64:
				w.PutFloat64(col, data[col].([]float64)[row])
			case columnTypeBytes:
				w.PutBytes(col, data[col].([][]byte)[row])
			}
		}
	}

	return w.Finish(), data
}

func testSchema(t *testing.T, rng *rand.Rand, rows int, schema []columnDef) {
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
			if schema[col] != r.Column(col) {
				t.Fatalf("schema mismatch: %v != %v\n", schema[col], r.Column(col))
			}
		}

		for col := range data {
			var got interface{}
			switch schema[col].Type {
			case columnTypeBool:
				got, _ = r.Bool(col)
			case columnTypeInt8:
				got, _ = r.Int8(col)
			case columnTypeInt16:
				got, _ = r.Int16(col)
				if v := uintptr(unsafe.Pointer(&(got.([]int16)[0]))); v%2 != 0 {
					t.Fatalf("expected 2-byte alignment, but found %x\n", v)
				}
			case columnTypeInt32:
				got, _ = r.Int32(col)
				if v := uintptr(unsafe.Pointer(&(got.([]int32)[0]))); v%4 != 0 {
					t.Fatalf("expected 2-byte alignment, but found %x\n", v)
				}
			case columnTypeInt64:
				got, _ = r.Int64(col)
				if v := uintptr(unsafe.Pointer(&(got.([]int64)[0]))); v%8 != 0 {
					t.Fatalf("expected 2-byte alignment, but found %x\n", v)
				}
			case columnTypeFloat32:
				got, _ = r.Float32(col)
				if v := uintptr(unsafe.Pointer(&(got.([]float32)[0]))); v%4 != 0 {
					t.Fatalf("expected 2-byte alignment, but found %x\n", v)
				}
			case columnTypeFloat64:
				got, _ = r.Float64(col)
				if v := uintptr(unsafe.Pointer(&(got.([]float64)[0]))); v%8 != 0 {
					t.Fatalf("expected 2-byte alignment, but found %x\n", v)
				}
			case columnTypeBytes:
				vals, offsets := r.Bytes(col)
				vals2 := make([][]byte, len(offsets))
				for i := range vals2 {
					s := int32(0)
					if i > 0 {
						s = offsets[i-1]
					}
					vals2[i] = vals[s:offsets[i]]
				}
				got = vals2
			}
			if !reflect.DeepEqual(data[col], got) {
				t.Fatalf("expected\n%+v\ngot\n%+v\n% x", data[col], got, r.data)
			}
		}
	})
}

func TestBlockWriter(t *testing.T) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	randInt := func(lo, hi int) int {
		return lo + rng.Intn(hi-lo)
	}
	testSchema(t, rng, randInt(1, 100), []columnDef{{Type: columnTypeBool}})
	testSchema(t, rng, randInt(1, 100), []columnDef{{Type: columnTypeInt8}})
	testSchema(t, rng, randInt(1, 100), []columnDef{{Type: columnTypeInt16}})
	testSchema(t, rng, randInt(1, 100), []columnDef{{Type: columnTypeInt32}})
	testSchema(t, rng, randInt(1, 100), []columnDef{{Type: columnTypeInt64}})
	testSchema(t, rng, randInt(1, 100), []columnDef{{Type: columnTypeFloat32}})
	testSchema(t, rng, randInt(1, 100), []columnDef{{Type: columnTypeFloat64}})
	testSchema(t, rng, randInt(1, 100), []columnDef{{Type: columnTypeBytes}})

	for i := 0; i < 100; i++ {
		schema := make([]columnDef, 2+rng.Intn(8))
		for j := range schema {
			schema[j].ID = int32(j)
			schema[j].Type = columnType(1 + rng.Intn(columnTypeBytes))
		}
		testSchema(t, rng, randInt(1, 100), schema)
	}
}

func BenchmarkBlockReader(b *testing.B) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	blocks := make([][]byte, 128)
	for i := range blocks {
		blocks[i], _ = randBlock(rng, 4096, []columnDef{{Type: columnTypeInt64}})
	}

	b.ResetTimer()
	var sum int64
	for i, k := 0, 0; i < b.N; i += k {
		r := newReader(blocks[rng.Intn(len(blocks))])
		vals, _ := r.Int64(0)

		k = len(vals)
		if k > b.N-i {
			k = b.N - i
		}
		for j := 0; j < k; j++ {
			sum += vals[j]
		}
	}
}
