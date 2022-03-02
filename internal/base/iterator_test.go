// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import (
	"math/rand"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func setRandUint64(v reflect.Value) uint64 {
	val := rand.Uint64()
	v.SetUint(val)
	return val
}

func TestInternalIteratorStatsMerge(t *testing.T) {
	var from, to, expected InternalIteratorStats
	n := reflect.ValueOf(from).NumField()
	for i := 0; i < n; i++ {
		switch reflect.ValueOf(from).Type().Field(i).Type.Kind() {
		case reflect.Uint64:
			v1 := setRandUint64(reflect.ValueOf(&from).Elem().Field(i))
			v2 := setRandUint64(reflect.ValueOf(&to).Elem().Field(i))
			reflect.ValueOf(&expected).Elem().Field(i).SetUint(v1 + v2)
		default:
			t.Fatalf("unknown kind %v", reflect.ValueOf(from).Type().Field(i).Type.Kind())
		}
	}
	to.Merge(from)
	require.Equal(t, expected, to)
}
