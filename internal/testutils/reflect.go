// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package testutils

import (
	"fmt"
	"reflect"
)

// AnyPointers returns true if the provided type contains any pointers.
func AnyPointers(typ reflect.Type) bool {
	kind := typ.Kind()
	switch kindPointers[kind] {
	case kindHasPointer:
		return true
	case kindNoPointer:
		return false
	}
	switch kind {
	case reflect.Struct:
		for i := range typ.NumField() {
			if AnyPointers(typ.Field(i).Type) {
				return true
			}
		}
		return false
	case reflect.Array:
		return AnyPointers(typ.Elem())
	default:
		panic(fmt.Sprintf("unexpected kind: %s", kind))
	}
}

type anyPointers int8

const (
	kindNoPointer anyPointers = iota
	kindHasPointer
	kindMaybeHasPointer
)

var kindPointers = []anyPointers{
	reflect.Invalid:       kindNoPointer,
	reflect.Bool:          kindNoPointer,
	reflect.Int:           kindNoPointer,
	reflect.Int8:          kindNoPointer,
	reflect.Int16:         kindNoPointer,
	reflect.Int32:         kindNoPointer,
	reflect.Int64:         kindNoPointer,
	reflect.Uint:          kindNoPointer,
	reflect.Uint8:         kindNoPointer,
	reflect.Uint16:        kindNoPointer,
	reflect.Uint32:        kindNoPointer,
	reflect.Uint64:        kindNoPointer,
	reflect.Uintptr:       kindNoPointer,
	reflect.Float32:       kindNoPointer,
	reflect.Float64:       kindNoPointer,
	reflect.Complex64:     kindNoPointer,
	reflect.Complex128:    kindNoPointer,
	reflect.Array:         kindMaybeHasPointer,
	reflect.Chan:          kindHasPointer,
	reflect.Func:          kindHasPointer,
	reflect.Interface:     kindHasPointer,
	reflect.Map:           kindHasPointer,
	reflect.Pointer:       kindHasPointer,
	reflect.Slice:         kindHasPointer,
	reflect.String:        kindHasPointer,
	reflect.Struct:        kindMaybeHasPointer,
	reflect.UnsafePointer: kindHasPointer,
}
