// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package testutils

// CheckErr can be used to simplify test code that expects no errors.
// Instead of:
//
//	v, err := SomeFunc()
//	if err != nil { .. }
//
// we can use:
//
//	v := testutils.CheckErr(someFunc())
func CheckErr[V any](v V, err error) V {
	if err != nil {
		panic(err)
	}
	return v
}

// CheckErr2 can be used to simplify test code that expects no errors.
// Instead of:
//
//	v, w, err := SomeFunc()
//	if err != nil { .. }
//
// we can use:
//
//	v, w := testutils.CheckErr2(someFunc())
func CheckErr2[V any, W any](v V, w W, err error) (V, W) {
	if err != nil {
		panic(err)
	}
	return v, w
}
