// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic_test

import (
	"fmt"
	"io"
	"math/rand/v2"

	"github.com/cockroachdb/pebble/metamorphic"
)

func ExampleExecute() {
	const seed = 1698702489658104000
	rng := rand.New(rand.NewPCG(0, seed))

	// Generate a random database by running the metamorphic test.
	testOpts := metamorphic.RandomOptions(rng, nil /* custom opt parsers */)
	ops := metamorphic.GenerateOps(rng, 10000, metamorphic.DefaultOpConfig())
	test, err := metamorphic.New(ops, testOpts, "" /* dir */, io.Discard)
	if err != nil {
		panic(err)
	}
	err = metamorphic.Execute(test)
	fmt.Print(err)
	// Output: <nil>
}
