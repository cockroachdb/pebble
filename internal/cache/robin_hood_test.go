// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cache

import (
	"testing"
	"time"

	"golang.org/x/exp/rand"
)

func TestRobinHoodMap(t *testing.T) {
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	rhMap := newRobinHoodMap(0)
	defer rhMap.free()

	goMap := make(map[key]*entry)

	randomKey := func() key {
		n := rng.Intn(len(goMap))
		for k := range goMap {
			if n == 0 {
				return k
			}
			n--
		}
		return key{}
	}

	ops := 10000 + rng.Intn(10000)
	for i := 0; i < ops; i++ {
		var which float64
		if len(goMap) > 0 {
			which = rng.Float64()
		}

		switch {
		case which < 0.4:
			// 40% insert.
			var k key
			k.id = rng.Uint64()
			k.fileNum = rng.Uint64()
			k.offset = rng.Uint64()
			e := &entry{}
			goMap[k] = e
			rhMap.Put(k, e)
			if len(goMap) != int(rhMap.Count()) {
				t.Fatalf("map sizes differ: %d != %d", len(goMap), rhMap.Count())
			}

		case which < 0.1:
			// 10% overwrite.
			k := randomKey()
			e := &entry{}
			goMap[k] = e
			rhMap.Put(k, e)
			if len(goMap) != int(rhMap.Count()) {
				t.Fatalf("map sizes differ: %d != %d", len(goMap), rhMap.Count())
			}

		case which < 0.75:
			// 25% delete.
			k := randomKey()
			delete(goMap, k)
			rhMap.Delete(k)
			if len(goMap) != int(rhMap.Count()) {
				t.Fatalf("map sizes differ: %d != %d", len(goMap), rhMap.Count())
			}

		default:
			// 25% lookup.
			k := randomKey()
			v := goMap[k]
			u := rhMap.Get(k)
			if v != u {
				t.Fatalf("%s: expected %p, but found %p", k, v, u)
			}
		}
	}

	t.Logf("map size: %d", len(goMap))
}
