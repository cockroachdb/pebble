// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"sort"

	"github.com/cockroachdb/pebble/v2/internal/testkeys"
)

// TryToSimplifyKeys parses the operations data and tries to reassign keys to
// single lowercase characters. Note that the result is not necessarily
// semantically equivalent.
//
// On success it returns the new operations data.
//
// If there are too many distinct keys, returns nil.
func TryToSimplifyKeys(opsData []byte, retainSuffixes bool) []byte {
	ops, err := parse(opsData, parserOpts{})
	if err != nil {
		panic(err)
	}
	keys := make(map[string]struct{})
	for i := range ops {
		for _, k := range ops[i].keys() {
			key := *k
			if retainSuffixes {
				key = key[:testkeys.Comparer.Split(key)]
			}
			keys[string(key)] = struct{}{}
		}
	}
	if len(keys) > ('z' - 'a' + 1) {
		return nil
	}
	sorted := sortedKeys(keys)
	ordinals := make(map[string]int, len(sorted))
	for i, k := range sorted {
		ordinals[k] = i
	}
	for i := range ops {
		for _, k := range ops[i].keys() {
			key := *k
			var suffix []byte
			if retainSuffixes {
				n := testkeys.Comparer.Split(key)
				suffix = key[n:]
				key = key[:n]
			}
			idx := ordinals[string(key)]
			newKey := []byte{'a' + byte(idx)}
			newKey = append(newKey, suffix...)
			*k = newKey
		}
	}
	return []byte(formatOps(ops))
}

func sortedKeys(in map[string]struct{}) []string {
	var sorted []string
	for k := range in {
		sorted = append(sorted, k)
	}
	sort.Slice(sorted, func(i, j int) bool {
		return testkeys.Comparer.Compare([]byte(sorted[i]), []byte(sorted[j])) < 0
	})
	return sorted
}
