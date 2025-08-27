// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"sort"

	"github.com/cockroachdb/pebble/v2/internal/base"
)

// TryToSimplifyKeys parses the operations data and tries to reassign keys to
// single lowercase characters. Note that the result is not necessarily
// semantically equivalent.
//
// On success it returns the new operations data.
//
// If there are too many distinct keys, returns nil.
func TryToSimplifyKeys(keyFormat KeyFormat, opsData []byte, retainSuffixes bool) []byte {
	ops, err := parse(opsData, parserOpts{
		parseFormattedUserKey:       keyFormat.ParseFormattedKey,
		parseFormattedUserKeySuffix: keyFormat.ParseFormattedKeySuffix,
	})
	if err != nil {
		panic(err)
	}
	keys := make(map[string]struct{})
	for i := range ops {
		ops[i].rewriteKeys(func(k UserKey) UserKey {
			if retainSuffixes {
				keys[string(k[:keyFormat.Comparer.Split(k)])] = struct{}{}
			} else {
				keys[string(k)] = struct{}{}
			}
			return k
		})
	}
	if len(keys) > ('z' - 'a' + 1) {
		return nil
	}
	sorted := sortedKeys(keyFormat.Comparer.Compare, keys)
	ordinals := make(map[string]int, len(sorted))
	for i, k := range sorted {
		ordinals[k] = i
	}
	for i := range ops {
		ops[i].rewriteKeys(func(k UserKey) UserKey {
			var suffix []byte
			if retainSuffixes {
				n := keyFormat.Comparer.Split(k)
				suffix = k[n:]
				k = k[:n]
			}
			idx := ordinals[string(k)]
			newKey := []byte{'a' + byte(idx)}
			return append(newKey, suffix...)
		})
	}
	return []byte(formatOps(keyFormat, ops))
}

func sortedKeys(cmp base.Compare, in map[string]struct{}) []string {
	var sorted []string
	for k := range in {
		sorted = append(sorted, k)
	}
	sort.Slice(sorted, func(i, j int) bool {
		return cmp([]byte(sorted[i]), []byte(sorted[j])) < 0
	})
	return sorted
}
