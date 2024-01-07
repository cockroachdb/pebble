// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import "sort"

// TryToSimplifyKeys parses the operations data and tries to reassign keys to
// single lowercase characters.
// On success it returns the new operations data.
// If there are too many distinct keys, returns nil (and no error).
func TryToSimplifyKeys(opsData []byte) ([]byte, error) {
	ops, err := parse(opsData, parserOpts{})
	if err != nil {
		return nil, err
	}
	keys := make(map[string]struct{})
	for i := range ops {
		for _, k := range ops[i].keys() {
			keys[string(*k)] = struct{}{}
		}
	}
	if len(keys) > ('z' - 'a' + 1) {
		return nil, nil
	}
	sorted := sortedKeys(keys)
	ordinals := make(map[string]int, len(sorted))
	for i, k := range sorted {
		ordinals[k] = i
	}
	for i := range ops {
		for _, k := range ops[i].keys() {
			idx := ordinals[string(*k)]
			*k = []byte{'a' + byte(idx)}
		}
	}
	return []byte(formatOps(ops)), nil
}

func sortedKeys(in map[string]struct{}) []string {
	var sorted []string
	for k := range in {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	return sorted
}
