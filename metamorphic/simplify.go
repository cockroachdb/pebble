// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"sort"

	"github.com/cockroachdb/pebble/internal/testkeys"
)

// TryToSimplifyKeys parses the operations data and tries to reassign keys to
// single lowercase characters. Note that the result is not necessarily
// semantically equivalent.
//
// On success it returns the new operations data.
//
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
	// TODO(radu): We reassign the user keys, ignoring the prefix and suffix
	// composition. This is sufficient to reproduce a class of problems, but if it
	// fails, we should try to simplify just the prefix and retain the suffix.
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
	sort.Slice(sorted, func(i, j int) bool {
		return testkeys.Comparer.Compare([]byte(sorted[i]), []byte(sorted[j])) < 0
	})
	return sorted
}
