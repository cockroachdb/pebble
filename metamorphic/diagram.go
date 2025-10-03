// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import "strings"

// TryToGenerateDiagram attempts to generate a user-readable ASCII diagram of
// the keys involved in the operations.
//
// If the diagram would be too large to be practical, returns the empty string
// (with no error).
func TryToGenerateDiagram(keyFormat KeyFormat, opsData []byte) (string, error) {
	ops, err := parse(opsData, parserOpts{
		parseFormattedUserKey:       keyFormat.ParseFormattedKey,
		parseFormattedUserKeySuffix: keyFormat.ParseFormattedKeySuffix,
		parseMaximumSuffixProperty:  keyFormat.ParseMaximumSuffixProperty,
	})
	if err != nil {
		return "", err
	}
	if len(ops) > 200 {
		return "", nil
	}
	keySet := make(map[string]struct{})
	for _, o := range ops {
		for _, r := range o.diagramKeyRanges() {
			keySet[string(r.Start)] = struct{}{}
			keySet[string(r.End)] = struct{}{}
		}
	}
	if len(keySet) == 0 {
		return "", nil
	}
	keys := sortedKeys(keyFormat.Comparer.Compare, keySet)
	axis1, axis2, pos := genAxis(keys)
	if len(axis1) > 200 {
		return "", nil
	}

	var rows []string
	for _, o := range ops {
		ranges := o.diagramKeyRanges()
		var row strings.Builder
		for _, r := range ranges {
			s := pos[string(r.Start)]
			e := pos[string(r.End)]
			for row.Len() < s {
				row.WriteByte(' ')
			}
			row.WriteByte('|')
			if e > s {
				for row.Len() < e {
					row.WriteByte('-')
				}
				row.WriteByte('|')
			}
		}
		for row.Len() <= len(axis1) {
			row.WriteByte(' ')
		}
		row.WriteString(o.formattedString(keyFormat))

		rows = append(rows, row.String())
	}
	rows = append(rows, axis1, axis2)
	return strings.Join(rows, "\n"), nil
}

// genAxis generates the horizontal key axis and returns two rows (one for axis
// one for labels), along with a map from key to column.

// Example:
//
//	axisRow:    |----|----|----|
//	labelRow:   a    bar  foo  zed
//	pos:        a:0, bar:5, foo:10, zed:15
func genAxis(keys []string) (axisRow string, labelRow string, pos map[string]int) {
	const minSpaceBetweenKeys = 4
	const minSpaceBetweenKeyLabels = 2
	var a, b strings.Builder
	pos = make(map[string]int)
	for i, k := range keys {
		if i > 0 {
			b.WriteString(strings.Repeat(" ", minSpaceBetweenKeyLabels))
			for b.Len() <= a.Len()+minSpaceBetweenKeys {
				b.WriteByte(' ')
			}
			for a.Len() < b.Len() {
				a.WriteByte('-')
			}
		}
		pos[k] = a.Len()
		a.WriteByte('|')
		b.WriteString(k)
	}
	return a.String(), b.String(), pos
}
