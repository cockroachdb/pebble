// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
)

type fileSamples struct {
	keys  [][]byte
	bytes []int64
}

func TestL0SplitsBasic(t *testing.T) {
	// TODO(sumeer): proper test
	fileSamplesMap := make(map[base.FileNum]fileSamples)
	cfg := splitConfig{
		splitBytes: 199,
		cmp:        bytes.Compare,
		sampleKeysFunc: func(fileNum base.FileNum, _ int64) ([][]byte, []int64, error) {
			s, _ := fileSamplesMap[fileNum]
			return s.keys, s.bytes, nil
		},
		obsoleteMu: &sync.Mutex{},
		obsoleteFn: func(_ []base.FileNum) {},
	}
	l0Splits := newL0Splits(cfg)
	f1 := FileMetadata{FileNum: 1}
	fileSamplesMap[1] = fileSamples{
		keys:  [][]byte{[]byte("a"), []byte("c"), []byte("e"), []byte("g")},
		bytes: []int64{100, 100, 100, 100},
	}
	l0Splits.update([]*FileMetadata{&f1}, nil)
	time.Sleep(2 * time.Second)
	printSplits(l0Splits.getSplits())

	f2 := FileMetadata{FileNum: 2}
	fileSamplesMap[2] = fileSamples{
		keys:  [][]byte{[]byte("b"), []byte("d"), []byte("f"), []byte("h")},
		bytes: []int64{100, 100, 100, 100},
	}
	l0Splits.update([]*FileMetadata{&f2}, nil)
	time.Sleep(2 * time.Second)
	printSplits(l0Splits.getSplits())

	l0Splits.update(nil, []base.FileNum{1})
	time.Sleep(2 * time.Second)
	printSplits(l0Splits.getSplits())

	l0Splits.update(nil, []base.FileNum{2})
	time.Sleep(2 * time.Second)
	printSplits(l0Splits.getSplits())
}

func printSplits(splits [][]byte) {
	var splitStrs []string
	for _, s := range splits {
		splitStrs = append(splitStrs, string(s))
	}
	fmt.Printf("splits: %s\n", strings.Join(splitStrs, ", "))
}
