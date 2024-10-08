// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

// objTag identifies the type for an object: DB, Batch, Iter, or Snapshot.
type objTag uint8

const (
	dbTag objTag = iota + 1
	batchTag
	iterTag
	snapTag
	externalObjTag
	numObjTags
)

var objTagPrefix = [numObjTags]string{
	dbTag:          "db",
	batchTag:       "batch",
	iterTag:        "iter",
	snapTag:        "snap",
	externalObjTag: "external",
}

// objID identifies a particular object. The top 4-bits store the tag
// identifying the type of object, while the bottom 28-bits store the slot used
// to index with the test.{batches,iters,snapshots,externalObjs} slices.
type objID uint32

func makeObjID(t objTag, slot uint32) objID {
	return objID((uint32(t) << 28) | slot)
}

func (i objID) tag() objTag {
	return objTag(i >> 28)
}

func (i objID) slot() uint32 {
	return uint32(i) & ((1 << 28) - 1)
}

func (i objID) String() string {
	return fmt.Sprintf("%s%d", objTagPrefix[i.tag()], i.slot())
}

func parseObjID(str string) (objID, error) {
	// To provide backward compatibility, treat "db" as "db1". Note that unlike
	// the others, db slots are 1-indexed.
	if str == "db" {
		str = "db1"
	}
	for tag := objTag(1); tag < numObjTags; tag++ {
		if strings.HasPrefix(str, objTagPrefix[tag]) {
			id, err := strconv.ParseInt(str[len(objTagPrefix[tag]):], 10, 32)
			if err != nil {
				return 0, err
			}
			return makeObjID(tag, uint32(id)), nil
		}
	}
	return 0, errors.Newf("unknown object type: %q", str)
}

// objIDSlice is an unordered set of integers used when random selection of an
// element is required.
type objIDSlice []objID

func (s objIDSlice) Len() int           { return len(s) }
func (s objIDSlice) Less(i, j int) bool { return s[i] < s[j] }
func (s objIDSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// Remove removes the specified integer from the set.
//
// TODO(peter): If this proves slow, we can replace this implementation with a
// map and a slice. The slice would provide for random selection of an element,
// while the map would provide quick location of an element to remove.
func (s *objIDSlice) remove(id objID) {
	n := len(*s)
	for j := 0; j < n; j++ {
		if (*s)[j] == id {
			(*s)[j], (*s)[n-1] = (*s)[n-1], (*s)[j]
			(*s) = (*s)[:n-1]
			break
		}
	}
}

func (s *objIDSlice) rand(rng *rand.Rand) objID {
	return (*s)[rng.IntN(len(*s))]
}

// objIDSet is an unordered set of object IDs.
type objIDSet map[objID]struct{}

// sortedKeys returns a sorted slice of the set's keys for deterministic
// iteration.
func (s objIDSet) sorted() []objID {
	keys := make(objIDSlice, 0, len(s))
	for id := range s {
		keys = append(keys, id)
	}
	sort.Sort(keys)
	return keys
}

// firstError returns the first non-nil error of err0 and err1, or nil if both
// are nil.
func firstError(err0, err1 error) error {
	if err0 != nil {
		return err0
	}
	return err1
}
