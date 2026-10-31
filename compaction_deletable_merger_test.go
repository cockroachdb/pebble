// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"fmt"
	"maps"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// deletableSumMerger wires base.NewDeletableSumValueMerger (a
// DeletableValueMerger whose merges resolving to zero report delete=true)
// into a *pebble.Merger usable by a DB.
var deletableSumMerger = &Merger{
	Name:  "deletable-sum-merger",
	Merge: base.NewDeletableSumValueMerger,
}

// TestCompactionDeletableMergerStress runs randomized sequences of Set, Merge,
// Delete, DeleteRange, Flush, Snapshot and Compact against a DB that uses
// deletableSumMerger, and after every operation compares reads at the latest
// state and at each open snapshot with a model.
//
// The model holds the visible value of each key. A Set stores its value, a
// Delete or DeleteRange removes the key, and a Merge adds its operand to the
// current value, or to zero when the key does not exist; a sum of zero removes
// the key. Set values include zero, which reads as "0" until a merge reaches
// it, so the test catches operands that compaction drops or turns into a
// tombstone without their base. Open snapshots split key histories into
// stripes. Flushes, and on odd seeds compactions into levels above the bottom,
// merge operands without their base: those seeds use a tiny LBaseMaxBytes and
// tiny target file sizes, so data spreads over several levels.
func TestCompactionDeletableMergerStress(t *testing.T) {
	ctx := context.Background()
	keys := []string{"a", "b", "c", "d"}
	seeds := uint64(100)
	if invariants.RaceEnabled {
		// Race builds run the test about 50 times slower.
		seeds = 10
	}
	for seed := uint64(0); seed < seeds; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(0, seed))
			opts := &Options{
				FS:     vfs.NewMem(),
				Merger: deletableSumMerger,
			}
			if seed%2 == 1 {
				// A tiny base level and one-key files spread the keys over
				// several levels. Compactions then also write into levels
				// above the bottom, where a merge may not reach its base.
				opts.LBaseMaxBytes = 1
				for i := range opts.TargetFileSizes {
					opts.TargetFileSizes[i] = 1
				}
			}
			db, err := Open("", opts)
			require.NoError(t, err)
			type snapshot struct {
				snap  *Snapshot
				model map[string]int64
				name  string
			}
			var snaps []snapshot
			defer func() {
				for _, s := range snaps {
					require.NoError(t, s.snap.Close())
				}
				require.NoError(t, db.Close())
			}()

			model := map[string]int64{}
			var history opHistory
			check := func(r Reader, want map[string]int64, view string) {
				got, err := readDeletableSums(r, keys)
				require.NoError(t, err, "view: %s\nops:\n%s", view, history)
				require.Equal(t, want, got, "view: %s\nops:\n%s", view, history)
			}

			for op := 0; op < 400; op++ {
				key := keys[rng.IntN(len(keys))]
				switch rng.IntN(12) {
				case 0, 1:
					v := rng.Int64N(5) - 2
					history = append(history, fmt.Sprintf("set %s %d", key, v))
					require.NoError(t, db.Set([]byte(key), []byte(strconv.FormatInt(v, 10)), NoSync))
					model[key] = v
				case 2, 3, 4:
					d := rng.Int64N(5) - 2
					history = append(history, fmt.Sprintf("merge %s %d", key, d))
					require.NoError(t, db.Merge([]byte(key), []byte(strconv.FormatInt(d, 10)), NoSync))
					if sum := model[key] + d; sum == 0 {
						delete(model, key)
					} else {
						model[key] = sum
					}
				case 5:
					history = append(history, fmt.Sprintf("delete %s", key))
					require.NoError(t, db.Delete([]byte(key), NoSync))
					delete(model, key)
				case 6:
					start, end := key, keys[rng.IntN(len(keys))]
					if start > end {
						start, end = end, start
					}
					history = append(history, fmt.Sprintf("delete-range %s-%s", start, end))
					require.NoError(t, db.DeleteRange([]byte(start), []byte(end), NoSync))
					for k := range model {
						if start <= k && k < end {
							delete(model, k)
						}
					}
				case 7:
					history = append(history, "flush")
					require.NoError(t, db.Flush())
				case 8:
					if len(snaps) < 4 {
						name := fmt.Sprintf("s%d", op)
						history = append(history, "snapshot "+name)
						snaps = append(snaps, snapshot{snap: db.NewSnapshot(), model: maps.Clone(model), name: name})
					} else {
						history = append(history, "close-snapshot "+snaps[0].name)
						require.NoError(t, snaps[0].snap.Close())
						snaps = snaps[1:]
					}
				case 9:
					start, end := key, keys[rng.IntN(len(keys))]
					if start > end {
						start, end = end, start
					}
					history = append(history, fmt.Sprintf("compact %s-%s", start, end))
					require.NoError(t, db.Compact(ctx, []byte(start), []byte(end+"\x00"), false))
				case 10, 11:
					history = append(history, "compact a-z")
					require.NoError(t, db.Compact(ctx, []byte("a"), []byte("z"), false))
				}
				check(db, model, "latest")
				for _, s := range snaps {
					check(s.snap, s.model, "snapshot "+s.name)
				}
			}
		})
	}
}

// opHistory lists the operations a test has run. Its String method joins them
// only when a failure message is formatted.
type opHistory []string

func (h opHistory) String() string {
	return strings.Join(h, "\n")
}

// readDeletableSums reads keys from r with both an iterator and Get, and
// returns the values that exist. It returns an error when the two reads
// disagree.
func readDeletableSums(r Reader, keys []string) (map[string]int64, error) {
	iter, err := r.NewIter(nil)
	if err != nil {
		return nil, err
	}
	got := map[string]int64{}
	for valid := iter.First(); valid; valid = iter.Next() {
		v, err := strconv.ParseInt(string(iter.Value()), 10, 64)
		if err != nil {
			return nil, errors.CombineErrors(err, iter.Close())
		}
		got[string(iter.Key())] = v
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	for _, k := range keys {
		value, closer, err := r.Get([]byte(k))
		if errors.Is(err, ErrNotFound) {
			if _, ok := got[k]; ok {
				return got, errors.Newf("Get(%s) is not found, iterator returned %d", k, got[k])
			}
			continue
		}
		if err != nil {
			return got, err
		}
		v, err := strconv.ParseInt(string(value), 10, 64)
		if err := errors.CombineErrors(err, closer.Close()); err != nil {
			return got, err
		}
		if iv, ok := got[k]; !ok || iv != v {
			return got, errors.Newf("Get(%s) = %d, iterator returned %d (present: %t)", k, v, iv, ok)
		}
	}
	return got, nil
}
