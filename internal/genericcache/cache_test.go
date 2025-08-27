// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package genericcache

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2/internal/testutils"
	"github.com/stretchr/testify/require"
)

type intKey int

// Randomly distribute small key values to shards.
var randPerm = rand.Perm(50)

func (k intKey) Shard(numShards int) int {
	if int(k) < len(randPerm) {
		return randPerm[k] % numShards
	}
	return int(k) % numShards
}

func TestBasic(t *testing.T) {
	initFn := func(ctx context.Context, k intKey, v ValueRef[intKey, string]) error {
		*v.Value() = fmt.Sprint(k)
		return nil
	}
	releaseFn := func(v *string) {
		*v = "bogus"
	}
	c := New[intKey, string](10, 1, initFn, releaseFn)
	ctx := context.Background()

	for i := range 100 {
		k := intKey(i % 10)
		ref, err := c.FindOrCreate(ctx, k)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprint(k), *ref.Value())
		ref.Unref()
	}
	m := c.Metrics()
	require.Equal(t, int64(10), m.Misses)

	for i := range 100 {
		k := intKey(i % 10)
		ref, err := c.FindOrCreate(ctx, k)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprint(k), *ref.Value())
		ref.Unref()
	}

	c.Close()
}

func TestFileCacheClockPro(t *testing.T) {
	// Reuse the data from the block cache. See
	// internal/cache/clockpro_test.go:TestCache.
	f, err := os.Open("../cache/testdata/cache")
	require.NoError(t, err)

	initFn := func(ctx context.Context, k intKey, v ValueRef[intKey, string]) error {
		*v.Value() = fmt.Sprint(k)
		return nil
	}
	releaseFn := func(v *string) {
		*v = "bogus"
	}
	// The cache must have a single shard of size 200 is required for the expected
	// test values.
	c := New[intKey, string](200, 1, initFn, releaseFn)
	defer c.Close()

	scanner := bufio.NewScanner(f)

	for line := 1; scanner.Scan(); line++ {
		fields := bytes.Fields(scanner.Bytes())

		key, err := strconv.Atoi(string(fields[0]))
		require.NoError(t, err)

		oldHits := c.shards[0].hits.Load()

		ref, err := c.FindOrCreate(context.Background(), intKey(key))
		require.NoError(t, err)
		require.Equal(t, fmt.Sprint(key), *ref.Value())
		ref.Unref()

		hit := c.shards[0].hits.Load() != oldHits
		wantHit := fields[1][0] == 'h'
		if hit != wantHit {
			t.Errorf("%d: cache hit mismatch: got %v, want %v\n", line, hit, wantHit)
		}
	}
}

func TestEvict(t *testing.T) {
	var initialized []int
	initFn := func(ctx context.Context, k intKey, v ValueRef[intKey, int]) error {
		initialized = append(initialized, int(k))
		*v.Value() = int(k)
		return nil
	}
	expectInitialized := func(vals ...int) {
		t.Helper()
		slices.Sort(initialized)
		slices.Sort(vals)
		require.Equal(t, initialized, vals)
		initialized = nil
	}
	var released []int
	expectReleased := func(vals ...int) {
		t.Helper()
		slices.Sort(released)
		slices.Sort(vals)
		require.Equal(t, released, vals)
		released = nil
	}
	releaseFn := func(v *int) {
		released = append(released, *v)
		*v = -1
	}
	c := New[intKey, int](20, 1+rand.IntN(4), initFn, releaseFn)
	ctx := context.Background()
	testutils.CheckErr(c.FindOrCreate(ctx, 1)).Unref()
	testutils.CheckErr(c.FindOrCreate(ctx, 2)).Unref()
	testutils.CheckErr(c.FindOrCreate(ctx, 3)).Unref()
	testutils.CheckErr(c.FindOrCreate(ctx, 4)).Unref()
	expectInitialized(1, 2, 3, 4)
	expectReleased()
	c.Evict(2)
	expectReleased(2)
	testutils.CheckErr(c.FindOrCreate(ctx, 2)).Unref()
	expectInitialized(2)
	c.EvictAll(func(k intKey) bool {
		return k%2 == 1
	})
	expectReleased(1, 3)
	testutils.CheckErr(c.FindOrCreate(ctx, 2)).Unref()
	expectInitialized()
	testutils.CheckErr(c.FindOrCreate(ctx, 3)).Unref()
	expectInitialized(3)

	c.Close()
	expectReleased(2, 3, 4)
}

func TestEvictPanic(t *testing.T) {
	initFn := func(ctx context.Context, k intKey, v ValueRef[intKey, int]) error {
		*v.Value() = int(k)
		return nil
	}
	releaseFn := func(v *int) {
		*v = -1
	}
	// The cache must have a single shard of size 200 is required for the expected
	// test values.
	c := New[intKey, int](20, 4, initFn, releaseFn)
	ctx := context.Background()
	testutils.CheckErr(c.FindOrCreate(ctx, 1)).Unref()
	testutils.CheckErr(c.FindOrCreate(ctx, 2)).Unref()
	testutils.CheckErr(c.FindOrCreate(ctx, 3)).Unref()
	testutils.CheckErr(c.FindOrCreate(ctx, 4)).Unref()

	ref := testutils.CheckErr(c.FindOrCreate(ctx, 3))
	require.Panics(t, func() {
		c.Evict(3)
	})
	ref.Unref()
	_, _ = c.FindOrCreate(ctx, 2)
	require.Panics(t, func() {
		c.Close()
	})
}

func TestErrorHandling(t *testing.T) {
	var fail atomic.Int32
	initFn := func(ctx context.Context, k intKey, v ValueRef[intKey, int]) error {
		if errVal := fail.Load(); errVal != 0 {
			time.Sleep(10 * time.Millisecond)
			return errors.Newf("%d", errVal)
		}
		*v.Value() = int(k)
		return nil
	}
	releaseFn := func(v *int) {
		*v = -1
	}
	c := New[intKey, int](20, 4, initFn, releaseFn)
	ctx := context.Background()

	fail.Store(1)
	var wg sync.WaitGroup
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func() {
			defer wg.Done()
			_, err := c.FindOrCreate(ctx, 1)
			require.ErrorContains(t, err, "1")
		}()
	}
	wg.Wait()

	fail.Store(2)
	// A new attempt should try again and return the new error.
	_, err := c.FindOrCreate(ctx, 1)
	require.ErrorContains(t, err, "2")

	fail.Store(0)
	// A new attempt should succeed.
	v, err := c.FindOrCreate(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, *v.Value(), 1)
	v.Unref()
}
