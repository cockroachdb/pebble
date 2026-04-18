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
	"github.com/cockroachdb/pebble/internal/testutils"
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
	valStr := func(k intKey, opts int) string {
		return fmt.Sprintf("%d:%d", k, opts)
	}
	initFn := func(
		ctx context.Context, k intKey, opts int, v ValueRef[intKey, string, int]) error {
		*v.Value() = valStr(k, opts)
		return nil
	}
	releaseFn := func(v *string) {
		*v = "bogus"
	}
	c := New[intKey, string, int](10, 1, initFn, releaseFn)
	ctx := context.Background()

	for i := range 100 {
		k := intKey(i % 10)
		ref, err := c.FindOrCreate(ctx, k, int(2*k))
		require.NoError(t, err)
		require.Equal(t, valStr(k, int(2*k)), *ref.Value())
		ref.Unref()
	}
	m := c.Metrics()
	require.Equal(t, int64(10), m.Misses)

	for i := range 100 {
		k := intKey(i % 10)
		ref, err := c.FindOrCreate(ctx, k, int(2*k))
		require.NoError(t, err)
		require.Equal(t, valStr(k, int(2*k)), *ref.Value())
		ref.Unref()
	}

	c.Close()
}

func TestClockPro(t *testing.T) {
	// Reuse the data from the block cache. See
	// internal/cache/clockpro_test.go:TestCache.
	f, err := os.Open("../cache/testdata/cache")
	require.NoError(t, err)

	initFn := func(ctx context.Context, k intKey, opts struct{}, v ValueRef[intKey, string, struct{}]) error {
		*v.Value() = fmt.Sprint(k)
		return nil
	}
	releaseFn := func(v *string) {
		*v = "bogus"
	}
	// The cache must have a single shard of size 200 is required for the expected
	// test values.
	c := New[intKey, string, struct{}](200, 1, initFn, releaseFn)
	defer c.Close()

	scanner := bufio.NewScanner(f)

	for line := 1; scanner.Scan(); line++ {
		fields := bytes.Fields(scanner.Bytes())

		key, err := strconv.Atoi(string(fields[0]))
		require.NoError(t, err)

		oldHits := c.shards[0].hits.Load()

		ref, err := c.FindOrCreate(context.Background(), intKey(key), struct{}{})
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
	initFn := func(ctx context.Context, k intKey, opts struct{}, v ValueRef[intKey, int, struct{}]) error {
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
	testutils.CheckErr(c.FindOrCreate(ctx, 1, struct{}{})).Unref()
	testutils.CheckErr(c.FindOrCreate(ctx, 2, struct{}{})).Unref()
	testutils.CheckErr(c.FindOrCreate(ctx, 3, struct{}{})).Unref()
	testutils.CheckErr(c.FindOrCreate(ctx, 4, struct{}{})).Unref()
	expectInitialized(1, 2, 3, 4)
	expectReleased()
	c.Evict(2)
	expectReleased(2)
	testutils.CheckErr(c.FindOrCreate(ctx, 2, struct{}{})).Unref()
	expectInitialized(2)
	c.EvictAll(func(k intKey) bool {
		return k%2 == 1
	})
	expectReleased(1, 3)
	testutils.CheckErr(c.FindOrCreate(ctx, 2, struct{}{})).Unref()
	expectInitialized()
	testutils.CheckErr(c.FindOrCreate(ctx, 3, struct{}{})).Unref()
	expectInitialized(3)

	c.Close()
	expectReleased(2, 3, 4)
}

func TestEvictPanic(t *testing.T) {
	initFn := func(ctx context.Context, k intKey, opts struct{}, v ValueRef[intKey, int, struct{}]) error {
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
	testutils.CheckErr(c.FindOrCreate(ctx, 1, struct{}{})).Unref()
	testutils.CheckErr(c.FindOrCreate(ctx, 2, struct{}{})).Unref()
	testutils.CheckErr(c.FindOrCreate(ctx, 3, struct{}{})).Unref()
	testutils.CheckErr(c.FindOrCreate(ctx, 4, struct{}{})).Unref()

	ref := testutils.CheckErr(c.FindOrCreate(ctx, 3, struct{}{}))
	require.Panics(t, func() {
		c.Evict(3)
	})
	ref.Unref()
	_, _ = c.FindOrCreate(ctx, 2, struct{}{})
	require.Panics(t, func() {
		c.Close()
	})
}

func TestErrorHandling(t *testing.T) {
	var fail atomic.Int32
	initFn := func(ctx context.Context, k intKey, opts struct{}, v ValueRef[intKey, int, struct{}]) error {
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
	ctx := t.Context()

	fail.Store(1)
	var wg sync.WaitGroup
	for range 3 {
		wg.Go(func() {
			_, err := c.FindOrCreate(ctx, 1, struct{}{})
			require.ErrorContains(t, err, "1")
		})
	}
	wg.Wait()

	fail.Store(2)
	// A new attempt should try again and return the new error.
	_, err := c.FindOrCreate(ctx, 1, struct{}{})
	require.ErrorContains(t, err, "2")

	fail.Store(0)
	// A new attempt should succeed.
	v, err := c.FindOrCreate(ctx, 1, struct{}{})
	require.NoError(t, err)
	require.Equal(t, *v.Value(), 1)
	v.Unref()
}

func TestContextCancellation(t *testing.T) {
	// Test that findOrCreateValue respects context cancellation when waiting
	// for another goroutine to initialize a value.
	ready := make(chan struct{})
	proceed := make(chan struct{})

	initFn := func(ctx context.Context, k intKey, opts struct{}, v ValueRef[intKey, int, struct{}]) error {
		close(ready)
		<-proceed
		*v.Value() = int(k)
		return nil
	}
	releaseFn := func(v *int) {
		*v = -1
	}
	c := New[intKey, int](20, 1, initFn, releaseFn)
	defer c.Close()

	// Start a goroutine that will initialize the value but block.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ref, err := c.FindOrCreate(context.Background(), 1, struct{}{})
		require.NoError(t, err)
		require.Equal(t, 1, *ref.Value())
		ref.Unref()
	}()

	// Wait for the first goroutine to start initializing.
	<-ready

	// Try to get the same value with a cancelled context.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.FindOrCreate(ctx, 1, struct{}{})
	require.ErrorIs(t, err, context.Canceled)

	// Also test with a context that gets cancelled while waiting.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel2()
	_, err = c.FindOrCreate(ctx2, 1, struct{}{})
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// Unblock the initializing goroutine.
	close(proceed)
	wg.Wait()

	// Now subsequent calls with a valid context should succeed.
	ref, err := c.FindOrCreate(context.Background(), 1, struct{}{})
	require.NoError(t, err)
	require.Equal(t, 1, *ref.Value())
	ref.Unref()
}

// TestNoConcurrentInitForSameKey is a regression test for a race where
// runHandCold could evict a cold node whose initValueFn was still running,
// allowing a third goroutine to start a parallel initValueFn for the same
// key — violating the exclusivity contract documented in cache.go.
func TestNoConcurrentInitForSameKey(t *testing.T) {
	const blockedKey intKey = 1
	var perKey sync.Map // intKey -> *atomic.Int32
	block := make(chan struct{})

	initFn := func(ctx context.Context, k intKey, opts struct{}, v ValueRef[intKey, int, struct{}]) error {
		ctr, _ := perKey.LoadOrStore(k, &atomic.Int32{})
		c := ctr.(*atomic.Int32)
		if n := c.Add(1); n > 1 {
			t.Errorf("concurrent InitValueFn for key %d (n=%d)", k, n)
		}
		defer c.Add(-1)
		if k == blockedKey {
			<-block
		}
		*v.Value() = int(k)
		return nil
	}
	releaseFn := func(v *int) { *v = -1 }

	// Single shard, small capacity so eviction is reachable with few keys.
	c := New[intKey, int, struct{}](2, 1, initFn, releaseFn)
	defer func() {
		// Drain remaining refs before Close.
		c.Close()
	}()
	ctx := context.Background()

	// Goroutine A: kicks off init for blockedKey; init blocks on `block`.
	type result struct {
		ref ValueRef[intKey, int, struct{}]
		err error
	}
	aDone := make(chan result, 1)
	go func() {
		ref, err := c.FindOrCreate(ctx, blockedKey, struct{}{})
		aDone <- result{ref, err}
	}()

	// Wait until blockedKey's init is in flight.
	require.Eventually(t, func() bool {
		v, ok := perKey.Load(blockedKey)
		return ok && v.(*atomic.Int32).Load() >= 1
	}, time.Second, time.Millisecond)

	// Goroutine B (this goroutine): apply eviction pressure with other keys
	// so runHandCold sweeps blockedKey's still-initializing cold node.
	for k := intKey(2); k <= 6; k++ {
		ref, err := c.FindOrCreate(ctx, k, struct{}{})
		require.NoError(t, err)
		ref.Unref()
	}

	// Goroutine C: ask for blockedKey again. Pre-fix this would observe
	// n.value == nil (cleared by runHandCold) and start a SECOND initFn for
	// blockedKey concurrently with A's. Post-fix it must wait on
	// v.initialized.
	cDone := make(chan result, 1)
	go func() {
		ref, err := c.FindOrCreate(ctx, blockedKey, struct{}{})
		cDone <- result{ref, err}
	}()

	// Give C time to either (incorrectly) start a second init or (correctly)
	// park on v.initialized. If a parallel init starts, the t.Errorf in
	// initFn will fire.
	time.Sleep(50 * time.Millisecond)

	// Unblock A's init.
	close(block)

	a := <-aDone
	require.NoError(t, a.err)
	require.Equal(t, int(blockedKey), *a.ref.Value())
	a.ref.Unref()

	cRes := <-cDone
	require.NoError(t, cRes.err)
	require.Equal(t, int(blockedKey), *cRes.ref.Value())
	cRes.ref.Unref()
}
