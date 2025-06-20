// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble_test

import (
	"bytes"
	"io"
	randv1 "math/rand"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"
	"testing/quick"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/metamorphic"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
	"github.com/stretchr/testify/require"
)

// TestIteratorErrors is a randomized test designed to ensure that errors
// encountered by reads are properly propagated through to the user. It uses the
// metamorphic tests configured with only write operations to first generate a
// random database. It then uses the metamorphic tests to run a random set of
// read operations against the generated database, randomly injecting errors at
// the VFS layer. If an error is injected over the course of an operation, it
// expects the error to surface to the operation output. If it doesn't, the test
// fails.
func TestIteratorErrors(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("Using seed %d", seed)
	rng := rand.New(rand.NewPCG(0, uint64(seed)))

	// Generate a random database by running the metamorphic test with the
	// WriteOpConfig. We'll perform ~10,000 random operations that mutate the
	// state of the database.
	kf := metamorphic.TestkeysKeyFormat
	testOpts := metamorphic.RandomOptions(rng, kf, nil /* custom opt parsers */)
	// With even a very small injection probability, it's relatively
	// unlikely that pebble.DebugCheckLevels will successfully complete
	// without being interrupted by an ErrInjected. Omit these checks.
	// TODO(jackson): Alternatively, we could wrap pebble.DebugCheckLevels,
	// mark the error value as having originated from CheckLevels, and retry
	// at most once. We would need to skip retrying on the second invocation
	// of DebugCheckLevels. It's all likely more trouble than it's worth.
	testOpts.Opts.DebugCheck = nil
	// Disable the physical FS so we don't need to worry about paths down below.
	if fs := testOpts.Opts.FS; fs == nil || fs == vfs.Default {
		testOpts.Opts.FS = vfs.NewMem()
	}

	{
		test, err := metamorphic.New(metamorphic.GenerateOps(
			rng, 10000, kf, metamorphic.WriteOpConfig()),
			testOpts, "" /* dir */, io.Discard)
		require.NoError(t, err)
		require.NoError(t, metamorphic.Execute(test))
	}
	t.Log("Constructed test database state")
	{
		testOpts.Opts.DisableTableStats = true
		testOpts.Opts.DisableAutomaticCompactions = true

		// Create an errorfs injector that injects ErrInjected on 5% of reads.
		// Wrap it in both a counter and a toggle so that we a) know whether an
		// error was injected over the course of an operation, and b) so that we
		// can disable error injection during Open.
		predicate := errorfs.And(errorfs.Reads, errorfs.Randomly(0.50, seed))
		counter := errorfs.Counter{Injector: errorfs.ErrInjected.If(predicate)}
		toggle := errorfs.Toggle{Injector: &counter}
		testOpts.Opts.FS = errorfs.Wrap(testOpts.Opts.FS, &toggle)
		testOpts.Opts.ReadOnly = true

		test, err := metamorphic.New(
			metamorphic.GenerateOps(rng, 5000, metamorphic.TestkeysKeyFormat, metamorphic.ReadOpConfig()),
			testOpts, "" /* dir */, &testWriter{t: t})
		require.NoError(t, err)

		defer func() {
			if r := recover(); r != nil {
				t.Errorf("last injected error: %+v", counter.LastError())
				panic(r)
			}
		}()

		// Begin injecting errors.
		toggle.On()

		prevCount := counter.Load()
		more := true
		for i := 0; more; i++ {
			var operationOutput string
			more, operationOutput, err = test.Step()
			// test.Step returns an error if the test called Fatalf. Error
			// injection should NOT trigger calls to Fatalf.
			if err != nil {
				t.Fatal(err)
			}
			newCount := counter.Load()
			if diff := newCount - prevCount; diff > 0 {
				if !strings.Contains(operationOutput, errorfs.ErrInjected.Error()) {
					t.Fatalf("Injected %d errors in op %d but the operation output %q does not contain the injected error: %+v",
						diff, i, operationOutput, counter.LastError())
				}
			}
			prevCount = newCount
		}
		t.Logf("Injected %d errors over the course of the test.", counter.Load())
	}
}

type testWriter struct {
	t *testing.T
}

func (w *testWriter) Write(b []byte) (int, error) {
	w.t.Log(string(bytes.TrimSpace(b)))
	return len(b), nil
}

func TestOptionsClone(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("Using seed %d", seed)
	rng := rand.New(rand.NewPCG(0, uint64(seed)))

	a := metamorphic.RandomOptions(rng, metamorphic.TestkeysKeyFormat, nil /* custom opt parsers */).Opts
	b := a.Clone()
	if rng.IntN(2) == 0 {
		a, b = b, a
	}
	before := a.String()
	mangle(reflect.ValueOf(b).Elem(), rng)
	after := a.String()
	require.Equal(t, before, after)
}

func mangle(v reflect.Value, rng *rand.Rand) {
	if !v.CanSet() {
		return
	}
	// Some of the time, generate a full new value.
	if rng.IntN(2) == 0 {
		genVal(v, rng)
		return
	}
	switch v.Type().Kind() {
	case reflect.Pointer:
		// If the pointer is to a type outside the pebble package, leave it alone;
		// Options.Clone() would never clone those objects.
		if v.Elem().CanSet() && v.Elem().Type().PkgPath() == reflect.TypeOf(pebble.Options{}).PkgPath() {
			mangle(v.Elem(), rng)
		}

	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			mangle(v.Field(i), rng)
		}

	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			mangle(v.Index(i), rng)
		}
	}
}

func genVal(v reflect.Value, rng *rand.Rand) {
	defer func() {
		if r := recover(); r != nil {
			// Ignore errors generating values (caused by unexported fields).
		}
	}()
	newVal, ok := quick.Value(v.Type(), randv1.New(randv1.NewSource(rng.Int64())))
	if ok {
		v.Set(newVal)
	}
}
