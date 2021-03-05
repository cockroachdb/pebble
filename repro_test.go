package pebble

import (
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"testing"
	"time"
)

func TestWithFinalizer(t *testing.T) {
	testFoo(t, true)
}

func TestWithoutFinalizer(t *testing.T) {
	testFoo(t, false)
}

func testFoo(t *testing.T, finalizer bool) {
	// Try our best to release what we can.
	debug.SetGCPercent(10) // default is 100, so this makes it more aggressive
	for i := 0; i < 5; i++ {
		debug.FreeOSMemory()
		runtime.GC()
	}

	// Write heap profile after cleanup.
	f, err := os.Create("heap.pprof")
	if err != nil {
		t.Fatal(err)
	}
	require.NoError(t, pprof.WriteHeapProfile(f))
	require.NoError(t, f.Close())

	// Do the actual work in a goroutine to make sure the restriction for
	// long-running goroutines described here does not apply to `-count N`:
	//
	// https://golang.org/doc/articles/race_detector#Runtime_Overheads
	ch := make(chan struct{})
	go func() {
		defer close(ch)

		var opts Options
		opts.EnsureDefaults()
		opts.FS = vfs.NewMem()
		opts.NoFinalizer = !finalizer
		db, err := Open("foo", &opts)
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			panic(err)
		}
	}()
	<-ch

	// Cheap version of leaktest; if we leak goroutines they
	// can reference memory.
	for i := 0; i < 2; i++ {
		if ng := runtime.NumGoroutine(); ng > 10 {
			if i == 0 {
				time.Sleep(time.Second)
			} else {
				t.Fatal(ng)
			}
		}
	}
}
