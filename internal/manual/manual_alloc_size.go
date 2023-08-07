package manual

import "sync/atomic"

var manualAllocSize atomic.Int64

// AllocSize returns the size of memory that is currently manually
// allocated (and not managed by the Go runtime)
func AllocSize() uint64 {
	return uint64(manualAllocSize.Load())
}
