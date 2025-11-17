// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package bytesprofile

import (
	"cmp"
	"fmt"
	"iter"
	"maps"
	"runtime"
	"slices"
	"strings"
	"sync"

	"github.com/cockroachdb/pebble/internal/humanize"
)

// Profile is a profile mapping stack traces to a cumulative count and byte sum.
type Profile struct {
	mu      sync.Mutex
	samples map[stack]aggSamples
}

// NewProfile creates a new profile.
func NewProfile() *Profile {
	return &Profile{samples: make(map[stack]aggSamples)}
}

type stack [30]uintptr

// trimmed returns the non-zero stack frames of stack.
func (s stack) trimmed() []uintptr {
	for i := range s {
		if s[i] == 0 {
			return s[:i]
		}
	}
	return s[:]
}

type aggSamples struct {
	bytes int64
	count int64
}

// Record records a sample of the given number of bytes with the calling stack trace.
func (p *Profile) Record(bytes int64) {
	var stack stack
	runtime.Callers(2, stack[:])
	p.mu.Lock()
	defer p.mu.Unlock()
	curr := p.samples[stack]
	curr.bytes += bytes
	curr.count++
	p.samples[stack] = curr
}

func (p *Profile) all() iter.Seq2[stack, aggSamples] {
	return func(yield func(stack, aggSamples) bool) {
		p.mu.Lock()
		defer p.mu.Unlock()
		// Sort the stacks by bytes in descending order.
		uniqueStacks := slices.SortedFunc(maps.Keys(p.samples), func(a, b stack) int {
			return -cmp.Compare(p.samples[a].bytes, p.samples[b].bytes)
		})
		for _, stack := range uniqueStacks {
			samples := p.samples[stack]
			if !yield(stack, samples) {
				break
			}
		}
	}
}

// TODO(jackson): We could add the ability to export the profile to a pprof file
// (which internally is just a protocol buffer). Ideally the Go standard library
// would provide facilities for this (e.g., golang/go#18454). The runtime/pprof
// library comes close with its definition of custom profiles, but they only
// support profiles tracking in-use resources.

// String returns a string representation of the stacks captured by the profile.
func (p *Profile) String() string {
	var sb strings.Builder
	var i int
	for stack, stats := range p.all() {
		if i > 0 {
			sb.WriteString("\n")
		}
		fmt.Fprintf(&sb, "%d: Count: %d (%s), Bytes: %d (%s)\n", i,
			stats.count, humanize.Count.Int64(stats.count),
			stats.bytes, humanize.Bytes.Int64(stats.bytes))
		frames := runtime.CallersFrames(stack.trimmed())
		for {
			frame, more := frames.Next()
			fmt.Fprintf(&sb, "  %s\n   %s:%d\n", frame.Function, frame.File, frame.Line)
			if !more {
				break
			}
		}
		i++
	}
	return sb.String()
}

// StackStats contains the stack trace and statistics for a given stack.
type StackStats struct {
	Stack string
	Bytes int64
	Count int64
}

// Collect returns a slice of StackStats, sorted by bytes in descending order.
func (p *Profile) Collect() []StackStats {
	var stats []StackStats
	var sb strings.Builder
	for stack, samples := range p.all() {
		sb.Reset()
		frames := runtime.CallersFrames(stack.trimmed())
		for {
			frame, more := frames.Next()
			fmt.Fprintf(&sb, "  %s\n   %s:%d", frame.Function, frame.File, frame.Line)
			if !more {
				break
			}
			sb.WriteString("\n")
		}

		stats = append(stats, StackStats{
			Stack: sb.String(),
			Bytes: samples.bytes,
			Count: samples.count,
		})
	}
	return stats
}
