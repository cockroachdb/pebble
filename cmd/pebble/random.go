// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"hash"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/internal/randvar"
	"github.com/cockroachdb/pebble/internal/rate"
	"golang.org/x/exp/rand"
)

type rateFlag struct {
	randvar.Flag
	fluctuateDuration time.Duration
	spec              string
}

func newRateFlag(spec string) *rateFlag {
	f := &rateFlag{}
	if err := f.Set(spec); err != nil {
		panic(err)
	}
	return f
}

func (f *rateFlag) String() string {
	return f.spec
}

// Type implements the Flag.Value interface.
func (f *rateFlag) Type() string {
	return "ratevar"
}

// Set implements the Flag.Value interface.
func (f *rateFlag) Set(spec string) error {
	parts := strings.Split(spec, "/")
	if len(parts) == 0 || len(parts) > 2 {
		return fmt.Errorf("invalid ratevar spec: %s", spec)
	}
	if err := f.Flag.Set(parts[0]); err != nil {
		return err
	}
	// Don't fluctuate by default.
	f.fluctuateDuration = time.Duration(0)
	if len(parts) == 2 {
		fluctuateDurationFloat, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			return err
		}
		f.fluctuateDuration = time.Duration(fluctuateDurationFloat) * time.Second
	}
	f.spec = spec
	return nil
}

func (f *rateFlag) newRateLimiter() *rate.Limiter {
	limiter := rate.NewLimiter(rate.Limit(f.Uint64()), 1)
	if f.fluctuateDuration != 0 {
		go func(limiter *rate.Limiter) {
			ticker := time.NewTicker(f.fluctuateDuration)
			for range ticker.C {
				limiter.SetLimit(rate.Limit(f.Uint64()))
			}
		}(limiter)
	}
	return limiter
}

type sequence struct {
	val         int64
	cycleLength int64
	seed        int64
}

func (s *sequence) write() int64 {
	return (atomic.AddInt64(&s.val, 1) - 1) % s.cycleLength
}

// read returns the last key index that has been written. Note that the returned
// index might not actually have been written yet, so a read operation cannot
// require that the key is present.
func (s *sequence) read() int64 {
	return atomic.LoadInt64(&s.val) % s.cycleLength
}

// keyGenerator generates read and write keys. Read keys may not yet exist and
// write keys may already exist.
type keyGenerator interface {
	writeKey() int64
	readKey() int64
	rand() *rand.Rand
	sequence() int64
}

type hashGenerator struct {
	seq    *sequence
	random *rand.Rand
	hasher hash.Hash
	buf    [sha1.Size]byte
}

func newHashGenerator(seq *sequence) *hashGenerator {
	return &hashGenerator{
		seq:    seq,
		random: rand.New(rand.NewSource(uint64(time.Now().UnixNano()))),
		hasher: sha1.New(),
	}
}

func (g *hashGenerator) hash(v int64) int64 {
	binary.BigEndian.PutUint64(g.buf[:8], uint64(v))
	binary.BigEndian.PutUint64(g.buf[8:16], uint64(g.seq.seed))
	g.hasher.Reset()
	_, _ = g.hasher.Write(g.buf[:16])
	g.hasher.Sum(g.buf[:0])
	return int64(binary.BigEndian.Uint64(g.buf[:8]))
}

func (g *hashGenerator) writeKey() int64 {
	return g.hash(g.seq.write())
}

func (g *hashGenerator) readKey() int64 {
	v := g.seq.read()
	if v == 0 {
		return 0
	}
	return g.hash(g.random.Int63n(v))
}

func (g *hashGenerator) rand() *rand.Rand {
	return g.random
}

func (g *hashGenerator) sequence() int64 {
	return atomic.LoadInt64(&g.seq.val)
}

type sequentialGenerator struct {
	seq    *sequence
	random *rand.Rand
}

func newSequentialGenerator(seq *sequence) *sequentialGenerator {
	return &sequentialGenerator{
		seq:    seq,
		random: rand.New(rand.NewSource(uint64(time.Now().UnixNano()))),
	}
}

func (g *sequentialGenerator) writeKey() int64 {
	return g.seq.write()
}

func (g *sequentialGenerator) readKey() int64 {
	v := g.seq.read()
	if v == 0 {
		return 0
	}
	return g.random.Int63n(v)
}

func (g *sequentialGenerator) rand() *rand.Rand {
	return g.random
}

func (g *sequentialGenerator) sequence() int64 {
	return atomic.LoadInt64(&g.seq.val)
}
