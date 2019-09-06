// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"hash"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/internal/randvar"
	"github.com/cockroachdb/pebble/internal/rate"
	"golang.org/x/exp/rand"
)

var randVarRE = regexp.MustCompile(`^(?:(uniform|zipf):)?(\d+)(?:-(\d+))?$`)

func newFluctuatingRateLimiter(maxOpsPerSec string) (*rate.Limiter, error) {
	rateDist, fluctuateDuration, err := parseRateSpec(maxOpsPerSec)
	if err != nil {
		return nil, err
	}
	limiter := rate.NewLimiter(rate.Limit(rateDist.Uint64()), 1)
	if fluctuateDuration != 0 {
		go func(limiter *rate.Limiter) {
			ticker := time.NewTicker(fluctuateDuration)
			for i := 0; ; i++ {
				select {
				case <-ticker.C:
					limiter.SetLimit(rate.Limit(rateDist.Uint64()))
				}
			}
		}(limiter)
	}
	return limiter, nil
}

func parseRandVarSpec(d string) (randvar.Static, error) {
	m := randVarRE.FindStringSubmatch(d)
	if m == nil {
		return nil, fmt.Errorf("invalid random var spec: %s", d)
	}

	min, err := strconv.Atoi(m[2])
	if err != nil {
		return nil, err
	}
	max := min
	if m[3] != "" {
		max, err = strconv.Atoi(m[3])
		if err != nil {
			return nil, err
		}
	}

	switch strings.ToLower(m[1]) {
	case "", "uniform":
		return randvar.NewUniform(nil, uint64(min), uint64(max)), nil
	case "zipf":
		return randvar.NewZipf(nil, uint64(min), uint64(max), 0.99)
	default:
		return nil, fmt.Errorf("unknown distribution: %s", m[1])
	}
}

func parseRateSpec(v string) (randvar.Static, time.Duration, error) {
	parts := strings.Split(v, "/")
	if len(parts) == 0 || len(parts) > 2 {
		return nil, 0, fmt.Errorf("invalid max-ops-per-sec spec: %s", v)
	}
	r, err := parseRandVarSpec(parts[0])
	if err != nil {
		return nil, 0, err
	}
	// Don't fluctuate by default.
	fluctuateDuration := time.Duration(0)
	if len(parts) == 2 {
		fluctuateDurationFloat, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			return nil, 0, err
		}
		fluctuateDuration = time.Duration(fluctuateDurationFloat) * time.Second
	}
	return r, fluctuateDuration, nil
}

func parseValuesSpec(v string) (randvar.Static, float64, error) {
	parts := strings.Split(v, "/")
	if len(parts) == 0 || len(parts) > 2 {
		return nil, 0, fmt.Errorf("invalid values spec: %s", v)
	}
	r, err := parseRandVarSpec(parts[0])
	if err != nil {
		return nil, 0, err
	}
	targetCompression := 1.0
	if len(parts) == 2 {
		targetCompression, err = strconv.ParseFloat(parts[1], 64)
		if err != nil {
			return nil, 0, err
		}
	}
	return r, targetCompression, nil
}

func randomBlock(
	r *rand.Rand, size int, targetCompressionRatio float64,
) []byte {
	uniqueSize := int(float64(size) / targetCompressionRatio)
	if uniqueSize < 1 {
		uniqueSize = 1
	}
	data := make([]byte, size)
	offset := 0
	for offset+8 <= uniqueSize {
		binary.LittleEndian.PutUint64(data[offset:], r.Uint64())
		offset += 8
	}
	word := r.Uint64()
	for offset < uniqueSize {
		data[offset] = byte(word)
		word >>= 8
		offset++
	}
	for offset < size {
		data[offset] = data[offset-uniqueSize]
		offset++
	}
	return data
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
