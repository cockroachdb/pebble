package errorfs

import (
	"encoding/binary"
	"fmt"
	"go/token"
	"hash/maphash"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2/internal/dsl"
)

// RandomLatency constructs an Injector that does not inject errors but instead
// injects random latency into operations that match the provided predicate. The
// amount of latency injected follows an exponential distribution with the
// provided mean. Latency injected is derived from the provided seed and is
// deterministic with respect to each file's path.
//
// If limit is nonzero, total latency injected over the lifetime of the Injector
// is capped to limit.
func RandomLatency(pred Predicate, mean time.Duration, seed int64, limit time.Duration) Injector {
	rl := &randomLatency{
		predicate: pred,
		mean:      mean,
		limit:     limit,
	}
	rl.keyedPrng.init(seed)
	return rl
}

func parseRandomLatency(p *Parser, s *dsl.Scanner) Injector {
	dur, err := time.ParseDuration(s.ConsumeString())
	if err != nil {
		panic(errors.Newf("parsing RandomLatency: %s", err))
	}
	lit := s.Consume(token.INT).Lit
	seed, err := strconv.ParseInt(lit, 10, 64)
	if err != nil {
		panic(err)
	}
	var pred Predicate
	tok := s.Scan()
	if tok.Kind == token.LPAREN || tok.Kind == token.IDENT {
		pred = p.predicates.ParseFromPos(s, tok)
		tok = s.Scan()
	}
	if tok.Kind != token.RPAREN {
		panic(errors.Errorf("errorfs: unexpected token %s; expected %s", tok.String(), token.RPAREN))
	}
	return RandomLatency(pred, dur, seed, 0 /* no limit */)
}

type randomLatency struct {
	predicate Predicate
	// mean is the mean duration injected each operation.
	mean time.Duration
	// limit configures a limit on total latency injected over the lifetime of
	// the Injector if nonzero.
	limit time.Duration
	// agg is the aggregate latency injected over the lifetime of the Injector.
	agg atomic.Int64
	keyedPrng
}

func (rl *randomLatency) String() string {
	if rl.predicate == nil {
		return fmt.Sprintf("(RandomLatency %q %d)", rl.mean, rl.rootSeed)
	}
	return fmt.Sprintf("(RandomLatency %q %d %s)", rl.mean, rl.rootSeed, rl.predicate)
}

func (rl *randomLatency) MaybeError(op Op) error {
	if rl.predicate != nil && !rl.predicate.Evaluate(op) {
		return nil
	}
	var dur time.Duration
	rl.keyedPrng.withKey(op.Path, func(prng *rand.Rand) {
		// We cap the max latency to 100x: Otherwise, it seems possible
		// (although very unlikely) ExpFloat64 generates a multiplier high
		// enough that causes a test timeout.
		dur = time.Duration(min(prng.ExpFloat64(), 20.0) * float64(rl.mean))
	})

	// Apply a limit on total latency injected over the lifetime of the
	// Injector, if one is configured.
	if rl.limit > 0 {
		if v := time.Duration(rl.agg.Add(int64(dur))); v-dur > rl.limit {
			// We'd already exceeded the limit before adding dur. Don't inject
			// anything.
			return nil
		} else if v > rl.limit {
			// We're about to exceed the limit. Cap the duration.
			dur -= v - rl.limit
		}
	}

	time.Sleep(dur)
	return nil
}

// keyedPrng maintains a separate prng per-key that's deterministic with
// respect to the key: its behavior for a particular key is deterministic
// regardless of intervening evaluations for operations on other keys. This can
// be used to ensure determinism despite nondeterministic concurrency if the
// concurrency is constrained to separate keys.
type keyedPrng struct {
	rootSeed int64
	mu       struct {
		sync.Mutex
		h           maphash.Hash
		perFilePrng map[string]*rand.Rand
	}
}

func (p *keyedPrng) init(rootSeed int64) {
	p.rootSeed = rootSeed
	p.mu.perFilePrng = make(map[string]*rand.Rand)
}

func (p *keyedPrng) withKey(key string, fn func(*rand.Rand)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	prng, ok := p.mu.perFilePrng[key]
	if !ok {
		// This is the first time an operation has been performed on the key.
		// Initialize the per-key prng by computing a deterministic hash of the
		// key.
		p.mu.h.Reset()
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], uint64(p.rootSeed))
		if _, err := p.mu.h.Write(b[:]); err != nil {
			panic(err)
		}
		if _, err := p.mu.h.WriteString(key); err != nil {
			panic(err)
		}
		seed := p.mu.h.Sum64()
		prng = rand.New(rand.NewPCG(0, seed))
		p.mu.perFilePrng[key] = prng
	}
	fn(prng)
}
