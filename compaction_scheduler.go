// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"sync"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
)

type CompactionGrantHandle = base.CompactionGrantHandle
type CompactionGrantHandleStats = base.CompactionGrantHandleStats
type CompactionGoroutineKind = base.CompactionGoroutineKind

const (
	CompactionGoroutinePrimary           = base.CompactionGoroutinePrimary
	CompactionGoroutineSSTableSecondary  = base.CompactionGoroutineSSTableSecondary
	CompactionGoroutineBlobFileSecondary = base.CompactionGoroutineBlobFileSecondary
)

// NB: This interface is experimental and subject to change.
//
// For instance, we may incorporate more information in TrySchedule and in the
// return value of Schedule to tell CompactionScheduler of the sub-category of
// compaction so that the scheduler can have more granular estimates. For
// example, the input or output level could affect the write bandwidth if the
// inputs are better cached (say at higher levels).

// CompactionScheduler is responsible for scheduling both automatic and manual
// compactions. In the case of multiple DB instances on a node (i.e. a
// multi-store configuration), implementations of CompactionScheduler may
// enforce a global maximum compaction concurrency. Additionally,
// implementations of CompactionScheduler may be resource aware and permit
// more than the compactions that are "allowed without permission" if
// resources are available.
//
// Locking: CompactionScheduler's mutexes are ordered after DBForCompaction
// mutexes. We need to specify some lock ordering since CompactionScheduler
// and DBForCompaction call into each other. This ordering choice is made to
// simplify the implementation of DBForCompaction. There are three exceptions
// to this DBForCompaction.GetAllowedWithoutPermission,
// CompactionScheduler.Unregister, CompactionGrantHandle.Done -- see those
// declarations for details.
type CompactionScheduler interface {
	// Register is called to register this DB and to specify the number of
	// goroutines that consume CPU in each compaction (see the CPU reporting
	// interface, CompactionGrantHandle.MeasureCPU). Must be called exactly once
	// by this DB if it successfully opens.
	Register(numGoroutinesPerCompaction int, db DBForCompaction)
	// Unregister is used to unregister the DB. Must be called once when the DB
	// is being closed. Unregister waits until all ongoing calls to
	// DBForCompaction are finished, so Unregister must not be called while
	// holding locks that DBForCompaction acquires in those calls.
	Unregister()
	// TrySchedule is called by DB when it wants to run a compaction. The bool
	// is true iff permission is granted, and in that case the
	// CompactionGrantHandle needs to be exercised by the DB.
	TrySchedule() (bool, CompactionGrantHandle)
	// UpdateGetAllowedWithoutPermission is to inform the scheduler that some
	// external behavior may have caused this value to change. It exists because
	// flushes are not otherwise visible to the CompactionScheduler, and can
	// cause the value to increase. CompactionScheduler implementation should do
	// periodic sampling (e.g. as done by
	// ConcurrencyLimitScheduler.periodicGranter), but this provides an
	// instantaneous opportunity to act.
	UpdateGetAllowedWithoutPermission()
}

// DBForCompaction is the interface implemented by the DB to interact with the
// CompactionScheduler.
type DBForCompaction interface {
	// GetAllowedWithoutPermission returns what is permitted at the DB-level
	// (there may be further restrictions at the node level, when there are
	// multiple DBs at a node, which is not captured by this number). This can
	// vary based on compaction backlog or other factors. This method must not
	// acquire any mutex in DBForCompaction that is covered by the general mutex
	// ordering rule stated earlier.
	GetAllowedWithoutPermission() int
	// GetWaitingCompaction returns true iff the DB can run a compaction. The
	// true return is accompanied by a populated WaitingForCompaction, that the
	// scheduler can use to pick across DBs or other work in the system. This
	// method should typically be efficient, in that the DB should try to cache
	// some state if its previous call to TrySchedule resulted in a failure to
	// get permission. It is ok if it is sometimes slow since all work scheduled
	// by CompactionScheduler is long-lived (often executing for multiple
	// seconds).
	GetWaitingCompaction() (bool, WaitingCompaction)
	// Schedule grants the DB permission to run a compaction. The DB returns
	// true iff it accepts the grant, in which case it must exercise the
	// CompactionGrantHandle.
	Schedule(CompactionGrantHandle) bool
}

// WaitingCompaction captures state for a compaction that can be used to
// prioritize wrt compactions in other DBs or other long-lived work in the
// system.
type WaitingCompaction struct {
	// Optional is true for a compaction that isn't necessary for maintaining an
	// overall healthy LSM. This value can be compared across compactions and
	// other long-lived work.
	Optional bool
	// Priority is the priority of a compaction. It is only compared across
	// compactions, and when the Optional value is the same.
	Priority int
	// Score is only compared across compactions. It is only compared across
	// compactions, and when the Optional and Priority are the same.
	Score float64
}

// Ordering is by priority and if the optional value is different, false is
// more important than true.
//
// The ordering here must be consistent with the order in which compactions
// are picked in compactionPickerByScore.pickAuto.
type compactionOptionalAndPriority struct {
	optional bool
	priority int
}

var scheduledCompactionMap map[compactionKind]compactionOptionalAndPriority
var manualCompactionPriority int

func init() {
	// Manual compactions have priority just below the score-rebased
	// compactions, since DB.pickAnyCompaction first picks score-based
	// compactions, and then manual compactions.
	manualCompactionPriority = 70
	scheduledCompactionMap = map[compactionKind]compactionOptionalAndPriority{}
	// Score-based-compactions have priorities {100, 90, 80}.
	//
	// We don't actually know if it is a compactionKindMove or
	// compactionKindCopy until a compactionKindDefault is turned from a
	// pickedCompaction into a compaction struct. So we will never see those
	// values here, but for completeness we include them.
	scheduledCompactionMap[compactionKindMove] = compactionOptionalAndPriority{priority: 100}
	scheduledCompactionMap[compactionKindCopy] = compactionOptionalAndPriority{priority: 90}
	scheduledCompactionMap[compactionKindDefault] = compactionOptionalAndPriority{priority: 80}
	scheduledCompactionMap[compactionKindTombstoneDensity] =
		compactionOptionalAndPriority{optional: true, priority: 60}
	scheduledCompactionMap[compactionKindElisionOnly] =
		compactionOptionalAndPriority{optional: true, priority: 50}
	scheduledCompactionMap[compactionKindBlobFileRewrite] =
		compactionOptionalAndPriority{optional: true, priority: 40}
	scheduledCompactionMap[compactionKindRead] =
		compactionOptionalAndPriority{optional: true, priority: 30}
	scheduledCompactionMap[compactionKindRewrite] =
		compactionOptionalAndPriority{optional: true, priority: 20}
}

// noopGrantHandle is used in cases that don't interact with a CompactionScheduler.
type noopGrantHandle struct{}

var _ CompactionGrantHandle = noopGrantHandle{}

func (h noopGrantHandle) Started()                                              {}
func (h noopGrantHandle) MeasureCPU(CompactionGoroutineKind)                    {}
func (h noopGrantHandle) CumulativeStats(stats base.CompactionGrantHandleStats) {}
func (h noopGrantHandle) Done()                                                 {}

// pickedCompactionCache is used to avoid the work of repeatedly picking a
// compaction that then fails to run immediately because TrySchedule returns
// false.
//
// The high-level approach is to construct a pickedCompaction in
// DB.maybeScheduleCompaction if there isn't one in the cache, and if
// TrySchedule returns false, to remember it. Ignoring flushes, the worst-case
// behavior is 1 of 2 pickedCompactions gets to run (so half the picking work
// is wasted). This worst-case happens when the system is running at the limit
// of the long-lived work (including compactions) it can support. In this
// setting, each started compaction invalidates the pickedCompaction in the
// cache when it completes, and the reason the cache has a pickedCompaction
// (that got invalidated) is that the CompactionScheduler called
// GetWaitingCompaction and decided not to run the pickedCompaction (some
// other work won). We consider the CPU overhead of this waste acceptable.
//
// For the default case of a ConcurrencyLimitScheduler, which only considers a
// single DB, the aforementioned worst-case is avoided by not constructing a
// new pickedCompaction in DB.maybeScheduleCompaction when
// pickedCompactionCache.isWaiting is already true (which became true once,
// when a backlog developed). Whenever a compaction completes and a new
// compaction can be started, the call to DBForCompaction.GetWaitingCompaction
// constructs a new pickedCompaction and caches it, and then this immediately
// gets to run when DBForCompaction.Schedule is called.
type pickedCompactionCache struct {
	// pc != nil => waiting.
	//
	// It is acceptable for waiting to be true and pc to be nil, when pc is
	// invalidated due to starting a compaction, or completing a
	// compaction/flush (since it changes the latest version).
	waiting bool
	pc      pickedCompaction
}

// invalidate the cache because a new Version is installed or a compaction is
// started (since a new in-progress compaction affects future compaction
// picking). The value of waiting is not changed.
func (c *pickedCompactionCache) invalidate() {
	c.pc = nil
}

// isWaiting returns the value of waiting.
func (c *pickedCompactionCache) isWaiting() bool {
	return c.waiting
}

// getForRunning returns a pickedCompaction if in the cache. The cache is
// cleared. It may return nil.
func (c *pickedCompactionCache) getForRunning() pickedCompaction {
	// NB: This does not set c.waiting = false, since there may be more
	// compactions to run.
	pc := c.pc
	c.pc = nil
	return pc
}

// setNotWaiting sets waiting to false.
func (c *pickedCompactionCache) setNotWaiting() {
	c.waiting = false
	c.pc = nil
}

// peek return the pickedCompaction, if any, in the cache.
func (c *pickedCompactionCache) peek() pickedCompaction {
	return c.pc
}

// add adds a pickedCompaction to the cache and sets waiting to true.
func (c *pickedCompactionCache) add(pc pickedCompaction) {
	c.waiting = true
	c.pc = pc
}

// ConcurrencyLimitScheduler is the default scheduler used by Pebble. It
// simply uses the concurrency limit retrieved from
// DBForCompaction.GetAllowedWithoutPermission to decide the number of
// compactions to schedule. ConcurrencyLimitScheduler must have its Register
// method called at most once -- i.e., it cannot be reused across DBs.
//
// Since the GetAllowedWithoutPermission value changes over time, the
// scheduler needs to be quite current in its sampling, especially if the
// value is increasing, to prevent lag in scheduling compactions. Calls to
// ConcurrencyLimitScheduler.Done and ConcurrencyLimitScheduler.TrySchedule
// are obvious places this value is sampled. However, since
// ConcurrencyLimitScheduler does not observe flushes (which can increase the
// value), and there can be situations where compactions last 10+ seconds,
// this sampling is not considered sufficient. Note that calls to
// ConcurrencyLimitScheduler.TrySchedule are dampened in
// DB.maybeScheduleCompaction when there is a waiting compaction (to prevent
// wasted computation of pickedCompaction). If DB.maybeScheduleCompaction
// always called ConcurrencyLimitScheduler.TrySchedule we would have no lag as
// DB.maybeScheduleCompaction is called on flush completion. Hence, we resort
// to having a background thread in ConcurrencyLimitScheduler sample the value
// every 100ms, plus sample in UpdateGetAllowedWithoutPermission.
type ConcurrencyLimitScheduler struct {
	ts schedulerTimeSource
	// db is set in Register, but not protected by mu since it is strictly
	// before any calls to the other methods.
	db DBForCompaction
	mu struct {
		sync.Mutex
		runningCompactions int
		// unregistered transitions once from false => true.
		unregistered bool
		// isGranting is used to (a) serialize granting from Done and
		// periodicGranter, (b) ensure that granting is stopped before returning
		// from Unregister.
		isGranting                   bool
		isGrantingCond               *sync.Cond
		lastAllowedWithoutPermission int
	}
	stopPeriodicGranterCh chan struct{}
	pokePeriodicGranterCh chan struct{}
	// Only non-nil in some tests.
	periodicGranterRanChForTesting chan struct{}
}

var _ CompactionScheduler = &ConcurrencyLimitScheduler{}

func newConcurrencyLimitScheduler(ts schedulerTimeSource) *ConcurrencyLimitScheduler {
	s := &ConcurrencyLimitScheduler{
		ts:                    ts,
		stopPeriodicGranterCh: make(chan struct{}),
		pokePeriodicGranterCh: make(chan struct{}, 1),
	}
	s.mu.isGrantingCond = sync.NewCond(&s.mu.Mutex)
	return s
}

func NewConcurrencyLimitSchedulerWithNoPeriodicGrantingForTest() *ConcurrencyLimitScheduler {
	s := &ConcurrencyLimitScheduler{
		ts: defaultTimeSource{},
	}
	s.mu.isGrantingCond = sync.NewCond(&s.mu.Mutex)
	return s
}

func (s *ConcurrencyLimitScheduler) Register(numGoroutinesPerCompaction int, db DBForCompaction) {
	s.db = db
	if s.stopPeriodicGranterCh != nil {
		go s.periodicGranter()
	}
}

func (s *ConcurrencyLimitScheduler) Unregister() {
	if s.stopPeriodicGranterCh != nil {
		s.stopPeriodicGranterCh <- struct{}{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.unregistered = true
	// Wait until isGranting becomes false. Since unregistered has been set to
	// true, once isGranting becomes false, no more granting will happen.
	for s.mu.isGranting {
		s.mu.isGrantingCond.Wait()
	}
}

func (s *ConcurrencyLimitScheduler) TrySchedule() (bool, CompactionGrantHandle) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.unregistered {
		return false, nil
	}
	s.mu.lastAllowedWithoutPermission = s.db.GetAllowedWithoutPermission()
	if s.mu.lastAllowedWithoutPermission > s.mu.runningCompactions {
		s.mu.runningCompactions++
		return true, s
	}
	return false, nil
}

func (s *ConcurrencyLimitScheduler) Started()                                              {}
func (s *ConcurrencyLimitScheduler) MeasureCPU(CompactionGoroutineKind)                    {}
func (s *ConcurrencyLimitScheduler) CumulativeStats(stats base.CompactionGrantHandleStats) {}

func (s *ConcurrencyLimitScheduler) Done() {
	s.mu.Lock()
	s.mu.runningCompactions--
	s.tryGrantLockedAndUnlock()
}

func (s *ConcurrencyLimitScheduler) UpdateGetAllowedWithoutPermission() {
	s.mu.Lock()
	allowedWithoutPermission := s.db.GetAllowedWithoutPermission()
	tryGrant := allowedWithoutPermission > s.mu.lastAllowedWithoutPermission
	s.mu.lastAllowedWithoutPermission = allowedWithoutPermission
	s.mu.Unlock()
	if tryGrant {
		select {
		case s.pokePeriodicGranterCh <- struct{}{}:
		default:
		}
	}
}

func (s *ConcurrencyLimitScheduler) tryGrantLockedAndUnlock() {
	defer s.mu.Unlock()
	if s.mu.unregistered {
		return
	}
	// Wait for turn to grant.
	for s.mu.isGranting {
		s.mu.isGrantingCond.Wait()
	}
	// INVARIANT: !isGranting.
	if s.mu.unregistered {
		return
	}
	s.mu.lastAllowedWithoutPermission = s.db.GetAllowedWithoutPermission()
	toGrant := s.mu.lastAllowedWithoutPermission - s.mu.runningCompactions
	if toGrant > 0 {
		s.mu.isGranting = true
	} else {
		return
	}
	s.mu.Unlock()
	// We call GetWaitingCompaction iff we can successfully grant, so that there
	// is no wasted pickedCompaction.
	//
	// INVARIANT: loop exits with s.mu unlocked.
	for toGrant > 0 {
		waiting, _ := s.db.GetWaitingCompaction()
		if !waiting {
			break
		}
		accepted := s.db.Schedule(s)
		if !accepted {
			break
		}
		s.mu.Lock()
		s.mu.runningCompactions++
		toGrant--
		s.mu.Unlock()
	}
	// Will be unlocked by the defer statement.
	s.mu.Lock()
	s.mu.isGranting = false
	s.mu.isGrantingCond.Broadcast()
}

func (s *ConcurrencyLimitScheduler) periodicGranter() {
	ticker := s.ts.newTicker(100 * time.Millisecond)
	for {
		select {
		case <-ticker.ch():
			s.mu.Lock()
			s.tryGrantLockedAndUnlock()
		case <-s.pokePeriodicGranterCh:
			s.mu.Lock()
			s.tryGrantLockedAndUnlock()
		case <-s.stopPeriodicGranterCh:
			ticker.stop()
			return
		}
		if s.periodicGranterRanChForTesting != nil {
			s.periodicGranterRanChForTesting <- struct{}{}
		}
	}
}

func (s *ConcurrencyLimitScheduler) adjustRunningCompactionsForTesting(delta int) {
	s.mu.Lock()
	s.mu.runningCompactions += delta
	if delta < 0 {
		s.tryGrantLockedAndUnlock()
	} else {
		s.mu.Unlock()
	}
}

// TriggerGrantingForTest explicitly triggers the granting mechanism. This is
// needed for tests that disable periodic granting but still need cached
// compactions to run when TrySchedule returns false.
func (s *ConcurrencyLimitScheduler) TriggerGrantingForTest() {
	s.mu.Lock()
	s.tryGrantLockedAndUnlock() // This unlocks s.mu
}

func (s *ConcurrencyLimitScheduler) isUnregisteredForTesting() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.unregistered
}

// schedulerTimeSource is used to abstract time.NewTicker for
// ConcurrencyLimitScheduler.
type schedulerTimeSource interface {
	newTicker(duration time.Duration) schedulerTicker
}

// schedulerTicker is used to abstract time.Ticker for
// ConcurrencyLimitScheduler.
type schedulerTicker interface {
	stop()
	ch() <-chan time.Time
}

// defaultTime is a schedulerTimeSource using the time package.
type defaultTimeSource struct{}

var _ schedulerTimeSource = defaultTimeSource{}

func (defaultTimeSource) newTicker(duration time.Duration) schedulerTicker {
	return (*defaultTicker)(time.NewTicker(duration))
}

// defaultTicker uses time.Ticker.
type defaultTicker time.Ticker

var _ schedulerTicker = &defaultTicker{}

func (t *defaultTicker) stop() {
	(*time.Ticker)(t).Stop()
}

func (t *defaultTicker) ch() <-chan time.Time {
	return (*time.Ticker)(t).C
}
