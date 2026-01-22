// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"sync"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/compression"
	"github.com/cockroachdb/pebble/internal/manifest"
)

// spanPolicyEnforcer is a background process that periodically scans the LSM to
// detect span policy violations and marks those files for compaction.
//
// Scan rate is determined by checkInterval. Scanning pauses when there are
// pending files violating policies marked for compaction.
type spanPolicyEnforcer struct {
	db  *DB
	cmp base.Compare

	// checkInterval is the time between file checks.
	checkInterval time.Duration

	// cursor tracks the current position in the LSM scan.
	cursor manifest.ScanCursor

	// stopCh is closed to signal the background goroutine to stop.
	stopCh chan struct{}
	// wg is used to wait for the background goroutine to finish.
	wg sync.WaitGroup
}

// SpanPolicyEnforcerOptions contains options for the policy enforcer.
type SpanPolicyEnforcerOptions struct {
	// CheckInterval is the time between file checks.
	//
	// Default: 1 second.
	CheckInterval time.Duration
}

// DefaultPolicyEnforcerOptions returns the default options.
func DefaultPolicyEnforcerOptions() SpanPolicyEnforcerOptions {
	return SpanPolicyEnforcerOptions{
		CheckInterval: time.Second,
	}
}

// newSpanPolicyEnforcer creates a new policy enforcer.
func newSpanPolicyEnforcer(db *DB, opts SpanPolicyEnforcerOptions) *spanPolicyEnforcer {
	if opts.CheckInterval <= 0 {
		opts.CheckInterval = time.Second
	}
	enforcer := &spanPolicyEnforcer{
		db:            db,
		cmp:           db.cmp,
		checkInterval: opts.CheckInterval,
		cursor:        manifest.ScanCursor{Level: 0},
		stopCh:        make(chan struct{}),
	}
	return enforcer
}

// Start begins the background policy enforcement goroutine.
func (s *spanPolicyEnforcer) Start() {
	s.wg.Add(1)
	go s.run()
}

// Stop stops the background policy enforcement goroutine and waits for it to
// finish.
func (s *spanPolicyEnforcer) Stop() {
	close(s.stopCh)
	// Broadcast in case the goroutine is waiting on db.mu.compact.cond.
	s.db.mu.Lock()
	s.db.mu.compact.cond.Broadcast()
	s.db.mu.Unlock()
	s.wg.Wait()
}

// isStopped returns true if Stop has been called.
func (s *spanPolicyEnforcer) isStopped() bool {
	select {
	case <-s.stopCh:
		return true
	default:
		return false
	}
}

// run is the main loop for the policy enforcer.
func (s *spanPolicyEnforcer) run() {
	defer s.wg.Done()
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		// Sleep to maintain the target rate.
		if !s.waitForInterval(timer, s.checkInterval) {
			return
		}

		nextFile, level, endOfScan := s.getNextFile(true /* waitForPendingWork */)
		if endOfScan {
			continue
		}
		if nextFile == nil {
			// File was compacting, skip it.
			continue
		}

		// Check if this file violates any policy.
		if s.checkPolicyViolation(nextFile) {
			s.markForEnforcement(nextFile, level)
		}
	}
}

// scanAll performs a single full scan of the LSM, checking all files for
// policy violations.
func (s *spanPolicyEnforcer) scanAll() {
	for {
		nextFile, level, endOfScan := s.getNextFile(false /* waitForPendingWork */)
		if endOfScan {
			return
		}
		if nextFile == nil {
			continue
		}
		if s.checkPolicyViolation(nextFile) {
			s.markForEnforcement(nextFile, level)
		}
	}
}

// getNextFile finds the file to check for policy violations, advances the cursor past
// the file, and returns the file. If waitForPendingWork is true, it blocks until there
// are no files marked for policy enforcement waiting to be compacted.
func (s *spanPolicyEnforcer) getNextFile(
	waitForPendingWork bool,
) (f *manifest.TableMetadata, level int, endOfScan bool) {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()

	if waitForPendingWork {
		// Wait until there's no pending compaction work from the enforcer
		// to prevent us from queuing up too much work at one time.
		for s.db.mu.compact.spanPolicyEnforcementFiles.Count() > 0 && !s.isStopped() {
			s.db.mu.compact.cond.Wait()
		}
		// Check if we're shutting down.
		if s.isStopped() {
			return nil, 0, true
		}
	}

	vers := s.db.mu.versions.currentVersion()
	nextFile, level := s.cursor.NextFile(s.cmp, vers)
	if nextFile == nil {
		// Reached end of scan. Reset cursor.
		s.cursor = manifest.ScanCursor{Level: 0}
		return nil, 0, true
	}

	// Advance cursor past this file.
	s.cursor = manifest.MakeScanCursorAfterFile(nextFile, level)

	// If the file is already compacting, skip it.
	if nextFile.IsCompacting() {
		return nil, 0, false
	}

	return nextFile, level, false
}

// waitForInterval waits for the interval or returns false if stopCh is closed.
func (s *spanPolicyEnforcer) waitForInterval(timer *time.Timer, interval time.Duration) bool {
	timer.Reset(interval)
	select {
	case <-s.stopCh:
		return false
	case <-timer.C:
		return true
	}
}

// checkPolicyViolation checks if a file violates any span policies across its
// key range.
func (s *spanPolicyEnforcer) checkPolicyViolation(f *manifest.TableMetadata) bool {
	// SpanPolicyFunc may be nil if not configured.
	if s.db.opts.Experimental.SpanPolicyFunc == nil {
		return false
	}

	props, ok := f.TableBacking.Properties()
	if !ok {
		// Properties not yet populated; skip this file for now.
		return false
	}

	// Get the file's key bounds and iterate over all policies that apply.
	// In practice, there should only be one policy that applies to a file.
	bounds := f.UserKeyBounds()
	for {
		policy, err := s.db.opts.Experimental.SpanPolicyFunc(bounds)
		if err != nil {
			// On error, skip further policy checks for this file.
			s.db.opts.Logger.Errorf(
				"policy enforcer: span policy lookup failed for %s: %v",
				f.TableNum,
				err,
			)
			break
		}

		// Check compression policy for this span.
		if s.checkCompressionViolation(props, policy) {
			return true
		}

		// TODO(xinhaoz): Check other policy violations here (tiering, file size).

		if len(policy.KeyRange.End) == 0 {
			// Policy extends to the end of the keyspace.
			break
		}
		cmpResult := s.cmp(bounds.End.Key, policy.KeyRange.End)
		if cmpResult < 0 || (cmpResult == 0 && bounds.End.Kind == base.Exclusive) {
			// Policy covers the key range.
			break
		}
		bounds.Start = policy.KeyRange.End
	}

	return false
}

// checkCompressionViolation checks if a file violates the compression policy.
// When PreferFastCompression is true, the file should only use the designated
// fast compression algorithms (Snappy or MinLZ) or no compression. Using Zstd
// or other slower compression algorithms is a violation.
func (s *spanPolicyEnforcer) checkCompressionViolation(
	props *manifest.TableBackingProperties, policy base.SpanPolicy,
) bool {
	if !policy.PreferFastCompression {
		return false
	}

	// If using PreferFastCompression, the file should only use fast compression
	// for all blocks.
	// Check compression settings used in this file. A violation occurs if
	// any blocks use a non-fast compression algorithm. Fast compression means
	// Snappy or MinLZ; Zstd is considered slow.
	for setting := range props.CompressionStats.All() {
		switch setting.Algorithm {
		case compression.NoAlgorithm, compression.Snappy, compression.MinLZ:
			continue
		default:
			// Unknown or other algorithms are also considered violations.
			return true
		}
	}
	return false
}

// markForEnforcement marks a file for policy enforcement compaction.
func (s *spanPolicyEnforcer) markForEnforcement(f *manifest.TableMetadata, level int) {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()

	vers := s.db.mu.versions.currentVersion()

	// Verify the file still exists at this level in the current version.
	// The file may have been compacted away between detection and marking.
	if !vers.Contains(level, f) {
		return
	}

	// Check if file is already marked.
	if s.db.mu.compact.spanPolicyEnforcementFiles.Contains(f, level) {
		return
	}

	// Mark the file for policy enforcement.
	s.db.mu.compact.spanPolicyEnforcementFiles.Insert(f, level)

	// Trigger compaction scheduling.
	s.db.maybeScheduleCompaction()
}
