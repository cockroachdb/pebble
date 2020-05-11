// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"errors"
	"os"

	errors2 "github.com/cockroachdb/pebble/errors"
	"github.com/cockroachdb/pebble/vfs"
)

// Reasons to mark the background error for.
type BackgroundErrorReason uint8

const (
	BgFlush BackgroundErrorReason = iota
	BgCompaction
	// Write/Sync to WAL.
	BgWrite
	// Write to memtable.
	BgMemtable
)

type Severity uint8

const (
	SeverityNoError            Severity = 0
	SeveritySoftError                   = 1
	SeverityHardError                   = 2
	SeverityFatalError                  = 3
	SeverityUnrecoverableError          = 4
)

// When user configured space limit in SstFileManager is reached.
// Only for compaction, flush.
var ErrSpaceLimit = errors.New("max allowed space was reached")

// TODO: Improve doc.
// *BackgroundError implements error. We provide the actual error as well as
// the severity for users to judge whether they want to reset or not. They can
// reset the error by setting Err to nil.
type BackgroundError struct {
	Err error
	Sev Severity
	Op  BackgroundErrorReason
}

func (b *BackgroundError) Error() string {
	if b.Err != nil {
		return b.Err.Error()
	}
	return ""
}

// Main aim of this is to record any error during a write operation
// and depending upon its severity, place the DB into read-only mode if needed.
// Write operations include writing to WAL, writing to memtable,
// memtable flush, compaction.

// TODO: Should we combine the methods this offers inside DB itself?
// Since we seem to do this. For example RocksDB's CompactionJob, FlushJob, IngestionJob etc
// are all methods defined over DB directly.

// TODO: Make sure that our db.mu protects the same state as required by RocksDB's mutex.
type errorHandler struct {
	db  *DB
	err *BackgroundError

	// A separate err used to record any errors during the
	// recovery process from hard errors.
	recoveryErr *BackgroundError

	// TODO: Define auto recovery.
	autoRecovery bool
	// Is recovery always auto?
	recoveryInProgress bool
}

// TODO: Document holding db.mu expectations for methods on errorHandler.

// TODO: Has DB been placed into read only mode?
func (h *errorHandler) isDBStopped() bool {
	return h.err != nil && h.err.Sev >= SeverityHardError
}

func (h *errorHandler) isBGWorkStopped() bool {
	// Bg work is stopped for SoftErrors as well if auto recovery is disabled.
	return h.err != nil && (h.err.Sev >= SeverityHardError || !h.autoRecovery)
}

func (h *errorHandler) isRecoveryInProgress() bool {
	return h.recoveryInProgress
}

// when is this called?
func (h *errorHandler) recoverFromBGError() {

}

// when is this called?
// Requires h.db.mu held.
func (h *errorHandler) clearBGError() error {
	// Signal that recovery succeeded.
	if h.recoveryErr == nil {
		err := h.err
		h.err = nil
		h.recoveryInProgress = false
		h.db.mu.Unlock()
		h.db.opts.EventListener.ErrorRecoveryCompleted(err)
		h.db.mu.Lock()
	}
	return h.recoveryErr
}

// Requires h.db.mu held.
func (h *errorHandler) cancelErrorRecovery() {
	h.autoRecovery = false
	if h.db.opts.SFM != nil {
		h.db.mu.Unlock()
		cancelled := h.db.opts.SFM.cancelErrorRecovery(h)
		h.db.mu.Lock()
		if cancelled {
			h.recoveryInProgress = false
		}
	}
}

func (h *errorHandler) getSeverity(err error, op BackgroundErrorReason) Severity {
	paranoidChecks := *h.db.opts.ParanoidChecks
	if op == BgMemtable {
		return SeverityFatalError
	}

	// Specific IO errors: NoSpaceError, ErrSpaceLimit.

	if vfs.IsNoSpaceError(err) {
		if !paranoidChecks {
			return SeverityNoError
		}
		switch op {
		case BgCompaction:
			return SeveritySoftError
		case BgFlush, BgWrite:
			return SeverityHardError
		default:
			panic("unreachable")
		}
	}
	if errors.Is(err, ErrSpaceLimit) {
		// Can only arise for compaction, flush, no need to check operation.
		return SeverityHardError
	}

	// Invariants not obeyed.
	var invariantErr *errors2.InvariantError
	if errors.As(err, &invariantErr) {
		if !paranoidChecks {
			return SeverityNoError
		}
		return SeverityUnrecoverableError
	}

	// IO error at all.
	// TODO: Check for os.SyscallError from FS.List.
	var pathErr *os.PathError
	isIOErr := errors.As(err, &pathErr)
	var linkErr *os.LinkError
	isIOErr = isIOErr || errors.As(err, &linkErr)
	if isIOErr {
		if !paranoidChecks {
			return SeverityNoError
		}
		return SeverityFatalError
	}

	// Default severity for all other errors.
	switch op {
	case BgCompaction, BgFlush:
		if !paranoidChecks {
			return SeverityNoError
		}
		return SeverityFatalError
	case BgWrite:
		return SeverityFatalError
	default:
		return SeverityFatalError
	}
}

// TODO: Could this be a one time decision with its result saved?
func (h *errorHandler) isNoSpaceErrorRecoveryPossible() bool {
	// We can't auto recover if SSTFileManager is nil or if FS does not support
	// giving us back the free space.
	// We rely on SFM to poll for enough disk space and recover.
	if h.db.opts.SFM == nil {
		return false
	}
	if _, err := h.db.opts.FS.GetFreeSpace(h.db.dirname); err == errors2.ErrNotSupported {
		return false
	}
	return true
}

/*
	Decide severity based on internal mapping.

	What depends on the severity? Well whether we turn off writes or not.
	What depends on a specific error? If auto recovery can be turned on.
*/

// TODO: document our decision making.
// Decide severity. TODO: Explain how.
// Ask user if they want to override.

// Requires h.db.mu is held.
// TODO: Why do we return back the error? It is never used.
func (h *errorHandler) setBGError(err error, op BackgroundErrorReason) error {
	if err == nil {
		return nil
	}
	sev := h.getSeverity(err, op)
	bgErr := &BackgroundError{
		Err: err,
		Sev: sev,
		Op:  op,
	}

	// If recovery was in progress, then this must be the error from recovery?
	// Since all other codepaths otherwise must've been stopped?
	// Could there be multiple recovery code paths?
	// So we only save the first one? (h.recoveryErr == nil check).
	if h.recoveryInProgress && h.recoveryErr == nil {
		h.recoveryErr = bgErr
	}

	autoRecovery := h.autoRecovery
	if sev >= SeverityFatalError {
		autoRecovery = false
	}

	// Error specific overrides.
	isNoSpaceErr := vfs.IsNoSpaceError(err)
	if isNoSpaceErr && !h.isNoSpaceErrorRecoveryPossible() {
		autoRecovery = false
	}

	h.db.mu.Unlock()
	h.db.opts.EventListener.BackgroundError(bgErr)
	if autoRecovery {
		h.db.opts.EventListener.ErrorRecoveryBegin(bgErr, &autoRecovery)
	}
	h.db.mu.Lock()

	if bgErr.Err != nil && bgErr.Sev > h.err.Sev {
		// Defensive copy since bgErr is coming back from user code.
		err := *bgErr
		h.err = &err
	} else {
		// This new error is less severe than previously encountered error. Don't
		// take any further action.
		return h.err
	}

	if autoRecovery {
		// TODO: why set this without outside of any of the auto recovery cases?
		// RocksDB is doing it. Check what code paths make use of this flag.
		// So seems there are other recovery ways than just polling for free space.
		h.recoveryInProgress = true
		// Kick off error specific recovery.
		if isNoSpaceErr {
			h.db.opts.SFM.startErrorRecovery(h, bgErr)
		}
	}
	return h.err
}
