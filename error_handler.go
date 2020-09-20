// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"os"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
)

// BackgroundErrorReason is an enum of possible operations whose failure may
// put the DB in read-only mode depending on the severity.
type BackgroundErrorReason uint8

const (
	// BgFlush is for errors during background flushes.
	BgFlush BackgroundErrorReason = iota
	// BgCompaction is for errors during background compactions.
	BgCompaction
	// BgWrite is for errors in writing to WAL.
	BgWrite
	// BgMemtable is for errors in writing to memtable.
	BgMemtable
	// BgManifestWrite is for errors in writing to MANIFEST.
	BgManifestWrite
)

// Severity of an error indicates whether recovery is possible and whether DB
// will be placed in read-only mode or not.
type Severity uint8

const (
	// SeverityNoError does not set the background error at all.
	SeverityNoError Severity = 0
	// SeveritySoftError does not place the DB in read-only mode and auto recovery
	// is possible for some of the errors.
	SeveritySoftError = 1
	// SeverityHardError places the DB in read-only mode and recovery might be
	// possible without needing to close then reopen the DB.
	SeverityHardError = 2
	// SeverityFatalError places the DB in read-only mode and recovery requires
	// closing then reopening the DB.
	SeverityFatalError = 3
	// SeverityUnrecoverableError places the DB in read-only mode and could mean
	// data loss, recovery from which is not possible.
	SeverityUnrecoverableError = 4
)

// BackgroundError captures the error occurred during any background operation
// along with its severity. An instance of this is passed in
// EventListener.BackgroundError.
type BackgroundError struct {
	err      error
	severity Severity
	reason   BackgroundErrorReason
}

// Reason returns the operation during which the error occurred.
func (b *BackgroundError) Reason() BackgroundErrorReason {
	return b.reason
}

// Severity returns the severity of the error.
func (b *BackgroundError) Severity() Severity {
	return b.severity
}

// Unwrap returns the error that occurred during the background operation.
func (b *BackgroundError) Unwrap() error {
	return b.err
}

func (b *BackgroundError) Error() string {
	if b.err != nil {
		return b.err.Error()
	}
	return ""
}

// Override results in ignoring this background error and won't place the DB
// in read-only mode.
func (b *BackgroundError) Override() {
	b.severity = SeverityNoError
	b.err = nil
}

// TODO: Make sure that our db.mu protects the same state as required by RocksDB's mutex.
type errorHandler struct {
	db  *DB
	err BackgroundError
}

// TODO: Document holding db.mu expectations for methods on errorHandler.

// TODO: Has DB been placed into read only mode?
func (h *errorHandler) isDBStopped() bool {
	return h.err.err != nil
}

func (h *errorHandler) isBGWorkStopped() bool {
	return h.err.err != nil
}

// Requires h.db.mu held.
func (h *errorHandler) clearBGError() {
	h.err.Override()
}

func (h *errorHandler) getSeverity(err error, op BackgroundErrorReason) Severity {
	paranoidChecks := *h.db.opts.ParanoidChecks
	if op == BgMemtable {
		return SeverityFatalError
	}

	// Specific IO errors: NoSpaceError.

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

	if errors.Is(err, base.ErrCorruption) {
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
		// TODO: kWrite i.e. WAL write failures should be fatal always?
		// independent of paranoid checks? because we don't know what's the
		// state of wal now. can bytes be partially written to it?
		// note: this diverges from rocksDB's behaviour.
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

/*
	Decide severity based on internal mapping.

	What depends on the severity? Well whether we turn off writes or not.
	What depends on a specific error? If auto recovery can be turned on.
*/

// TODO: document our decision making.
// Decide severity. TODO: Explain how.
// Ask user if they want to override.

// Requires h.db.mu is held.
func (h *errorHandler) setBGError(err error, op BackgroundErrorReason) {
	if err == nil {
		return
	}
	sev := h.getSeverity(err, op)
	bgErr := &BackgroundError{
		err:      err,
		severity: sev,
		reason:   op,
	}

	h.db.mu.Unlock()
	// Note: Even if the severity is SeverityNoError, we still notify the user
	// about the background error. But we won't set errorHandler.err since
	// SeverityNoError is the lowest severity.
	h.db.opts.EventListener.BackgroundError(bgErr)
	h.db.mu.Lock()

	if bgErr.err != nil && bgErr.severity > h.err.severity {
		h.err = *bgErr
	}
}

func (h *errorHandler) getBGError() error {
	err := h.err
	return &err
}
