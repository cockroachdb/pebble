// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"os"

	"github.com/cockroachdb/errors"
	errors2 "github.com/cockroachdb/pebble/errors"
	"github.com/cockroachdb/pebble/vfs"
)

// BackgroundErrorReason are the possible background operations during which
// if an error occurs, subject to its severity, the DB is placed in read only
// mode.
type BackgroundErrorReason uint8

const (
	// BgFlush is for errors occurring during background flushes.
	BgFlush BackgroundErrorReason = iota
	// BgCompaction is for errors occurring during background compactions.
	BgCompaction
	// BgWrite is for errors occurring while writing to WAL.
	BgWrite
	// BgMemtable is for errors occurring while writing to memtable.
	BgMemtable
)

// Severity of an error helps decide whether to place the DB in read only mode
// and the operation effort needed for recovery.
type Severity uint8

const (
	// SeverityNoError does not set the background error at all.
	SeverityNoError Severity = 0
	// SeveritySoftError does not place the DB in read only mode and auto recovery
	// is possible for some of the errors.
	SeveritySoftError = 1
	// SeverityHardError places the DB in read only mode and recovery is possible
	// without needing to close then reopen the DB.
	SeverityHardError = 2
	// SeverityFatalError places the DB in read only mode and recovery requires
	// closing then reopening the DB.
	SeverityFatalError = 3
	// SeverityUnrecoverableError places the DB in read only mode and could mean
	// data loss, recovery from which is not possible.
	SeverityUnrecoverableError = 4
)

// BackgroundError captures the error occurred along with its severity and the
// operation during which it occurred. An instance of this is passed to the
// user in the EventListener.BackgroundError to allow overriding the
// background error.
type BackgroundError struct {
	// TODO: unexport err, severity?
	Err      error
	Severity Severity
	op       BackgroundErrorReason
}

// Reason returns the operation during which the error occurred.
func (b *BackgroundError) Reason() BackgroundErrorReason {
	return b.op
}

// Unwrap returns the error that occurred during the background operation.
func (b *BackgroundError) Unwrap() error {
	return b.Err
}

func (b *BackgroundError) Error() string {
	if b.Err != nil {
		return b.Err.Error()
	}
	return ""
}

// Reset overrides the error by setting the Err, Severity to nil.
func (b *BackgroundError) Reset() {
	b.Severity = SeverityNoError
	b.Err = nil
}

// Main aim of this is to record any error during a write operation
// and depending upon its severity, place the DB into read-only mode if needed.
// Write operations include writing to WAL, writing to memtable,
// memtable flush, compaction.

// TODO: Make sure that our db.mu protects the same state as required by RocksDB's mutex.
type errorHandler struct {
	db *DB
	// Zero value: Err is nil, Severity is SeverityNoError.
	err BackgroundError
}

// TODO: Document holding db.mu expectations for methods on errorHandler.

// TODO: Has DB been placed into read only mode?
func (h *errorHandler) isDBStopped() bool {
	return h.err.Err != nil
}

func (h *errorHandler) isBGWorkStopped() bool {
	return h.err.Err != nil
}

// Requires h.db.mu held.
func (h *errorHandler) clearBGError() {
	h.err.Reset()
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
		Err:      err,
		Severity: sev,
		op:       op,
	}

	h.db.mu.Unlock()
	// Note: Even if the severity is SeverityNoError, we still notify the user
	// about the background error. But we won't set errorHandler.err since
	// SeverityNoError is the lowest severity.
	h.db.opts.EventListener.BackgroundError(bgErr)
	h.db.mu.Lock()

	if bgErr.Err != nil && bgErr.Severity > h.err.Severity {
		h.err = *bgErr
	}
}

func (h *errorHandler) getBGError() error {
	err := h.err
	return &err
}
