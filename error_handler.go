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
	SeverityNoError Severity = 0
	// Not placed in read only mode. Auto recovery possible.
	SeveritySoftError = 1
	// Placed in read only mode, but can recover without needing to close/re-open DB.
	SeverityHardError = 2
	// Placed in read only mode, but can recover by closing the DB and reopening it.
	SeverityFatalError = 3
	// Placed in read only mode, could mean data is corrupted.
	SeverityUnrecoverableError = 4
)

// TODO: Improve doc.
// *BackgroundError implements error. We provide the actual error as well as
// the severity for users to judge whether they want to reset or not. They can
// reset the error by setting Err to nil.
type BackgroundError struct {
	Err      error
	Severity Severity
	op       BackgroundErrorReason
}

func (b *BackgroundError) Reason() BackgroundErrorReason {
	return b.op
}

func (b *BackgroundError) Error() string {
	if b.Err != nil {
		return b.Err.Error()
	}
	return ""
}

// TODO: better doc.
// Override the background error by resetting the error,
// severity to nil. Callable by user from the
// event listener callback.
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
