// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package errutil

import (
	goErr "errors"
	"fmt"

	"github.com/cockroachdb/errors/safedetails"
	"github.com/cockroachdb/errors/withstack"
)

// New creates an error with a simple error message.
// A stack trace is retained.
//
// Detail output:
// - message via `Error()` and formatting using `%v`/`%s`/`%q`.
// - everything when formatting with `%+v`.
// - stack trace (not message) via `errors.GetSafeDetails()`.
// - stack trace (not message) in Sentry reports.
func New(msg string) error {
	return NewWithDepth(1, msg)
}

// NewWithDepth is like New() except the depth to capture the stack
// trace is configurable.
// See the doc of `New()` for more details.
func NewWithDepth(depth int, msg string) error {
	err := goErr.New(msg)
	err = withstack.WithStackDepth(err, 1+depth)
	return err
}

// Newf creates an error with a formatted error message.
// A stack trace is retained.
// See the doc of `New()` for more details.
func Newf(format string, args ...interface{}) error {
	return NewWithDepthf(1, format, args...)
}

// NewWithDepthf is like Newf() except the depth to capture the stack
// trace is configurable.
// See the doc of `New()` for more details.
func NewWithDepthf(depth int, format string, args ...interface{}) error {
	err := fmt.Errorf(format, args...)
	if format != "" || len(args) > 0 {
		err = safedetails.WithSafeDetails(err, format, args...)
	}
	err = withstack.WithStackDepth(err, 1+depth)
	return err
}

// Wrap wraps an error with a message prefix.
// A stack trace is retained.
//
// Detail output:
// - original error message + prefix via `Error()` and formatting using `%v`/`%s`/`%q`.
// - everything when formatting with `%+v`.
// - stack trace (not message) via `errors.GetSafeDetails()`.
// - stack trace (not message) in Sentry reports.
func Wrap(err error, msg string) error {
	return WrapWithDepth(1, err, msg)
}

// WrapWithDepth is like Wrap except the depth to capture the stack
// trace is configurable.
// The the doc of `Wrap()` for more details.
func WrapWithDepth(depth int, err error, msg string) error {
	if msg != "" {
		err = WithMessage(err, msg)
	}
	err = withstack.WithStackDepth(err, depth+1)
	return err
}

// Wrapf wraps an error with a formatted message prefix. A stack
// trace is also retained. If the format is empty, no prefix is added,
// but the extra arguments are still processed for reportable strings.
//
// Detail output:
// - original error message + prefix via `Error()` and formatting using `%v`/`%s`/`%q`.
// - everything when formatting with `%+v`.
// - stack trace (not message) and redacted details via `errors.GetSafeDetails()`.
// - stack trace (not message) and redacted details in Sentry reports.
func Wrapf(err error, format string, args ...interface{}) error {
	return WrapWithDepthf(1, err, format, args...)
}

// WrapWithDepthf is like Wrapf except the depth to capture the stack
// trace is configurable.
// The the doc of `Wrapf()` for more details.
func WrapWithDepthf(depth int, err error, format string, args ...interface{}) error {
	if format != "" || len(args) > 0 {
		err = WithMessagef(err, format, args...)
		err = safedetails.WithSafeDetails(err, format, args...)
	}
	err = withstack.WithStackDepth(err, depth+1)
	return err
}
