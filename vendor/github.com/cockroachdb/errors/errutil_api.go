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

package errors

import (
	"fmt"

	"github.com/cockroachdb/errors/barriers"
	"github.com/cockroachdb/errors/errbase"
	"github.com/cockroachdb/errors/errutil"
)

// New forwards a definition.
func New(msg string) error { return errutil.NewWithDepth(1, msg) }

// NewWithDepth forwards a definition.
func NewWithDepth(depth int, msg string) error { return errutil.NewWithDepth(depth+1, msg) }

// Newf forwards a definition.
func Newf(format string, args ...interface{}) error { return errutil.NewWithDepthf(1, format, args...) }

// NewWithDepthf forwards a definition.
func NewWithDepthf(depth int, format string, args ...interface{}) error {
	return errutil.NewWithDepthf(depth+1, format, args...)
}

// Errorf forwards a definition.
func Errorf(format string, args ...interface{}) error {
	return errutil.NewWithDepthf(1, format, args...)
}

// Cause is provided for compatibility with github.com/pkg/errors.
func Cause(err error) error { return errbase.UnwrapAll(err) }

// Unwrap is provided for compatibility with xerrors.
func Unwrap(err error) error { return errbase.UnwrapOnce(err) }

// Wrapper is provided for compatibility with xerrors.
type Wrapper interface {
	Unwrap() error
}

// Formatter is provided for compatibility with xerrors.
type Formatter = errbase.Formatter

// Printer is provided for compatibility with xerrors.
type Printer = errbase.Printer

// FormatError can be used to implement the fmt.Formatter interface.
func FormatError(err error, s fmt.State, verb rune) { errbase.FormatError(err, s, verb) }

// Opaque is provided for compatibility with xerrors.
func Opaque(err error) error { return barriers.Handled(err) }

// WithMessage forwards a definition.
func WithMessage(err error, msg string) error { return errutil.WithMessage(err, msg) }

// WithMessagef forwards a definition.
func WithMessagef(err error, format string, args ...interface{}) error {
	return errutil.WithMessagef(err, format, args...)
}

// Wrap forwards a definition.
func Wrap(err error, msg string) error { return errutil.WrapWithDepth(1, err, msg) }

// WrapWithDepth forwards a definition.
func WrapWithDepth(depth int, err error, msg string) error {
	return errutil.WrapWithDepth(depth+1, err, msg)
}

// Wrapf forwards a definition.
func Wrapf(err error, format string, args ...interface{}) error {
	return errutil.WrapWithDepthf(1, err, format, args...)
}

// WrapWithDepthf forwards a definition.
func WrapWithDepthf(depth int, err error, format string, args ...interface{}) error {
	return errutil.WrapWithDepthf(depth+1, err, format, args...)
}

// AssertionFailedf forwards a definition.
func AssertionFailedf(format string, args ...interface{}) error {
	return errutil.AssertionFailedWithDepthf(1, format, args...)
}

// AssertionFailedWithDepthf forwards a definition.
func AssertionFailedWithDepthf(depth int, format string, args ...interface{}) error {
	return errutil.AssertionFailedWithDepthf(depth+1, format, args...)
}

// NewAssertionErrorWithWrappedErrf forwards a definition.
func NewAssertionErrorWithWrappedErrf(origErr error, format string, args ...interface{}) error {
	return errutil.NewAssertionErrorWithWrappedErrDepthf(1, origErr, format, args...)
}

// HandleAsAssertionFailure forwards a definition.
func HandleAsAssertionFailure(origErr error) error {
	return errutil.HandleAsAssertionFailureDepth(1, origErr)
}

// HandleAsAssertionFailureDepth forwards a definition.
func HandleAsAssertionFailureDepth(depth int, origErr error) error {
	return errutil.HandleAsAssertionFailureDepth(1+depth, origErr)
}

// As forwards a definition
func As(err error, target interface{}) bool { return errutil.As(err, target) }
