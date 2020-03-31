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
	"context"
	"fmt"

	"github.com/cockroachdb/errors/errbase"
	"github.com/gogo/protobuf/proto"
)

// WithMessage annotates err with a new message.
// If err is nil, WithMessage returns nil.
func WithMessage(err error, message string) error {
	if err == nil {
		return nil
	}
	return &withMessage{
		cause: err,
		msg:   message,
	}
}

// WithMessagef annotates err with the format specifier.
// If err is nil, WithMessagef returns nil.
func WithMessagef(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	return &withMessage{
		cause: err,
		msg:   fmt.Sprintf(format, args...),
	}
}

type withMessage struct {
	cause error
	msg   string
}

func (w *withMessage) Error() string { return fmt.Sprintf("%s: %v", w.msg, w.cause) }
func (w *withMessage) Cause() error  { return w.cause }
func (w *withMessage) Unwrap() error { return w.cause }

func (w *withMessage) Format(s fmt.State, verb rune) { errbase.FormatError(w, s, verb) }
func (w *withMessage) FormatError(p errbase.Printer) (next error) {
	p.Print(w.msg)
	return w.cause
}

// A message wrapper is decoded exactly.
func decodeMessage(
	ctx context.Context, cause error, msg string, _ []string, _ proto.Message,
) error {
	return &withMessage{cause: cause, msg: msg}
}

func init() {
	tn := errbase.GetTypeKey((*withMessage)(nil))
	errbase.RegisterWrapperDecoder(tn, decodeMessage)
}
