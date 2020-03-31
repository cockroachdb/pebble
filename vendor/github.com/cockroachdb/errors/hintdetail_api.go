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

import "github.com/cockroachdb/errors/hintdetail"

// ErrorHinter forwards a definition.
type ErrorHinter = hintdetail.ErrorHinter

// ErrorDetailer forwards a definition.
type ErrorDetailer = hintdetail.ErrorDetailer

// WithHint forwards a definition.
func WithHint(err error, msg string) error { return hintdetail.WithHint(err, msg) }

// WithHintf forwards a definition.
func WithHintf(err error, format string, args ...interface{}) error {
	return hintdetail.WithHintf(err, format, args...)
}

// WithDetail forwards a definition.
func WithDetail(err error, msg string) error { return hintdetail.WithDetail(err, msg) }

// WithDetailf forwards a definition.
func WithDetailf(err error, format string, args ...interface{}) error {
	return hintdetail.WithDetailf(err, format, args...)
}

// GetAllHints forwards a definition.
func GetAllHints(err error) []string { return hintdetail.GetAllHints(err) }

// FlattenHints forwards a definition.
func FlattenHints(err error) string { return hintdetail.FlattenHints(err) }

// GetAllDetails forwards a definition.
func GetAllDetails(err error) []string { return hintdetail.GetAllDetails(err) }

// FlattenDetails forwards a definition.
func FlattenDetails(err error) string { return hintdetail.FlattenDetails(err) }
