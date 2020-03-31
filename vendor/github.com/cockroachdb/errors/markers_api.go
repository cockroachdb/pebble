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

import "github.com/cockroachdb/errors/markers"

// Is forwards a definition.
func Is(err, reference error) bool { return markers.Is(err, reference) }

// If forwards a definition.
func If(err error, pred func(err error) (interface{}, bool)) (interface{}, bool) {
	return markers.If(err, pred)
}

// IsAny forwards a definition.
func IsAny(err error, references ...error) bool { return markers.IsAny(err, references...) }

// Mark forwards a definition.
func Mark(err error, reference error) error { return markers.Mark(err, reference) }
