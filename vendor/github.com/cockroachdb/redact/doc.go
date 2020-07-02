// Copyright 2020 The Cockroach Authors.
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

// Package redact provides facilities for separating “safe” and
// “unsafe” pieces of data when logging and constructing error object.
//
// An item is said to be “safe” if it is proven to not contain
// PII or otherwise confidential information that should not escape
// the boundaries of the current system, for example via telemetry
// or crash reporting. Conversely, data is considered “unsafe”
// until/unless it is known to be “safe”.
package redact
