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

import "github.com/cockroachdb/errors/withstack"

// WithStack forwards a definition.
func WithStack(err error) error { return withstack.WithStackDepth(err, 1) }

// WithStackDepth forwards a definition.
func WithStackDepth(err error, depth int) error { return withstack.WithStackDepth(err, depth+1) }

// ReportableStackTrace forwards a definition.
type ReportableStackTrace = withstack.ReportableStackTrace

// GetOneLineSource forwards a definition.
func GetOneLineSource(err error) (file string, line int, fn string, ok bool) {
	return withstack.GetOneLineSource(err)
}

// GetReportableStackTrace forwards a definition.
func GetReportableStackTrace(err error) *ReportableStackTrace {
	return withstack.GetReportableStackTrace(err)
}
