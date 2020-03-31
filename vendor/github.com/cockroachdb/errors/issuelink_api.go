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

import "github.com/cockroachdb/errors/issuelink"

// WithIssueLink forwards a definition.
func WithIssueLink(err error, issue IssueLink) error { return issuelink.WithIssueLink(err, issue) }

// IssueLink forwards a definition.
type IssueLink = issuelink.IssueLink

// UnimplementedError forwards a definition.
func UnimplementedError(issueLink IssueLink, msg string) error {
	return issuelink.UnimplementedError(issueLink, msg)
}

// UnimplementedErrorf forwards a definition.
func UnimplementedErrorf(issueLink IssueLink, format string, args ...interface{}) error {
	return issuelink.UnimplementedErrorf(issueLink, format, args...)
}

// GetAllIssueLinks forwards a definition.
func GetAllIssueLinks(err error) (issues []IssueLink) { return issuelink.GetAllIssueLinks(err) }

// HasIssueLink forwards a definition.
func HasIssueLink(err error) bool { return issuelink.HasIssueLink(err) }

// IsIssueLink forwards a definition.
func IsIssueLink(err error) bool { return issuelink.IsIssueLink(err) }

// HasUnimplementedError forwards a definition.
func HasUnimplementedError(err error) bool { return issuelink.HasUnimplementedError(err) }

// IsUnimplementedError forwards a definition.
func IsUnimplementedError(err error) bool { return issuelink.IsUnimplementedError(err) }

// UnimplementedErrorHint forwards a definition.
const UnimplementedErrorHint = issuelink.UnimplementedErrorHint
