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
	"context"

	"github.com/cockroachdb/errors/contexttags"
	"github.com/cockroachdb/logtags"
)

// WithContextTags forwards a definition.
func WithContextTags(err error, ctx context.Context) error {
	return contexttags.WithContextTags(err, ctx)
}

// GetContextTags forwards a definition.
func GetContextTags(err error) []*logtags.Buffer { return contexttags.GetContextTags(err) }
