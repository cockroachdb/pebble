//go:build tools

package tools

import (
	_ "github.com/cockroachdb/crlfmt"
	_ "github.com/jordanlewis/gcassert/cmd/gcassert"
	_ "honnef.co/go/tools/cmd/staticcheck"
)
