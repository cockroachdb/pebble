//go:build tools
// +build tools

package tools

import (
	_ "github.com/cockroachdb/crlfmt"
	_ "golang.org/x/lint/golint"
	_ "honnef.co/go/tools/cmd/staticcheck"
)
