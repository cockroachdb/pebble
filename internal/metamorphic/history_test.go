// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHistoryLogger(t *testing.T) {
	var buf bytes.Buffer
	h := newHistory("", &buf)
	l := h.Logger()
	l.Infof("hello\nworld\n")
	l.Fatalf("hello\n\nworld")

	expected := `// INFO: hello
// INFO: world
// FATAL: hello
// FATAL: 
// FATAL: world
`
	if actual := buf.String(); expected != actual {
		t.Fatalf("expected\n%s\nbut found\n%s", expected, actual)
	}
}

func TestHistoryFail(t *testing.T) {
	var buf bytes.Buffer
	h := newHistory("foo", &buf)
	h.Recordf("bar")
	require.False(t, h.Failed())
	h.Recordf("foo bar")
	require.True(t, h.Failed())
}
