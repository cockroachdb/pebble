// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/petermattis/pebble/internal/base"
)

var stdout = io.Writer(os.Stdout)
var stderr = io.Writer(os.Stderr)
var osExit = os.Exit
var timeNow = time.Now

type key []byte

func (k *key) String() string {
	return string(*k)
}

func (k *key) Type() string {
	return "key"
}

func (k *key) Set(v string) error {
	switch {
	case strings.HasPrefix(v, "hex:"):
		v = strings.TrimPrefix(v, "hex:")
		b, err := hex.DecodeString(v)
		if err != nil {
			return err
		}
		*k = key(b)

	case strings.HasPrefix(v, "raw:"):
		*k = key(strings.TrimPrefix(v, "raw:"))

	default:
		*k = key(v)
	}
	return nil
}

type formatter struct {
	spec string
	fn   func(w io.Writer, v []byte)
}

func (f *formatter) String() string {
	return f.spec
}

func (f *formatter) Type() string {
	return "formatter"
}

func (f *formatter) Set(spec string) error {
	f.spec = spec
	switch spec {
	case "null":
		f.fn = formatNull
	case "quoted":
		f.fn = formatQuoted
	case "size":
		f.fn = formatSize
	default:
		if strings.Count(spec, "%") != 1 {
			return fmt.Errorf("unknown formatter: %q", spec)
		}
		f.fn = func(w io.Writer, v []byte) {
			fmt.Fprintf(w, f.spec, v)
		}
	}
	return nil
}

func (f *formatter) mustSet(spec string) {
	if err := f.Set(spec); err != nil {
		panic(err)
	}
}

func formatNull(w io.Writer, v []byte) {
}

func formatQuoted(w io.Writer, v []byte) {
	q := strconv.AppendQuote(make([]byte, 0, len(v)), string(v))
	q = q[1 : len(q)-1]
	w.Write(q)
}

func formatSize(w io.Writer, v []byte) {
	fmt.Fprintf(w, "<%d>", len(v))
}

func formatKey(w io.Writer, fmtKey formatter, key *base.InternalKey) bool {
	if fmtKey.spec == "null" {
		return false
	}
	fmtKey.fn(w, key.UserKey)
	fmt.Fprintf(w, "#%d,%d", key.SeqNum(), key.Kind())
	return true
}

func formatKeyRange(w io.Writer, fmtKey formatter, start, end *base.InternalKey) {
	if fmtKey.spec == "null" {
		return
	}
	w.Write([]byte{'['})
	formatKey(w, fmtKey, start)
	w.Write([]byte{'-'})
	formatKey(w, fmtKey, end)
	w.Write([]byte{']'})
}

func formatKeyValue(
	w io.Writer, fmtKey formatter, fmtValue formatter, key *base.InternalKey, value []byte,
) {
	needDelimiter := formatKey(w, fmtKey, key)
	if fmtValue.spec != "null" {
		if needDelimiter {
			w.Write([]byte{' '})
		}
		fmtValue.fn(stdout, value)
	}
	w.Write([]byte{'\n'})
}
