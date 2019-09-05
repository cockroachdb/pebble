// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
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
	fn   base.Formatter
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
		f.fn = func(v []byte) fmt.Formatter {
			return fmtFormatter{f.spec, v}
		}
	}
	return nil
}

func (f *formatter) mustSet(spec string) {
	if err := f.Set(spec); err != nil {
		panic(err)
	}
}

type fmtFormatter struct {
	fmt string
	v   []byte
}

func (f fmtFormatter) Format(s fmt.State, c rune) {
	fmt.Fprintf(s, f.fmt, f.v)
}

type nullFormatter struct{}

func (nullFormatter) Format(s fmt.State, c rune) {
}

func formatNull(v []byte) fmt.Formatter {
	return nullFormatter{}
}

func formatQuoted(v []byte) fmt.Formatter {
	return base.FormatBytes(v)
}

type sizeFormatter []byte

func (v sizeFormatter) Format(s fmt.State, c rune) {
	fmt.Fprintf(s, "<%d>", len(v))
}

func formatSize(v []byte) fmt.Formatter {
	return sizeFormatter(v)
}

func formatKey(w io.Writer, fmtKey formatter, key *base.InternalKey) bool {
	if fmtKey.spec == "null" {
		return false
	}
	fmt.Fprintf(w, "%s", key.Pretty(fmtKey.fn))
	return true
}

func formatKeyRange(w io.Writer, fmtKey formatter, start, end *base.InternalKey) {
	if fmtKey.spec == "null" {
		return
	}
	fmt.Fprintf(w, "[%s-%s]", start.Pretty(fmtKey.fn), end.Pretty(fmtKey.fn))
}

func formatKeyValue(
	w io.Writer, fmtKey formatter, fmtValue formatter, key *base.InternalKey, value []byte,
) {
	if key.Kind() == base.InternalKeyKindRangeDelete {
		if fmtKey.spec != "null" {
			fmt.Fprintf(w, "%s-%s#%d,%d",
				fmtKey.fn(key.UserKey), fmtKey.fn(value),
				key.SeqNum(), key.Kind())
		}
	} else {
		needDelimiter := formatKey(w, fmtKey, key)
		if fmtValue.spec != "null" {
			if needDelimiter {
				w.Write([]byte{' '})
			}
			fmt.Fprintf(w, "%s", fmtValue.fn(value))
		}
	}
	w.Write([]byte{'\n'})
}
