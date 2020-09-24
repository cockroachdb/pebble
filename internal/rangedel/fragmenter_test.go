// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangedel

import (
	"bytes"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/stretchr/testify/require"
)

var tombstoneRe = regexp.MustCompile(`(\d+):\s*(\w+)-*(\w+)`)

func parseTombstone(t *testing.T, s string) Tombstone {
	m := tombstoneRe.FindStringSubmatch(s)
	if len(m) != 4 {
		t.Fatalf("expected 4 components, but found %d: %s", len(m), s)
	}
	seqNum, err := strconv.Atoi(m[1])
	require.NoError(t, err)
	return Tombstone{
		Start: base.MakeInternalKey([]byte(m[2]), uint64(seqNum), base.InternalKeyKindRangeDelete),
		End:   []byte(m[3]),
	}
}

func buildTombstones(
	t *testing.T, cmp base.Compare, formatKey base.FormatKey, s string,
) []Tombstone {
	var tombstones []Tombstone
	f := &Fragmenter{
		Cmp:    cmp,
		Format: formatKey,
		Emit: func(fragmented []Tombstone) {
			tombstones = append(tombstones, fragmented...)
		},
	}
	for _, line := range strings.Split(s, "\n") {
		if strings.HasPrefix(line, "flush-to ") {
			parts := strings.Split(line, " ")
			if len(parts) != 2 {
				t.Fatalf("expected 2 components, but found %d: %s", len(parts), line)
			}
			f.FlushTo([]byte(parts[1]))
			continue
		} else if strings.HasPrefix(line, "truncate-and-flush-to ") {
			parts := strings.Split(line, " ")
			if len(parts) != 2 {
				t.Fatalf("expected 2 components, but found %d: %s", len(parts), line)
			}
			f.TruncateAndFlushTo([]byte(parts[1]))
			continue
		}

		t := parseTombstone(t, line)
		f.Add(t.Start, t.End)
	}
	f.Finish()
	return tombstones
}

func formatTombstones(tombstones []Tombstone) string {
	isLetter := func(b []byte) bool {
		if len(b) != 1 {
			return false
		}
		return b[0] >= 'a' && b[0] <= 'z'
	}

	var buf bytes.Buffer
	for _, v := range tombstones {
		if v.Empty() {
			fmt.Fprintf(&buf, "<empty>\n")
			continue
		}
		if !isLetter(v.Start.UserKey) || !isLetter(v.End) || v.Start.UserKey[0] == v.End[0] {
			fmt.Fprintf(&buf, "%d: %s-%s\n", v.Start.SeqNum(), v.Start.UserKey, v.End)
			continue
		}
		fmt.Fprintf(&buf, "%d: %s%s%s%s\n",
			v.Start.SeqNum(),
			strings.Repeat(" ", int(v.Start.UserKey[0]-'a')),
			v.Start.UserKey,
			strings.Repeat("-", int(v.End[0]-v.Start.UserKey[0]-1)),
			v.End)
	}
	return buf.String()
}

func TestFragmenter(t *testing.T) {
	cmp := base.DefaultComparer.Compare
	fmtKey := base.DefaultComparer.FormatKey

	var getRe = regexp.MustCompile(`(\w+)#(\d+)`)

	parseGet := func(t *testing.T, s string) (string, int) {
		m := getRe.FindStringSubmatch(s)
		if len(m) != 3 {
			t.Fatalf("expected 3 components, but found %d", len(m))
		}
		seq, err := strconv.Atoi(m[2])
		require.NoError(t, err)
		return m[1], seq
	}

	var iter base.InternalIterator

	// Returns true if the specified <key,seq> pair is deleted at the specified
	// read sequence number. Get ignores tombstones newer than the read sequence
	// number. This is a simple version of what full processing of range
	// tombstones looks like.
	deleted := func(key []byte, seq, readSeq uint64) bool {
		tombstone := Get(cmp, iter, key, readSeq)
		return tombstone.Deletes(seq)
	}

	datadriven.RunTest(t, "testdata/fragmenter", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "build":
			return func() (result string) {
				defer func() {
					if r := recover(); r != nil {
						result = fmt.Sprint(r)
					}
				}()

				tombstones := buildTombstones(t, cmp, fmtKey, d.Input)
				iter = NewIter(cmp, tombstones)
				return formatTombstones(tombstones)
			}()

		case "get":
			if len(d.CmdArgs) != 1 {
				return fmt.Sprintf("expected 1 argument, but found %s", d.CmdArgs)
			}
			if d.CmdArgs[0].Key != "t" {
				return fmt.Sprintf("expected timestamp argument, but found %s", d.CmdArgs[0])
			}
			readSeq, err := strconv.Atoi(d.CmdArgs[0].Vals[0])
			require.NoError(t, err)

			var results []string
			for _, p := range strings.Split(d.Input, " ") {
				key, seq := parseGet(t, p)
				if deleted([]byte(key), uint64(seq), uint64(readSeq)) {
					results = append(results, "deleted")
				} else {
					results = append(results, "alive")
				}
			}
			return strings.Join(results, " ")

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

func TestFragmenterDeleted(t *testing.T) {
	datadriven.RunTest(t, "testdata/fragmenter_deleted", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "build":
			f := &Fragmenter{
				Cmp:    base.DefaultComparer.Compare,
				Format: base.DefaultComparer.FormatKey,
				Emit: func(fragmented []Tombstone) {
				},
			}
			var buf bytes.Buffer
			for _, line := range strings.Split(d.Input, "\n") {
				switch {
				case strings.HasPrefix(line, "add "):
					t := parseTombstone(t, strings.TrimPrefix(line, "add "))
					f.Add(t.Start, t.End)
				case strings.HasPrefix(line, "deleted "):
					key := base.ParseInternalKey(strings.TrimPrefix(line, "deleted "))
					func() {
						defer func() {
							if r := recover(); r != nil {
								fmt.Fprintf(&buf, "%s: %s\n", key, r)
							}
						}()
						fmt.Fprintf(&buf, "%s: %t\n", key, f.Deleted(key, base.InternalKeySeqNumMax))
					}()
				}
			}
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

func TestFragmenterFlushTo(t *testing.T) {
	cmp := base.DefaultComparer.Compare
	fmtKey := base.DefaultComparer.FormatKey

	datadriven.RunTest(t, "testdata/fragmenter_flush_to", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "build":
			return func() (result string) {
				defer func() {
					if r := recover(); r != nil {
						result = fmt.Sprint(r)
					}
				}()

				tombstones := buildTombstones(t, cmp, fmtKey, d.Input)
				return formatTombstones(tombstones)
			}()

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

func TestFragmenterTruncateAndFlushTo(t *testing.T) {
	cmp := base.DefaultComparer.Compare
	fmtKey := base.DefaultComparer.FormatKey

	datadriven.RunTest(t, "testdata/fragmenter_truncate_and_flush_to", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "build":
			return func() (result string) {
				defer func() {
					if r := recover(); r != nil {
						result = fmt.Sprint(r)
					}
				}()

				tombstones := buildTombstones(t, cmp, fmtKey, d.Input)
				return formatTombstones(tombstones)
			}()

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}
