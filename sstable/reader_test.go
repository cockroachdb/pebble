// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/petermattis/pebble/bloom"
	"github.com/petermattis/pebble/cache"
	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/internal/datadriven"
	"github.com/petermattis/pebble/vfs"
	"golang.org/x/exp/rand"
)

// iterAdapter adapts the new Iterator API which returns the key and value from
// positioning methods (Seek*, First, Last, Next, Prev) to the old API which
// returned a boolean corresponding to Valid. Only used by test code.
type iterAdapter struct {
	*Iterator
}

func (i *iterAdapter) verify(key *db.InternalKey, val []byte) bool {
	valid := key != nil
	if valid != i.Valid() {
		panic(fmt.Sprintf("inconsistent valid: %t != %t", valid, i.Valid()))
	}
	if valid {
		if db.InternalCompare(bytes.Compare, *key, i.Key()) != 0 {
			panic(fmt.Sprintf("inconsistent key: %s != %s", *key, i.Key()))
		}
		if !bytes.Equal(val, i.Value()) {
			panic(fmt.Sprintf("inconsistent value: [% x] != [% x]", val, i.Value()))
		}
	}
	return valid
}

func (i *iterAdapter) SeekGE(key []byte) bool {
	return i.verify(i.Iterator.SeekGE(key))
}

func (i *iterAdapter) SeekPrefixGE(prefix, key []byte) bool {
	return i.verify(i.Iterator.SeekPrefixGE(prefix, key))
}

func (i *iterAdapter) SeekLT(key []byte) bool {
	return i.verify(i.Iterator.SeekLT(key))
}

func (i *iterAdapter) First() bool {
	return i.verify(i.Iterator.First())
}

func (i *iterAdapter) Last() bool {
	return i.verify(i.Iterator.Last())
}

func (i *iterAdapter) Next() bool {
	return i.verify(i.Iterator.Next())
}

func (i *iterAdapter) Prev() bool {
	return i.verify(i.Iterator.Prev())
}

func (i *iterAdapter) Key() db.InternalKey {
	return *i.Iterator.Key()
}

func TestReader(t *testing.T) {
	levelOpts := map[string]db.LevelOptions{
		// No bloom filters.
		"default": db.LevelOptions{},
		"bloom10bit": db.LevelOptions{
			// The standard policy.
			FilterPolicy: bloom.FilterPolicy(10),
			FilterType:   db.TableFilter,
		},
		"bloom1bit": db.LevelOptions{
			// A policy with many false positives.
			FilterPolicy: bloom.FilterPolicy(1),
			FilterType:   db.TableFilter,
		},
		"bloom100bit": db.LevelOptions{
			// A policy unlikely to have false positives.
			FilterPolicy: bloom.FilterPolicy(100),
			FilterType:   db.TableFilter,
		},
	}

	opts := map[string]*db.Options{
		"default": {},
		"prefixFilter": {
			Comparer: fixtureComparer,
		},
	}

	testDirs := map[string]string {
		"default": "testdata/reader",
		"prefixFilter": "testdata/prefixreader",
	}

	for lName, levelOpt := range levelOpts {
		for oName, opt := range opts {
			levelOpt.EnsureDefaults()
			o := *opt
			o.Levels = []db.LevelOptions{levelOpt}
			o.EnsureDefaults()

			t.Run(fmt.Sprintf("opts=%s,levelOpts=%s", oName, lName), func(t *testing.T) {
				runTestReader(t, o, testDirs[oName])
			})
		}
	}
}

func runTestReader(t *testing.T, o db.Options, dir string) {
	makeIkeyValue := func(s string) (db.InternalKey, []byte) {
		j := strings.Index(s, ":")
		k := strings.Index(s, "=")
		seqNum, err := strconv.Atoi(s[j+1 : k])
		if err != nil {
			panic(err)
		}
		return db.MakeInternalKey([]byte(s[:j]), uint64(seqNum), db.InternalKeyKindSet), []byte(s[k+1:])
	}

	mem := vfs.NewMem()
	var r *Reader

	datadriven.Walk(t, dir, func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(d *datadriven.TestData) string {
			switch d.Cmd {
			case "build":
				if r != nil {
					r.Close()
					mem.Remove("sstable")
				}

				f, err := mem.Create("sstable")
				if err != nil {
					return err.Error()
				}
				w := NewWriter(f, &o, o.Levels[0])
				for _, e := range strings.Split(strings.TrimSpace(d.Input), ",") {
					k, v := makeIkeyValue(e)
					w.Add(k, v)
				}
				w.Close()

				f, err = mem.Open("sstable")
				if err != nil {
					return err.Error()
				}
				r = NewReader(f, 0, &o)
				return ""

			case "iter":
				for _, arg := range d.CmdArgs {
					switch arg.Key {
					case "globalSeqNum":
						if len(arg.Vals) != 1 {
							return fmt.Sprintf("%s: arg %s expects 1 value", d.Cmd, arg.Key)
						}
						v, err := strconv.Atoi(arg.Vals[0])
						if err != nil {
							return err.Error()
						}
						r.Properties.GlobalSeqNum = uint64(v)
					default:
						return fmt.Sprintf("%s: unknown arg: %s", d.Cmd, arg.Key)
					}
				}

				iter := iterAdapter{r.NewIter(nil /* lower */, nil /* upper */)}
				if err := iter.Error(); err != nil {
					t.Fatal(err)
				}

				var b bytes.Buffer
				var prefix []byte
				for _, line := range strings.Split(d.Input, "\n") {
					parts := strings.Fields(line)
					if len(parts) == 0 {
						continue
					}
					switch parts[0] {
					case "seek-ge":
						if len(parts) != 2 {
							return fmt.Sprintf("seek-ge <key>\n")
						}
						prefix = nil
						iter.SeekGE([]byte(strings.TrimSpace(parts[1])))
					case "seek-prefix-ge":
						if len(parts) != 2 {
							return fmt.Sprintf("seek-prefix-ge <key>\n")
						}
						prefix = []byte(strings.TrimSpace(parts[1]))
						iter.SeekPrefixGE(prefix, prefix /* key */)
					case "seek-lt":
						if len(parts) != 2 {
							return fmt.Sprintf("seek-lt <key>\n")
						}
						prefix = nil
						iter.SeekLT([]byte(strings.TrimSpace(parts[1])))
					case "first":
						prefix = nil
						iter.First()
					case "last":
						prefix = nil
						iter.Last()
					case "next":
						iter.Next()
					case "prev":
						iter.Prev()
					}
					if iter.Valid() && checkValidPrefix(prefix, iter.Key().UserKey){
						fmt.Fprintf(&b, "<%s:%d>", iter.Key().UserKey, iter.Key().SeqNum())
					} else if err := iter.Error(); err != nil {
						fmt.Fprintf(&b, "<err=%v>", err)
					} else {
						fmt.Fprintf(&b, ".")
					}
				}
				b.WriteString("\n")
				return b.String()

			case "get":
				var b bytes.Buffer
				for _, k := range strings.Split(d.Input, "\n") {
					v, err := r.get([]byte(k))
					if err != nil {
						fmt.Fprintf(&b, "<err: %s>\n", err)
					} else {
						fmt.Fprintln(&b, string(v))
					}
				}
				return b.String()
			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	})
}

func checkValidPrefix(prefix, key []byte) bool {
	return prefix == nil || bytes.HasPrefix(key, prefix)
}

func buildBenchmarkTable(b *testing.B, blockSize, restartInterval int) (*Reader, [][]byte) {
	mem := vfs.NewMem()
	f0, err := mem.Create("bench")
	if err != nil {
		b.Fatal(err)
	}
	defer f0.Close()

	w := NewWriter(f0, nil, db.LevelOptions{
		BlockRestartInterval: restartInterval,
		BlockSize:            blockSize,
		FilterPolicy:         nil,
	})

	var keys [][]byte
	var ikey db.InternalKey
	for i := uint64(0); i < 1e6; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		keys = append(keys, key)
		ikey.UserKey = key
		w.Add(ikey, nil)
	}

	if err := w.Close(); err != nil {
		b.Fatal(err)
	}

	// Re-open that filename for reading.
	f1, err := mem.Open("bench")
	if err != nil {
		b.Fatal(err)
	}
	return NewReader(f1, 0, &db.Options{
		Cache: cache.New(128 << 20),
	}), keys
}

func BenchmarkTableIterSeekGE(b *testing.B) {
	const blockSize = 32 << 10

	for _, restartInterval := range []int{16} {
		b.Run(fmt.Sprintf("restart=%d", restartInterval),
			func(b *testing.B) {
				r, keys := buildBenchmarkTable(b, blockSize, restartInterval)
				it := r.NewIter(nil /* lower */, nil /* upper */)
				rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					it.SeekGE(keys[rng.Intn(len(keys))])
				}
			})
	}
}

func BenchmarkTableIterSeekLT(b *testing.B) {
	const blockSize = 32 << 10

	for _, restartInterval := range []int{16} {
		b.Run(fmt.Sprintf("restart=%d", restartInterval),
			func(b *testing.B) {
				r, keys := buildBenchmarkTable(b, blockSize, restartInterval)
				it := r.NewIter(nil /* lower */, nil /* upper */)
				rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					it.SeekLT(keys[rng.Intn(len(keys))])
				}
			})
	}
}

func BenchmarkTableIterNext(b *testing.B) {
	const blockSize = 32 << 10

	for _, restartInterval := range []int{16} {
		b.Run(fmt.Sprintf("restart=%d", restartInterval),
			func(b *testing.B) {
				r, _ := buildBenchmarkTable(b, blockSize, restartInterval)
				it := r.NewIter(nil /* lower */, nil /* upper */)

				b.ResetTimer()
				var sum int64
				var key *db.InternalKey
				for i := 0; i < b.N; i++ {
					if key == nil {
						key, _ = it.First()
					}
					sum += int64(binary.BigEndian.Uint64(key.UserKey))
					key, _ = it.Next()
				}
				if testing.Verbose() {
					fmt.Fprint(ioutil.Discard, sum)
				}
			})
	}
}

func BenchmarkTableIterPrev(b *testing.B) {
	const blockSize = 32 << 10

	for _, restartInterval := range []int{16} {
		b.Run(fmt.Sprintf("restart=%d", restartInterval),
			func(b *testing.B) {
				r, _ := buildBenchmarkTable(b, blockSize, restartInterval)
				it := r.NewIter(nil /* lower */, nil /* upper */)

				b.ResetTimer()
				var sum int64
				var key *db.InternalKey
				for i := 0; i < b.N; i++ {
					if key == nil {
						key, _ = it.Last()
					}
					sum += int64(binary.BigEndian.Uint64(key.UserKey))
					key, _ = it.Prev()
				}
				if testing.Verbose() {
					fmt.Fprint(ioutil.Discard, sum)
				}
			})
	}
}
