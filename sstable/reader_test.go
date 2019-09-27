// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/vfs"
	"golang.org/x/exp/rand"
)

// iterAdapter adapts the new Iterator API which returns the key and value from
// positioning methods (Seek*, First, Last, Next, Prev) to the old API which
// returned a boolean corresponding to Valid. Only used by test code.
type iterAdapter struct {
	Iterator
	key *InternalKey
	val []byte
}

func newIterAdapter(iter Iterator) *iterAdapter {
	return &iterAdapter{
		Iterator: iter,
	}
}

func (i *iterAdapter) update(key *InternalKey, val []byte) bool {
	i.key = key
	i.val = val
	return i.key != nil
}

func (i *iterAdapter) SeekGE(key []byte) bool {
	return i.update(i.Iterator.SeekGE(key))
}

func (i *iterAdapter) SeekPrefixGE(prefix, key []byte) bool {
	return i.update(i.Iterator.SeekPrefixGE(prefix, key))
}

func (i *iterAdapter) SeekLT(key []byte) bool {
	return i.update(i.Iterator.SeekLT(key))
}

func (i *iterAdapter) First() bool {
	return i.update(i.Iterator.First())
}

func (i *iterAdapter) Last() bool {
	return i.update(i.Iterator.Last())
}

func (i *iterAdapter) Next() bool {
	return i.update(i.Iterator.Next())
}

func (i *iterAdapter) Prev() bool {
	return i.update(i.Iterator.Prev())
}

func (i *iterAdapter) Key() *InternalKey {
	return i.key
}

func (i *iterAdapter) Value() []byte {
	return i.val
}

func (i *iterAdapter) Valid() bool {
	return i.key != nil
}

func TestReader(t *testing.T) {
	tableOpts := map[string]TableOptions{
		// No bloom filters.
		"default": TableOptions{},
		"bloom10bit": TableOptions{
			// The standard policy.
			FilterPolicy: bloom.FilterPolicy(10),
			FilterType:   base.TableFilter,
		},
		"bloom1bit": TableOptions{
			// A policy with many false positives.
			FilterPolicy: bloom.FilterPolicy(1),
			FilterType:   base.TableFilter,
		},
		"bloom100bit": TableOptions{
			// A policy unlikely to have false positives.
			FilterPolicy: bloom.FilterPolicy(100),
			FilterType:   base.TableFilter,
		},
	}

	blockSizes := map[string]int{
		"5bytes":   5,
		"10bytes":  10,
		"25bytes":  25,
		"Maxbytes": math.MaxInt32,
	}

	opts := map[string]*Options{
		"default": {},
		"prefixFilter": {
			Comparer: fixtureComparer,
		},
	}

	testDirs := map[string]string{
		"default":      "testdata/reader",
		"prefixFilter": "testdata/prefixreader",
	}

	for dName, blockSize := range blockSizes {
		for iName, indexBlockSize := range blockSizes {
			for lName, tableOpt := range tableOpts {
				for oName, opt := range opts {
					tableOpt.BlockSize = blockSize
					tableOpt.IndexBlockSize = indexBlockSize
					tableOpt.EnsureDefaults()
					o := *opt
					o.Levels = []TableOptions{tableOpt}
					o.EnsureDefaults()

					t.Run(
						fmt.Sprintf("opts=%s,tableOpts=%s,blockSize=%s,indexSize=%s",
							oName, lName, dName, iName),
						func(t *testing.T) { runTestReader(t, o, testDirs[oName], nil /* Reader */) })
				}
			}
		}
	}
}

func TestHamletReader(t *testing.T) {
	prebuiltSSTs := []string{
		"testdata/h.ldb",
		"testdata/h.sst",
		"testdata/h.no-compression.sst",
		"testdata/h.no-compression.two_level_index.sst",
		"testdata/h.block-bloom.no-compression.sst",
		"testdata/h.table-bloom.no-compression.prefix_extractor.no_whole_key_filter.sst",
		"testdata/h.table-bloom.no-compression.sst",
	}

	for _, prebuiltSST := range prebuiltSSTs {
		f, err := os.Open(filepath.FromSlash(prebuiltSST))
		if err != nil {
			t.Fatal(err)
		}

		r, err := NewReader(f, 0 /* dbNum */, 0 /* fileNum */, nil)
		if err != nil {
			t.Fatal(err)
		}

		t.Run(
			fmt.Sprintf("sst=%s", prebuiltSST),
			func(t *testing.T) { runTestReader(t, Options{}, "testdata/hamletreader", r) },
		)
	}
}

func runTestReader(t *testing.T, o Options, dir string, r *Reader) {
	makeIkeyValue := func(s string) (InternalKey, []byte) {
		j := strings.Index(s, ":")
		k := strings.Index(s, "=")
		seqNum, err := strconv.Atoi(s[j+1 : k])
		if err != nil {
			panic(err)
		}
		return base.MakeInternalKey([]byte(s[:j]), uint64(seqNum), InternalKeyKindSet), []byte(s[k+1:])
	}

	mem := vfs.NewMem()
	var dbNum uint64

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
				r, err = NewReader(f, dbNum, 0, &o)
				if err != nil {
					return err.Error()
				}
				dbNum++
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

				iter := newIterAdapter(r.NewIter(nil /* lower */, nil /* upper */))
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
					if iter.Valid() && checkValidPrefix(prefix, iter.Key().UserKey) {
						fmt.Fprintf(&b, "<%s:%d>", iter.Key().UserKey, iter.Key().SeqNum())
					} else if err := iter.Error(); err != nil {
						fmt.Fprintf(&b, "<err=%v>", err)
					} else {
						fmt.Fprintf(&b, ".")
					}
					b.WriteString("\n")
				}
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

func TestReaderCheckComparerMerger(t *testing.T) {
	const testTable = "test"

	testComparer := &base.Comparer{
		Name:      "test.comparer",
		Compare:   base.DefaultComparer.Compare,
		Equal:     base.DefaultComparer.Equal,
		Separator: base.DefaultComparer.Separator,
		Successor: base.DefaultComparer.Successor,
	}
	testMerger := &base.Merger{
		Name:  "test.merger",
		Merge: base.DefaultMerger.Merge,
	}
	opts := &Options{
		Comparer: testComparer,
		Merger:   testMerger,
	}

	mem := vfs.NewMem()
	f0, err := mem.Create(testTable)
	if err != nil {
		t.Fatal(err)
	}
	w := NewWriter(f0, opts, TableOptions{})
	if err := w.Set([]byte("test"), nil); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		comparers []*base.Comparer
		mergers   []*base.Merger
		expected  string
	}{
		{
			[]*base.Comparer{testComparer},
			[]*base.Merger{testMerger},
			"",
		},
		{
			[]*base.Comparer{testComparer, base.DefaultComparer},
			[]*base.Merger{testMerger, base.DefaultMerger},
			"",
		},
		{
			[]*base.Comparer{},
			[]*base.Merger{testMerger},
			"unknown comparer test.comparer",
		},
		{
			[]*base.Comparer{base.DefaultComparer},
			[]*base.Merger{testMerger},
			"unknown comparer test.comparer",
		},
		{
			[]*base.Comparer{testComparer},
			[]*base.Merger{},
			"unknown merger test.merger",
		},
		{
			[]*base.Comparer{testComparer},
			[]*base.Merger{base.DefaultMerger},
			"unknown merger test.merger",
		},
	}

	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			f1, err := mem.Open(testTable)
			if err != nil {
				t.Fatal(err)
			}
			comparers := make(Comparers)
			for _, comparer := range c.comparers {
				comparers[comparer.Name] = comparer
			}
			mergers := make(Mergers)
			for _, merger := range c.mergers {
				mergers[merger.Name] = merger
			}
			r, err := NewReader(f1, 0, 0, nil, comparers, mergers)
			if err != nil {
				if !strings.HasSuffix(err.Error(), c.expected) {
					t.Fatalf("expected %q, but found %q", c.expected, err.Error())
				}
			} else if c.expected != "" {
				t.Fatalf("expected %q, but found success", c.expected)
			}
			_ = r.Close()
		})
	}
}
func checkValidPrefix(prefix, key []byte) bool {
	return prefix == nil || bytes.HasPrefix(key, prefix)
}

func TestBytesIteratedCompressed(t *testing.T) {
	blockSizes := []int{10, 100, 1000, 4096, math.MaxInt32}
	for _, blockSize := range blockSizes {
		for _, indexBlockSize := range blockSizes {
			for _, numEntries := range []uint64{0, 1, 1e5} {
				r := buildTestTable(t, numEntries, blockSize, indexBlockSize, SnappyCompression)
				var bytesIterated, prevIterated uint64
				citer := r.NewCompactionIter(&bytesIterated)
				for key, _ := citer.First(); key != nil; key, _ = citer.Next() {
					if bytesIterated < prevIterated {
						t.Fatalf("bytesIterated moved backward: %d < %d", bytesIterated, prevIterated)
					}
					prevIterated = bytesIterated
				}

				expected := r.Properties.DataSize
				// There is some inaccuracy due to compression estimation.
				if bytesIterated < expected*99/100 || bytesIterated > expected*101/100 {
					t.Fatalf("bytesIterated: got %d, want %d", bytesIterated, expected)
				}
			}
		}
	}
}

func TestBytesIteratedUncompressed(t *testing.T) {
	blockSizes := []int{10, 100, 1000, 4096, math.MaxInt32}
	for _, blockSize := range blockSizes {
		for _, indexBlockSize := range blockSizes {
			for _, numEntries := range []uint64{0, 1, 1e5} {
				r := buildTestTable(t, numEntries, blockSize, indexBlockSize, NoCompression)
				var bytesIterated, prevIterated uint64
				citer := r.NewCompactionIter(&bytesIterated)
				for key, _ := citer.First(); key != nil; key, _ = citer.Next() {
					if bytesIterated < prevIterated {
						t.Fatalf("bytesIterated moved backward: %d < %d", bytesIterated, prevIterated)
					}
					prevIterated = bytesIterated
				}

				expected := r.Properties.DataSize
				if bytesIterated != expected {
					t.Fatalf("bytesIterated: got %d, want %d", bytesIterated, expected)
				}
			}
		}
	}
}

func buildTestTable(
	t *testing.T,
	numEntries uint64,
	blockSize, indexBlockSize int,
	compression Compression,
) *Reader {
	mem := vfs.NewMem()
	f0, err := mem.Create("test")
	if err != nil {
		t.Fatal(err)
	}
	defer f0.Close()

	w := NewWriter(f0, nil, TableOptions{
		BlockSize:      blockSize,
		IndexBlockSize: indexBlockSize,
		Compression:    compression,
		FilterPolicy:   nil,
	})

	var ikey InternalKey
	for i := uint64(0); i < numEntries; i++ {
		key := make([]byte, 8+i%3)
		value := make([]byte, i%100)
		binary.BigEndian.PutUint64(key, i)
		ikey.UserKey = key
		w.Add(ikey, value)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Re-open that filename for reading.
	f1, err := mem.Open("test")
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewReader(f1, 0, 0, &Options{
		Cache: cache.New(128 << 20),
	})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func buildBenchmarkTable(b *testing.B, blockSize, restartInterval int) (*Reader, [][]byte) {
	mem := vfs.NewMem()
	f0, err := mem.Create("bench")
	if err != nil {
		b.Fatal(err)
	}
	defer f0.Close()

	w := NewWriter(f0, nil, TableOptions{
		BlockRestartInterval: restartInterval,
		BlockSize:            blockSize,
		FilterPolicy:         nil,
	})

	var keys [][]byte
	var ikey InternalKey
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
	r, err := NewReader(f1, 0, 0, &Options{
		Cache: cache.New(128 << 20),
	})
	if err != nil {
		b.Fatal(err)
	}
	return r, keys
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
				var key *InternalKey
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
				var key *InternalKey
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
