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

package sstable

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/petermattis/pebble/bloom"
	"github.com/petermattis/pebble/db"
	"github.com/pkg/errors"
)

const (
	uncompressed = false
	compressed   = true

	noPrefixBloom = false
	prefixBloom   = true

	noFullKeyBloom = false
	fullKeyBloom   = true
)

var fixtureComparer = func() *db.Comparer {
	c := *db.DefaultComparer
	// NB: this is named as such only to match the built-in RocksDB comparer.
	c.Name = "leveldb.BytewiseComparator"
	c.Split = func(a []byte) (prefix []byte, stable bool, _ db.Version) {
		// NB: this matches logic in testdata/make-table.cc.
		if len(a) >= 3 {
			sl := a[:len(a)-2]
			// NB: the prefix is stable as for any extension of the key we'd
			// still only return the first two characters.
			return sl, true, 1
		}
		// NB: the prefix isn't stable; appending to the key would return a
		// different prefix.
		return a, false, 1
	}
	return &c
}()

type fixtureOpts struct {
	compression  bool
	fullKeyBloom bool
	prefixBloom  bool
}

func (o fixtureOpts) String() string {
	return fmt.Sprintf(
		"compressed=%t,fullKeyBloom=%t,prefixBloom=%t",
		o.compression, o.fullKeyBloom, o.prefixBloom,
	)
}

var fixtures = map[fixtureOpts]struct {
	filename string
	comparer *db.Comparer
}{
	{compressed, noFullKeyBloom, noPrefixBloom}: {
		"testdata/h.sst", nil,
	},
	{uncompressed, noFullKeyBloom, noPrefixBloom}: {
		"testdata/h.no-compression.sst", nil,
	},
	{uncompressed, fullKeyBloom, noPrefixBloom}: {
		"testdata/h.table-bloom.no-compression.sst", nil,
	},
	{uncompressed, noFullKeyBloom, prefixBloom}: {
		"testdata/h.table-bloom.no-compression.prefix_extractor.no_whole_key_filter.sst",
		fixtureComparer,
	},
}

func runTestFixtureOutput(opts fixtureOpts) error {
	fixture, ok := fixtures[opts]
	if !ok {
		return errors.Errorf("fixture missing: %+v", opts)
	}

	compression := db.NoCompression
	if opts.compression {
		compression = db.SnappyCompression
	}

	var fp db.FilterPolicy
	if opts.fullKeyBloom || opts.prefixBloom {
		fp = bloom.FilterPolicy(10)
	}
	ftype := db.TableFilter

	// Check that a freshly made table is byte-for-byte equal to a pre-made
	// table.
	want, err := ioutil.ReadFile(filepath.FromSlash(fixture.filename))
	if err != nil {
		return err
	}

	f, err := build(compression, fp, ftype, fixture.comparer)
	if err != nil {
		return err
	}
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	got := make([]byte, stat.Size())
	_, err = f.ReadAt(got, 0)
	if err != nil {
		return err
	}

	if !bytes.Equal(got, want) {
		i := 0
		for ; i < len(got) && i < len(want) && got[i] == want[i]; i++ {
		}
		ioutil.WriteFile("fail.txt", got, 0644)
		return fmt.Errorf("built table %s does not match pre-made table. From byte %d onwards,\ngot:\n% x\nwant:\n% x",
			fixture.filename, i, got[i:], want[i:])
	}
	return nil
}

func TestFixtureOutput(t *testing.T) {
	for opt := range fixtures {
		t.Run(opt.String(), func(t *testing.T) {
			if err := runTestFixtureOutput(opt); err != nil {
				t.Fatal(err)
			}
		})
	}
}
