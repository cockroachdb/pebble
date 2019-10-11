// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import "testing"

func TestMetricsFormat(t *testing.T) {
	var m Metrics
	m.BlockCache.Size = 1
	m.BlockCache.Count = 2
	m.BlockCache.Hits = 3
	m.BlockCache.Misses = 4
	m.Compact.Count = 5
	m.Compact.EstimatedDebt = 6
	m.Flush.Count = 7
	m.Filter.Hits = 8
	m.Filter.Misses = 9
	m.MemTable.Size = 10
	m.MemTable.Count = 11
	m.TableCache.Size = 12
	m.TableCache.Count = 13
	m.TableCache.Hits = 14
	m.TableCache.Misses = 15
	m.TableIters = 16
	m.WAL.Files = 17
	m.WAL.ObsoleteFiles = 18
	m.WAL.Size = 19
	m.WAL.BytesIn = 20
	m.WAL.BytesWritten = 21

	for i := range m.Levels {
		l := &m.Levels[i]
		base := uint64((i + 1) * 100)
		l.NumFiles = int64(base) + 1
		l.Size = base + 2
		l.Score = float64(base) + 3
		l.BytesIn = base + 4
		l.BytesIngested = base + 4
		l.BytesMoved = base + 6
		l.BytesRead = base + 7
		l.BytesWritten = base + 8
		l.TablesCompacted = base + 9
		l.TablesFlushed = base + 10
		l.TablesIngested = base + 11
		l.TablesMoved = base + 12
	}

	const expected = `
__level_____count____size___score______in__ingest(sz_cnt)____move(sz_cnt)___write(sz_cnt)____read___w-amp
    WAL        17    19 B       -    20 B       -       -       -       -    21 B       -       -     1.1
      0       101   102 B  103.00   104 B   104 B     111   106 B     112   108 B     219   107 B     1.0
      1       201   202 B  203.00   204 B   204 B     211   206 B     212   208 B     419   207 B     1.0
      2       301   302 B  303.00   304 B   304 B     311   306 B     312   308 B     619   307 B     1.0
      3       401   402 B  403.00   404 B   404 B     411   406 B     412   408 B     819   407 B     1.0
      4       501   502 B  503.00   504 B   504 B     511   506 B     512   508 B   1.0 K   507 B     1.0
      5       601   602 B  603.00   604 B   604 B     611   606 B     612   608 B   1.2 K   607 B     1.0
      6       701   702 B  703.00   704 B   704 B     711   706 B     712   708 B   1.4 K   707 B     1.0
  total      2807   2.7 K       -   2.8 K   2.8 K   2.9 K   2.8 K   2.9 K   5.6 K   5.7 K   2.8 K     2.0
  flush         7
compact         5     6 B          (size == estimated-debt)
 memtbl        11    10 B
 bcache         2     1 B   42.9%  (score == hit-rate)
 tcache        13    12 B   48.3%  (score == hit-rate)
 titers        16
 filter         -       -   47.1%  (score == utility)
`
	if s := "\n" + m.String(); expected != s {
		t.Fatalf("expected%s\nbut found%s", expected, s)
	}
}
