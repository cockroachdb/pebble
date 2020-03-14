package manifest

import (
	"fmt"
	"io"
	"math"
	"os"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/record"
	"github.com/stretchr/testify/require"
)

/*

L0SubLevels:
file count: 609, sublevels: 11, intervals: 1218, flush keys: 379
0.10: file count: 2, bytes: 3775993, width (mean, max): 3, 4, interval range: [436, 442]
0.9: file count: 54, bytes: 151465577, width (mean, max): 3, 7, interval range: [3, 444]
0.8: file count: 1, bytes: 701471, width (mean, max): 444, 444, interval range: [0, 443]
0.7: file count: 99, bytes: 266905768, width (mean, max): 4, 9, interval range: [252, 931]
0.6: file count: 80, bytes: 273839761, width (mean, max): 9, 372, interval range: [1, 1014]
0.5: file count: 98, bytes: 356833026, width (mean, max): 4, 11, interval range: [344, 1047]
0.4: file count: 140, bytes: 653752749, width (mean, max): 5, 19, interval range: [161, 1192]
0.3: file count: 1, bytes: 163509468, width (mean, max): 1204, 1204, interval range: [2, 1205]
0.2: file count: 9, bytes: 28629286, width (mean, max): 5, 10, interval range: [95, 256]
0.1: file count: 41, bytes: 177376921, width (mean, max): 5, 12, interval range: [55, 1217]
0.0: file count: 84, bytes: 367989736, width (mean, max): 4, 11, interval range: [4, 1216]

0.8 may be a wide flush or a wide range tombstone.
0.3 must be the output of an intra-L0 compaction

The following is the list of L0 => Lbase that can run concurrently. The first one picked up the
wide file in 0.3. This was without the heuristic that tries to cut a compaction at 100MB

topLevel: 8
checked level: 8, [0, 443], files [0, 1)
checked level: 7, [0, 443], files [0, 21)
checked level: 6, [0, 443], files [0, 1)
checked level: 5, [0, 443], files [0, 12)
checked level: 4, [0, 443], files [0, 31)
checked level: 3, [0, 1205], files [0, 1)
checked level: 2, [0, 1205], files [0, 9)
checked level: 1, [0, 1205], files [0, 38)
checked level: 0, [0, 1205], files [0, 81)
0: compaction: filecount: 195, bytes: 926702251, interval: [0, 1205], seed depth: 7

topLevel: 1
checked level: 1, [1207, 1211], files [38, 39)
checked level: 0, [1206, 1211], files [81, 83)
1: compaction: filecount: 3, bytes: 14652713, interval: [1206, 1211], seed depth: 2

topLevel: 1
checked level: 1, [1213, 1214], files [39, 40)
checked level: 0, [1212, 1216], files [83, 84)
2: compaction: filecount: 2, bytes: 16596725, interval: [1212, 1216], seed depth: 2

With the 100MB heuristic we can run 26 concurrent compactions without involving the bad
file at sublevel 3.

0: compaction: filecount: 4, bytes: 11399669, interval: [237, 246], seed depth: 3
1: compaction: filecount: 3, bytes: 14652713, interval: [1206, 1211], seed depth: 2
2: compaction: filecount: 2, bytes: 16596725, interval: [1212, 1216], seed depth: 2
3: compaction: filecount: 3, bytes: 11965275, interval: [225, 236], seed depth: 3
4: compaction: filecount: 3, bytes: 10829141, interval: [111, 120], seed depth: 3
5: compaction: filecount: 3, bytes: 14185941, interval: [247, 256], seed depth: 3
6: compaction: filecount: 3, bytes: 11400008, interval: [217, 224], seed depth: 3
7: compaction: filecount: 2, bytes: 7622622, interval: [210, 215], seed depth: 2
8: compaction: filecount: 2, bytes: 7621555, interval: [201, 207], seed depth: 2
9: compaction: filecount: 2, bytes: 7640036, interval: [179, 190], seed depth: 2
10: compaction: filecount: 3, bytes: 11391135, interval: [103, 110], seed depth: 3
11: compaction: filecount: 2, bytes: 7617182, interval: [163, 168], seed depth: 2
12: compaction: filecount: 3, bytes: 11391738, interval: [95, 102], seed depth: 3
13: compaction: filecount: 2, bytes: 7622224, interval: [171, 176], seed depth: 2
14: compaction: filecount: 2, bytes: 7622492, interval: [193, 198], seed depth: 2
15: compaction: filecount: 2, bytes: 7616986, interval: [122, 128], seed depth: 2
16: compaction: filecount: 2, bytes: 7618598, interval: [290, 294], seed depth: 2
17: compaction: filecount: 2, bytes: 7619495, interval: [284, 288], seed depth: 2
18: compaction: filecount: 2, bytes: 7584766, interval: [87, 94], seed depth: 2
19: compaction: filecount: 2, bytes: 10150518, interval: [130, 136], seed depth: 2
20: compaction: filecount: 2, bytes: 7541698, interval: [82, 86], seed depth: 2
21: compaction: filecount: 2, bytes: 7616889, interval: [138, 142], seed depth: 2
22: compaction: filecount: 2, bytes: 6774834, interval: [144, 151], seed depth: 2
23: compaction: filecount: 3, bytes: 7558659, interval: [71, 80], seed depth: 2
24: compaction: filecount: 2, bytes: 7549280, interval: [63, 70], seed depth: 2
25: compaction: filecount: 3, bytes: 7618214, interval: [154, 160], seed depth: 2

With the same 100MB heuristic for intra-L0 we can run 52 concurrent intra-L0 compactions:

0: intra-L0 compaction: filecount: 8, bytes: 30353229, interval: [733, 748], seed depth: 4
1: intra-L0 compaction: filecount: 9, bytes: 30337597, interval: [653, 670], seed depth: 4
2: intra-L0 compaction: filecount: 10, bytes: 30338843, interval: [749, 768], seed depth: 4
3: intra-L0 compaction: filecount: 8, bytes: 30354023, interval: [637, 652], seed depth: 4
4: intra-L0 compaction: filecount: 7, bytes: 25272106, interval: [623, 636], seed depth: 4
5: intra-L0 compaction: filecount: 8, bytes: 25254673, interval: [919, 934], seed depth: 4
6: intra-L0 compaction: filecount: 6, bytes: 18745964, interval: [489, 500], seed depth: 4
7: intra-L0 compaction: filecount: 4, bytes: 15177549, interval: [879, 886], seed depth: 4
8: intra-L0 compaction: filecount: 6, bytes: 20241966, interval: [603, 614], seed depth: 4
9: intra-L0 compaction: filecount: 6, bytes: 18791138, interval: [591, 602], seed depth: 4
10: intra-L0 compaction: filecount: 6, bytes: 18782030, interval: [887, 898], seed depth: 4
11: intra-L0 compaction: filecount: 5, bytes: 15169913, interval: [581, 590], seed depth: 4
12: intra-L0 compaction: filecount: 5, bytes: 20232753, interval: [571, 580], seed depth: 4
13: intra-L0 compaction: filecount: 4, bytes: 15159107, interval: [615, 622], seed depth: 4
14: intra-L0 compaction: filecount: 5, bytes: 20234377, interval: [869, 878], seed depth: 4
15: intra-L0 compaction: filecount: 8, bytes: 25254650, interval: [817, 832], seed depth: 4
16: intra-L0 compaction: filecount: 9, bytes: 30335614, interval: [553, 570], seed depth: 4
17: intra-L0 compaction: filecount: 7, bytes: 25258832, interval: [523, 536], seed depth: 4
18: intra-L0 compaction: filecount: 4, bytes: 15176601, interval: [683, 690], seed depth: 4
19: intra-L0 compaction: filecount: 8, bytes: 30339306, interval: [537, 552], seed depth: 4
20: intra-L0 compaction: filecount: 5, bytes: 15176635, interval: [779, 788], seed depth: 4
21: intra-L0 compaction: filecount: 5, bytes: 15153738, interval: [513, 522], seed depth: 4
22: intra-L0 compaction: filecount: 5, bytes: 20240738, interval: [899, 908], seed depth: 4
23: intra-L0 compaction: filecount: 6, bytes: 20231485, interval: [501, 512], seed depth: 4
24: intra-L0 compaction: filecount: 8, bytes: 30353275, interval: [853, 868], seed depth: 4
25: intra-L0 compaction: filecount: 5, bytes: 15178381, interval: [909, 918], seed depth: 4
26: intra-L0 compaction: filecount: 5, bytes: 18780256, interval: [789, 798], seed depth: 4
27: intra-L0 compaction: filecount: 5, bytes: 20241051, interval: [799, 808], seed depth: 4
28: intra-L0 compaction: filecount: 5, bytes: 18782174, interval: [691, 700], seed depth: 4
29: intra-L0 compaction: filecount: 6, bytes: 20233883, interval: [671, 682], seed depth: 4
30: intra-L0 compaction: filecount: 10, bytes: 30340272, interval: [833, 852], seed depth: 4
31: intra-L0 compaction: filecount: 7, bytes: 25253556, interval: [719, 732], seed depth: 4
32: intra-L0 compaction: filecount: 5, bytes: 20232470, interval: [769, 778], seed depth: 4
33: intra-L0 compaction: filecount: 4, bytes: 15178456, interval: [711, 718], seed depth: 4
34: intra-L0 compaction: filecount: 5, bytes: 20240381, interval: [701, 710], seed depth: 4
35: intra-L0 compaction: filecount: 4, bytes: 15169773, interval: [481, 488], seed depth: 4
36: intra-L0 compaction: filecount: 4, bytes: 15176939, interval: [809, 816], seed depth: 4
37: intra-L0 compaction: filecount: 4, bytes: 14087891, interval: [967, 974], seed depth: 3
38: intra-L0 compaction: filecount: 6, bytes: 22805328, interval: [975, 986], seed depth: 3
39: intra-L0 compaction: filecount: 3, bytes: 11403605, interval: [961, 966], seed depth: 3
40: intra-L0 compaction: filecount: 3, bytes: 15201876, interval: [955, 960], seed depth: 3
41: intra-L0 compaction: filecount: 5, bytes: 22804212, interval: [945, 954], seed depth: 3
42: intra-L0 compaction: filecount: 3, bytes: 8169809, interval: [994, 1000], seed depth: 3
43: intra-L0 compaction: filecount: 4, bytes: 15194220, interval: [473, 480], seed depth: 3
44: intra-L0 compaction: filecount: 4, bytes: 13663665, interval: [465, 472], seed depth: 3
45: intra-L0 compaction: filecount: 5, bytes: 22787681, interval: [935, 944], seed depth: 3
46: intra-L0 compaction: filecount: 4, bytes: 13654751, interval: [451, 458], seed depth: 3
47: intra-L0 compaction: filecount: 3, bytes: 9121694, interval: [445, 450], seed depth: 3
48: intra-L0 compaction: filecount: 3, bytes: 9120998, interval: [459, 464], seed depth: 3
49: intra-L0 compaction: filecount: 3, bytes: 11403623, interval: [987, 992], seed depth: 3
50: intra-L0 compaction: filecount: 5, bytes: 22786166, interval: [1001, 1010], seed depth: 3
51: intra-L0 compaction: filecount: 4, bytes: 18264902, interval: [1011, 1018], seed depth: 3

Fast enough (all the time is in the initial sorting and searching in the string keys):
BenchmarkL0SubLevelsInit-16           	L0 filecount: 609
    2000	    960921 ns/op	  480683 B/op	    4600 allocs/op
BenchmarkL0SubLevelsInitAndPick-16    	L0 filecount: 609
    2000	   1018633 ns/op	  541058 B/op	    4630 allocs/op
*/

func readManifest(filename string) (*Version, error) {
	f, err := os.Open("testdata/MANIFEST_import")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	rr := record.NewReader(f, 0 /* logNum */)
	var v *Version
	for {
		r, err := rr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		var ve VersionEdit
		if err = ve.Decode(r); err != nil {
			return nil, err
		}
		var bve BulkVersionEdit
		bve.Accumulate(&ve)
		if v, _, err = bve.Apply(v, base.DefaultComparer.Compare, DefaultLogger, base.DefaultFormatter); err != nil {
			return nil, err
		}
	}
	fmt.Printf("L0 filecount: %d\n", len(v.Files[0]))
	return v, nil
}

func TestL0SubLevels_LargeImportL0(t *testing.T) {
	v, err := readManifest("testdata/MANIFEST_import")
	require.NoError(t, err)

	subLevels := NewL0SubLevels(v.Files[0], base.DefaultComparer.Compare, DefaultLogger, base.DefaultFormatter, 5<<20)
	fmt.Printf("L0SubLevels:\n%s\n\n", subLevels)

	for i := 0; ; i++ {
		c := subLevels.PickBaseCompaction(2, nil)
		if c == nil {
			break
		}
		fmt.Printf("%d: base compaction: filecount: %d, bytes: %d, interval: [%d, %d], seed depth: %d\n",
			i, len(c.Files), c.fileBytes, c.minIntervalIndex, c.maxIntervalIndex, c.seedIntervalStackDepthReduction)
		subLevels.UpdateStateForStartedCompaction(c, true)
	}

	for i := 0; ; i++ {
		c := subLevels.PickIntraL0Compaction(math.MaxUint64, 2)
		if c == nil {
			break
		}
		fmt.Printf("%d: intra-L0 compaction: filecount: %d, bytes: %d, interval: [%d, %d], seed depth: %d\n",
			i, len(c.Files), c.fileBytes, c.minIntervalIndex, c.maxIntervalIndex, c.seedIntervalStackDepthReduction)
		subLevels.UpdateStateForStartedCompaction(c, false)
	}
}

func BenchmarkL0SubLevelsInit(b *testing.B) {
	v, err := readManifest("testdata/MANIFEST_import")
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		sl := NewL0SubLevels(v.Files[0], base.DefaultComparer.Compare, DefaultLogger, base.DefaultFormatter, 5<<20)
		if sl == nil {
			panic("bug")
		}
	}
}

func BenchmarkL0SubLevelsInitAndPick(b *testing.B) {
	v, err := readManifest("testdata/MANIFEST_import")
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		sl := NewL0SubLevels(v.Files[0], base.DefaultComparer.Compare, DefaultLogger, base.DefaultFormatter, 5<<20)
		if sl == nil {
			panic("bug")
		}
		c := sl.PickBaseCompaction(2, nil)
		if c == nil {
			panic("bug")
		}
	}
}
