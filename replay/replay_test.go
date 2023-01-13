package replay

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datatest"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/rangekey"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

var expOpts = struct {
	L0CompactionConcurrency   int
	CompactionDebtConcurrency int
	MinDeletionRate           int
	ReadCompactionRate        int64
	ReadSamplingMultiplier    int64
	TableCacheShards          int
	KeyValidationFunc         func(userKey []byte) error
	ValidateOnIngest          bool
	LevelMultiplier           int
	MultiLevelCompaction      bool
	MaxWriterConcurrency      int
	ForceWriterParallelism    bool
	CPUWorkPermissionGranter  pebble.CPUWorkPermissionGranter
	PointTombstoneWeight      float64
	EnableValueBlocks         func() bool
	ShortAttributeExtractor   pebble.ShortAttributeExtractor
	RequiredInPlaceValueBound pebble.UserKeyPrefixBound
	LongAttributeExtractor    base.LongAttributeExtractor
	BlobValueSizeThreshold    int
}{TableCacheShards: 2}

func runReplayTest(t *testing.T, path string) {
	fs := vfs.NewMem()
	var ctx context.Context
	var r Runner
	var ct *datatest.CompactionTracker
	datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "cat":
			var buf bytes.Buffer
			for _, arg := range td.CmdArgs {
				f, err := fs.Open(arg.String())
				if err != nil {
					fmt.Fprintf(&buf, "%s: %s\n", arg, err)
					continue
				}
				io.Copy(&buf, f)
				require.NoError(t, f.Close())
			}
			return buf.String()
		case "corpus":
			for _, arg := range td.CmdArgs {
				t.Run(fmt.Sprintf("corpus/%s", arg.String()), func(t *testing.T) {
					collectCorpus(t, fs, arg.String())
				})
			}
			return ""
		case "list-files":
			return runListFiles(t, fs, td)
		case "replay":
			name := td.CmdArgs[0].String()
			pacerVariant := td.CmdArgs[1].String()
			var pacer Pacer
			if pacerVariant == "reference" {
				pacer = PaceByReferenceReadAmp{}
			} else if pacerVariant == "fixed" {
				i, err := strconv.Atoi(td.CmdArgs[2].String())
				require.NoError(t, err)
				pacer = PaceByFixedReadAmp(i)
			} else {
				pacer = Unpaced{}
			}

			// Convert the testdata/replay:235 datadriven command position into
			// a run directory suffixed with the line number: eg, 'run-235'
			lineOffset := strings.LastIndexByte(td.Pos, ':')
			require.Positive(t, lineOffset)
			runDir := fmt.Sprintf("run-%s", td.Pos[lineOffset+1:])
			if err := fs.MkdirAll(runDir, os.ModePerm); err != nil {
				return err.Error()
			}

			checkpointDir := fs.PathJoin(name, "checkpoint")
			ok, err := vfs.Clone(fs, fs, checkpointDir, runDir)
			if err != nil {
				return err.Error()
			} else if !ok {
				return fmt.Sprintf("%q does not exist", checkpointDir)
			}

			opts := &pebble.Options{
				FS:                        fs,
				Comparer:                  testkeys.Comparer,
				FormatMajorVersion:        pebble.FormatRangeKeys,
				L0CompactionFileThreshold: 1,
				Experimental:              expOpts,
			}
			ct = datatest.NewCompactionTracker(opts)

			r = Runner{
				RunDir:       runDir,
				WorkloadFS:   fs,
				WorkloadPath: name,
				Pacer:        pacer,
				Opts:         opts,
			}
			ctx = context.Background()
			if err := r.Run(ctx); err != nil {
				return err.Error()
			}
			return ""
		case "scan-keys":
			var buf bytes.Buffer
			it := r.d.NewIter(nil)
			defer it.Close()
			for valid := it.First(); valid; valid = it.Next() {
				fmt.Fprintf(&buf, "%s: %s\n", it.Key(), it.Value())
			}
			if err := it.Error(); err != nil {
				fmt.Fprintln(&buf, err.Error())
			}
			return buf.String()
		case "tree":
			return fs.String()
		case "wait-for-compactions":
			var target int
			if len(td.CmdArgs) == 1 {
				i, err := strconv.Atoi(td.CmdArgs[0].String())
				require.NoError(t, err)
				target = i
			}
			ct.WaitForInflightCompactionsToEqual(target)
			return ""
		case "wait":
			if _, err := r.Wait(); err != nil {
				return err.Error()
			}
			return ""
		case "close":
			if err := r.Close(); err != nil {
				return err.Error()
			}
			return ""
		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}

func TestReplay(t *testing.T) {
	runReplayTest(t, "testdata/replay")
}

func TestReplayPaced(t *testing.T) {
	runReplayTest(t, "testdata/replay_paced")
}

func TestLoadFlushedSSTableKeys(t *testing.T) {
	var buf bytes.Buffer
	var fileNums []base.FileNum
	opts := &pebble.Options{
		DisableAutomaticCompactions: true,
		EventListener: &pebble.EventListener{
			FlushEnd: func(info pebble.FlushInfo) {
				for _, tbl := range info.Output {
					fileNums = append(fileNums, tbl.FileNum)
				}
			},
		},
		FS:                 vfs.NewMem(),
		Comparer:           testkeys.Comparer,
		FormatMajorVersion: pebble.FormatRangeKeys,
	}
	d, err := pebble.Open("", opts)
	require.NoError(t, err)
	defer d.Close()

	var flushBufs flushBuffers
	datadriven.RunTest(t, "testdata/flushed_sstable_keys", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "commit":
			b := d.NewIndexedBatch()
			if err := datatest.DefineBatch(td, b); err != nil {
				return err.Error()
			}
			if err := b.Commit(nil); err != nil {
				return err.Error()
			}
			return ""
		case "flush":
			if err := d.Flush(); err != nil {
				return err.Error()
			}

			b := d.NewBatch()
			err := loadFlushedSSTableKeys(b, opts.FS, "", fileNums, opts.MakeReaderOptions(), &flushBufs)
			if err != nil {
				b.Close()
				return err.Error()
			}

			br, _ := pebble.ReadBatch(b.Repr())
			for kind, ukey, v, ok := br.Next(); ok; kind, ukey, v, ok = br.Next() {
				fmt.Fprintf(&buf, "%s.%s", ukey, kind)
				switch kind {
				case base.InternalKeyKindRangeDelete,
					base.InternalKeyKindRangeKeyDelete:
					fmt.Fprintf(&buf, "-%s", v)
				case base.InternalKeyKindSet,
					base.InternalKeyKindMerge:
					fmt.Fprintf(&buf, ": %s", v)
				case base.InternalKeyKindRangeKeySet, base.InternalKeyKindRangeKeyUnset:
					s, err := rangekey.Decode(base.MakeInternalKey(ukey, 0, kind), v, nil)
					if err != nil {
						return err.Error()
					}
					if kind == base.InternalKeyKindRangeKeySet {
						fmt.Fprintf(&buf, "-%s: %s → %s", s.End, s.Keys[0].Suffix, s.Keys[0].Value)
					} else {
						fmt.Fprintf(&buf, "-%s: %s", s.End, s.Keys[0].Suffix)
					}
				case base.InternalKeyKindDelete, base.InternalKeyKindSingleDelete:
				default:
					fmt.Fprintf(&buf, ": %x", v)
				}
				fmt.Fprintln(&buf)
			}

			s := buf.String()
			buf.Reset()
			require.NoError(t, b.Close())

			fileNums = fileNums[:0]
			return s
		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}

func collectCorpus(t *testing.T, fs *vfs.MemFS, name string) {
	require.NoError(t, fs.RemoveAll("build"))
	require.NoError(t, fs.MkdirAll("build", os.ModePerm))

	var d *pebble.DB
	var wc *WorkloadCollector
	defer func() {
		if d != nil {
			require.NoError(t, d.Close())
		}
	}()
	datadriven.RunTest(t, filepath.Join("testdata", "corpus", name), func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "commit":
			b := d.NewBatch()
			if err := datatest.DefineBatch(td, b); err != nil {
				return err.Error()
			}
			if err := b.Commit(nil); err != nil {
				return err.Error()
			}
			return ""
		case "flush":
			require.NoError(t, d.Flush())
			return ""
		case "list-files":
			return runListFiles(t, fs, td)
		case "open":
			wc = NewWorkloadCollector("build")
			opts := pebble.Options{
				Comparer:                    testkeys.Comparer,
				DisableAutomaticCompactions: true,
				FormatMajorVersion:          pebble.FormatRangeKeys,
				FS:                          fs,
				MaxManifestFileSize:         96,
				Experimental:                expOpts,
			}
			wc.Attach(&opts)
			var err error
			d, err = pebble.Open("build", &opts)
			require.NoError(t, err)
			return ""
		case "close":
			err := d.Close()
			require.NoError(t, err)
			d = nil
			return ""
		case "start":
			require.NoError(t, fs.MkdirAll(name, os.ModePerm))
			require.NotNil(t, wc)
			wc.Start(fs, name)
			require.NoError(t, d.Checkpoint(fs.PathJoin(name, "checkpoint"), pebble.WithFlushedWAL()))
			return "started"
		case "stat":
			var buf bytes.Buffer
			for _, arg := range td.CmdArgs {
				fi, err := fs.Stat(arg.String())
				if err != nil {
					fmt.Fprintf(&buf, "%s: %s\n", arg.String(), err)
					continue
				}
				fmt.Fprintf(&buf, "%s:\n", arg.String())
				fmt.Fprintf(&buf, "  size: %d\n", fi.Size())
			}
			return buf.String()
		case "stop":
			wc.Stop()
			<-wc.done
			return "stopped"
		case "tree":
			return fs.String()
		case "make-file":
			dir := td.CmdArgs[0].String()
			require.NoError(t, fs.MkdirAll(dir, os.ModePerm))
			fT := td.CmdArgs[1].String()
			filePath := fs.PathJoin(dir, td.CmdArgs[2].String())

			if fT != "file" {
				fileNumInt, err := strconv.Atoi(td.CmdArgs[2].String())
				require.NoError(t, err)
				fileNum := base.FileNum(fileNumInt)
				switch fT {
				case "table":
					filePath = base.MakeFilepath(fs, dir, base.FileTypeTable, fileNum)
				case "log":
					filePath = base.MakeFilepath(fs, dir, base.FileTypeLog, fileNum)
				case "manifest":
					filePath = base.MakeFilepath(fs, dir, base.FileTypeManifest, fileNum)
				}
			}
			f, err := fs.Create(filePath)
			require.NoError(t, err)
			b, err := hex.DecodeString(strings.ReplaceAll(td.Input, "\n", ""))
			require.NoError(t, err)
			_, err = f.Write(b)
			require.NoError(t, err)
			return "created"
		case "find-workload-files":
			var buf bytes.Buffer
			dir := td.CmdArgs[0].String()
			m, s, err := findWorkloadFiles(dir, fs)

			fmt.Fprintln(&buf, "manifests")
			sort.Strings(m)
			for _, elem := range m {
				fmt.Fprintf(&buf, "  %s\n", elem)
			}
			var res []string
			for key := range s {
				res = append(res, key.String())
			}
			sort.Strings(res)

			fmt.Fprintln(&buf, "sstables")
			for _, elem := range res {
				fmt.Fprintf(&buf, "  %s\n", elem)
			}
			fmt.Fprintln(&buf, "error")
			if err != nil {
				fmt.Fprintf(&buf, "  %s\n", err.Error())
			}
			return buf.String()
		case "find-manifest-start":
			var buf bytes.Buffer
			dir := td.CmdArgs[0].String()
			m, _, err := findWorkloadFiles(dir, fs)
			sort.Strings(m)
			require.NoError(t, err)
			i, o, err := findManifestStart(dir, fs, m)
			errString := "nil"
			if err != nil {
				errString = err.Error()
			}
			fmt.Fprintf(&buf, "index: %d, offset: %d, error: %s\n", i, o, errString)
			return buf.String()
		case "delete-all":
			err := fs.RemoveAll(td.CmdArgs[0].String())
			if err != nil {
				return err.Error()
			}
			return ""
		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}

func TestCollectCorpus(t *testing.T) {
	fs := vfs.NewMem()
	datadriven.Walk(t, "testdata/corpus", func(t *testing.T, path string) {
		collectCorpus(t, fs, filepath.Base(path))
		fs = vfs.NewMem()
	})
}

func runListFiles(t *testing.T, fs vfs.FS, td *datadriven.TestData) string {
	var buf bytes.Buffer
	for _, arg := range td.CmdArgs {
		name := arg.String()
		ls, err := fs.List(name)
		if err != nil {
			fmt.Fprintf(&buf, "%s: %s\n", name, err)
			continue
		}
		sort.Strings(ls)
		fmt.Fprintf(&buf, "%s:\n", name)
		for _, dirent := range ls {
			fmt.Fprintf(&buf, "  %s\n", dirent)
		}
	}
	return buf.String()
}

func TestBenchmarkString(t *testing.T) {
	m := Metrics{
		TotalWriteAmp:       5.6,
		WriteStalls:         105,
		WriteStallsDuration: time.Minute,
	}
	m.Ingest.BytesIntoL0 = 5 << 20
	m.Ingest.BytesWeightedByLevel = 9 << 20
	require.Equal(t, strings.TrimSpace(`
BenchmarkReplay/tpcc/CompactionCounts      1              0    compactions              0        default              0         delete              0        elision              0           move              0           read              0        rewrite              0     multilevel
BenchmarkReplay/tpcc/WriteAmp              1            5.6           wamp
BenchmarkReplay/tpcc/WriteStalls           1            105         stalls             60     stall-secs
BenchmarkReplay/tpcc/IngestedIntoL0        1        5242880          bytes
BenchmarkReplay/tpcc/IngestWeightedByLevel 1        9437184          bytes`),
		strings.TrimSpace(m.BenchmarkString("tpcc")))
}
