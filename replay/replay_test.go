package replay

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/internal/datatest"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/rangekey"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// TODO(jackson,leon): Add datadriven end-to-end unit tests.
// TODO(jackson,leon): Add datadriven unit test for findWorkloadFiles.
// TODO(jackson,leon): Add datadriven unit test for findManifestStart.

func TestReplay(t *testing.T) {
	fs := vfs.NewMem()
	var ctx context.Context
	var r Runner
	datadriven.RunTest(t, "testdata/replay", func(td *datadriven.TestData) string {
		switch td.Cmd {
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
				DisableAutomaticCompactions: true,
				FS:                          fs,
				Comparer:                    testkeys.Comparer,
				FormatMajorVersion:          pebble.FormatRangeKeys,
			}

			r = Runner{
				RunDir:       runDir,
				WorkloadFS:   fs,
				WorkloadPath: name,
				Pacer:        Unpaced{},
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
		case "wait":
			if err := r.Wait(); err != nil {
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
	datadriven.RunTest(t, "testdata/flushed_sstable_keys", func(td *datadriven.TestData) string {
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
	datadriven.RunTest(t, filepath.Join("testdata", "corpus", name), func(td *datadriven.TestData) string {
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
			}
			wc.Attach(&opts)
			var err error
			d, err = pebble.Open("build", &opts)
			require.NoError(t, err)
			return ""
		case "start":
			require.NoError(t, fs.MkdirAll(name, os.ModePerm))
			require.NotNil(t, wc)
			wc.Start(fs, name)
			require.NoError(t, d.Checkpoint(fs.PathJoin(name, "checkpoint")))
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
		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}

func TestCollectCorpus(t *testing.T) {
	fs := vfs.NewMem()
	datadriven.Walk(t, "testdata/corpus", func(t *testing.T, path string) {
		collectCorpus(t, fs, filepath.Base(path))
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
