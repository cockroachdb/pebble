// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestDownloadCursor(t *testing.T) {
	cmp := bytes.Compare
	objProvider := initDownloadTestProvider(t)

	var vers *manifest.Version
	var cursor downloadCursor
	datadriven.RunTest(t, "testdata/download_cursor", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			var err error
			const flushSplitBytes = 10 * 1024 * 1024
			l0Organizer := manifest.NewL0Organizer(base.DefaultComparer, flushSplitBytes)
			vers, err = manifest.ParseVersionDebug(base.DefaultComparer, l0Organizer, td.Input)
			if err != nil {
				td.Fatalf(t, "%v", err)
			}
			return vers.DebugString()

		case "cursor":
			var lower, upper string
			td.ScanArgs(t, "lower", &lower)
			td.ScanArgs(t, "upper", &upper)
			bounds := base.UserKeyBoundsEndExclusive([]byte(lower), []byte(upper))

			var buf strings.Builder
			for _, line := range strings.Split(td.Input, "\n") {
				fields := strings.Fields(line)
				fmt.Fprintf(&buf, "%s:\n", fields[0])
				switch cmd := fields[0]; cmd {
				case "start":
					cursor = downloadCursor{
						level:  0,
						key:    bounds.Start,
						seqNum: 0,
					}
					fmt.Fprintf(&buf, "  %s\n", cursor)

				case "next-file":
					f, level := cursor.NextExternalFile(cmp, objProvider, bounds, vers)
					if f != nil {
						// Verify that fCursor still points to this file.
						f2, level2 := makeCursorAtFile(f, level).NextExternalFile(cmp, objProvider, bounds, vers)
						if f != f2 {
							td.Fatalf(t, "nextExternalFile returned different file")
						}
						if level != level2 {
							td.Fatalf(t, "nextExternalFile returned different level")
						}
						cursor = makeCursorAfterFile(f, level)
					}
					fmt.Fprintf(&buf, "  file: %v  level: %d\n", f, level)

				case "iterate":
					for {
						f, level := cursor.NextExternalFile(cmp, objProvider, bounds, vers)
						if f == nil {
							fmt.Fprintf(&buf, "  no more files\n")
							break
						}
						fmt.Fprintf(&buf, "  file: %v  level: %d\n", f, level)
						cursor = makeCursorAfterFile(f, level)
					}

				default:
					td.Fatalf(t, "unknown cursor command %q", cmd)
				}
			}
			return buf.String()

		default:
			td.Fatalf(t, "unknown command: %s", td.Cmd)
			return ""
		}
	})
}

func TestDownloadTask(t *testing.T) {
	cmp := bytes.Compare
	objProvider := initDownloadTestProvider(t)
	d := DB{
		cmp:         cmp,
		objProvider: objProvider,
	}

	var vers *manifest.Version
	var l0Organizer *manifest.L0Organizer
	var task *downloadSpanTask
	printTask := func(b *strings.Builder) {
		for i := range task.bookmarks {
			fmt.Fprintf(b, "bookmark %d: %s  end-bound=%q\n", i, task.bookmarks[i].start, task.bookmarks[i].endBound.Key)
		}
		fmt.Fprintf(b, "cursor: %s\n", task.cursor.String())
	}
	datadriven.RunTest(t, "testdata/download_task", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			var err error
			const flushSplitBytes = 10 * 1024 * 1024
			l0Organizer = manifest.NewL0Organizer(base.DefaultComparer, flushSplitBytes)
			vers, err = manifest.ParseVersionDebug(base.DefaultComparer, l0Organizer, td.Input)
			if err != nil {
				td.Fatalf(t, "%v", err)
			}
			return ""

		case "set-compacting":
			// Parse a list of tables that are compacting and set compacting status on
			// all tables in the current version.
			compacting := make(map[base.TableNum]struct{})
			for _, f := range strings.Fields(td.Input) {
				n, err := strconv.Atoi(f)
				require.NoError(t, err)
				compacting[base.TableNum(n)] = struct{}{}
			}
			for _, lm := range vers.Levels {
				for f := range lm.All() {
					if _, ok := compacting[f.TableNum]; ok {
						f.CompactionState = manifest.CompactionStateCompacting
						delete(compacting, f.TableNum)
					} else {
						f.CompactionState = manifest.CompactionStateNotCompacting
					}
				}
			}
			for n := range compacting {
				td.Fatalf(t, "unknowm table %s", n)
			}
			return ""

		case "new-task":
			var start, end string
			td.ScanArgs(t, "start", &start)
			td.ScanArgs(t, "end", &end)
			var ok bool
			task, ok = d.newDownloadSpanTask(vers, DownloadSpan{
				StartKey: []byte(start),
				EndKey:   []byte(end),
			})
			if !ok {
				return "nothing to do"
			}
			var buf strings.Builder
			printTask(&buf)
			return buf.String()

		case "try-launch":
			if task == nil {
				return "no task"
			}
			var buf strings.Builder
			maxConcurrentDownloads := 1
			td.MaybeScanArgs(t, "max-concurrent-downloads", &maxConcurrentDownloads)
			task.testing.launchDownloadCompaction = func(f *manifest.TableMetadata) (chan error, bool) {
				ch := make(chan error, 1)
				if td.HasArg("fail") {
					fmt.Fprintf(&buf, "launching download for %s and cancelling it\n", f.TableNum)
					ch <- ErrCancelledCompaction
				} else {
					fmt.Fprintf(&buf, "downloading %s\n", f.TableNum)
					f.Virtual = false
					f.TableBacking.DiskFileNum = base.DiskFileNum(f.TableNum)
					ch <- nil
				}
				return ch, true
			}
			res := d.tryLaunchDownloadCompaction(task, vers, l0Organizer, compactionEnv{}, maxConcurrentDownloads)
			printTask(&buf)
			if res == downloadTaskCompleted {
				fmt.Fprintf(&buf, "task completed")
				task = nil
			}
			return buf.String()

		default:
			td.Fatalf(t, "unknown command: %s", td.Cmd)
			return ""
		}
	})
}

// initDownloadTestProvider returns an objstorage provider that is initialized
// with local backings 1 through 99 and external backings 100 through 199.
func initDownloadTestProvider(t *testing.T) objstorage.Provider {
	providerSettings := objstorageprovider.Settings{
		Logger: base.DefaultLogger,
	}
	providerSettings.Local.FS = vfs.NewMem()
	providerSettings.Remote.StorageFactory = remote.MakeSimpleFactory(map[remote.Locator]remote.Storage{
		"": remote.NewInMem(),
	})

	objProvider, err := objstorageprovider.Open(providerSettings)
	require.NoError(t, err)

	// Create some dummy backings. 1 through 99 are local, 100 through 199 are external.

	for i := base.DiskFileNum(1); i < 100; i++ {
		w, _, err := objProvider.Create(context.Background(), base.FileTypeTable, i, objstorage.CreateOptions{})
		require.NoError(t, err)
		require.NoError(t, w.Finish())
	}
	var remoteObjs []objstorage.RemoteObjectToAttach
	for i := base.DiskFileNum(100); i < 200; i++ {
		backing, err := objProvider.CreateExternalObjectBacking("", fmt.Sprintf("external-%d", i))
		require.NoError(t, err)
		remoteObjs = append(remoteObjs, objstorage.RemoteObjectToAttach{
			FileNum:  i,
			FileType: base.FileTypeTable,
			Backing:  backing,
		})
	}
	_, err = objProvider.AttachRemoteObjects(remoteObjs)
	require.NoError(t, err)
	return objProvider
}
