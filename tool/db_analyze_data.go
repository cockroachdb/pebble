// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"slices"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/humanize"
	"github.com/cockroachdb/pebble/sstable/compressionanalyzer"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/tokenbucket"
	"github.com/spf13/cobra"
)

// Minimum samples required per analyzer bucket to report the bucket at all.
const minSamples = 10

func (d *dbT) runAnalyzeData(cmd *cobra.Command, args []string) {
	stdout, stderr := cmd.OutOrStdout(), cmd.OutOrStderr()
	isTTY := isTTY(stdout)

	dir := args[0]
	if err := d.initOptions(dir); err != nil {
		fmt.Fprintf(stderr, "error initializing options: %s\n", err)
		return
	}
	rng := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	files, err := makeFileSet(d.opts.FS, dir, rng)
	if err != nil {
		fmt.Fprintf(stderr, "error loading file list: %s\n", err)
		return
	}
	if files.Done() {
		fmt.Fprintf(stderr, "no sstables found\n")
		return
	}
	totalSize := files.TotalSize()
	// We do not recalculate the target size every time we refresh the file list.
	// If the database is growing rapidly, we might not be able to keep up.
	targetSize := totalSize
	if d.analyzeData.samplePercent > 0 && d.analyzeData.samplePercent < 100 {
		targetSize = (totalSize*int64(d.analyzeData.samplePercent) + 99) / 100
	}
	var readLimiter *tokenbucket.TokenBucket
	if d.analyzeData.readMBPerSec > 0 {
		readLimiter = &tokenbucket.TokenBucket{}
		rate := tokenbucket.TokensPerSecond(d.analyzeData.readMBPerSec) * (1 << 20)
		burst := tokenbucket.Tokens(rate * 0.1)
		readLimiter.Init(rate, burst)
	}
	if isTTY {
		fmt.Fprintf(stdout, "Found %d files, total size %s.\n", len(files.files), humanize.Bytes.Int64(totalSize))
		if d.analyzeData.readMBPerSec > 0 {
			fmt.Fprintf(stdout, "Limiting read bandwidth to %s/s.\n", humanize.Bytes.Int64(int64(d.analyzeData.readMBPerSec)<<20))
		} else {
			fmt.Fprintf(stdout, "No read bandwidth limiting.\n")
		}
		if d.analyzeData.samplePercent > 0 && d.analyzeData.samplePercent < 100 {
			fmt.Fprintf(stdout, "Stopping after samping %d%% of the data", d.analyzeData.samplePercent)
			if d.analyzeData.timeout > 0 {
				fmt.Fprintf(stdout, " or after %s", d.analyzeData.timeout)
			}
			fmt.Fprintf(stdout, ".\n")
		} else if d.analyzeData.timeout > 0 {
			fmt.Fprintf(stdout, "Stopping after %s.", d.analyzeData.timeout)
		}
	}

	startTime := time.Now()
	lastReportTime := startTime

	analyzer := compressionanalyzer.NewFileAnalyzer(readLimiter, d.opts.MakeReaderOptions())
	var sampled int64
	const reportPeriod = 10 * time.Second
	for {
		shouldStop := files.Done() || sampled >= targetSize ||
			(d.analyzeData.timeout > 0 && time.Since(startTime) > d.analyzeData.timeout)
		// Every 10 seconds, we:
		//  - print the current results and progress (if on a tty);
		//  - write the output CSV file (so we get some results even if the command
		//    is interrupted);
		//  - refresh the list of files.
		if shouldStop || time.Since(lastReportTime) > reportPeriod {
			if isTTY {
				// Clear screen.
				fmt.Fprint(stdout, "\033[2J\033[H")
			}
			if isTTY || shouldStop {
				partialResults := analyzer.Buckets().String(minSamples)
				fmt.Fprintf(stdout, "\n%s\n", partialResults)
				percentage := min(float64(sampled*100)/float64(totalSize), 100)
				if files.Done() {
					percentage = 100
				}
				fmt.Fprintf(stdout, "Sampled %.2f%% (%s)\n", percentage, humanize.Bytes.Int64(sampled))
			}
			if err := analyzeSaveCSVFile(analyzer, d.analyzeData.outputCSVFile); err != nil {
				fmt.Fprintf(stderr, "error writing CSV file: %s\n", err)
				return
			}
			if shouldStop {
				return
			}
			if err := files.Refresh(); err != nil {
				fmt.Fprintf(stderr, "error loading file list: %s\n", err)
				return
			}
			lastReportTime = time.Now()
		}
		// Sample a file and analyze it.
		path, size := files.Sample()
		err := analyzer.SSTable(context.Background(), d.opts.FS, path)
		if err != nil {
			// We ignore errors from files that are deleted from under us.
			if !errors.Is(err, os.ErrNotExist) {
				fmt.Fprintf(stderr, "error reading file %s: %s\n", path, err)
			}
			continue
		}
		sampled += size
	}
}

func analyzeSaveCSVFile(a *compressionanalyzer.FileAnalyzer, path string) error {
	csv := a.Buckets().ToCSV(minSamples)
	return os.WriteFile(path, []byte(csv), 0o666)
}

// We avoid files that are very large to prevent excessive memory usage. Note
// that we have seen cases where large files contain a giant top index block, so
// even getting the block layout of the file would use a lot of memory.
const analyzeMaxFileSize = 512 * 1024 * 1024

type fileSet struct {
	fs  vfs.FS
	dir string
	rng *rand.Rand

	files     []fileInSet
	sampleIdx []int
}

type fileInSet struct {
	filename    string
	fileType    base.FileType
	size        int64
	samplingKey float64
	sampled     bool
}

func makeFileSet(fs vfs.FS, dir string, rng *rand.Rand) (fileSet, error) {
	s := fileSet{
		fs:  fs,
		dir: dir,
		rng: rng,
	}
	return s, s.Refresh()
}

// generateSamplingKey generates a key for a file of a given size. When sorted
// by increasing keys, the files correspond to a random weighted sampling
// without replacement. This is the Efraimidis–Spirakis algorithm.
func samplingKey(rng *rand.Rand, size int64) float64 {
	return -math.Log(rng.Float64()) / float64(size)
}

func (s *fileSet) Refresh() error {
	filenames, err := s.fs.List(s.dir)
	if err != nil {
		return err
	}
	slices.Sort(filenames)
	oldFiles := slices.Clone(s.files)
	s.files = s.files[:0]
	now := time.Now()

	newFile := func(filename string) {
		// New file.
		fileType, _, ok := base.ParseFilename(s.fs, filename)
		if !ok || fileType != base.FileTypeTable {
			return
		}
		fileInfo, err := s.fs.Stat(s.fs.PathJoin(s.dir, filename))
		if err != nil {
			// Files can get deleted from under us, so we tolerate errors.
			return
		}
		// We ignore files that are less than 15 seconds old. This is to avoid
		// trying to read a file that is still being written.
		if now.Sub(fileInfo.ModTime()) < 15*time.Second {
			return
		}
		size := fileInfo.Size()
		if size == 0 || size > analyzeMaxFileSize {
			return
		}
		s.files = append(s.files, fileInSet{
			filename:    filename,
			fileType:    fileType,
			size:        size,
			samplingKey: samplingKey(s.rng, size),
		})
	}

	// Go through the two lists of sorted files.
	for i, j := 0, 0; i < len(filenames) || j < len(oldFiles); {
		switch {
		case i < len(filenames) && (j == len(oldFiles) || filenames[i] < oldFiles[j].filename):
			// New file.
			newFile(filenames[i])
			i++
		case j < len(oldFiles) && (j == len(filenames) || filenames[i] > oldFiles[j].filename):
			// File deleted.
			j++
		default:
			// Existing file.
			s.files = append(s.files, oldFiles[j])
			i++
			j++
		}
	}
	// Generate the samples.
	s.sampleIdx = s.sampleIdx[:0]
	for i := range s.files {
		if !s.files[i].sampled {
			s.sampleIdx = append(s.sampleIdx, i)
		}
	}
	slices.SortFunc(s.sampleIdx, func(i, j int) int {
		return cmp.Compare(s.files[i].samplingKey, s.files[j].samplingKey)
	})
	return nil
}

func (s *fileSet) TotalSize() int64 {
	var sum int64
	for i := range s.files {
		sum += s.files[i].size
	}
	return sum
}

func (s *fileSet) Done() bool {
	return len(s.sampleIdx) == 0
}

// Sample returns a random file from the set (which was not previously sampled),
// weighted by size.
func (s *fileSet) Sample() (path string, size int64) {
	idx := s.sampleIdx[0]
	s.sampleIdx = s.sampleIdx[1:]
	s.files[idx].sampled = true
	return s.fs.PathJoin(s.dir, s.files[idx].filename), s.files[idx].size
}

func isTTY(out io.Writer) bool {
	f, ok := out.(*os.File)
	if !ok {
		return false
	}

	// 2. Stat the file and check ModeCharDevice
	fi, err := f.Stat()
	if err != nil {
		return false
	}

	return fi.Mode()&os.ModeCharDevice != 0
}
