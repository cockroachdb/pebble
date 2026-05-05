// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package bench

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/cockroachkvs"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/replay"
	"github.com/cockroachdb/pebble/sstable/tablefilters"
	"github.com/cockroachdb/pebble/vfs"
)

// ReplayConfig configures the replay benchmark.
type ReplayConfig struct {
	Name             string
	Pacer            PacerFlag
	RunDir           string
	Count            int
	MaxWritesMB      uint64
	StreamLogs       bool
	CheckpointDir    string
	IgnoreCheckpoint bool
	OptionsString    string
	MaxCacheSize     int64

	cleanUpFuncs []func() error
}

// DefaultReplayConfig returns a ReplayConfig with the same defaults as the
// cmd/pebble CLI.
func DefaultReplayConfig() ReplayConfig {
	return ReplayConfig{
		Pacer: PacerFlag{Pacer: replay.PaceByFixedReadAmp(10)},
		Count: 1,
	}
}

// args returns the command-line arguments needed to re-invoke the benchmark
// with the same options. Used for the multi-run exec'ing path.
func (c *ReplayConfig) args() (args []string) {
	if c.Name != "" {
		args = append(args, "--name", c.Name)
	}
	if c.Pacer.spec != "" {
		args = append(args, "--pacer", c.Pacer.spec)
	}
	if c.RunDir != "" {
		args = append(args, "--run-dir", c.RunDir)
	}
	if c.Count != 0 {
		args = append(args, "--count", fmt.Sprint(c.Count))
	}
	if c.MaxWritesMB != 0 {
		args = append(args, "--max-writes", fmt.Sprint(c.MaxWritesMB))
	}
	if c.MaxCacheSize != 0 {
		args = append(args, "--max-cache-size", fmt.Sprint(c.MaxCacheSize))
	}
	if c.StreamLogs {
		args = append(args, "--stream-logs")
	}
	if c.CheckpointDir != "" {
		args = append(args, "--checkpoint-dir", c.CheckpointDir)
	}
	if c.IgnoreCheckpoint {
		args = append(args, "--ignore-checkpoint")
	}
	if c.OptionsString != "" {
		args = append(args, "--options", c.OptionsString)
	}
	return args
}

// RunReplay runs the replay benchmark with the given workload path. The
// stdout writer captures progress output. verbose enables Options dumping.
// If c.Count > 1 the binary execs itself for each subsequent run.
func (c *ReplayConfig) RunReplay(stdout io.Writer, verbose bool, workloadPath string) error {
	if c.IgnoreCheckpoint && c.CheckpointDir != "" {
		return errors.Newf("cannot provide both --checkpoint-dir and --ignore-checkpoint")
	}

	if err := c.runOnce(stdout, verbose, workloadPath); err != nil {
		return err
	}
	c.Count--

	// If necessary, run it again. We exec ourselves so that each run is
	// independent of any state accumulated within the Go runtime / Pebble.
	if c.Count > 0 {
		fmt.Printf("%d runs remaining.", c.Count)
		executable, err := os.Executable()
		if err != nil {
			return err
		}
		execArgs := append(append([]string{executable, "bench", "replay"}, c.args()...), workloadPath)
		_ = syscall.Exec(executable, execArgs, os.Environ())
	}
	return nil
}

func (c *ReplayConfig) runOnce(stdout io.Writer, verbose bool, workloadPath string) error {
	defer func() { _ = c.cleanUp() }()
	if c.Name == "" {
		c.Name = vfs.Default.PathBase(workloadPath)
	}

	r := &replay.Runner{
		RunDir:       c.RunDir,
		WorkloadFS:   vfs.Default,
		WorkloadPath: workloadPath,
		Pacer:        c.Pacer,
		Opts:         &pebble.Options{},
	}
	if c.MaxWritesMB > 0 {
		r.MaxWriteBytes = c.MaxWritesMB * (1 << 20)
	}
	if err := c.initRunDir(r); err != nil {
		return err
	}
	if err := c.initOptions(r); err != nil {
		return err
	}
	if verbose {
		fmt.Fprintln(stdout, "Options:")
		fmt.Fprintln(stdout, r.Opts.String())
	}

	ctx := context.Background()
	if err := r.Run(ctx); err != nil {
		return errors.Wrapf(err, "starting workload")
	}

	m, err := r.Wait()
	if err != nil {
		return errors.Wrapf(err, "waiting for workload to complete")
	}
	if err := r.Close(); err != nil {
		return errors.Wrapf(err, "cleaning up")
	}
	fmt.Fprintln(stdout, "Workload complete.")
	if err := m.WriteBenchmarkString(c.Name, stdout); err != nil {
		return err
	}
	for _, plot := range m.Plots(120 /* width */, 30 /* height */) {
		fmt.Fprintln(stdout, plot.Name)
		fmt.Fprintln(stdout, plot.Plot)
		fmt.Fprintln(stdout)
	}
	fmt.Fprintln(stdout, m.Final.String())
	return nil
}

func (c *ReplayConfig) initRunDir(r *replay.Runner) error {
	if r.RunDir == "" {
		wd, err := os.Getwd()
		if err != nil {
			return err
		}
		r.RunDir, err = os.MkdirTemp(wd, "replay-")
		if err != nil {
			return err
		}
		c.cleanUpFuncs = append(c.cleanUpFuncs, func() error {
			return os.RemoveAll(r.RunDir)
		})
	}
	if !c.IgnoreCheckpoint {
		checkpointDir := c.getCheckpointDir(r)
		fmt.Printf("%s: Attempting to initialize with checkpoint %q.\n", time.Now().Format(time.RFC3339), checkpointDir)
		ok, err := vfs.Clone(
			r.WorkloadFS,
			vfs.Default,
			checkpointDir,
			filepath.Join(r.RunDir),
			vfs.CloneTryLink)
		if err != nil {
			return err
		}
		if !ok {
			return errors.Newf("no checkpoint %q exists; you may re-run with --ignore-checkpoint", checkpointDir)
		}
		fmt.Printf("%s: Run directory initialized with checkpoint %q.\n", time.Now().Format(time.RFC3339), checkpointDir)
	}
	return nil
}

func (c *ReplayConfig) initOptions(r *replay.Runner) error {
	if !c.IgnoreCheckpoint {
		ls, err := r.WorkloadFS.List(c.getCheckpointDir(r))
		if err != nil {
			return err
		}
		sort.Strings(ls)
		var optionsFilepath string
		for _, l := range ls {
			path := r.WorkloadFS.PathJoin(r.WorkloadPath, "checkpoint", l)
			typ, _, ok := base.ParseFilename(r.WorkloadFS, path)
			if ok && typ == base.FileTypeOptions {
				optionsFilepath = path
			}
		}
		f, err := r.WorkloadFS.Open(optionsFilepath)
		if err != nil {
			return err
		}
		o, err := io.ReadAll(f)
		if err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		if err := r.Opts.Parse(string(o), c.parseHooks()); err != nil {
			return err
		}
	}
	if err := c.ParseCustomOptions(c.OptionsString, r.Opts); err != nil {
		return err
	}

	if c.StreamLogs {
		r.Opts.AddEventListener(pebble.MakeLoggingEventListener(pebble.DefaultLogger))
	}
	r.Opts.EnsureDefaults()
	return nil
}

func (c *ReplayConfig) getCheckpointDir(r *replay.Runner) string {
	if c.CheckpointDir != "" {
		return c.CheckpointDir
	}
	return r.WorkloadFS.PathJoin(r.WorkloadPath, `checkpoint`)
}

func (c *ReplayConfig) parseHooks() *pebble.ParseHooks {
	return &pebble.ParseHooks{
		NewComparer: makeComparer,
		NewFilterPolicy: func(name string) (pebble.TableFilterPolicy, error) {
			if p, ok := tablefilters.PolicyFromName(name); ok {
				return p, nil
			}
			return nil, errors.Errorf("invalid filter policy name %q", name)
		},
		NewMerger: makeMerger,
	}
}

// ParseCustomOptions parses Pebble Options passed through a CLI flag.
// Ordinarily Pebble Options are specified through an INI file with newlines
// delimiting fields. That doesn't translate well to a CLI interface, so this
// function accepts fields are that delimited by any whitespace.
func (c *ReplayConfig) ParseCustomOptions(optsStr string, opts *pebble.Options) error {
	if optsStr == "" {
		return nil
	}
	value := strings.TrimSpace(optsStr)
	var buf bytes.Buffer
	for len(value) > 0 {
		i := strings.IndexFunc(value, func(r rune) bool {
			return r == '[' || unicode.IsSpace(r)
		})
		switch {
		case i == -1:
			buf.WriteString(value)
			value = value[len(value):]
		case value[i] == '[':
			j := i + strings.IndexRune(value[i:], ']')
			buf.WriteString(value[:j+1])
			value = value[j+1:]
		case unicode.IsSpace(rune(value[i])):
			buf.WriteString(value[:i])
			buf.WriteRune('\n')
			value = strings.TrimSpace(value[i+1:])
		}
	}
	if err := opts.Parse(buf.String(), c.parseHooks()); err != nil {
		return err
	}
	if c.MaxCacheSize != 0 && opts.CacheSize > c.MaxCacheSize {
		opts.CacheSize = c.MaxCacheSize
	}
	return nil
}

func (c *ReplayConfig) cleanUp() error {
	for _, f := range c.cleanUpFuncs {
		if err := f(); err != nil {
			return err
		}
	}
	return nil
}

func makeComparer(name string) (*pebble.Comparer, error) {
	switch name {
	case base.DefaultComparer.Name:
		return base.DefaultComparer, nil
	case "cockroach_comparator":
		return &cockroachkvs.Comparer, nil
	default:
		return nil, errors.Newf("unrecognized comparer %q", name)
	}
}

func makeMerger(name string) (*pebble.Merger, error) {
	switch name {
	case base.DefaultMerger.Name:
		return base.DefaultMerger, nil
	case "cockroach_merge_operator":
		merger := new(pebble.Merger)
		merger.Merge = func(key, value []byte) (pebble.ValueMerger, error) {
			return &overwriteValueMerger{value: append([]byte{}, value...)}, nil
		}
		merger.Name = name
		return merger, nil
	default:
		return nil, errors.Newf("unrecognized comparer %q", name)
	}
}

// PacerFlag implements pflag.Value (and the standard flag.Value) so cmd/pebble
// can bind a --pacer flag directly into a ReplayConfig.
type PacerFlag struct {
	replay.Pacer
	spec string
}

var _ flag.Value = (*PacerFlag)(nil)

func (f *PacerFlag) String() string { return f.spec }
func (f *PacerFlag) Type() string   { return "pacer" }

func (f *PacerFlag) Set(spec string) error {
	f.spec = spec
	switch {
	case spec == "unpaced":
		f.Pacer = replay.Unpaced{}
	case spec == "reference-ramp":
		f.Pacer = replay.PaceByReferenceReadAmp{}
	case strings.HasPrefix(spec, "fixed-ramp="):
		rAmp, err := strconv.Atoi(strings.TrimPrefix(spec, "fixed-ramp="))
		if err != nil {
			return errors.Newf("unable to parse fixed r-amp: %s", err)
		}
		f.Pacer = replay.PaceByFixedReadAmp(rAmp)
	default:
		return errors.Newf("unrecognized pacer spec: %q", errors.Safe(spec))
	}
	return nil
}

type overwriteValueMerger struct {
	value []byte
}

func (o *overwriteValueMerger) MergeNewer(value []byte) error {
	o.value = append(o.value[:0], value...)
	return nil
}

func (o *overwriteValueMerger) MergeOlder(value []byte) error {
	return nil
}

func (o *overwriteValueMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	return o.value, nil, nil
}
