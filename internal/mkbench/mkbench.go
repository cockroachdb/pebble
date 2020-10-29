// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// mkbench is a utility for processing the raw nightly benchmark data in JSON
// data that can be visualized by docs/js/app.js. The raw data is expected to
// be stored in dated directories underneath the "data/" directory:
//
//   data/YYYYMMDD/.../<file>
//
// The files are expected to be bzip2 compressed. Within each file mkbench
// looks for Go-bench-style lines of the form:
//
//   Benchmark<name> %d %f ops/sec %d read %d write %f r-amp %f w-amp
//
// The output is written to "data.js". In order to avoid reading all of the raw
// data to regenerate "data.js" on every run, mkbench first reads "data.js",
// noting which days have already been processed and exluding files in those
// directories from being read. This has the additional effect of merging the
// existing "data.js" with new raw data, which avoids needing to have all of
// the raw data present to construct a new "data.js" (only the new raw data is
// necessary).
//
// The nightly Pebble benchmarks are orchestrated from the CockroachDB
// repo:
//
//   https://github.com/cockroachdb/cockroach/blob/master/build/teamcity-nightly-pebble.sh
package main

import (
	"bufio"
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/cockroachdb/errors/oserror"
)

type run struct {
	opsSec     float64
	readBytes  int64
	writeBytes int64
	readAmp    float64
	writeAmp   float64
}

func (r run) formatCSV() string {
	return fmt.Sprintf("%.1f,%d,%d,%.1f,%.1f",
		r.opsSec, r.readBytes, r.writeBytes, r.readAmp, r.writeAmp)
}

type workload struct {
	days map[string][]run // data -> runs
}

type loader struct {
	cookedDays map[string]bool      // set of already cooked days
	data       map[string]*workload // workload name -> workload data
}

func newLoader() *loader {
	return &loader{
		cookedDays: make(map[string]bool),
		data:       make(map[string]*workload),
	}
}

func (l *loader) addRun(name, day string, r run) {
	w := l.data[name]
	if w == nil {
		w = &workload{days: make(map[string][]run)}
		l.data[name] = w
	}
	w.days[day] = append(w.days[day], r)
}

func (l *loader) loadCooked(path string) {
	data, err := ioutil.ReadFile(path)
	if oserror.IsNotExist(err) {
		return
	}
	if err != nil {
		log.Fatal(err)
	}

	data = bytes.TrimSpace(data)

	prefix := []byte("data = ")
	if !bytes.HasPrefix(data, prefix) {
		log.Fatalf("missing '%s' prefix", prefix)
	}
	data = bytes.TrimPrefix(data, prefix)

	suffix := []byte(";")
	if !bytes.HasSuffix(data, suffix) {
		log.Fatalf("missing '%s' suffix", suffix)
	}
	data = bytes.TrimSuffix(data, suffix)

	m := make(map[string]string)
	if err := json.Unmarshal(data, &m); err != nil {
		log.Fatal(err)
	}

	for name, data := range m {
		s := bufio.NewScanner(strings.NewReader(data))
		for s.Scan() {
			line := s.Text()
			line = strings.Replace(line, ",", " ", -1)

			var r run
			var day string
			n, err := fmt.Sscanf(line, "%s %f %d %d %f %f",
				&day, &r.opsSec, &r.readBytes, &r.writeBytes, &r.readAmp, &r.writeAmp)
			if err != nil || n != 6 {
				log.Fatalf("%s: %+v", line, err)
			}
			l.cookedDays[day] = true
			l.addRun(name, day, r)
		}
	}
}

func (l *loader) loadRaw(dir string) {
	var walkFn filepath.WalkFunc
	walkFn = func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info == nil {
			return nil
		}
		if !info.Mode().IsRegular() && !info.Mode().IsDir() {
			info, err = os.Stat(path)
			if err == nil && info.Mode().IsDir() {
				_ = filepath.Walk(path+string(os.PathSeparator), walkFn)
			}
			return nil
		}

		parts := strings.Split(path, string(os.PathSeparator))
		if len(parts) < 2 {
			return nil // stumble forward on invalid paths
		}

		day := parts[1]
		if l.cookedDays[day] {
			return nil
		}

		f, err := os.Open(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%+v\n", err)
			return nil // stumble forward on error
		}
		defer f.Close()

		r := io.Reader(f)
		if strings.HasSuffix(path, ".bz2") {
			r = bzip2.NewReader(f)
		} else if strings.HasSuffix(path, ".gz") {
			var err error
			r, err = gzip.NewReader(f)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%+v\n", err)
				return nil // stumble forward on error
			}
		}

		s := bufio.NewScanner(r)
		for s.Scan() {
			line := s.Text()
			if !strings.HasPrefix(line, "Benchmark") {
				continue
			}

			var r run
			var name string
			var ops int64
			n, err := fmt.Sscanf(line,
				"Benchmark%s %d %f ops/sec %d read %d write %f r-amp %f w-amp",
				&name, &ops, &r.opsSec, &r.readBytes, &r.writeBytes, &r.readAmp, &r.writeAmp)
			if err != nil || n != 7 {
				fmt.Fprintf(os.Stderr, "%s: %v\n", s.Text(), err)
				// Stumble forward on error.
				continue
			}

			fmt.Fprintf(os.Stderr, "%s: adding %s\n", day, name)
			l.addRun(name, day, r)
		}
		return nil
	}

	_ = filepath.Walk(dir, walkFn)
}

func (l *loader) cook(path string) {
	m := make(map[string]string)
	for name, workload := range l.data {
		m[name] = l.cookWorkload(workload)
	}

	out := []byte("data = ")
	out = append(out, prettyJSON(m)...)
	out = append(out, []byte(";")...)
	if err := ioutil.WriteFile(path, out, 0644); err != nil {
		log.Fatal(err)
	}
}

func (l *loader) cookWorkload(w *workload) string {
	days := make([]string, 0, len(w.days))
	for day := range w.days {
		days = append(days, day)
	}
	sort.Strings(days)

	var buf bytes.Buffer
	for _, day := range days {
		fmt.Fprintf(&buf, "%s,%s\n", day, l.cookDay(w.days[day]))
	}
	return buf.String()
}

func (l *loader) cookDay(runs []run) string {
	if len(runs) == 1 {
		return runs[0].formatCSV()
	}

	// The benchmarks show significant run-to-run variance due to
	// instance-to-instance performance variability on AWS. We attempt to smooth
	// out this variance by excluding outliers: any run that is more than one
	// stddev from the average, and then taking the average of the remaining
	// runs. Note that the runs on a given day are all from the same SHA, so this
	// smoothing will not affect exceptional day-to-day performance changes.

	var sum float64
	for i := range runs {
		sum += runs[i].opsSec
	}
	mean := sum / float64(len(runs))

	var sum2 float64
	for i := range runs {
		v := runs[i].opsSec - mean
		sum2 += v * v
	}

	stddev := math.Sqrt(sum2 / float64(len(runs)))
	lo := mean - stddev
	hi := mean + stddev

	var avg run
	var count int
	for i := range runs {
		r := &runs[i]
		if r.opsSec < lo || r.opsSec > hi {
			continue
		}
		count++
		avg.opsSec += r.opsSec
		avg.readBytes += r.readBytes
		avg.writeBytes += r.writeBytes
		avg.readAmp += r.readAmp
		avg.writeAmp += r.writeAmp
	}

	avg.opsSec /= float64(count)
	avg.readBytes /= int64(count)
	avg.writeBytes /= int64(count)
	avg.readAmp /= float64(count)
	avg.writeAmp /= float64(count)
	return avg.formatCSV()
}

func prettyJSON(v interface{}) []byte {
	data, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		log.Fatal(err)
	}
	return data
}

func main() {
	const raw = "data"
	const cooked = "data.js"

	log.SetFlags(log.Lshortfile)

	l := newLoader()
	l.loadCooked(cooked)
	l.loadRaw(raw)
	l.cook(cooked)
}
