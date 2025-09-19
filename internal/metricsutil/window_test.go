// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build go1.25

package metricsutil

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/cockroachdb/crlib/crtime"
)

func TestWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		startTime := crtime.NowMono()
		w := NewWindow[time.Duration](startTime.Elapsed)
		w.Start()
		defer w.Stop()
		<-time.After(5 * time.Minute)
		v, x := w.TenMinutesAgo()
		if v != 0 || x != 0 {
			t.Fatalf("expected empty, got %v %v", v, x)
		}
		<-time.After(5*time.Minute + time.Second)
		v, x = w.TenMinutesAgo()
		if x := x.Elapsed(); x < 9*time.Minute || x > 11*time.Minute {
			t.Fatalf("expected ~10md, got %s", x)
		}
		if v < 0 || v > time.Minute {
			t.Fatalf("expected ~0, got %s", v)
		}

		<-time.After(20*time.Minute + time.Second)
		v, x = w.TenMinutesAgo()
		if x := x.Elapsed(); x < 9*time.Minute || x > 11*time.Minute {
			t.Fatalf("expected ~10m, got %s", x)
		}
		if v < 19*time.Minute || v > 21*time.Minute {
			t.Fatalf("expected ~0, got %s", v)
		}

		<-time.After(30*time.Minute + time.Second)
		v, x = w.OneHourAgo()
		if x := x.Elapsed(); x < 50*time.Minute || x > 70*time.Minute {
			t.Fatalf("expected ~1h, got %s", x)
		}
		if v < 0 || v > 10*time.Minute {
			t.Fatalf("expected 0-10m, got %s", v)
		}
	})
}
