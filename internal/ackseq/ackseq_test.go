// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package ackseq

import (
	"strings"
	"testing"
)

func TestAckInOrder(t *testing.T) {
	s := New(0)
	for i := 0; i < 10; i++ {
		seq := s.Next()
		count, err := s.Ack(seq)
		if err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("expected count 1, got %d", count)
		}
	}
}

func TestAckOutOfOrder(t *testing.T) {
	s := New(0)
	seqs := make([]uint64, 5)
	for i := range seqs {
		seqs[i] = s.Next()
	}
	// Ack in reverse order; only the last ack should advance the base.
	for i := len(seqs) - 1; i > 0; i-- {
		count, err := s.Ack(seqs[i])
		if err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("expected count 0, got %d", count)
		}
	}
	count, err := s.Ack(seqs[0])
	if err != nil {
		t.Fatal(err)
	}
	if count != len(seqs) {
		t.Fatalf("expected count %d, got %d", len(seqs), count)
	}
}

func TestAckDoubleAck(t *testing.T) {
	s := New(0)
	seq := s.Next()
	if _, err := s.Ack(seq); err != nil {
		t.Fatal(err)
	}
	seq = s.Next()
	if _, err := s.Ack(seq); err != nil {
		t.Fatal(err)
	}
	// Ack seq again (double ack, but base has advanced past it).
	_, err := s.Ack(seq)
	if err == nil {
		t.Fatal("expected error for double ack")
	}
	if !strings.Contains(err.Error(), "out of valid range") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAckBelowBase(t *testing.T) {
	s := New(5)
	seq := s.Next() // returns 5
	if _, err := s.Ack(seq); err != nil {
		t.Fatal(err)
	}
	// Ack a seqNum below the initial base.
	_, err := s.Ack(0)
	if err == nil {
		t.Fatal("expected error for seqNum below base")
	}
	if !strings.Contains(err.Error(), "out of valid range") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAckBeyondWindow(t *testing.T) {
	s := New(0)
	// Don't call Next() enough times; just try to ack a huge seqNum.
	_, err := s.Ack(windowSize)
	if err == nil {
		t.Fatal("expected error for seqNum beyond window")
	}
	if !strings.Contains(err.Error(), "out of valid range") {
		t.Fatalf("unexpected error: %v", err)
	}
}
