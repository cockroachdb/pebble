package db

import (
	"testing"
)

func TestLevelOptions(t *testing.T) {
	var opts *Options
	opts = opts.EnsureDefaults()

	testCases := []struct {
		level          int
		maxBytes       int64
		targetFileSize int64
	}{
		{0, 64 << 20, 4 << 20},
		{1, (10 * 64) << 20, (2 * 4) << 20},
		{2, (100 * 64) << 20, (4 * 4) << 20},
		{3, (1000 * 64) << 20, (8 * 4) << 20},
		{4, (10000 * 64) << 20, (16 * 4) << 20},
		{5, (100000 * 64) << 20, (32 * 4) << 20},
		{6, (1000000 * 64) << 20, (64 * 4) << 20},
	}
	for _, c := range testCases {
		l := opts.Level(c.level)
		if c.maxBytes != l.MaxBytes {
			t.Fatalf("%d: expected max-bytes %d, but found %d",
				c.level, c.maxBytes, l.MaxBytes)
		}
		if c.targetFileSize != l.TargetFileSize {
			t.Fatalf("%d: expected target-file-size %d, but found %d",
				c.level, c.targetFileSize, l.TargetFileSize)
		}
	}
}
