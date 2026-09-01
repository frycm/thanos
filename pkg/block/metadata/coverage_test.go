// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package metadata

import (
	"testing"

	"github.com/oklog/ulid/v2"
)

// The oracle uses individual milliseconds, independently of the interval
// sorting and joining algorithm. It checks split coverage, gaps, overlaps,
// duplicate intervals, negative timestamps and multiple source lineages.
func FuzzSourceCoverage(f *testing.F) {
	f.Add([]byte{0, 0, 16, 1, 0, 16})
	f.Add([]byte{0, 0, 8, 0, 9, 16, 1, 0, 16})
	f.Add([]byte{0, 0, 8, 0, 8, 16, 1, 0, 16})
	f.Add([]byte{0, 8, 9, 1, 8, 9, 0, 8, 9})
	f.Fuzz(func(t *testing.T, data []byte) {
		coverage := SourceCoverage{}
		var points [2][16]bool
		for i := 0; i+2 < len(data) && i < 150; i += 3 {
			source := int(data[i] % 2)
			start, end := int(data[i+1]%17), int(data[i+2]%17)
			if start >= end {
				continue
			}
			m := &Meta{}
			m.MinTime, m.MaxTime = int64(start-8), int64(end-8)
			m.Compaction.Sources = []ulid.ULID{ulid.MustNew(uint64(source+1), nil)}
			coverage.Add(m)
			for ts := start; ts < end; ts++ {
				points[source][ts] = true
			}
		}
		m := &Meta{}
		m.MinTime, m.MaxTime = -8, 8
		m.Compaction.Sources = []ulid.ULID{ulid.MustNew(1, nil), ulid.MustNew(2, nil)}
		for start := -8; start < 8; start++ {
			for end := start; end < 8; end++ {
				want := true
				for source := range 2 {
					for ts := start; ts <= end; ts++ {
						want = want && points[source][ts+8]
					}
				}
				if got := coverage.Covers(m, int64(start), int64(end)); got != want {
					t.Fatalf("coverage of [%d,%d]: got %v, want %v; data=%v", start, end, got, want, data)
				}
			}
		}
	})
}
