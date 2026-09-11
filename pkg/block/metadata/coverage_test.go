// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package metadata

import (
	"testing"

	"github.com/efficientgo/core/testutil"
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

func TestSourceCoverageCoveringBlocks(t *testing.T) {
	src1, src2 := ulid.MustNew(1, nil), ulid.MustNew(2, nil)
	block := func(id uint64, mint, maxt int64, sources ...ulid.ULID) *Meta {
		m := &Meta{}
		m.ULID = ulid.MustNew(id, nil)
		m.MinTime, m.MaxTime = mint, maxt
		m.Compaction.Sources = sources
		return m
	}
	coverage := SourceCoverage{}
	a, b, c, d := block(10, 0, 50, src1), block(11, 50, 100, src1, src2), block(12, 100, 150, src2), block(13, 0, 100, ulid.MustNew(3, nil))
	for _, m := range []*Meta{a, b, c, d} {
		coverage.Add(m)
	}

	raw := block(20, 0, 150, src1, src2)
	// Every block sharing a source and touching the range, once each, and
	// nothing from an unrelated lineage.
	testutil.Equals(t, []ulid.ULID{a.ULID, b.ULID, c.ULID}, coverage.CoveringBlocks(raw, raw.MinTime, raw.MaxTime-1))
	testutil.Equals(t, []ulid.ULID{a.ULID}, coverage.CoveringBlocks(raw, 0, 49))
	testutil.Equals(t, []ulid.ULID{b.ULID, c.ULID}, coverage.CoveringBlocks(raw, 50, 149))
	testutil.Equals(t, 0, len(coverage.CoveringBlocks(raw, 200, 300)))
	testutil.Equals(t, 0, len(coverage.CoveringBlocks(block(21, 0, 150, ulid.MustNew(4, nil)), 0, 149)))
}
