// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package metadata

import (
	"cmp"
	"slices"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/oklog/ulid/v2"
)

// The oracle uses individual milliseconds, independently of the interval
// sweep. It checks split coverage, gaps, overlaps, duplicate intervals,
// negative timestamps and covers holding one, the other or both sources.
func FuzzSourcesCovered(f *testing.F) {
	f.Add([]byte{0, 0, 16, 1, 0, 16})
	f.Add([]byte{0, 0, 8, 0, 9, 16, 1, 0, 16})
	f.Add([]byte{0, 0, 8, 0, 8, 16, 1, 0, 16})
	f.Add([]byte{0, 8, 9, 1, 8, 9, 0, 8, 9})
	f.Add([]byte{2, 0, 16})
	f.Fuzz(func(t *testing.T, data []byte) {
		sources := []ulid.ULID{ulid.MustNew(1, nil), ulid.MustNew(2, nil)}
		var covers []*Meta
		var points [2][16]bool
		for i := 0; i+2 < len(data) && i < 150; i += 3 {
			// 0 and 1 hold one source, 2 holds both.
			which := int(data[i] % 3)
			start, end := int(data[i+1]%17), int(data[i+2]%17)
			if start >= end {
				continue
			}
			m := &Meta{}
			m.MinTime, m.MaxTime = int64(start-8), int64(end-8)
			if which == 2 {
				m.Compaction.Sources = sources
			} else {
				m.Compaction.Sources = []ulid.ULID{sources[which]}
			}
			covers = append(covers, m)
			for ts := start; ts < end; ts++ {
				for source := range 2 {
					if which == 2 || which == source {
						points[source][ts] = true
					}
				}
			}
		}
		slices.SortStableFunc(covers, func(a, b *Meta) int { return cmp.Compare(a.MinTime, b.MinTime) })
		m := &Meta{}
		m.MinTime, m.MaxTime = -8, 8
		m.Compaction.Sources = sources
		for start := -8; start < 8; start++ {
			for end := start; end < 8; end++ {
				want := true
				for source := range 2 {
					for ts := start; ts <= end; ts++ {
						want = want && points[source][ts+8]
					}
				}
				if got := SourcesCovered(m, covers, int64(start), int64(end)); got != want {
					t.Fatalf("coverage of [%d,%d]: got %v, want %v; data=%v", start, end, got, want, data)
				}
			}
		}
	})
}

func TestSourcesCovered_NoSourcesIsNeverCovered(t *testing.T) {
	cover := &Meta{}
	cover.MinTime, cover.MaxTime = 0, 100
	cover.Compaction.Sources = []ulid.ULID{ulid.MustNew(1, nil)}
	orphan := &Meta{}
	orphan.MinTime, orphan.MaxTime = 0, 100
	testutil.Equals(t, false, SourcesCovered(orphan, []*Meta{cover}, 0, 99))
}

func TestHeldSources(t *testing.T) {
	ids := func(ns ...uint64) []ulid.ULID {
		var out []ulid.ULID
		for _, n := range ns {
			out = append(out, ulid.MustNew(n, nil))
		}
		return out
	}
	for _, tc := range []struct {
		name           string
		coarser, finer []ulid.ULID
		held           []int
	}{
		{name: "all", coarser: ids(1, 2, 3), finer: ids(1, 3), held: []int{0, 1}},
		{name: "none", coarser: ids(4), finer: ids(1, 3), held: nil},
		{name: "first only", coarser: ids(1, 2), finer: ids(1, 3), held: []int{0}},
		{name: "last only", coarser: ids(3), finer: ids(1, 3), held: []int{1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			held := HeldSources(tc.coarser, tc.finer)
			var got []int
			for i := range tc.finer {
				if Holds(held, i) {
					got = append(got, i)
				}
			}
			testutil.Equals(t, tc.held, got)
		})
	}

	// Beyond one word of the bitset.
	var finer []ulid.ULID
	for n := range uint64(130) {
		finer = append(finer, ulid.MustNew(n+1, nil))
	}
	held := HeldSources(finer[100:], finer)
	for i := range finer {
		testutil.Equals(t, i >= 100, Holds(held, i), "source %d", i)
	}
	testutil.Assert(t, HeldSources(finer, finer) == nil, "a block holding all sources has no bitset")
}

func TestSortedSources(t *testing.T) {
	a, b, c := ulid.MustNew(1, nil), ulid.MustNew(2, nil), ulid.MustNew(3, nil)
	sorted := []ulid.ULID{a, b, c}
	testutil.Equals(t, &sorted[0], &SortedSources(sorted)[0])
	unsorted := []ulid.ULID{c, a, b, a}
	testutil.Equals(t, sorted, SortedSources(unsorted))
	testutil.Equals(t, []ulid.ULID{c, a, b, a}, unsorted)
}
