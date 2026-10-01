// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package metadata

import (
	"slices"

	"github.com/oklog/ulid/v2"
)

// A block's lineage is its compaction sources: the blocks its data was made
// from. A coarser block holds a finer block's data where it was made from the
// same sources and spans the same time. The functions below are shared by the
// store gateway's query-time block selection and its sync-time resolution
// filter, so both judge coverage the same way.

// SortedSources returns the sources sorted and without duplicates. Compaction
// writes them so, and then they are returned as they are.
func SortedSources(sources []ulid.ULID) []ulid.ULID {
	for i := 1; i < len(sources); i++ {
		if sources[i-1].Compare(sources[i]) >= 0 {
			sorted := slices.Clone(sources)
			slices.SortFunc(sorted, ulid.ULID.Compare)
			return slices.Compact(sorted)
		}
	}
	return sources
}

// HeldSources returns a bitset of the finer block's sources that the coarser
// block holds, or nil if it holds them all. Both must be sorted and unique.
func HeldSources(coarser, finer []ulid.ULID) []uint64 {
	var held []uint64
	j := 0
	for i, src := range finer {
		for j < len(coarser) && coarser[j].Compare(src) < 0 {
			j++
		}
		if j < len(coarser) && coarser[j] == src {
			j++
			if held != nil {
				held[i/64] |= 1 << (i % 64)
			}
			continue
		}
		if held == nil {
			// The first source the coarser block lacks; it holds all before.
			held = make([]uint64, (len(finer)+63)/64)
			for k := range i {
				held[k/64] |= 1 << (k % 64)
			}
		}
	}
	return held
}

// Holds reports whether a bitset HeldSources returned has source i.
func Holds(held []uint64, i int) bool {
	return held == nil || held[i/64]&(1<<(i%64)) != 0
}

// SourcesCovered reports whether the covers together hold every source of m
// over [mint, maxt], both inclusive: for each source, the covers holding it
// span the range without a gap. Compaction can split one source across
// several blocks, so a cover holding a source does not cover the source's
// range on its own. The covers must be ordered by min time. A block without
// sources is never covered: nothing proves where its data went.
func SourcesCovered(m *Meta, covers []*Meta, mint, maxt int64) bool {
	sources := SortedSources(m.Compaction.Sources)
	if len(sources) == 0 || mint > maxt {
		return false
	}
	type candidate struct {
		minTime, maxTime int64
		held             []uint64
	}
	var candidates []candidate
	for _, c := range covers {
		if c.MinTime > maxt {
			break
		}
		if c.MaxTime <= mint {
			continue
		}
		candidates = append(candidates, candidate{minTime: c.MinTime, maxTime: c.MaxTime, held: HeldSources(SortedSources(c.Compaction.Sources), sources)})
	}
	for i := range sources {
		start := mint
		for _, c := range candidates {
			if c.minTime > start {
				break
			}
			if c.maxTime <= start || !Holds(c.held, i) {
				continue
			}
			start = c.maxTime
			if start > maxt {
				break
			}
		}
		if start <= maxt {
			return false
		}
	}
	return true
}
