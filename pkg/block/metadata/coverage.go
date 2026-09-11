// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package metadata

import (
	"cmp"
	"github.com/oklog/ulid/v2"
	"slices"
)

type sourceInterval struct {
	min, max int64
	block    ulid.ULID // The block whose range this is.
}

// SourceCoverage records the time intervals represented by each source block.
type SourceCoverage map[ulid.ULID][]sourceInterval

// Add records the sources and half-open time range of a block.
func (c SourceCoverage) Add(m *Meta) {
	for _, id := range m.Compaction.Sources {
		spans := append(c[id], sourceInterval{m.MinTime, m.MaxTime, m.ULID})
		slices.SortFunc(spans, func(a, b sourceInterval) int { return cmp.Compare(a.min, b.min) })
		c[id] = spans
	}
}

// CoveringBlocks returns the recorded blocks that share a source with m and
// intersect [mint, maxt] within m's range - every block that can contribute to
// Covers for that range, sorted and without duplicates.
func (c SourceCoverage) CoveringBlocks(m *Meta, mint, maxt int64) []ulid.ULID {
	start, end := max(mint, m.MinTime), min(maxt, m.MaxTime-1)
	if start > end {
		return nil
	}
	var ids []ulid.ULID
	for _, id := range m.Compaction.Sources {
		for _, span := range c[id] {
			if span.min > end {
				break
			}
			if span.max > start {
				ids = append(ids, span.block)
			}
		}
	}
	slices.SortFunc(ids, func(a, b ulid.ULID) int { return a.Compare(b) })
	return slices.Compact(ids)
}

// Covers requires both genealogy and time coverage. Compaction can split one
// source across several blocks, so a source ID alone does not cover its range.
func (c SourceCoverage) Covers(m *Meta, mint, maxt int64) bool {
	if len(m.Compaction.Sources) == 0 || mint > maxt || m.MinTime >= m.MaxTime {
		return false
	}
	for _, id := range m.Compaction.Sources {
		start, end := max(mint, m.MinTime), min(maxt, m.MaxTime-1)
		for _, span := range c[id] {
			if span.min > start {
				break
			}
			start = max(start, span.max)
			if start > end {
				break
			}
		}
		if start <= end {
			return false
		}
	}
	return true
}
