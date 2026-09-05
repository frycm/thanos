// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"cmp"
	"slices"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/block/metadata"
)

// getForSourceCoverage runs under s.mtx. The resolution filter can retain a
// finer block whose unique sources overlap a coarse block in time. Time-only
// gap filling would never query it. Prefer coarser data within the request's
// limit, but also select every finer block not covered by that data.
func (s *bucketBlockSet) getForSourceCoverage(mint, maxt, maxResolution int64, matchers []*labels.Matcher) []*bucketBlock {
	covered := metadata.SourceCoverage{}
	var selected []*bucketBlock
	for i, resolution := range s.resolutions {
		if resolution > maxResolution {
			continue
		}
		var atLevel []*bucketBlock
		for _, b := range s.blocks[i] {
			if b.meta.MaxTime <= mint {
				continue
			}
			if b.meta.MinTime > maxt {
				break
			}
			if len(matchers) > 0 && !b.matchRelabelLabels(matchers) {
				continue
			}
			if !covered.Covers(b.meta, mint, maxt) {
				atLevel = append(atLevel, b)
			}
		}
		// Same-resolution overlap remains handled by the existing series merger.
		// Only a strictly coarser level can replace a finer candidate here.
		for _, b := range atLevel {
			covered.Add(b.meta)
		}
		selected = append(selected, atLevel...)
	}

	// A retained raw block may contain both uncovered and already-downsampled
	// sources. If the finer selection completely replaces a coarse block, query
	// only the finer copies. Partially overlapping lineages still need both
	// blocks; the existing query iterator handles overlapping chunks.
	finer := metadata.SourceCoverage{}
	keep := make([]bool, len(selected))
	for i := len(selected) - 1; i >= 0; {
		end := i
		resolution := selected[i].meta.Thanos.Downsample.Resolution
		for i >= 0 && selected[i].meta.Thanos.Downsample.Resolution == resolution {
			keep[i] = !finer.Covers(selected[i].meta, mint, maxt)
			i--
		}
		for j := i + 1; j <= end; j++ {
			if keep[j] {
				finer.Add(selected[j].meta)
			}
		}
	}
	result := make([]*bucketBlock, 0, len(selected))
	for i, b := range selected {
		if keep[i] {
			result = append(result, b)
		}
	}
	slices.SortStableFunc(result, func(a, b *bucketBlock) int {
		return cmp.Or(cmp.Compare(a.meta.MinTime, b.meta.MinTime), cmp.Compare(a.meta.MaxTime, b.meta.MaxTime))
	})
	return result
}
