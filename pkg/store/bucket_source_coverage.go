// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"cmp"
	"context"
	"slices"

	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/block/metadata"
)

// Try replacements first, then load only the fallbacks still needed. This
// avoids loading all historical raw blocks on every cold start and preserves
// the old blocks until a replacement was actually added to the store.
func (s *BucketStore) syncResolutionFallbacks(ctx context.Context, metas map[ulid.ULID]*metadata.Meta) error {
	if s.resolutionFilter == nil {
		return nil
	}
	loaded := map[ulid.ULID]*metadata.Meta{}
	for id, m := range metas {
		if s.getBlock(id) != nil {
			loaded[id] = m
		}
	}
	fallbacks := s.resolutionFilter.FallbacksFor(loaded)
	if len(fallbacks) > 0 {
		// These counters are not exported: the fetcher's counters describe
		// its metadata pass; the reporter below describes the served fallback.
		gauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "fallback_filter"}, []string{"state"})
		for _, filter := range s.resolutionFallbackFilters {
			if err := filter.Filter(ctx, fallbacks, gauge, gauge); err != nil {
				return errors.Wrap(err, "filter resolution fallbacks")
			}
		}
		for id, m := range fallbacks {
			metas[id] = m
			if s.getBlock(id) != nil {
				continue
			}
			if err := s.blockLifecycleCallback.PreAdd(*m); err != nil {
				return errors.Wrap(err, "prepare resolution fallback")
			}
			if err := s.addBlock(ctx, m); err != nil {
				return errors.Wrap(err, "load resolution fallback")
			}
		}
	}
	return s.resolutionFilter.Reporter().Filter(ctx, metas, nil, nil)
}

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
