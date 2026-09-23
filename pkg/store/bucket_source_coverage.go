// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

// fallbackFilterGauge receives the counts of the filters re-run on fallback
// metadata. They are not exported: the fetcher's counters describe its
// metadata pass, and the reporter describes the served fallbacks.
var fallbackFilterGauge = promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{Name: "fallback_filter", Help: "Counts of the metadata filters re-run on fallback blocks; unregistered."}, []string{"state"})

// Try replacements first, then load only the fallbacks still needed. This
// avoids loading all historical raw blocks on every cold start and preserves
// the old blocks until a replacement was actually added to the store.
//
// A fallback that fails to load is tolerated the way SyncBlocks tolerates any
// other block: the failure is logged and the sync goes on, so one unreadable
// block neither keeps the store from becoming ready nor stops outdated blocks
// from being dropped. The next sync retries it.
func (s *BucketStore) syncResolutionFallbacks(ctx context.Context, metas map[ulid.ULID]*metadata.Meta) error {
	if s.resolutionFilter == nil {
		return nil
	}
	fallbacks := s.resolutionFilter.FallbacksFor(metas, s.usableReplacement)
	if len(fallbacks) > 0 {
		for _, filter := range s.resolutionFallbackFilters {
			if err := filter.Filter(ctx, fallbacks, fallbackFilterGauge, fallbackFilterGauge); err != nil {
				return errors.Wrap(err, "filter resolution fallbacks")
			}
		}
		for id, m := range fallbacks {
			metas[id] = m
			if s.getBlock(id) != nil {
				continue
			}
			if err := s.blockLifecycleCallback.PreAdd(*m); err != nil {
				level.Warn(s.logger).Log("msg", "resolution fallback rejected by the block lifecycle callback", "block", id, "err", err)
				continue
			}
			if err := s.addBlock(ctx, m); err != nil {
				level.Warn(s.logger).Log("msg", "loading resolution fallback failed; its range stays unserved until the next sync", "block", id, "err", err)
				continue
			}
		}
	}
	return s.resolutionFilter.Reporter().Filter(ctx, metas, nil, nil)
}

// usableReplacement reports whether a cover is loaded and its index header
// readable, so a finer block may be retired behind it. Blocks that hid
// something when they were added were verified then; a block loaded before
// it became a cover is verified now, once. One that fails is dropped from the
// store so it is not selected, and the sync that re-adds it verifies again.
func (s *BucketStore) usableReplacement(m *metadata.Meta) bool {
	b := s.getBlock(m.ULID)
	if b == nil {
		return false
	}
	s.mtx.RLock()
	_, verified := s.replacementsVerified[m.ULID]
	s.mtx.RUnlock()
	if verified {
		return true
	}
	if _, err := b.indexHeaderReader.IndexVersion(); err != nil {
		level.Warn(s.logger).Log("msg", "resolution replacement has an unreadable index header; serving the finer blocks instead", "block", m.ULID, "err", err)
		if err := s.removeBlock(m.ULID); err != nil {
			level.Warn(s.logger).Log("msg", "drop of unreadable resolution replacement failed", "block", m.ULID, "err", err)
		}
		return false
	}
	s.mtx.Lock()
	if _, stillLoaded := s.blocks[m.ULID]; stillLoaded {
		s.replacementsVerified[m.ULID] = struct{}{}
	}
	s.mtx.Unlock()
	return true
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

// belowResolutionWarning returns the warning for a request asking for data
// finer than the minimum resolution whose answer misses data, or nil. The
// block set never substitutes coarser blocks for finer ones, so such a
// request gets nothing for the ranges of the blocks the resolution filter hid
// behind covers at the minimum. A request whose range this store serves only
// from blocks it kept uncovered, or loaded back as fallbacks, gets a complete
// answer and no warning: a warning there would fail it needlessly under a
// strict partial response strategy.
func (s *BucketStore) belowResolutionWarning(req *storepb.SeriesRequest, matchers, blockMatchers []*labels.Matcher) error {
	if s.resolutionFilter == nil {
		return nil
	}
	floor := s.resolutionFilter.MinimumResolution()
	if floor <= 0 || req.MaxResolutionWindow >= floor {
		return nil
	}
	missing := 0
	for _, m := range s.resolutionFilter.Hidden() {
		if m.MinTime > req.MaxTime || m.MaxTime <= req.MinTime {
			continue
		}
		if s.getBlock(m.ULID) != nil {
			continue
		}
		if !metaMatches(m, matchers, blockMatchers) {
			continue
		}
		missing++
	}
	if missing == 0 {
		return nil
	}
	return errors.Errorf(
		"this store serves blocks downsampled to %s or coarser (--min-block-resolution), but the request asks for finer data (max_source_resolution=%s); "+
			"%d block(s) in the requested range are served only at %[1]s, so their ranges are missing from this answer; "+
			"raise the query's max_source_resolution, enable --query.auto-downsampling on the querier, or route the query to a store serving finer blocks",
		time.Duration(floor)*time.Millisecond, time.Duration(req.MaxResolutionWindow)*time.Millisecond, missing)
}

// metaMatches reports whether a block the store does not hold could answer
// the request: its external labels agree with every matcher naming one of
// them, as a block set's labels do, and it passes the request's block
// matchers.
func metaMatches(m *metadata.Meta, matchers, blockMatchers []*labels.Matcher) bool {
	lset := labels.FromMap(m.Thanos.Labels)
	for _, matcher := range matchers {
		if v := lset.Get(matcher.Name); v != "" && !matcher.Matches(v) {
			return false
		}
	}
	for _, matcher := range blockMatchers {
		if !matcher.Matches(lset.Get(matcher.Name)) {
			return false
		}
	}
	return true
}
