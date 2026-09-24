// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"slices"
	"strings"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func TestBucketBlockSetSourceCoverage(t *testing.T) {
	const coarse = int64(300000)
	id := func(n uint64) ulid.ULID { return ulid.MustNew(n, nil) }
	type spec struct {
		id            uint64
		min, max, res int64
		sources       []ulid.ULID
	}
	for _, tc := range []struct {
		name     string
		blocks   []spec
		matchers []*labels.Matcher
		want     []uint64
	}{
		{name: "distinct overlapping sources", blocks: []spec{{1, 0, 100, 0, []ulid.ULID{id(1)}}, {2, 0, 100, coarse, []ulid.ULID{id(2)}}}, want: []uint64{1, 2}},
		{name: "covered raw is omitted", blocks: []spec{{1, 0, 100, 0, []ulid.ULID{id(1)}}, {2, 0, 100, coarse, []ulid.ULID{id(1)}}}, want: []uint64{2}},
		{name: "raw replaces redundant coarse", blocks: []spec{{1, 0, 100, 0, []ulid.ULID{id(1), id(2)}}, {2, 0, 100, coarse, []ulid.ULID{id(1)}}}, want: []uint64{1}},
		{name: "partially shared genealogy needs both", blocks: []spec{{1, 0, 100, 0, []ulid.ULID{id(1), id(2)}}, {2, 0, 100, coarse, []ulid.ULID{id(1), id(3)}}}, want: []uint64{1, 2}},
		{name: "partial time coverage keeps raw", blocks: []spec{{1, 0, 100, 0, []ulid.ULID{id(1)}}, {2, 0, 50, coarse, []ulid.ULID{id(1)}}}, want: []uint64{1}},
		{name: "split source fully covered", blocks: []spec{{1, 0, 100, 0, []ulid.ULID{id(1)}}, {2, 0, 50, coarse, []ulid.ULID{id(1)}}, {3, 50, 100, coarse, []ulid.ULID{id(1)}}}, want: []uint64{2, 3}},
		{name: "gap between split covers", blocks: []spec{{1, 0, 100, 0, []ulid.ULID{id(1)}}, {2, 0, 40, coarse, []ulid.ULID{id(1)}}, {3, 50, 100, coarse, []ulid.ULID{id(1)}}}, want: []uint64{1}},
		{name: "unknown genealogy stays visible", blocks: []spec{{1, 0, 100, 0, nil}, {2, 0, 100, coarse, []ulid.ULID{id(2)}}}, want: []uint64{1, 2}},
		{name: "excluded cover cannot hide raw", blocks: []spec{{1, 0, 100, 0, []ulid.ULID{id(1)}}, {2, 0, 100, coarse, []ulid.ULID{id(1)}}}, matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, block.BlockIDLabel, id(1).String())}, want: []uint64{1}},
		{name: "request never gets a coarser level", blocks: []spec{{1, 0, 100, 0, []ulid.ULID{id(1)}}, {2, 0, 100, 3600000, []ulid.ULID{id(2)}}}, want: []uint64{1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			set := newBucketBlockSet(labels.EmptyLabels())
			set.sourceCoverage = true
			for _, b := range tc.blocks {
				m := &metadata.Meta{}
				m.ULID, m.MinTime, m.MaxTime = id(b.id), b.min, b.max
				m.Compaction.Sources = b.sources
				m.Thanos.Downsample.Resolution = b.res
				testutil.Ok(t, set.add(&bucketBlock{meta: m, relabelLabels: labels.FromStrings(block.BlockIDLabel, m.ULID.String())}))
			}
			got := set.getFor(0, 99, coarse, tc.matchers)
			ids := make([]uint64, 0, len(got))
			for _, b := range got {
				ids = append(ids, b.meta.ULID.Time())
			}
			slices.Sort(ids)
			testutil.Equals(t, tc.want, ids)
		})
	}
}

func TestResolutionFilterAndSelectionPreserveUncoveredRaw(t *testing.T) {
	rawID, coarseID := ulid.MustNew(1, nil), ulid.MustNew(2, nil)
	for _, tcase := range []struct {
		name string

		coarseMaxTime int64
		coarseSources []ulid.ULID
	}{
		{
			name:          "coarse block of another lineage",
			coarseMaxTime: 100,
			coarseSources: []ulid.ULID{coarseID},
		},
		{
			name:          "coarse block covering part of the time range",
			coarseMaxTime: 50,
			coarseSources: []ulid.ULID{rawID},
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			raw := &metadata.Meta{}
			raw.ULID = rawID
			raw.MinTime, raw.MaxTime = 0, 100
			raw.Compaction.Sources = []ulid.ULID{raw.ULID}
			coarse := *raw
			coarse.ULID = coarseID
			coarse.MaxTime = tcase.coarseMaxTime
			coarse.Compaction.Sources = tcase.coarseSources
			coarse.Thanos.Downsample.Resolution = 300000
			metas := map[ulid.ULID]*metadata.Meta{raw.ULID: raw, coarse.ULID: &coarse}
			gauge := promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{Name: "test"}, []string{"state"})
			testutil.Ok(t, block.NewResolutionMetaFilter(log.NewNopLogger(), 300000, 3600000, nil).Filter(t.Context(), metas, gauge, nil))
			testutil.Assert(t, metas[raw.ULID] != nil, "filter must retain uncovered raw")
			set := newBucketBlockSet(labels.EmptyLabels())
			set.sourceCoverage = true
			for _, m := range metas {
				testutil.Ok(t, set.add(&bucketBlock{meta: m}))
			}
			got := set.getFor(0, 99, 300000, nil)
			testutil.Assert(t, slices.ContainsFunc(got, func(b *bucketBlock) bool { return b.meta.ULID == raw.ULID }), "retained raw must be queried")
		})
	}
}

// TestBelowResolutionWarning: a request for data finer than the minimum
// resolution is warned about exactly when blocks hidden behind covers at the
// minimum would have answered it - its range and labels reach them, and the
// store did not load them back as fallbacks. Otherwise the answer is
// complete, and a warning would fail it needlessly under a strict partial
// response strategy.
func TestBelowResolutionWarning(t *testing.T) {
	const res5m = int64(300000)
	source := ulid.MustNew(1, nil)
	rawID, coverID := ulid.MustNew(10, nil), ulid.MustNew(11, nil)
	meta := func(id ulid.ULID, res int64) *metadata.Meta {
		m := &metadata.Meta{}
		m.ULID = id
		m.MinTime, m.MaxTime = 1000, 2000
		m.Compaction.Sources = []ulid.ULID{source}
		m.Thanos.Labels = map[string]string{"tenant": "a"}
		m.Thanos.Downsample.Resolution = res
		return m
	}
	f := block.NewResolutionMetaFilter(log.NewNopLogger(), res5m, int64(3600000), nil)
	metas := map[ulid.ULID]*metadata.Meta{rawID: meta(rawID, 0), coverID: meta(coverID, res5m)}
	synced := promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{Name: "synced"}, []string{"state"})
	testutil.Ok(t, f.Filter(t.Context(), metas, synced, nil))
	testutil.Equals(t, 1, len(f.Hidden()))

	tenant := func(v string) []*labels.Matcher {
		return []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "tenant", v)}
	}
	req := func(mint, maxt, res int64) *storepb.SeriesRequest {
		return &storepb.SeriesRequest{MinTime: mint, MaxTime: maxt, MaxResolutionWindow: res}
	}

	for _, tcase := range []struct {
		name string

		req           *storepb.SeriesRequest
		matchers      []*labels.Matcher
		blockMatchers []*labels.Matcher
		// loadedBack marks the hidden block as loaded back as a fallback.
		loadedBack bool

		expectedErr string
	}{
		{
			name:        "finer request reaching the hidden block",
			req:         req(0, 5000, 0),
			matchers:    tenant("a"),
			expectedErr: "1 block(s)",
		},
		{
			name:     "request at the minimum resolution",
			req:      req(0, 5000, res5m),
			matchers: tenant("a"),
		},
		{
			name:     "hidden block ends where the range starts",
			req:      req(2000, 5000, 0),
			matchers: tenant("a"),
		},
		{
			name:     "range before the hidden block",
			req:      req(0, 999, 0),
			matchers: tenant("a"),
		},
		{
			name:     "another tenant's request misses nothing",
			req:      req(0, 5000, 0),
			matchers: tenant("b"),
		},
		{
			name:          "block matchers exclude the hidden block",
			req:           req(0, 5000, 0),
			blockMatchers: tenant("b"),
		},
		{
			name:       "hidden block loaded back as a fallback is served",
			req:        req(0, 5000, 0),
			matchers:   tenant("a"),
			loadedBack: true,
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			s := &BucketStore{resolutionFilter: f, blocks: map[ulid.ULID]*bucketBlock{}}
			if tcase.loadedBack {
				s.blocks[rawID] = &bucketBlock{}
			}
			err := s.belowResolutionWarning(tcase.req, tcase.matchers, tcase.blockMatchers)
			if tcase.expectedErr == "" {
				testutil.Ok(t, err)
				return
			}
			testutil.NotOk(t, err)
			testutil.Assert(t, strings.Contains(err.Error(), tcase.expectedErr), "the warning names the gap: %v", err)
		})
	}
}
