// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
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
	for _, partialTime := range []bool{false, true} {
		raw := &metadata.Meta{}
		raw.ULID = ulid.MustNew(1, nil)
		raw.MinTime, raw.MaxTime = 0, 100
		raw.Compaction.Sources = []ulid.ULID{raw.ULID}
		coarse := *raw
		coarse.ULID = ulid.MustNew(2, nil)
		coarse.Thanos.Downsample.Resolution = 300000
		if partialTime {
			coarse.MaxTime = 50
		} else {
			coarse.Compaction.Sources = []ulid.ULID{coarse.ULID}
		}
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
	testutil.Ok(t, f.Filter(context.Background(), metas, synced, nil))
	testutil.Equals(t, 1, len(f.Hidden()))

	s := &BucketStore{resolutionFilter: f, blocks: map[ulid.ULID]*bucketBlock{}}
	tenant := func(v string) []*labels.Matcher {
		return []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "tenant", v)}
	}
	req := func(mint, maxt, res int64) *storepb.SeriesRequest {
		return &storepb.SeriesRequest{MinTime: mint, MaxTime: maxt, MaxResolutionWindow: res}
	}

	err := s.belowResolutionWarning(req(0, 5000, 0), tenant("a"), nil)
	testutil.NotOk(t, err)
	testutil.Assert(t, strings.Contains(err.Error(), "1 block(s)"), "the warning names the gap: %v", err)

	testutil.Ok(t, s.belowResolutionWarning(req(0, 5000, res5m), tenant("a"), nil))
	testutil.Ok(t, s.belowResolutionWarning(req(2000, 5000, 0), tenant("a"), nil), "the hidden block ends where the range starts")
	testutil.Ok(t, s.belowResolutionWarning(req(0, 999, 0), tenant("a"), nil))
	testutil.Ok(t, s.belowResolutionWarning(req(0, 5000, 0), tenant("b"), nil), "another tenant's request misses nothing")
	testutil.Ok(t, s.belowResolutionWarning(req(0, 5000, 0), nil, tenant("b")), "the block matchers exclude it")

	// Loaded back as a fallback, the block is served.
	s.blocks[rawID] = &bucketBlock{}
	testutil.Ok(t, s.belowResolutionWarning(req(0, 5000, 0), tenant("a"), nil))
}
