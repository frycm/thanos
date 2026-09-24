// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package downsample

import (
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/oklog/ulid/v2"

	"github.com/thanos-io/thanos/pkg/block/metadata"
)

func planMeta(id ulid.ULID, resolution int64, maxt int64, sources ...ulid.ULID) *metadata.Meta {
	m := &metadata.Meta{}
	m.ULID = id
	m.MinTime = 0
	m.MaxTime = maxt
	m.Thanos.Downsample.Resolution = resolution
	if len(sources) == 0 {
		sources = []ulid.ULID{id}
	}
	m.Compaction.Sources = sources
	return m
}

func TestPlanPicksBlocksThatNeedDownsampling(t *testing.T) {
	raw := ulid.MustNew(1, nil)
	metas := map[ulid.ULID]*metadata.Meta{
		raw: planMeta(raw, ResLevel0, ResLevel1DownsampleRange),
	}

	got, err := Plan(metas, PlanOptions{})
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(got))
	testutil.Equals(t, raw, got[0].Meta.ULID)
	testutil.Equals(t, ResLevel1, got[0].TargetResolution)
}

func TestPlanSkipsBlocksThatAreTooShort(t *testing.T) {
	raw := ulid.MustNew(1, nil)
	metas := map[ulid.ULID]*metadata.Meta{
		// One millisecond short of the range that yields two chunks.
		raw: planMeta(raw, ResLevel0, ResLevel1DownsampleRange-1),
	}

	got, err := Plan(metas, PlanOptions{})
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(got))
}

// TestPlanLeavesNoDownsampleMarkedBlocksOut: a marked block is never a
// candidate, and a marked downsampled block covers nothing, as if neither
// were in view.
func TestPlanLeavesNoDownsampleMarkedBlocksOut(t *testing.T) {
	raw, marked, markedDown := ulid.MustNew(1, nil), ulid.MustNew(2, nil), ulid.MustNew(3, nil)
	metas := map[ulid.ULID]*metadata.Meta{
		raw:        planMeta(raw, ResLevel0, ResLevel1DownsampleRange),
		marked:     planMeta(marked, ResLevel0, ResLevel1DownsampleRange),
		markedDown: planMeta(markedDown, ResLevel1, ResLevel1DownsampleRange, raw),
	}

	got, err := Plan(metas, PlanOptions{})
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(got), "unmarked, the 5m block covers raw and only the other raw block is left")
	testutil.Equals(t, marked, got[0].Meta.ULID)

	got, err = Plan(metas, PlanOptions{NoDownsampleMarked: map[ulid.ULID]*metadata.NoDownsampleMark{
		marked:     {ID: marked},
		markedDown: {ID: markedDown},
	}})
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(got), "the marked block is no candidate, and the marked 5m block no coverage")
	testutil.Equals(t, raw, got[0].Meta.ULID)
	testutil.Equals(t, ResLevel1, got[0].TargetResolution)
}

func TestPlanSkipsBlocksAlreadyCovered(t *testing.T) {
	raw := ulid.MustNew(1, nil)
	already := ulid.MustNew(2, nil)

	metas := map[ulid.ULID]*metadata.Meta{
		raw: planMeta(raw, ResLevel0, ResLevel1DownsampleRange),
		// A 5m block built from that raw block already exists.
		already: planMeta(already, ResLevel1, ResLevel1DownsampleRange, raw),
	}

	got, err := Plan(metas, PlanOptions{})
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(got))
}

func TestPlanAdvancesFiveMinuteBlocksToOneHour(t *testing.T) {
	fiveMin := ulid.MustNew(1, nil)
	metas := map[ulid.ULID]*metadata.Meta{
		fiveMin: planMeta(fiveMin, ResLevel1, ResLevel2DownsampleRange),
	}

	got, err := Plan(metas, PlanOptions{})
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(got))
	testutil.Equals(t, ResLevel2, got[0].TargetResolution)
}

func TestPlanIgnoresFullyDownsampledBlocks(t *testing.T) {
	oneHour := ulid.MustNew(1, nil)
	metas := map[ulid.ULID]*metadata.Meta{
		oneHour: planMeta(oneHour, ResLevel2, ResLevel2DownsampleRange),
	}

	got, err := Plan(metas, PlanOptions{})
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(got))
}

func TestPlanRejectsUnknownResolution(t *testing.T) {
	odd := ulid.MustNew(1, nil)
	metas := map[ulid.ULID]*metadata.Meta{
		odd: planMeta(odd, 1234, 100),
	}

	_, err := Plan(metas, PlanOptions{})
	testutil.NotOk(t, err)
}

// TestPlanIsDeterministic asserts the same bucket state always produces the same
// order of work, which matters when the plan drives task dispatch.
func TestPlanIsDeterministic(t *testing.T) {
	metas := map[ulid.ULID]*metadata.Meta{}
	for i := 1; i <= 5; i++ {
		id := ulid.MustNew(uint64(i), nil)
		metas[id] = planMeta(id, ResLevel0, ResLevel1DownsampleRange)
	}

	first, err := Plan(metas, PlanOptions{})
	testutil.Ok(t, err)
	for i := 0; i < 5; i++ {
		got, err := Plan(metas, PlanOptions{})
		testutil.Ok(t, err)
		testutil.Equals(t, len(first), len(got))
		for j := range first {
			testutil.Equals(t, first[j].Meta.ULID, got[j].Meta.ULID)
		}
	}
}

// TestPlanScopesCoverageByExternalLabels: a downsampled block covers a source
// only for blocks of its own stream. The outputs of a compaction split by
// series record the same sources while each holds other series, so once one
// shard is downsampled the other must still be a candidate, and a shard's
// own downsampled block still covers it.
func TestPlanScopesCoverageByExternalLabels(t *testing.T) {
	raw1, raw2, down1 := ulid.MustNew(1, nil), ulid.MustNew(2, nil), ulid.MustNew(3, nil)
	source := ulid.MustNew(9, nil)
	shard := func(m *metadata.Meta, v string) *metadata.Meta {
		m.Thanos.Labels = map[string]string{"tenant": "a", "__compactor_shard__": v}
		return m
	}
	metas := map[ulid.ULID]*metadata.Meta{
		raw1:  shard(planMeta(raw1, ResLevel0, ResLevel1DownsampleRange, source), "1_of_2"),
		raw2:  shard(planMeta(raw2, ResLevel0, ResLevel1DownsampleRange, source), "2_of_2"),
		down1: shard(planMeta(down1, ResLevel1, ResLevel1DownsampleRange, source), "1_of_2"),
	}

	got, err := Plan(metas, PlanOptions{})
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(got), "shard 2 has never been downsampled")
	testutil.Equals(t, raw2, got[0].Meta.ULID)
}

// TestCoverageAcrossShards: once an unsplit block's downsampled block was
// compacted into a split one, the shards hold its series between them and
// cover it together - otherwise it would be downsampled again every pass,
// and the result retired again by the shards' set. A coarser downsampled
// block covers each of its shards, and one shard never covers another.
func TestCoverageAcrossShards(t *testing.T) {
	source, other := ulid.MustNew(9, nil), ulid.MustNew(8, nil)
	meta := func(id ulid.ULID, tenant, shard string, sources ...ulid.ULID) *metadata.Meta {
		m := &metadata.Meta{}
		m.ULID = id
		m.Compaction.Sources = sources
		m.Thanos.Labels = map[string]string{"tenant": tenant}
		if shard != "" {
			m.Thanos.Labels[metadata.CompactorShardLabel] = shard
		}
		return m
	}
	raw := func(shard string) *metadata.Meta { return meta(ulid.MustNew(1, nil), "a", shard, source) }
	covering := func(tenant string, shards ...string) coverage {
		c := coverage{}
		for i, v := range shards {
			// A shard of a compaction lists every source of the plan.
			c.add(meta(ulid.MustNew(uint64(100+i), nil), tenant, v, source, other))
		}
		return c
	}
	for _, tc := range []struct {
		name    string
		block   *metadata.Meta
		cover   coverage
		covered bool
	}{
		{name: "unsplit by every shard", block: raw(""), cover: covering("a", "1_of_2", "2_of_2"), covered: true},
		{name: "unsplit by shards of mixed counts", block: raw(""), cover: covering("a", "1_of_2", "2_of_4", "4_of_4"), covered: true},
		{name: "unsplit by one shard", block: raw(""), cover: covering("a", "1_of_2"), covered: false},
		{name: "a shard by the unsplit block", block: raw("3_of_4"), cover: covering("a", ""), covered: true},
		{name: "a shard by a coarser shard holding it", block: raw("3_of_4"), cover: covering("a", "1_of_2"), covered: true},
		{name: "a shard by a coarser shard not holding it", block: raw("2_of_4"), cover: covering("a", "1_of_2"), covered: false},
		{name: "a shard by its finer shards", block: raw("1_of_2"), cover: covering("a", "1_of_4", "3_of_4"), covered: true},
		{name: "a shard by another shard", block: raw("2_of_2"), cover: covering("a", "1_of_2"), covered: false},
		{name: "unsplit by another stream's shards", block: raw(""), cover: covering("b", "1_of_2", "2_of_2"), covered: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testutil.Equals(t, tc.covered, tc.cover.covers(tc.block))
		})
	}
}
