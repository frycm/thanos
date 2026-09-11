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

	got, err := Plan(metas)
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

	got, err := Plan(metas)
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(got))
}

func TestPlanSkipsBlocksAlreadyCovered(t *testing.T) {
	raw := ulid.MustNew(1, nil)
	already := ulid.MustNew(2, nil)

	metas := map[ulid.ULID]*metadata.Meta{
		raw: planMeta(raw, ResLevel0, ResLevel1DownsampleRange),
		// A 5m block built from that raw block already exists.
		already: planMeta(already, ResLevel1, ResLevel1DownsampleRange, raw),
	}

	got, err := Plan(metas)
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(got))
}

func TestPlanAdvancesFiveMinuteBlocksToOneHour(t *testing.T) {
	fiveMin := ulid.MustNew(1, nil)
	metas := map[ulid.ULID]*metadata.Meta{
		fiveMin: planMeta(fiveMin, ResLevel1, ResLevel2DownsampleRange),
	}

	got, err := Plan(metas)
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(got))
	testutil.Equals(t, ResLevel2, got[0].TargetResolution)
}

func TestPlanIgnoresFullyDownsampledBlocks(t *testing.T) {
	oneHour := ulid.MustNew(1, nil)
	metas := map[ulid.ULID]*metadata.Meta{
		oneHour: planMeta(oneHour, ResLevel2, ResLevel2DownsampleRange),
	}

	got, err := Plan(metas)
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(got))
}

func TestPlanRejectsUnknownResolution(t *testing.T) {
	odd := ulid.MustNew(1, nil)
	metas := map[ulid.ULID]*metadata.Meta{
		odd: planMeta(odd, 1234, 100),
	}

	_, err := Plan(metas)
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

	first, err := Plan(metas)
	testutil.Ok(t, err)
	for i := 0; i < 5; i++ {
		got, err := Plan(metas)
		testutil.Ok(t, err)
		testutil.Equals(t, len(first), len(got))
		for j := range first {
			testutil.Equals(t, first[j].Meta.ULID, got[j].Meta.ULID)
		}
	}
}
