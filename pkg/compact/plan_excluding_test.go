// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"context"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"

	"github.com/thanos-io/thanos/pkg/block/metadata"
)

// twoHours is the smallest compaction range used by these tests.
const twoHours = int64(2 * time.Hour / time.Millisecond)

// rangeBlocks builds n consecutive 2h blocks starting at t=0.
func rangeBlocks(n int) []*metadata.Meta {
	metas := make([]*metadata.Meta, 0, n)
	for i := 0; i < n; i++ {
		m := meta(ulid.MustNew(uint64(i+1), nil), int64(i)*twoHours, int64(i+1)*twoHours)
		m.Compaction.Level = 1
		m.Compaction.Sources = []ulid.ULID{m.ULID}
		metas = append(metas, m)
	}
	return metas
}

// TestPlanExcludingProducesDisjointPlans asserts that planning repeatedly with
// everything already in flight excluded yields plans that share no source blocks
// and do not overlap in time. That is what makes it safe to hand several plans
// for one group to different workers at the same time.
func TestPlanExcludingProducesDisjointPlans(t *testing.T) {
	ctx := context.Background()

	// 12 consecutive 2h blocks: enough for several independent 8h compactions.
	cg := testGroup(t, rangeBlocks(12)...)
	planner := NewTSDBBasedPlanner(log.NewNopLogger(), []int64{twoHours, 2 * twoHours, 4 * twoHours})

	var (
		plans    [][]*metadata.Meta
		inflight = map[ulid.ULID]struct{}{}
	)
	for {
		plan, _, err := cg.PlanExcluding(ctx, planner, inflight, make(chan error, 1))
		testutil.Ok(t, err)
		if len(plan) == 0 {
			break
		}
		for _, m := range plan {
			// The same block must never appear in two plans.
			_, dup := inflight[m.ULID]
			testutil.Assert(t, !dup, "block %s was planned twice", m.ULID)
			inflight[m.ULID] = struct{}{}
		}
		plans = append(plans, plan)
	}

	testutil.Assert(t, len(plans) > 1, "expected more than one plan for 12 blocks, got %d", len(plans))

	// Plans must not overlap in time, otherwise compacting them concurrently
	// would produce overlapping blocks and halt the compactor.
	for i := range plans {
		for j := i + 1; j < len(plans); j++ {
			a, b := span(plans[i]), span(plans[j])
			testutil.Assert(t, a.max <= b.min || b.max <= a.min,
				"plans %d %v and %d %v overlap in time", i, a, j, b)
		}
	}
}

// TestPlanExcludingEverythingPlansNothing asserts that once every block is in
// flight there is no further work to hand out.
func TestPlanExcludingEverythingPlansNothing(t *testing.T) {
	ctx := context.Background()

	blocks := rangeBlocks(8)
	cg := testGroup(t, blocks...)
	planner := NewTSDBBasedPlanner(log.NewNopLogger(), []int64{twoHours, 2 * twoHours, 4 * twoHours})

	all := map[ulid.ULID]struct{}{}
	for _, m := range blocks {
		all[m.ULID] = struct{}{}
	}

	plan, _, err := cg.PlanExcluding(ctx, planner, all, make(chan error, 1))
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(plan))
}

// TestPlanExcludingNothingMatchesPlan asserts the exclusion variant with an
// empty set behaves exactly like plain planning.
func TestPlanExcludingNothingMatchesPlan(t *testing.T) {
	ctx := context.Background()

	cg := testGroup(t, rangeBlocks(8)...)
	planner := NewTSDBBasedPlanner(log.NewNopLogger(), []int64{twoHours, 2 * twoHours, 4 * twoHours})

	want, _, err := cg.Plan(ctx, planner, make(chan error, 1))
	testutil.Ok(t, err)

	got, _, err := cg.PlanExcluding(ctx, planner, nil, make(chan error, 1))
	testutil.Ok(t, err)
	testutil.Equals(t, want, got)
}

type timeSpan struct{ min, max int64 }

func span(metas []*metadata.Meta) timeSpan {
	s := timeSpan{min: metas[0].MinTime, max: metas[0].MaxTime}
	for _, m := range metas {
		if m.MinTime < s.min {
			s.min = m.MinTime
		}
		if m.MaxTime > s.max {
			s.max = m.MaxTime
		}
	}
	return s
}
