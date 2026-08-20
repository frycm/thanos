// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
)

// The real planner can bridge holes left by in-flight work. Source IDs alone
// do not establish that two workers will produce time-disjoint blocks.
func TestPlanGroupMixedSizePlansDoNotOverlap(t *testing.T) {
	h := int64(time.Hour / time.Millisecond)
	for _, resolution := range []int64{0, 300000, 3600000} {
		for _, vertical := range []bool{false, true} {
			logger := log.NewNopLogger()
			bkt := objstore.NewInMemBucket()
			cnt := func() prometheus.Counter { return prometheus.NewCounter(prometheus.CounterOpts{Name: "test"}) }
			cg, err := compact.NewGroup(logger, bkt, "g", labels.EmptyLabels(), resolution, false, vertical,
				cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), metadata.NoneFunc, 1, 1)
			testutil.Ok(t, err)
			for i, tr := range [][2]int64{{0, 8}, {8, 10}, {10, 12}, {12, 14}, {14, 16}, {16, 24}, {48, 50}, {50, 52}} {
				m := &metadata.Meta{}
				m.ULID = ulid.MustNew(uint64(i+1), nil)
				m.MinTime, m.MaxTime = tr[0]*h, tr[1]*h
				m.Compaction.Sources = []ulid.ULID{m.ULID}
				m.Thanos.Downsample.Resolution = resolution
				testutil.Ok(t, cg.AppendMeta(m))
			}
			planner := compact.NewTSDBBasedPlanner(logger, []int64{h, 2 * h, 8 * h, 48 * h, 336 * h})
			first, _, err := cg.Plan(t.Context(), planner, make(chan error, 1))
			testutil.Ok(t, err)
			testutil.Equals(t, 4, len(first))
			e := NewRemotePlanExecutor(logger, bkt, testScheduler(t, bkt, ManagerConfig{}), planner, 4)
			plans := e.planGroup(t.Context(), cg, first)
			testutil.Equals(t, 1, len(plans)) // The next plan would enclose [8h,16h).
		}
	}
}

func TestPlanGroupDispatchesTimeDisjointPlans(t *testing.T) {
	logger := log.NewNopLogger()
	bkt := objstore.NewInMemBucket()
	cnt := func() prometheus.Counter { return prometheus.NewCounter(prometheus.CounterOpts{Name: "test"}) }
	cg, err := compact.NewGroup(logger, bkt, "g", labels.EmptyLabels(), 0, false, true,
		cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), metadata.NoneFunc, 1, 1)
	testutil.Ok(t, err)
	for i := range 12 {
		m := &metadata.Meta{}
		m.ULID = ulid.MustNew(uint64(i+1), nil)
		m.MinTime, m.MaxTime = int64(i)*2000, int64(i+1)*2000
		testutil.Ok(t, cg.AppendMeta(m))
	}
	planner := compact.NewTSDBBasedPlanner(logger, []int64{2000, 4000, 8000})
	first, _, err := cg.Plan(t.Context(), planner, make(chan error, 1))
	testutil.Ok(t, err)
	plans := NewRemotePlanExecutor(logger, bkt, testScheduler(t, bkt, ManagerConfig{}), planner, 4).planGroup(t.Context(), cg, first)
	testutil.Equals(t, 4, len(plans))
}
