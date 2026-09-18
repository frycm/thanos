// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"context"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
)

func TestRefillWhileOnePlanIsStillRunning(t *testing.T) {
	cg, sched, executor := schedulingFixture(t, 4)
	first, err := cg.Plan(t.Context(), executor.planner, make(chan error, 1))
	testutil.Ok(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); _, _ = executor.Execute(ctx, "", cg, first) }()
	defer func() { cancel(); <-done }()
	var leased []*Task
	deadline := time.Now().Add(5 * time.Second)
	for len(leased) < 4 && time.Now().Before(deadline) {
		task, err := sched.Lease(ctx, LeaseRequest{WorkerID: "review-worker"})
		testutil.Ok(t, err)
		if task != nil {
			leased = append(leased, task)
		} else {
			time.Sleep(time.Millisecond)
		}
	}
	testutil.Equals(t, 4, len(leased))
	// Empty-source plans complete without output blocks. Leave one task leased,
	// modeling a long-running compaction while three workers become available.
	for _, task := range leased[1:] {
		testutil.Ok(t, sched.Report(ctx, Result{TaskID: task.ID, LeaseToken: task.LeaseToken, Generation: task.Generation, Outcome: OutcomeCompleted}))
	}
	deadline = time.Now().Add(5 * time.Second)
	var next *Task
	for time.Now().Before(deadline) {
		next, err = sched.Lease(ctx, LeaseRequest{WorkerID: "free-worker"})
		testutil.Ok(t, err)
		if next != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	testutil.Assert(t, next != nil, "three free workers receive no further work while one batch member remains leased, despite more independent plans")
}

func TestParkedPlanDoesNotStarveDisjointWork(t *testing.T) {
	logger := log.NewNopLogger()
	bkt := objstore.NewInMemBucket()
	cnt := func() prometheus.Counter { return promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "test"}) }
	cg, err := compact.NewGroup(logger, bkt, "g", labels.EmptyLabels(), 0, false, false,
		cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), metadata.NoneFunc, 1, 1)
	testutil.Ok(t, err)
	h := int64(time.Hour / time.Millisecond)
	for i, tr := range [][2]int64{{0, 8}, {8, 10}, {10, 12}, {12, 14}, {14, 16}, {16, 24}, {48, 72}, {72, 96}, {400, 402}} {
		m := &metadata.Meta{}
		m.ULID = ulid.MustNew(uint64(i+1), nil)
		m.MinTime, m.MaxTime = tr[0]*h, tr[1]*h
		m.Compaction.Sources = []ulid.ULID{m.ULID}
		testutil.Ok(t, cg.AppendMeta(m))
	}
	planner := compact.NewTSDBBasedPlanner(logger, []int64{h, 2 * h, 8 * h, 48 * h, 336 * h})
	first, err := cg.Plan(t.Context(), planner, make(chan error, 1))
	testutil.Ok(t, err)
	testutil.Equals(t, 4, len(first.Sources))
	sched := testScheduler(t, bkt, ManagerConfig{})
	task, err := CompactionTask(cg, first)
	testutil.Ok(t, err)
	sched.MarkOversized(task, "review: parked first plan")
	executor := NewRemotePlanExecutor(logger, bkt, sched, planner, 1, nil)
	// Independently show the real planner can produce the healthy [48h,96h) plan.
	exclude := map[ulid.ULID]struct{}{}
	for i := uint64(1); i <= 6; i++ {
		exclude[ulid.MustNew(i, nil)] = struct{}{}
	}
	healthyPlan, err := cg.PlanExcluding(t.Context(), planner, exclude, make(chan error, 1))
	healthy := healthyPlan.Sources
	testutil.Ok(t, err)
	testutil.Equals(t, 2, len(healthy))
	testutil.Equals(t, 48*h, healthy[0].MinTime)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { _, err := executor.Execute(ctx, "", cg, first); done <- err }()
	defer cancel()
	var leased *Task
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		leased, err = sched.Lease(ctx, LeaseRequest{WorkerID: "healthy-worker"})
		testutil.Ok(t, err)
		if leased != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if leased == nil {
		cancel()
		<-done
		t.Fatal("parked first plan starved independent work")
	}
	testutil.Equals(t, 48*h, leased.ExpectedMinTime)
	testutil.Equals(t, 96*h, leased.ExpectedMaxTime)
	testutil.Ok(t, sched.Report(ctx, Result{TaskID: leased.ID, LeaseToken: leased.LeaseToken, Generation: leased.Generation, Outcome: OutcomeCompleted}))
	testutil.Ok(t, <-done)
}

// planGroup collects the first slots to inspect the production iterator without
// dispatching tasks. Execute consumes the same iterator as slots become free.
func (e *RemotePlanExecutor) planGroup(ctx context.Context, cg *compact.Group, first []*metadata.Meta) [][]*metadata.Meta {
	p := groupPlans{executor: e, group: cg, first: compact.Plan{Sources: first}, excluded: map[ulid.ULID]struct{}{}}
	var plans [][]*metadata.Meta
	for len(plans) < e.maxInflightPerGroup {
		plan, err := p.next(ctx)
		if err != nil {
			panic(err)
		}
		if plan.Empty() {
			break
		}
		plans = append(plans, plan.Sources)
	}
	return plans
}

func schedulingFixture(t *testing.T, slots int) (*compact.Group, *Scheduler, *RemotePlanExecutor) {
	t.Helper()
	logger := log.NewNopLogger()
	bkt := objstore.NewInMemBucket()
	cnt := func() prometheus.Counter { return promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "test"}) }
	cg, err := compact.NewGroup(logger, bkt, "g", labels.EmptyLabels(), 0, false, false,
		cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), metadata.NoneFunc, 1, 1)
	testutil.Ok(t, err)
	for i := range 20 {
		m := &metadata.Meta{}
		m.ULID = ulid.MustNew(uint64(i+1), nil)
		m.MinTime, m.MaxTime = int64(i)*2000, int64(i+1)*2000
		m.Compaction.Sources = []ulid.ULID{m.ULID}
		testutil.Ok(t, cg.AppendMeta(m))
	}
	planner := compact.NewTSDBBasedPlanner(logger, []int64{2000, 4000, 8000})
	sched := testScheduler(t, bkt, ManagerConfig{})
	executor := NewRemotePlanExecutor(logger, bkt, sched, planner, slots, nil)

	return cg, sched, executor
}

func TestGroupExecutionStopsRefillingOnErrorAndPreservesHalt(t *testing.T) {
	cg, sched, executor := schedulingFixture(t, 2)
	sched.conf.MaxAttempts = 1
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	first, err := cg.Plan(ctx, executor.planner, make(chan error, 1))
	testutil.Ok(t, err)
	done := make(chan error, 1)
	go func() { _, err := executor.Execute(ctx, "", cg, first); done <- err }()
	var tasks []*Task
	deadline := time.Now().Add(5 * time.Second)
	for len(tasks) < 2 && time.Now().Before(deadline) {
		task, err := sched.Lease(ctx, LeaseRequest{WorkerID: "worker"})
		testutil.Ok(t, err)
		if task != nil {
			tasks = append(tasks, task)
		} else {
			time.Sleep(time.Millisecond)
		}
	}
	if len(tasks) != 2 {
		cancel()
		<-done
		t.Fatal("expected two concurrent tasks")
	}
	for i, outcome := range []Outcome{OutcomeFailedRetryable, OutcomeFailedHalt} {
		task := tasks[i]
		testutil.Ok(t, sched.Report(ctx, Result{TaskID: task.ID, LeaseToken: task.LeaseToken, Generation: task.Generation, Outcome: outcome}))
	}
	err = <-done
	testutil.Assert(t, compact.IsHaltError(err), "halt must outweigh retry: %v", err)
	task, err := sched.Lease(ctx, LeaseRequest{WorkerID: "free-worker"})
	testutil.Ok(t, err)
	testutil.Assert(t, task == nil, "a failed group must not dispatch more work")
}
