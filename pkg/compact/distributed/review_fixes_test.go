// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
)

// This file holds regression tests for the findings of the multi-agent review
// of the manager/worker split, one test per finding, named after it.

// TestEffectiveHeartbeatInterval pins down that a worker paces its heartbeats
// off the lease TTL the task carries when its own flag is too slow for it,
// since nothing else validates the two processes' flags against each other.
func TestEffectiveHeartbeatInterval(t *testing.T) {
	for _, tc := range []struct {
		configured, ttl, want time.Duration
	}{
		{30 * time.Second, 5 * time.Minute, 30 * time.Second},      // Defaults: plenty of margin.
		{30 * time.Second, 20 * time.Second, 20 * time.Second / 3}, // TTL below the interval: beat at TTL/3.
		{30 * time.Second, 60 * time.Second, 20 * time.Second},     // Margin too thin: tightened to TTL/3.
		{30 * time.Second, 0, 30 * time.Second},                    // No TTL on the task: trust the flag.
	} {
		got := effectiveHeartbeatInterval(tc.configured, tc.ttl)
		testutil.Equals(t, tc.want, got)
	}
}

// stubPlanner always returns the same plan.
type stubPlanner struct{ plan []*metadata.Meta }

func (p stubPlanner) Plan(_ context.Context, _ []*metadata.Meta, _ chan error, _ any) ([]*metadata.Meta, error) {
	return p.plan, nil
}

// TestLeaseRevocationsPersistImmediately pins down that requeue and abandonment
// transitions hit the journal as they happen, not up to a lease TTL later. The
// journal is what a worker's fail-closed pre-upload check reads, so an
// in-memory-only revocation would let a partitioned worker upload for a task
// somebody else already holds.
func TestLeaseRevocationsPersistImmediately(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	sched, err := NewScheduler(context.Background(), log.NewNopLogger(), bkt, prometheus.NewRegistry(), ManagerConfig{
		JournalID: "shard-revoke",
		LeaseTTL:  time.Hour, // So nothing expires or persists on its own schedule.
	})
	testutil.Ok(t, err)

	_, err = sched.Submit(context.Background(), Task{ID: "t-revoke", Type: TaskCompaction})
	testutil.Ok(t, err)
	leased, err := sched.Lease(context.Background(), LeaseRequest{WorkerID: "w1", Accepts: []TaskType{TaskCompaction}})
	testutil.Ok(t, err)
	testutil.Assert(t, leased != nil, "the task must lease")

	// Force the lease to look expired while the periodic persist is not due,
	// then run maintenance: the requeue alone must reach the bucket.
	sched.mtx.Lock()
	sched.tasks["t-revoke"].entry.Lease.ExpiresAt = time.Now().Add(-time.Minute)
	sched.lastPersist = time.Now()
	sched.mtx.Unlock()
	testutil.Ok(t, sched.Maintain())

	j, err := ReadJournal(context.Background(), bkt, "shard-revoke")
	testutil.Ok(t, err)
	testutil.Equals(t, StatePending, j.Tasks["t-revoke"].State)
}

// TestOwnershipCheckRejectsLongExpiredLease pins down that the fail-closed
// pre-upload gate refuses a lease whose recorded expiry is further in the past
// than the grace allows. Without it, a worker partitioned from the manager but
// not from the bucket sailed through the gate on a stale journal.
func TestOwnershipCheckRejectsLongExpiredLease(t *testing.T) {
	ctx := context.Background()
	bkt := objstore.NewInMemBucket()

	writeLease := func(expiresAt time.Time) {
		j := NewJournal("shard-grace", "")
		j.Generation = 3
		j.Owner = "owner"
		j.Tasks["t1"] = &TaskEntry{
			Task:  Task{ID: "t1"},
			State: StateLeased,
			Lease: &Lease{WorkerID: "w1", Token: "tok", Generation: 3, ExpiresAt: expiresAt},
		}
		testutil.Ok(t, WriteJournal(ctx, bkt, j))
	}

	// Recorded expiry lags a healthy worker's by at most one TTL: confirmed.
	writeLease(time.Now().Add(-time.Second))
	got, _ := CheckOwnership(ctx, bkt, "shard-grace", "t1", "tok", 3, time.Minute)
	testutil.Equals(t, OwnershipConfirmed, got)

	// Recorded expiry beyond the grace: the manager no longer vouches for us.
	writeLease(time.Now().Add(-2 * time.Minute))
	got, err := CheckOwnership(ctx, bkt, "shard-grace", "t1", "tok", 3, time.Minute)
	testutil.Equals(t, OwnershipLost, got)
	testutil.NotOk(t, err)

	// Grace zero keeps the old semantics for callers that cannot judge it.
	got, _ = CheckOwnership(ctx, bkt, "shard-grace", "t1", "tok", 3, 0)
	testutil.Equals(t, OwnershipConfirmed, got)
}

// TestAbortStreakIsCappedAndBackedOff pins down that a task aborted on every
// lease does not livelock: aborts do not consume attempts, so before the cap a
// task aborted forever was requeued forever - while its submitter waited with
// no timeout - and each requeue was leaseable immediately.
func TestAbortStreakIsCappedAndBackedOff(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	sched, err := NewScheduler(context.Background(), log.NewNopLogger(), bkt, prometheus.NewRegistry(), ManagerConfig{
		JournalID:   "shard-abort",
		LeaseTTL:    time.Millisecond, // Caps the requeue backoff for the test.
		MaxAttempts: 3,
	})
	testutil.Ok(t, err)

	resultCh, err := sched.Submit(context.Background(), Task{ID: "t-abort", Type: TaskCompaction})
	testutil.Ok(t, err)

	lease := func() *Task {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			task, err := sched.Lease(context.Background(), LeaseRequest{WorkerID: "w1", Accepts: []TaskType{TaskCompaction}})
			testutil.Ok(t, err)
			if task != nil {
				return task
			}
			time.Sleep(time.Millisecond) // The requeue backoff has to pass first.
		}
		t.Fatal("task never became leaseable again")
		return nil
	}

	cap := abortCapFor(3)
	for range cap {
		task := lease()
		testutil.Ok(t, sched.Report(context.Background(), Result{
			TaskID:       task.ID,
			LeaseToken:   task.LeaseToken,
			Generation:   task.Generation,
			Outcome:      OutcomeAbortedStoreUnreachable,
			ErrorMessage: "injected abort",
		}))
	}

	// The cap fails the task instead of requeueing it a cap+1-th time, and the
	// submitter finally hears about it.
	select {
	case res := <-resultCh:
		testutil.Equals(t, OutcomeFailedRetryable, res.Outcome)
		testutil.Assert(t, strings.Contains(res.ErrorMessage, "aborted"), "the failure must name the abort streak: %s", res.ErrorMessage)
	case <-time.After(5 * time.Second):
		t.Fatal("the capped abort streak never reached the submitter")
	}

	j, err := ReadJournal(context.Background(), bkt, "shard-abort")
	testutil.Ok(t, err)
	testutil.Equals(t, StateFailed, j.Tasks["t-abort"].State)
	testutil.Equals(t, cap, j.Tasks["t-abort"].Aborts)
}

// TestAbortedRequeueBacksOff pins down that an aborted task is not leaseable
// again immediately.
func TestAbortedRequeueBacksOff(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	sched, err := NewScheduler(context.Background(), log.NewNopLogger(), bkt, prometheus.NewRegistry(), ManagerConfig{
		JournalID: "shard-backoff",
		LeaseTTL:  200 * time.Millisecond,
	})
	testutil.Ok(t, err)

	_, err = sched.Submit(context.Background(), Task{ID: "t-backoff", Type: TaskCompaction})
	testutil.Ok(t, err)
	task, err := sched.Lease(context.Background(), LeaseRequest{WorkerID: "w1"})
	testutil.Ok(t, err)
	testutil.Ok(t, sched.Report(context.Background(), Result{
		TaskID: task.ID, LeaseToken: task.LeaseToken, Generation: task.Generation,
		Outcome: OutcomeAbortedOwnershipLost,
	}))

	// Immediately after the abort the task is parked.
	again, err := sched.Lease(context.Background(), LeaseRequest{WorkerID: "w1"})
	testutil.Ok(t, err)
	testutil.Assert(t, again == nil, "an aborted task must back off before it is leaseable again")

	// After the backoff (capped at the lease TTL) it comes back.
	time.Sleep(250 * time.Millisecond)
	again, err = sched.Lease(context.Background(), LeaseRequest{WorkerID: "w1"})
	testutil.Ok(t, err)
	testutil.Assert(t, again != nil, "the task must be leaseable after the backoff")
}

// TestLeaseRefusesMismatchedJournalID pins down that a worker configured for a
// different journal is refused loudly instead of being handed tasks it would
// abort forever against the wrong journal.
func TestLeaseRefusesMismatchedJournalID(t *testing.T) {
	sched, err := NewScheduler(context.Background(), log.NewNopLogger(), objstore.NewInMemBucket(), prometheus.NewRegistry(), ManagerConfig{
		JournalID: "shard-a",
	})
	testutil.Ok(t, err)

	_, err = sched.Lease(context.Background(), LeaseRequest{WorkerID: "w1", JournalID: "shard-b"})
	testutil.NotOk(t, err)
	testutil.Assert(t, strings.Contains(err.Error(), "shard-b"), "the refusal must name both journals: %v", err)

	_, err = sched.Lease(context.Background(), LeaseRequest{WorkerID: "w1", JournalID: "shard-a"})
	testutil.Ok(t, err)
}

// listPlanner returns its canned plans one Plan call at a time.
type listPlanner struct {
	plans [][]*metadata.Meta
	calls int
}

func (p *listPlanner) Plan(_ context.Context, _ []*metadata.Meta, _ chan error, _ any) ([]*metadata.Meta, error) {
	if p.calls >= len(p.plans) {
		return nil, nil
	}
	p.calls++
	return p.plans[p.calls-1], nil
}

// TestPlanGroupKeepsConcurrentPlansTimeDisjoint pins down that concurrent
// plans for one group may not overlap in time. Disjoint source ULIDs are not
// enough: the planner selects range parts "potentially with gaps", so a second
// plan could bracket the first, both outputs would overlap, and the next
// cycle's overlap check would halt the shard.
func TestPlanGroupKeepsConcurrentPlansTimeDisjoint(t *testing.T) {
	logger := log.NewNopLogger()
	bkt := objstore.NewInMemBucket()
	cnt := func() prometheus.Counter { return prometheus.NewCounter(prometheus.CounterOpts{Name: "test"}) }

	meta := func(minT, maxT int64) *metadata.Meta {
		m := &metadata.Meta{}
		m.ULID = ulid.MustNew(uint64(minT)+1, nil)
		m.MinTime, m.MaxTime = minT, maxT
		return m
	}

	newGroup := func(vertical bool, metas ...*metadata.Meta) *compact.Group {
		cg, err := compact.NewGroup(logger, bkt, "g1", labels.EmptyLabels(), 0, false, vertical,
			cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), metadata.NoneFunc, 1, 1)
		testutil.Ok(t, err)
		for _, m := range metas {
			testutil.Ok(t, cg.AppendMeta(m))
		}
		return cg
	}

	first := []*metadata.Meta{meta(8000, 12000), meta(12000, 16000)}

	// A second plan whose envelope brackets the first ([0,18000) vs [8000,16000))
	// must be deferred, even though its source blocks are disjoint.
	bracket := []*metadata.Meta{meta(0, 8000), meta(16000, 18000)}
	sched := &Scheduler{journal: NewJournal("t", "")}
	e := NewRemotePlanExecutor(logger, bkt, sched, &listPlanner{plans: [][]*metadata.Meta{bracket}}, 4, nil, nil)
	plans := e.planGroup(context.Background(), newGroup(false, append(append([]*metadata.Meta{}, first...), bracket...)...), first)
	testutil.Equals(t, 1, len(plans))

	// A genuinely disjoint second plan still runs concurrently.
	disjoint := []*metadata.Meta{meta(16000, 18000), meta(18000, 20000)}
	e = NewRemotePlanExecutor(logger, bkt, sched, &listPlanner{plans: [][]*metadata.Meta{disjoint}}, 4, nil, nil)
	plans = e.planGroup(context.Background(), newGroup(false, append(append([]*metadata.Meta{}, first...), disjoint...)...), first)
	testutil.Equals(t, 2, len(plans))

	// Under vertical compaction blocks overlap by design, so disjoint sources
	// prove nothing: one plan at a time.
	e = NewRemotePlanExecutor(logger, bkt, sched, &listPlanner{plans: [][]*metadata.Meta{disjoint}}, 4, nil, nil)
	plans = e.planGroup(context.Background(), newGroup(true, append(append([]*metadata.Meta{}, first...), disjoint...)...), first)
	testutil.Equals(t, 1, len(plans))
}

// TestDispatchDownsamplingRecordsFailures pins down that a failed downsample
// task increments the same per-resolution failure counter the in-process
// downsampler feeds - previously manager mode exported the pre-seeded series
// frozen at zero, so failure alerts could never fire.
func TestDispatchDownsamplingRecordsFailures(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	sched, err := NewScheduler(context.Background(), log.NewNopLogger(), bkt, prometheus.NewRegistry(), ManagerConfig{
		JournalID:   "shard-ds",
		MaxAttempts: 1,
	})
	testutil.Ok(t, err)

	// A stand-in worker that fails everything it leases.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		for ctx.Err() == nil {
			task, err := sched.Lease(ctx, LeaseRequest{WorkerID: "w1"})
			if err == nil && task != nil {
				_ = sched.Report(ctx, Result{
					TaskID: task.ID, LeaseToken: task.LeaseToken, Generation: task.Generation,
					Outcome: OutcomeFailedRetryable, ErrorMessage: "injected failure",
				})
			}
			time.Sleep(time.Millisecond)
		}
	}()

	// A raw block old and large enough that downsample.Plan selects it.
	m := &metadata.Meta{}
	m.ULID = ulid.MustNew(1, nil)
	m.MinTime, m.MaxTime = 0, downsample.ResLevel1DownsampleRange
	m.Compaction.Sources = []ulid.ULID{m.ULID}
	m.Thanos.Labels = map[string]string{"ext": "1"}

	failures := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_ds_failures"}, []string{"resolution"})
	err = DispatchDownsampling(context.Background(), log.NewNopLogger(), bkt, sched,
		map[ulid.ULID]*metadata.Meta{m.ULID: m}, 1, metadata.NoneFunc, 1, false, nil, nil, failures)
	testutil.NotOk(t, err)
	testutil.Equals(t, 1.0, promtestutil.ToFloat64(failures.WithLabelValues(m.Thanos.ResolutionString())))
}

// TestWorkerSeenIsPruned pins down that the worker liveness map forgets workers
// gone for many lease TTLs, instead of growing forever under pod churn.
func TestWorkerSeenIsPruned(t *testing.T) {
	sched, err := NewScheduler(context.Background(), log.NewNopLogger(), objstore.NewInMemBucket(), prometheus.NewRegistry(), ManagerConfig{
		JournalID: "shard-seen",
		LeaseTTL:  10 * time.Millisecond,
	})
	testutil.Ok(t, err)

	sched.mtx.Lock()
	sched.workerSeen["fresh"] = time.Now()
	sched.workerSeen["long-gone"] = time.Now().Add(-time.Hour)
	sched.mtx.Unlock()

	testutil.Ok(t, sched.Maintain())

	sched.mtx.Lock()
	defer sched.mtx.Unlock()
	_, fresh := sched.workerSeen["fresh"]
	_, gone := sched.workerSeen["long-gone"]
	testutil.Equals(t, true, fresh)
	testutil.Equals(t, false, gone)
}

// countingExecutor records how many Execute calls run at once.
type countingExecutor struct {
	mtx     sync.Mutex
	active  int
	maxSeen int
}

func (e *countingExecutor) Execute(_ context.Context, _ string, _ *compact.Group, _ []*metadata.Meta, _ bool) ([]ulid.ULID, error) {
	e.mtx.Lock()
	e.active++
	if e.active > e.maxSeen {
		e.maxSeen = e.active
	}
	e.mtx.Unlock()
	time.Sleep(50 * time.Millisecond)
	e.mtx.Lock()
	e.active--
	e.mtx.Unlock()
	return nil, compact.ErrPlanDeferred
}

// TestGroupCompactRunsAreSerialized pins down the contract Group.Compact always
// had before the executor seam: one compaction run per group at a time, for
// external callers that relied on it.
func TestGroupCompactRunsAreSerialized(t *testing.T) {
	cnt := func() prometheus.Counter { return prometheus.NewCounter(prometheus.CounterOpts{Name: "test"}) }
	cg, err := compact.NewGroup(log.NewNopLogger(), objstore.NewInMemBucket(), "g1", labels.EmptyLabels(), 0, false, false,
		cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), metadata.NoneFunc, 1, 1)
	testutil.Ok(t, err)
	m := &metadata.Meta{}
	m.ULID = ulid.MustNew(1, nil)
	m.MinTime, m.MaxTime = 0, 1000
	testutil.Ok(t, cg.AppendMeta(m))

	e := &countingExecutor{}
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := cg.CompactWithExecutor(context.Background(), t.TempDir(), stubPlanner{plan: []*metadata.Meta{m}}, e)
			testutil.Ok(t, err)
		}()
	}
	wg.Wait()
	testutil.Equals(t, 1, e.maxSeen)
}

// TestOversizedLimitsIgnoreUnknownSizes pins down that blocks which report no
// stats can never be refused: an absent figure is not evidence of size.
func TestOversizedLimitsIgnoreUnknownSizes(t *testing.T) {
	task := Task{ID: "t", Type: TaskCompaction, SourceBlocks: []string{"a"}}
	conf := ManagerConfig{MaxTaskSeries: 1, MaxTaskIndexBytes: 1}
	testutil.Equals(t, "", oversizedReason(task, conf))
}

// TestDispatchDownsamplingRefusesOversizedBlocks pins down that the size gate
// also covers downsample tasks. The blocks downsampling picks up include ones
// marked no-compact for exceeding the index size limit - the biggest blocks in
// the bucket - so assuming any worker can hold them is exactly wrong.
func TestDispatchDownsamplingRefusesOversizedBlocks(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	sched, err := NewScheduler(context.Background(), log.NewNopLogger(), bkt, prometheus.NewRegistry(), ManagerConfig{
		JournalID:         "shard-ds-big",
		MaxTaskIndexBytes: 1024,
	})
	testutil.Ok(t, err)

	m := &metadata.Meta{}
	m.ULID = ulid.MustNew(1, nil)
	m.MinTime, m.MaxTime = 0, downsample.ResLevel1DownsampleRange
	m.Compaction.Sources = []ulid.ULID{m.ULID}
	m.Thanos.Labels = map[string]string{"ext": "1"}
	m.Thanos.Files = []metadata.File{{RelPath: "index", SizeBytes: 4096}}

	// No worker exists; if the gate failed, Dispatch would hang on Submit's
	// result channel, so returning at all proves the refusal.
	testutil.Ok(t, DispatchDownsampling(context.Background(), log.NewNopLogger(), bkt, sched,
		map[ulid.ULID]*metadata.Meta{m.ULID: m}, 1, metadata.NoneFunc, 1, false, nil, nil, nil))

	j, err := ReadJournal(context.Background(), bkt, "shard-ds-big")
	testutil.Ok(t, err)
	found := false
	for _, e := range j.Tasks {
		if e.State == StateOversized {
			found = true
			testutil.Equals(t, TaskDownsample, e.Task.Type)
			testutil.Equals(t, int64(4096), e.Task.ExpectedIndexBytes)
		}
	}
	testutil.Assert(t, found, "the refusal must be recorded in the journal")
}
