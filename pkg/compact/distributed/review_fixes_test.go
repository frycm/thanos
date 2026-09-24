// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"context"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compact/compacttest"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
)

// This file holds regression tests for the findings of the multi-agent review
// of the manager/worker split, one test per finding, named after it.

// TestWorkerShutdownReportsAbortNotHalt pins down that a worker being asked to
// shut down mid-task reports an abort, not a halt. The compaction seam wraps a
// canceled compaction in a halt error, and before the shutdown triage that
// halt traveled to the manager and stopped the whole shard.
func TestReviewWorkerShutdownReportsAbortNotHalt(t *testing.T) {
	c := newTestCluster(t)

	w1 := c.startWorker("w1")
	_ = gateChunks(w1) // Hold w1 mid-download so the shutdown arrives mid-task.

	cg, toCompact := c.makeGroup(labels.FromStrings("ext", "1"))
	outcome := c.execute(cg, toCompact)

	c.waitFor("w1 to lease the task", func() bool {
		e := c.journalTask(StateLeased)
		return e != nil && e.Lease.WorkerID == "w1"
	})

	// Shut w1 down. Its report still goes out on a background context.
	w1.cancel()
	<-w1.done

	c.waitFor("the shutdown abort to be reported and the task requeued", func() bool {
		e := c.journalTask(StatePending)
		return e != nil && e.LastError != nil && e.LastError.Outcome == OutcomeAbortedWorkerShutdown
	})
	testutil.Equals(t, 1.0, counterValue(t, w1.reg, "thanos_compact_worker_tasks_total", string(OutcomeAbortedWorkerShutdown)))

	// The shard is not halted: another worker picks the task up and finishes.
	c.startWorker("w2")
	var got executeOutcome
	select {
	case got = <-outcome:
	case <-time.After(30 * time.Second):
		t.Fatal("compaction did not finish after the worker restart")
	}
	testutil.Ok(t, got.err)
	testutil.Equals(t, 1, len(got.compIDs))
}

// TestWorkerChecksumReadBackFailureAbortsNotCompletes pins down that a worker
// which cannot read back the checksum of a block it uploaded reports an abort,
// never a checksum-less completion. The manager rejects a completion without
// checksums, so the old behavior threw away the whole finished task on one
// read blip - and on the downsample path even crashed the manager.
func TestWorkerChecksumReadBackFailureAbortsNotCompletes(t *testing.T) {
	old := metaChecksumRetryBackoff
	metaChecksumRetryBackoff = 10 * time.Millisecond
	t.Cleanup(func() { metaChecksumRetryBackoff = old })

	c := newTestCluster(t)
	w1 := c.startWorker("w1")

	cg, toCompact := c.makeGroup(labels.FromStrings("ext", "1"))
	sources := map[string]bool{}
	for _, m := range toCompact {
		sources[m.ULID.String()] = true
	}

	// Fail reads of any meta.json that is neither a source block's nor the
	// journal: that is exactly the read-back of the freshly uploaded result.
	w1.bkt.SetOnGet(func(_ context.Context, name string) error {
		if !strings.HasSuffix(name, "meta.json") || strings.HasPrefix(name, JournalPrefix) {
			return nil
		}
		if sources[strings.SplitN(name, "/", 2)[0]] {
			return nil
		}
		return errors.New("injected: meta read-back unavailable")
	})

	outcome := c.execute(cg, toCompact)

	c.waitFor("the checksum failure to surface as a store abort", func() bool {
		return counterValue(t, w1.reg, "thanos_compact_worker_tasks_total", string(OutcomeAbortedStoreUnreachable)) >= 1
	})
	testutil.Equals(t, 0.0, counterValue(t, w1.reg, "thanos_compact_worker_tasks_total", string(OutcomeCompleted)))

	// Once the store recovers, the requeued task completes for real.
	w1.bkt.SetOnGet(nil)
	var got executeOutcome
	select {
	case got = <-outcome:
	case <-time.After(30 * time.Second):
		t.Fatal("compaction did not finish after the store recovered")
	}
	testutil.Ok(t, got.err)
	testutil.Equals(t, 1, len(got.compIDs))
}

// TestSubmitJournalBlipIsRetryable pins down that a transient journal write
// failure during Submit surfaces as a retryable error. A plain error would fall
// through the compactor's wait loop and exit the whole manager process, whose
// restart voids every in-flight lease.
func TestSubmitJournalBlipIsRetryable(t *testing.T) {
	bkt := compacttest.NewHookBucket(objstore.NewInMemBucket())
	sched, err := NewScheduler(t.Context(), log.NewNopLogger(), bkt, prometheus.NewRegistry(), ManagerConfig{
		JournalID: "shard-blip",
	})
	testutil.Ok(t, err)

	bkt.SetOnUpload(func(_ context.Context, name string) error {
		if strings.HasPrefix(name, JournalPrefix) {
			return errors.New("injected: journal write failed")
		}
		return nil
	})

	_, err = sched.Submit(t.Context(), Task{ID: "t-blip", Type: TaskCompaction})
	testutil.NotOk(t, err)
	testutil.Assert(t, compact.IsRetryError(err), "a journal blip in Submit must be retryable, got: %v", err)
	testutil.Assert(t, !compact.IsHaltError(err), "a journal blip in Submit must not halt")

	// The submission was rolled back: once the journal recovers the same task
	// can be submitted again.
	bkt.SetOnUpload(nil)
	_, err = sched.Submit(t.Context(), Task{ID: "t-blip", Type: TaskCompaction})
	testutil.Ok(t, err)
}

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

// TestAbandonedSourcesAreNotReplanned pins down the whole abandonment story: a
// task that loses its worker MaxAttempts times is abandoned with its own
// last_error, delivered as OutcomeAbandoned, and - crucially - its source
// blocks are not replanned into a fresh task with a fresh attempt budget, which
// used to repeat the worker-killing cycle forever.
func TestAbandonedSourcesAreNotReplanned(t *testing.T) {
	c := newTestCluster(t)

	cg, toCompact := c.makeGroup(labels.FromStrings("ext", "1"))
	outcome := c.execute(cg, toCompact)

	// Lose the worker MaxAttempts (3) times in a row, the hard way.
	for range 3 {
		w := c.startWorker("doomed")
		_ = gateChunks(w)
		c.waitFor("the doomed worker to lease the task", func() bool {
			e := c.journalTask(StateLeased)
			return e != nil
		})
		w.dead.Store(true)
		w.cancel()
		<-w.done
		c.waitFor("the lease to expire", func() bool {
			return c.journalTask(StateLeased) == nil
		})
	}

	var got executeOutcome
	select {
	case got = <-outcome:
	case <-time.After(15 * time.Second):
		t.Fatal("the abandoned task never reported back to its submitter")
	}
	// The submitter learns about it as a retryable failure so the manager's
	// control loop survives, and the journal records the real story.
	testutil.NotOk(t, got.err)
	testutil.Assert(t, compact.IsRetryError(got.err), "abandonment must not halt or crash the manager, got: %v", got.err)

	entry := c.journalTask(StateAbandoned)
	testutil.Assert(t, entry != nil, "the journal must record the abandoned task")
	testutil.Equals(t, 3, entry.Attempts)
	testutil.Assert(t, entry.LastError != nil, "abandonment must record its own last_error")
	testutil.Equals(t, OutcomeAbandoned, entry.LastError.Outcome)

	// Replanning the same blocks defers instead of minting a fresh task.
	redo := c.execute(cg, toCompact)
	select {
	case got = <-redo:
	case <-time.After(15 * time.Second):
		t.Fatal("replanning the abandoned blocks did not return")
	}
	testutil.Assert(t, errors.Is(got.err, compact.ErrPlanDeferred), "abandoned sources must be deferred, got: %v", got.err)

	// And the control loop turns the deferral into "do not rerun the group".
	stub := stubPlanner{plan: toCompact}
	rerun, ids, err := cg.CompactWithExecutor(t.Context(), t.TempDir(),
		stub, NewRemotePlanExecutor(c.logger, c.manager, c.sched, nil, 1, nil))
	testutil.Ok(t, err)
	testutil.Equals(t, false, rerun)
	testutil.Equals(t, 0, len(ids))
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
	sched, err := NewScheduler(t.Context(), log.NewNopLogger(), bkt, prometheus.NewRegistry(), ManagerConfig{
		JournalID: "shard-revoke",
		LeaseTTL:  time.Hour, // So nothing expires or persists on its own schedule.
	})
	testutil.Ok(t, err)

	_, err = sched.Submit(t.Context(), Task{ID: "t-revoke", Type: TaskCompaction})
	testutil.Ok(t, err)
	leased, err := sched.Lease(t.Context(), LeaseRequest{WorkerID: "w1", Accepts: []TaskType{TaskCompaction}})
	testutil.Ok(t, err)
	testutil.Assert(t, leased != nil, "the task must lease")

	// Force the lease to look expired while the periodic persist is not due,
	// then run maintenance: the requeue alone must reach the bucket.
	sched.mtx.Lock()
	sched.tasks["t-revoke"].entry.Lease.ExpiresAt = time.Now().Add(-time.Minute)
	sched.lastPersist = time.Now()
	sched.mtx.Unlock()
	testutil.Ok(t, sched.Maintain())

	j, err := ReadJournal(t.Context(), bkt, "shard-revoke")
	testutil.Ok(t, err)
	testutil.Equals(t, StatePending, j.Tasks["t-revoke"].State)
}

// TestOwnershipCheckRejectsLongExpiredLease pins down that the fail-closed
// pre-upload gate refuses a lease whose recorded expiry is further in the past
// than the grace allows. Without it, a worker partitioned from the manager but
// not from the bucket sailed through the gate on a stale journal.
func TestOwnershipCheckRejectsLongExpiredLease(t *testing.T) {
	ctx := t.Context()
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
	sched, err := NewScheduler(t.Context(), log.NewNopLogger(), bkt, prometheus.NewRegistry(), ManagerConfig{
		JournalID:   "shard-abort",
		LeaseTTL:    time.Millisecond, // Caps the requeue backoff for the test.
		MaxAttempts: 3,
	})
	testutil.Ok(t, err)

	resultCh, err := sched.Submit(t.Context(), Task{ID: "t-abort", Type: TaskCompaction})
	testutil.Ok(t, err)

	lease := func() *Task {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			task, err := sched.Lease(t.Context(), LeaseRequest{WorkerID: "w1", Accepts: []TaskType{TaskCompaction}})
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
		testutil.Ok(t, sched.Report(t.Context(), Result{
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

	j, err := ReadJournal(t.Context(), bkt, "shard-abort")
	testutil.Ok(t, err)
	testutil.Equals(t, StateAbandoned, j.Tasks["t-abort"].State)
	testutil.Equals(t, cap, j.Tasks["t-abort"].Aborts)
}

// TestAbortedRequeueBacksOff pins down that an aborted task is not leaseable
// again immediately.
func TestAbortedRequeueBacksOff(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	sched, err := NewScheduler(t.Context(), log.NewNopLogger(), bkt, prometheus.NewRegistry(), ManagerConfig{
		JournalID: "shard-backoff",
		LeaseTTL:  200 * time.Millisecond,
	})
	testutil.Ok(t, err)

	_, err = sched.Submit(t.Context(), Task{ID: "t-backoff", Type: TaskCompaction})
	testutil.Ok(t, err)
	task, err := sched.Lease(t.Context(), LeaseRequest{WorkerID: "w1"})
	testutil.Ok(t, err)
	testutil.Ok(t, sched.Report(t.Context(), Result{
		TaskID: task.ID, LeaseToken: task.LeaseToken, Generation: task.Generation,
		Outcome: OutcomeAbortedOwnershipLost,
	}))

	// Immediately after the abort the task is parked.
	again, err := sched.Lease(t.Context(), LeaseRequest{WorkerID: "w1"})
	testutil.Ok(t, err)
	testutil.Assert(t, again == nil, "an aborted task must back off before it is leaseable again")

	// After the backoff (capped at the lease TTL) it comes back.
	time.Sleep(250 * time.Millisecond)
	again, err = sched.Lease(t.Context(), LeaseRequest{WorkerID: "w1"})
	testutil.Ok(t, err)
	testutil.Assert(t, again != nil, "the task must be leaseable after the backoff")
}

// TestLeaseRefusesMismatchedJournalID pins down that a worker configured for a
// different journal is refused loudly instead of being handed tasks it would
// abort forever against the wrong journal.
func TestLeaseRefusesMismatchedJournalID(t *testing.T) {
	sched, err := NewScheduler(t.Context(), log.NewNopLogger(), objstore.NewInMemBucket(), prometheus.NewRegistry(), ManagerConfig{
		JournalID: "shard-a",
	})
	testutil.Ok(t, err)

	_, err = sched.Lease(t.Context(), LeaseRequest{WorkerID: "w1", JournalID: "shard-b"})
	testutil.NotOk(t, err)
	testutil.Assert(t, strings.Contains(err.Error(), "shard-b"), "the refusal must name both journals: %v", err)

	_, err = sched.Lease(t.Context(), LeaseRequest{WorkerID: "w1", JournalID: "shard-a"})
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
	cnt := func() prometheus.Counter { return promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "test"}) }

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
	e := NewRemotePlanExecutor(logger, bkt, sched, &listPlanner{plans: [][]*metadata.Meta{bracket}}, 4, nil)
	plans := e.planGroup(t.Context(), newGroup(false, append(append([]*metadata.Meta{}, first...), bracket...)...), first)
	testutil.Equals(t, 1, len(plans))

	// A genuinely disjoint second plan still runs concurrently.
	disjoint := []*metadata.Meta{meta(16000, 18000), meta(18000, 20000)}
	e = NewRemotePlanExecutor(logger, bkt, sched, &listPlanner{plans: [][]*metadata.Meta{disjoint}}, 4, nil)
	plans = e.planGroup(t.Context(), newGroup(false, append(append([]*metadata.Meta{}, first...), disjoint...)...), first)
	testutil.Equals(t, 2, len(plans))

	// Vertical compaction also permits independent plans when their full time envelopes are disjoint.
	e = NewRemotePlanExecutor(logger, bkt, sched, &listPlanner{plans: [][]*metadata.Meta{disjoint}}, 4, nil)
	plans = e.planGroup(t.Context(), newGroup(true, append(append([]*metadata.Meta{}, first...), disjoint...)...), first)
	testutil.Equals(t, 2, len(plans))
}

// vetoChecker refuses to let any block be deleted.
type vetoChecker struct{}

func (vetoChecker) CanDelete(_ *compact.Group, _ ulid.ULID) bool { return false }

// TestRemoteFinalizeRespectsDeletableCheckerAndRecordsMetrics pins down that
// manager-mode source retirement honors the BlockDeletableChecker extension
// point - the in-process executor always did - and that the group's compaction
// and garbage-collection counters move, instead of flatlining the moment a
// deployment flips to manager mode.
func TestRemoteFinalizeRespectsDeletableCheckerAndRecordsMetrics(t *testing.T) {
	c := newTestCluster(t)
	c.startWorker("w1")

	run := func(ext labels.Labels, checker compact.BlockDeletableChecker) ([]*metadata.Meta, prometheus.Counter, prometheus.Counter) {
		cg, toCompact := c.makeGroup(ext)
		compactions := promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "test_compactions"})
		gcBlocks := promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "test_gc"})
		cnt := func() prometheus.Counter { return promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "test"}) }
		// Rebuild the group with counters this test can read.
		mcg, err := compact.NewGroup(c.logger, c.manager, cg.Key(), ext, 0, false, false,
			compactions, cnt(), cnt(), cnt(), cnt(), gcBlocks, cnt(), cnt(), metadata.NoneFunc, 1, 1)
		testutil.Ok(t, err)
		for _, m := range toCompact {
			testutil.Ok(t, mcg.AppendMeta(m))
		}

		e := NewRemotePlanExecutor(c.logger, c.manager, c.sched, nil, 1, checker)
		ids, err := e.Execute(t.Context(), "", mcg, compact.Plan{Sources: toCompact})
		testutil.Ok(t, err)
		testutil.Equals(t, 1, len(ids))
		return toCompact, compactions, gcBlocks
	}

	hasDeletionMark := func(id ulid.ULID) bool {
		ok, err := c.shared.Exists(t.Context(), path.Join(id.String(), metadata.DeletionMarkFilename))
		testutil.Ok(t, err)
		return ok
	}

	// With the default checker the sources are retired and both counters move.
	toCompact, compactions, gcBlocks := run(labels.FromStrings("ext", "allow"), nil)
	for _, m := range toCompact {
		testutil.Assert(t, hasDeletionMark(m.ULID), "source %s must be deletion-marked", m.ULID)
	}
	testutil.Equals(t, 1.0, promtestutil.ToFloat64(compactions))
	testutil.Equals(t, 2.0, promtestutil.ToFloat64(gcBlocks))

	// A vetoing checker keeps every source, exactly as it does in-process.
	toCompact, compactions, gcBlocks = run(labels.FromStrings("ext", "veto"), vetoChecker{})
	for _, m := range toCompact {
		testutil.Assert(t, !hasDeletionMark(m.ULID), "the checker's veto on %s must hold", m.ULID)
	}
	testutil.Equals(t, 1.0, promtestutil.ToFloat64(compactions))
	testutil.Equals(t, 0.0, promtestutil.ToFloat64(gcBlocks))
}

// TestDispatchDownsamplingRecordsFailures pins down that a failed downsample
// task increments the same per-resolution failure counter the in-process
// downsampler feeds - previously manager mode exported the pre-seeded series
// frozen at zero, so failure alerts could never fire.
func TestDispatchDownsamplingRecordsFailures(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	sched, err := NewScheduler(t.Context(), log.NewNopLogger(), bkt, prometheus.NewRegistry(), ManagerConfig{
		JournalID:   "shard-ds",
		MaxAttempts: 1,
	})
	testutil.Ok(t, err)

	// A stand-in worker that fails everything it leases.
	ctx := t.Context()
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

	failures := promauto.With(nil).NewCounterVec(prometheus.CounterOpts{Name: "test_ds_failures"}, []string{"resolution"})
	err = DispatchDownsampling(t.Context(), log.NewNopLogger(), bkt, sched,
		map[ulid.ULID]*metadata.Meta{m.ULID: m}, downsample.PlanOptions{}, 1, metadata.NoneFunc, 1, false, nil, failures)
	testutil.NotOk(t, err)
	testutil.Equals(t, 1.0, promtestutil.ToFloat64(failures.WithLabelValues(m.Thanos.ResolutionString())))
}

// TestHeartbeatsAreNotStalledByJournalIO pins down that journal uploads happen
// outside the scheduler's state lock. Previously every Submit/Lease/Report held
// the lock across a bucket GET+PUT, so a slow object store froze all heartbeats
// - and frozen heartbeats are expired leases and discarded work.
func TestHeartbeatsAreNotStalledByJournalIO(t *testing.T) {
	bkt := compacttest.NewHookBucket(objstore.NewInMemBucket())
	sched, err := NewScheduler(t.Context(), log.NewNopLogger(), bkt, prometheus.NewRegistry(), ManagerConfig{
		JournalID: "shard-slow",
	})
	testutil.Ok(t, err)

	// Lease a task whose heartbeats we can measure.
	_, err = sched.Submit(t.Context(), Task{ID: "t-slow", Type: TaskCompaction})
	testutil.Ok(t, err)
	task, err := sched.Lease(t.Context(), LeaseRequest{WorkerID: "w1"})
	testutil.Ok(t, err)

	// Make every journal write take its time, and start a Submit that has to
	// sit in that slow upload.
	uploadStarted := make(chan struct{})
	bkt.SetOnUpload(func(_ context.Context, name string) error {
		if strings.HasPrefix(name, JournalPrefix) {
			select {
			case uploadStarted <- struct{}{}:
			default:
			}
			time.Sleep(500 * time.Millisecond)
		}
		return nil
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = sched.Submit(t.Context(), Task{ID: "t-slow-2", Type: TaskCompaction})
	}()
	<-uploadStarted

	// The heartbeat must not wait for that upload.
	start := time.Now()
	resp := sched.Heartbeat(HeartbeatRequest{TaskID: task.ID, LeaseToken: task.LeaseToken, Generation: task.Generation})
	elapsed := time.Since(start)
	testutil.Equals(t, true, resp.Acknowledged)
	testutil.Assert(t, elapsed < 250*time.Millisecond, "heartbeat stalled behind journal I/O for %s", elapsed)
	<-done
}

// TestWorkerSeenIsPruned pins down that the worker liveness map forgets workers
// gone for many lease TTLs, instead of growing forever under pod churn.
func TestWorkerSeenIsPruned(t *testing.T) {
	sched, err := NewScheduler(t.Context(), log.NewNopLogger(), objstore.NewInMemBucket(), prometheus.NewRegistry(), ManagerConfig{
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

func (e *countingExecutor) Execute(_ context.Context, _ string, _ *compact.Group, _ compact.Plan) ([]ulid.ULID, error) {
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
	cnt := func() prometheus.Counter { return promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "test"}) }
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
		wg.Go(func() {
			_, _, err := cg.CompactWithExecutor(t.Context(), t.TempDir(), stubPlanner{plan: []*metadata.Meta{m}}, e)
			testutil.Ok(t, err)
		})
	}
	wg.Wait()
	testutil.Equals(t, 1, e.maxSeen)
}

// TestOversizedTasksAreRefusedNotDispatched pins down tier-1 admission control:
// a plan whose expected size exceeds the configured worker capacity is never
// handed to a worker. It is recorded in the journal as oversized with the
// reason, counted, parked so the next pass does not re-refuse it into a fresh
// journal entry, and the group is deferred instead of rerun - turning "three
// workers died at minute 40" into an immediate, named refusal.
func TestOversizedTasksAreRefusedNotDispatched(t *testing.T) {
	c := newTestCluster(t)
	// A worker is running, and must never see the task.
	w1 := c.startWorker("w1")

	cg, toCompact := c.makeGroup(labels.FromStrings("ext", "1"))
	var wantSeries uint64
	for _, m := range toCompact {
		wantSeries += m.Stats.NumSeries
	}
	testutil.Assert(t, wantSeries > 0, "the fixture blocks must report series counts")

	// A limit just below what the plan needs.
	c.sched.conf.MaxTaskSeries = wantSeries - 1

	e := NewRemotePlanExecutor(c.logger, c.manager, c.sched, nil, 1, nil)
	_, err := e.Execute(t.Context(), "", cg, compact.Plan{Sources: toCompact})
	testutil.Assert(t, errors.Is(err, compact.ErrPlanDeferred), "an oversized plan must be deferred, got: %v", err)

	entry := c.journalTask(StateOversized)
	testutil.Assert(t, entry != nil, "the journal must record the oversized task")
	testutil.Equals(t, OutcomeOversized, entry.LastError.Outcome)
	testutil.Assert(t, strings.Contains(entry.LastError.Message, "max-task-series"), "the reason must name the limit: %s", entry.LastError.Message)
	testutil.Equals(t, wantSeries, entry.Task.ExpectedSeries)

	// A second pass parks on the existing entry instead of minting another.
	_, err = e.Execute(t.Context(), "", cg, compact.Plan{Sources: toCompact})
	testutil.Assert(t, errors.Is(err, compact.ErrPlanDeferred), "the parked plan must stay deferred, got: %v", err)
	oversized := 0
	for _, e := range c.journal().Tasks {
		if e.State == StateOversized {
			oversized++
		}
	}
	testutil.Equals(t, 1, oversized)

	// The worker never leased anything.
	testutil.Equals(t, 0.0, counterValue(t, w1.reg, "thanos_compact_worker_tasks_total", string(OutcomeCompleted)))

	// Raising the limit lets the same plan through.
	c.sched.conf.MaxTaskSeries = 0
	// The parked entry has to be cleared, as an operator would clear it.
	c.sched.mtx.Lock()
	for id, e := range c.sched.journal.Tasks {
		if e.State == StateOversized {
			delete(c.sched.journal.Tasks, id)
		}
	}
	c.sched.mtx.Unlock()
	ids, err := e.Execute(t.Context(), "", cg, compact.Plan{Sources: toCompact})
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(ids))
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
	sched, err := NewScheduler(t.Context(), log.NewNopLogger(), bkt, prometheus.NewRegistry(), ManagerConfig{
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
	testutil.Ok(t, DispatchDownsampling(t.Context(), log.NewNopLogger(), bkt, sched,
		map[ulid.ULID]*metadata.Meta{m.ULID: m}, downsample.PlanOptions{}, 1, metadata.NoneFunc, 1, false, nil, nil))

	j, err := ReadJournal(t.Context(), bkt, "shard-ds-big")
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
