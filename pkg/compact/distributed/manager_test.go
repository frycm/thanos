// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/compact"
)

func testScheduler(t *testing.T, bkt objstore.Bucket, conf ManagerConfig) *Scheduler {
	t.Helper()
	if conf.JournalID == "" {
		conf.JournalID = "shard-a"
	}
	s, err := NewScheduler(context.Background(), log.NewNopLogger(), bkt, prometheus.NewRegistry(), conf)
	testutil.Ok(t, err)
	return s
}

func submitTask(t *testing.T, s *Scheduler) <-chan Result {
	t.Helper()
	ch, err := s.Submit(context.Background(), Task{ID: "t1", Type: TaskCompaction, SourceBlocks: []string{"src-t1"}})
	testutil.Ok(t, err)
	return ch
}

// TestSchedulerLeaseLifecycle walks a task from queued through leased to done.
func TestSchedulerLeaseLifecycle(t *testing.T) {
	ctx := context.Background()
	bkt := objstore.NewInMemBucket()
	s := testScheduler(t, bkt, ManagerConfig{})

	resultCh := submitTask(t, s)

	task, err := s.Lease(ctx, LeaseRequest{WorkerID: "w1"})
	testutil.Ok(t, err)
	testutil.Assert(t, task != nil, "expected a task to be leased")
	testutil.Equals(t, "t1", task.ID)
	testutil.Assert(t, task.LeaseToken != "", "expected a lease token")

	// A second worker gets nothing, the queue is empty.
	none, err := s.Lease(ctx, LeaseRequest{WorkerID: "w2"})
	testutil.Ok(t, err)
	testutil.Assert(t, none == nil, "expected no task for the second worker")

	// The holder can heartbeat; a stale token cannot.
	testutil.Equals(t, true, s.Heartbeat(HeartbeatRequest{TaskID: "t1", LeaseToken: task.LeaseToken, Generation: task.Generation}).Acknowledged)
	testutil.Equals(t, false, s.Heartbeat(HeartbeatRequest{TaskID: "t1", LeaseToken: "wrong", Generation: task.Generation}).Acknowledged)

	testutil.Ok(t, s.Report(ctx, Result{
		TaskID: "t1", LeaseToken: task.LeaseToken, Generation: task.Generation,
		Outcome: OutcomeCompleted, OutputBlocks: []string{"out"},
	}))

	select {
	case res := <-resultCh:
		testutil.Equals(t, OutcomeCompleted, res.Outcome)
	case <-time.After(time.Second):
		t.Fatal("expected the submitter to be woken up")
	}

	j, err := ReadJournal(ctx, bkt, "shard-a")
	testutil.Ok(t, err)
	testutil.Equals(t, StateCompleted, j.Tasks["t1"].State)
}

// TestSchedulerExpiredLeaseIsRequeued asserts a task whose worker went silent
// goes back to the queue rather than being stuck with that worker.
func TestSchedulerExpiredLeaseIsRequeued(t *testing.T) {
	ctx := context.Background()
	s := testScheduler(t, objstore.NewInMemBucket(), ManagerConfig{LeaseTTL: time.Millisecond})

	submitTask(t, s)
	first, err := s.Lease(ctx, LeaseRequest{WorkerID: "w1"})
	testutil.Ok(t, err)
	testutil.Assert(t, first != nil, "expected a task")

	time.Sleep(10 * time.Millisecond)

	second, err := s.Lease(ctx, LeaseRequest{WorkerID: "w2"})
	testutil.Ok(t, err)
	testutil.Assert(t, second != nil, "expected the expired task to be handed to another worker")
	testutil.Equals(t, "t1", second.ID)
	testutil.Assert(t, second.LeaseToken != first.LeaseToken, "expected a fresh lease token")

	// The original worker can no longer heartbeat or report.
	testutil.Equals(t, false, s.Heartbeat(HeartbeatRequest{TaskID: "t1", LeaseToken: first.LeaseToken, Generation: first.Generation}).Acknowledged)
}

// TestSchedulerAbortedResultDoesNotCountAsAttempt asserts that a worker
// discarding its work returns the task to the queue without burning an attempt,
// because nothing about the task itself failed.
func TestSchedulerAbortedResultDoesNotCountAsAttempt(t *testing.T) {
	for _, outcome := range []Outcome{OutcomeAbortedStoreUnreachable, OutcomeAbortedOwnershipLost, OutcomeAbortedWorkerShutdown} {
		t.Run(string(outcome), func(t *testing.T) {
			ctx := context.Background()
			bkt := objstore.NewInMemBucket()
			s := testScheduler(t, bkt, ManagerConfig{})

			submitTask(t, s)
			task, err := s.Lease(ctx, LeaseRequest{WorkerID: "w1"})
			testutil.Ok(t, err)

			testutil.Ok(t, s.Report(ctx, Result{
				TaskID: "t1", LeaseToken: task.LeaseToken, Generation: task.Generation,
				Outcome: outcome,
			}))

			j, err := ReadJournal(ctx, bkt, "shard-a")
			testutil.Ok(t, err)
			testutil.Equals(t, StatePending, j.Tasks["t1"].State)
			testutil.Equals(t, 0, j.Tasks["t1"].Attempts)

			again, err := s.Lease(ctx, LeaseRequest{WorkerID: "w2"})
			testutil.Ok(t, err)
			testutil.Assert(t, again != nil, "expected the task to be available again")
		})
	}
}

// TestSchedulerGivesUpAfterMaxAttempts asserts a task that keeps failing ends up
// failed rather than retried forever.
func TestSchedulerGivesUpAfterMaxAttempts(t *testing.T) {
	ctx := context.Background()
	s := testScheduler(t, objstore.NewInMemBucket(), ManagerConfig{MaxAttempts: 2})

	resultCh := submitTask(t, s)

	for i := 0; i < 2; i++ {
		task, err := s.Lease(ctx, LeaseRequest{WorkerID: "w1"})
		testutil.Ok(t, err)
		testutil.Assert(t, task != nil, "expected attempt %d to be leased", i)
		testutil.Ok(t, s.Report(ctx, Result{
			TaskID: "t1", LeaseToken: task.LeaseToken, Generation: task.Generation,
			Outcome: OutcomeFailedRetryable, ErrorMessage: "boom",
		}))
	}

	select {
	case res := <-resultCh:
		testutil.Equals(t, OutcomeFailedRetryable, res.Outcome)
	case <-time.After(time.Second):
		t.Fatal("expected the task to be given up on")
	}

	none, err := s.Lease(ctx, LeaseRequest{WorkerID: "w1"})
	testutil.Ok(t, err)
	testutil.Assert(t, none == nil, "a failed task must not be handed out again")
}

// TestSchedulerTakeoverVoidsOldLeases asserts a restarted manager bumps the
// journal generation, which invalidates leases handed out by its predecessor.
func TestSchedulerTakeoverVoidsOldLeases(t *testing.T) {
	ctx := context.Background()
	bkt := objstore.NewInMemBucket()

	first := testScheduler(t, bkt, ManagerConfig{})
	submitTask(t, first)
	leased, err := first.Lease(ctx, LeaseRequest{WorkerID: "w1"})
	testutil.Ok(t, err)

	// A new manager takes over the same journal. The unfinished task is dropped
	// rather than carried over: the new manager replans it, and carrying it in
	// the journal would only leak it, since nothing would ever lease it.
	second := testScheduler(t, bkt, ManagerConfig{})
	testutil.Assert(t, second.Generation() > first.Generation(), "expected the generation to be bumped")

	j, err := ReadJournal(ctx, bkt, "shard-a")
	testutil.Ok(t, err)
	_, carried := j.Tasks["t1"]
	testutil.Assert(t, !carried, "an unfinished task must not survive a takeover")

	// The worker of the previous manager can no longer prove it owns the task.
	got, _ := CheckOwnership(ctx, bkt, "shard-a", "t1", leased.LeaseToken, leased.Generation)
	testutil.Equals(t, OwnershipLost, got)
}

// TestSchedulerHaltsWhenAnotherManagerTakesOver asserts the surviving manager
// stops rather than writing over a journal a second manager now owns.
func TestSchedulerHaltsWhenAnotherManagerTakesOver(t *testing.T) {
	ctx := context.Background()
	bkt := objstore.NewInMemBucket()

	first := testScheduler(t, bkt, ManagerConfig{})
	_ = testScheduler(t, bkt, ManagerConfig{}) // takes over

	_, err := first.Submit(ctx, Task{ID: "t2", Type: TaskCompaction})
	testutil.NotOk(t, err)
	testutil.Equals(t, true, compact.IsHaltError(err))
}

// TestErrorClassSurvivesTheWire asserts every failure class a worker can report
// is rebuilt as the same class of error on the manager, which is what lets the
// compactor's control loop react to a remote failure as it would a local one.
func TestErrorClassSurvivesTheWire(t *testing.T) {
	block := "01ARZ3NDEKTSV4RRFFQ69G5FAV"

	halt := ReconstructError(Result{Outcome: OutcomeFailedHalt, ErrorMessage: "boom"})
	testutil.Equals(t, true, compact.IsHaltError(halt))

	retry := ReconstructError(Result{Outcome: OutcomeFailedRetryable, ErrorMessage: "boom"})
	testutil.Equals(t, true, compact.IsRetryError(retry))

	i347 := ReconstructError(Result{Outcome: OutcomeFailedIssue347, OffendingBlock: block, ErrorMessage: "boom"})
	testutil.Equals(t, true, compact.IsIssue347Error(i347))

	ooo := ReconstructError(Result{Outcome: OutcomeFailedOOOChunks, OffendingBlock: block, ErrorMessage: "boom"})
	testutil.Equals(t, true, compact.IsOutOfOrderChunkError(ooo))

	// A malformed block ID degrades to a retry rather than losing the failure.
	testutil.Equals(t, true, compact.IsRetryError(ReconstructError(Result{Outcome: OutcomeFailedIssue347, OffendingBlock: "nope"})))

	// Aborted work is not a compaction failure.
	testutil.Ok(t, ReconstructError(Result{Outcome: OutcomeAbortedOwnershipLost}))
	testutil.Ok(t, ReconstructError(Result{Outcome: OutcomeAbortedStoreUnreachable}))
	testutil.Ok(t, ReconstructError(Result{Outcome: OutcomeCompleted}))
	testutil.Ok(t, ReconstructError(Result{Outcome: OutcomeAbortedWorkerShutdown}))
}

// countingBucket counts uploads, to observe journal writes.
type countingBucket struct {
	objstore.Bucket
	uploads int
}

func (b *countingBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	b.uploads++
	return b.Bucket.Upload(ctx, name, r, opts...)
}

// TestSchedulerMaintainKeepsJournalFresh asserts an idle manager still writes
// its journal once per lease TTL, and not more often. That write is what makes
// the journal's timestamp a liveness signal the rollback tool can rely on: a
// journal not written for a few TTLs belongs to no running manager.
func TestSchedulerMaintainKeepsJournalFresh(t *testing.T) {
	ctx := context.Background()
	bkt := &countingBucket{Bucket: objstore.NewInMemBucket()}
	s := testScheduler(t, bkt, ManagerConfig{LeaseTTL: 30 * time.Millisecond})

	before, err := ReadJournal(ctx, bkt, "shard-a")
	testutil.Ok(t, err)
	writesAfterTakeover := bkt.uploads

	// Within a TTL of the last write, maintenance does not touch the journal.
	testutil.Ok(t, s.Maintain())
	testutil.Equals(t, writesAfterTakeover, bkt.uploads)

	time.Sleep(50 * time.Millisecond)
	testutil.Ok(t, s.Maintain())
	testutil.Equals(t, writesAfterTakeover+1, bkt.uploads)

	after, err := ReadJournal(ctx, bkt, "shard-a")
	testutil.Ok(t, err)
	testutil.Assert(t, after.UpdatedAt.After(before.UpdatedAt), "the journal timestamp must advance")
}

// TestSchedulerMaintainReportsTakeover asserts the maintenance tick surfaces
// a journal takeover as a halt once it writes the journal, so a manager that
// is idle - and therefore never submits or records anything - still learns
// that it has to stop.
func TestSchedulerMaintainReportsTakeover(t *testing.T) {
	ctx := context.Background()
	bkt := objstore.NewInMemBucket()
	s := testScheduler(t, bkt, ManagerConfig{LeaseTTL: 10 * time.Millisecond})

	j, err := ReadJournal(ctx, bkt, "shard-a")
	testutil.Ok(t, err)
	j.Owner = "somebody-else"
	testutil.Ok(t, WriteJournal(ctx, bkt, j))

	time.Sleep(20 * time.Millisecond)
	err = s.Maintain()
	testutil.NotOk(t, err)
	testutil.Equals(t, true, compact.IsHaltError(err))
}
