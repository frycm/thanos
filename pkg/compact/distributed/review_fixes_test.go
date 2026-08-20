// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
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

// TestOversizedLimitsIgnoreUnknownSizes pins down that blocks which report no
// stats can never be refused: an absent figure is not evidence of size.
func TestOversizedLimitsIgnoreUnknownSizes(t *testing.T) {
	task := Task{ID: "t", Type: TaskCompaction, SourceBlocks: []string{"a"}}
	conf := ManagerConfig{MaxTaskSeries: 1, MaxTaskIndexBytes: 1}
	testutil.Equals(t, "", oversizedReason(task, conf))
}
