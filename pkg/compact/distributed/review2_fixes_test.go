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

// This file holds regression tests for the findings of the second review of
// the manager/worker split, the one on data safety and races.

// TestReadJournalRejectsForeignBody pins down that a journal whose body names
// another shard is not read as this shard's. A journal copied under another
// path by mistake would otherwise be taken over as that shard's, and every
// write of the manager would then go to the path the body names - the other
// shard's journal, with two managers writing it.
func TestReadJournalRejectsForeignBody(t *testing.T) {
	ctx := context.Background()
	bkt := objstore.NewInMemBucket()

	testutil.Ok(t, WriteJournal(ctx, bkt, NewJournal("shard-a", "")))
	r, err := bkt.Get(ctx, JournalPath("shard-a"))
	testutil.Ok(t, err)
	testutil.Ok(t, bkt.Upload(ctx, JournalPath("shard-b"), r))
	testutil.Ok(t, r.Close())

	_, err = ReadJournal(ctx, bkt, "shard-b")
	testutil.NotOk(t, err)
	testutil.Assert(t, strings.Contains(err.Error(), "shard-a"), "the error must name the journal found, got: %v", err)

	_, err = NewScheduler(ctx, log.NewNopLogger(), bkt, prometheus.NewRegistry(), ManagerConfig{JournalID: "shard-b"})
	testutil.NotOk(t, err)

	// shard-a's own journal is untouched by the refusal.
	j, err := ReadJournal(ctx, bkt, "shard-a")
	testutil.Ok(t, err)
	testutil.Equals(t, "shard-a", j.JournalID)
}

// TestLeaseRefusesMismatchedDedupConfig pins down that a worker whose merge
// configuration differs from the manager's is refused a lease. The merge
// function and the replica labels are baked into the worker's compactor and
// leave no trace in the blocks, so nothing downstream would catch a worker
// merging the sources differently than the manager planned for.
func TestLeaseRefusesMismatchedDedupConfig(t *testing.T) {
	sched, err := NewScheduler(context.Background(), log.NewNopLogger(), objstore.NewInMemBucket(), prometheus.NewRegistry(), ManagerConfig{
		JournalID: "shard-a", DedupFunc: "penalty", DedupReplicaLabels: []string{"replica", "rule_replica"},
	})
	testutil.Ok(t, err)

	for _, tc := range []struct {
		name string
		req  LeaseRequest
		want string
	}{
		{"no dedup func", LeaseRequest{WorkerID: "w1", DedupReplicaLabels: []string{"replica", "rule_replica"}}, "--deduplication.func"},
		{"other dedup func", LeaseRequest{WorkerID: "w1", DedupFunc: "other", DedupReplicaLabels: []string{"replica", "rule_replica"}}, "--deduplication.func"},
		{"no replica labels", LeaseRequest{WorkerID: "w1", DedupFunc: "penalty"}, "--deduplication.replica-label"},
		{"fewer replica labels", LeaseRequest{WorkerID: "w1", DedupFunc: "penalty", DedupReplicaLabels: []string{"replica"}}, "--deduplication.replica-label"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sched.Lease(context.Background(), tc.req)
			testutil.NotOk(t, err)
			testutil.Assert(t, strings.Contains(err.Error(), tc.want), "the refusal must name the flag to fix: %v", err)
			testutil.Assert(t, strings.Contains(err.Error(), "w1"), "the refusal must name the worker: %v", err)
		})
	}

	// The same configuration in any order is accepted.
	_, err = sched.Lease(context.Background(), LeaseRequest{WorkerID: "w1", DedupFunc: "penalty", DedupReplicaLabels: []string{"rule_replica", "replica"}})
	testutil.Ok(t, err)

	// A manager on the defaults accepts a worker on the defaults, which is
	// what every existing deployment sends.
	plain, err := NewScheduler(context.Background(), log.NewNopLogger(), objstore.NewInMemBucket(), prometheus.NewRegistry(), ManagerConfig{JournalID: "shard-b"})
	testutil.Ok(t, err)
	_, err = plain.Lease(context.Background(), LeaseRequest{WorkerID: "w1"})
	testutil.Ok(t, err)
	_, err = plain.Lease(context.Background(), LeaseRequest{WorkerID: "w1", DedupFunc: "penalty", DedupReplicaLabels: []string{"replica"}})
	testutil.NotOk(t, err)
}

// TestTaskCarriesDedupFunc pins down that every task is stamped with the
// manager's merge function, so that a worker can refuse one it was not built
// for even when the manager that planned it is gone.
func TestTaskCarriesDedupFunc(t *testing.T) {
	sched, err := NewScheduler(context.Background(), log.NewNopLogger(), objstore.NewInMemBucket(), prometheus.NewRegistry(), ManagerConfig{
		JournalID: "shard-a", DedupFunc: "penalty", DedupReplicaLabels: []string{"replica"},
	})
	testutil.Ok(t, err)
	_, err = sched.Submit(context.Background(), Task{ID: "t1", Type: TaskCompaction})
	testutil.Ok(t, err)

	task, err := sched.Lease(context.Background(), LeaseRequest{WorkerID: "w1", DedupFunc: "penalty", DedupReplicaLabels: []string{"replica"}})
	testutil.Ok(t, err)
	testutil.Assert(t, task != nil, "the task must be leasable")
	testutil.Equals(t, "penalty", task.Group.DedupFunc)

	// The stamp survives the journal.
	j, err := ReadJournal(context.Background(), sched.bkt, "shard-a")
	testutil.Ok(t, err)
	testutil.Equals(t, "penalty", j.Tasks["t1"].Task.Group.DedupFunc)
}

// TestWorkerRefusesTaskForOtherDedupFunc pins down that a worker fails, rather
// than executes, a task planned for another merge function - retryable, so a
// matching worker can still pick it up.
func TestWorkerRefusesTaskForOtherDedupFunc(t *testing.T) {
	w, err := NewWorker(log.NewNopLogger(), objstore.NewInMemBucket(), nil, nil, prometheus.NewRegistry(), WorkerConfig{
		WorkerID: "w1", JournalID: "shard-a", DataDir: t.TempDir(),
	})
	testutil.Ok(t, err)

	res := w.execute(context.Background(), Task{ID: "t1", Type: TaskCompaction, Group: GroupSpec{DedupFunc: "penalty"}}, nil)
	testutil.Equals(t, OutcomeFailedRetryable, res.Outcome)
	testutil.Assert(t, strings.Contains(res.ErrorMessage, "penalty"), "the failure must name the function the task was planned for: %s", res.ErrorMessage)
}

// TestMaintainPrunesAndUnparks pins down that the journal is pruned while the
// manager runs, and that an operator can release a parked task by writing an
// unpark marker, without restarting the manager. Before, terminal entries were
// only pruned when a scheduler was created, so a parked block set stayed
// parked - stalling its group under vertical compaction - until a restart, and
// the journal grew without bound in the meantime.
func TestMaintainPrunesAndUnparks(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	sched, err := NewScheduler(context.Background(), log.NewNopLogger(), bkt, prometheus.NewRegistry(), ManagerConfig{
		JournalID: "shard-a", JournalRetention: time.Hour, LeaseTTL: time.Hour,
	})
	testutil.Ok(t, err)

	sources := []string{"01ARZ3NDEKTSV4RRFFQ69G5FAV"}
	sched.MarkOversized(Task{ID: "t-big", Type: TaskCompaction, SourceBlocks: sources}, "too big")
	sched.MarkOversized(Task{ID: "t-old", Type: TaskCompaction, SourceBlocks: []string{"01BX5ZZKBKACTAV9WEVGEMMVRZ"}}, "too big, long ago")
	sched.mtx.Lock()
	sched.journal.Tasks["t-old"].UpdatedAt = time.Now().Add(-2 * time.Hour)
	sched.mtx.Unlock()
	testutil.Equals(t, true, sched.SourcesParked(sources))

	// A maintenance tick ages the old entry out, and persists that.
	testutil.Ok(t, sched.Maintain())
	testutil.Equals(t, false, sched.SourcesParked([]string{"01BX5ZZKBKACTAV9WEVGEMMVRZ"}))
	testutil.Equals(t, true, sched.SourcesParked(sources))
	j, err := ReadJournal(context.Background(), bkt, "shard-a")
	testutil.Ok(t, err)
	_, ok := j.Tasks["t-old"]
	testutil.Equals(t, false, ok)

	// The operator releases the other one; a marker for an unknown task and
	// one for a task that is not parked are ignored.
	testutil.Ok(t, bkt.Upload(context.Background(), UnparkPath("shard-a", "t-big"), strings.NewReader("")))
	testutil.Ok(t, bkt.Upload(context.Background(), UnparkPath("shard-a", "t-unknown"), strings.NewReader("")))
	_, err = sched.Submit(context.Background(), Task{ID: "t-live", Type: TaskCompaction})
	testutil.Ok(t, err)
	testutil.Ok(t, bkt.Upload(context.Background(), UnparkPath("shard-a", "t-live"), strings.NewReader("")))

	testutil.Ok(t, sched.Maintain())
	testutil.Equals(t, false, sched.SourcesParked(sources))
	j, err = ReadJournal(context.Background(), bkt, "shard-a")
	testutil.Ok(t, err)
	_, ok = j.Tasks["t-big"]
	testutil.Equals(t, false, ok)
	testutil.Equals(t, StatePending, j.Tasks["t-live"].State)

	// The markers are consumed once the journal is written.
	for _, id := range []string{"t-big", "t-unknown", "t-live"} {
		exists, err := bkt.Exists(context.Background(), UnparkPath("shard-a", id))
		testutil.Ok(t, err)
		testutil.Equals(t, false, exists, "marker for %s must be removed", id)
	}
}
