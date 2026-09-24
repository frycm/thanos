// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"
	"go.uber.org/atomic"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compact/compacttest"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/logutil"
)

type heartbeatFailureClient struct {
	TaskClient
	calls atomic.Int64
	block bool
}

func (c *heartbeatFailureClient) Heartbeat(ctx context.Context, _ HeartbeatRequest) (HeartbeatResponse, error) {
	if c.calls.Add(1) == 1 {
		return HeartbeatResponse{Acknowledged: true}, nil
	}
	if c.block {
		<-ctx.Done()
		return HeartbeatResponse{}, ctx.Err()
	}
	return HeartbeatResponse{}, errors.New("manager unreachable")
}

func TestWorkerStopsAfterUnacknowledgedLeaseTTL(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out real lease TTLs")
	}
	for _, blocked := range []bool{false, true} {
		t.Run(map[bool]string{false: "failed requests", true: "blocked request"}[blocked], func(t *testing.T) {
			client := &heartbeatFailureClient{block: blocked}
			w := &Worker{logger: log.NewNopLogger(), client: client, conf: WorkerConfig{HeartbeatInterval: time.Hour}}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			acknowledged := testAtomicBool(true)
			done := make(chan struct{})
			go func() {
				defer close(done)
				w.heartbeat(ctx, Task{ID: ulid.Make().String(), LeaseTTL: 90 * time.Millisecond}, acknowledged, cancel)
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("worker kept running past an unacknowledged lease TTL")
			}
			testutil.Equals(t, false, acknowledged.Load())
			testutil.NotOk(t, ctx.Err())
			testutil.Assert(t, client.calls.Load() >= 2, "must heartbeat immediately and retry within the TTL")
		})
	}
}

func TestWorkerRejectsTaskPathBeforeTouchingDisk(t *testing.T) {
	root := t.TempDir()
	victim := filepath.Join(root, "victim", "keep")
	testutil.Ok(t, os.MkdirAll(filepath.Dir(victim), 0750))
	testutil.Ok(t, os.WriteFile(victim, []byte("keep"), 0600))
	w := &Worker{conf: WorkerConfig{DataDir: filepath.Join(root, "worker")}}
	for _, tc := range []struct {
		name string
		id   string
	}{
		{name: "relative path to a sibling", id: "../victim"},
		{name: "parent directory", id: ".."},
		{name: "absolute path", id: filepath.Dir(victim)},
		{name: "empty", id: ""},
		{name: "not a ULID", id: "not-a-ulid"},
		{name: "lowercase ULID", id: strings.ToLower(ulid.Make().String())},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := w.execute(t.Context(), Task{ID: tc.id, Type: TaskCompaction}, testAtomicBool(true))
			testutil.Equals(t, OutcomeFailedRetryable, res.Outcome)
			testutil.Assert(t, strings.Contains(res.ErrorMessage, "ULID"), "must reject the ID before executing: %s", res.ErrorMessage)
			b, err := os.ReadFile(victim)
			testutil.Ok(t, err)
			testutil.Equals(t, "keep", string(b))
		})
	}
	_, err := os.Stat(w.conf.DataDir)
	testutil.Assert(t, os.IsNotExist(err), "invalid tasks must not create directories")
}

func TestWorkerStartupCleansAbandonedTaskDirectories(t *testing.T) {
	dir := t.TempDir()
	abandoned := filepath.Join(dir, ulid.Make().String())
	unrelated := filepath.Join(dir, "operator-files")
	outside := t.TempDir()
	for _, d := range []string{abandoned, unrelated, outside} {
		testutil.Ok(t, os.MkdirAll(d, 0750))
		testutil.Ok(t, os.WriteFile(filepath.Join(d, "keep"), []byte("content"), 0600))
	}
	link := filepath.Join(dir, ulid.Make().String())
	testutil.Ok(t, os.Symlink(outside, link))
	w, err := NewWorker(log.NewNopLogger(), nil, nil, nil, prometheus.NewRegistry(), WorkerConfig{JournalID: "j", DataDir: dir})
	testutil.Ok(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	testutil.Assert(t, errors.Is(w.Run(ctx), context.Canceled), "startup cleanup precedes the lease loop")
	_, err = os.Stat(abandoned)
	testutil.Assert(t, os.IsNotExist(err), "old task downloads must be removed")
	for _, d := range []string{unrelated, outside, link} {
		_, err = os.Stat(filepath.Join(d, "keep"))
		testutil.Ok(t, err)
	}
}

func TestWorkerResourceErrorsDoNotHaltManager(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want Outcome
	}{
		{name: "no space left", err: syscall.ENOSPC, want: OutcomeFailedRetryable},
		{name: "too many open files", err: syscall.EMFILE, want: OutcomeFailedRetryable},
		{name: "no space left writing chunks", err: &os.PathError{Op: "write", Path: "chunks", Err: syscall.ENOSPC}, want: OutcomeFailedRetryable},
		{name: "invalid index", err: errors.New("invalid index"), want: OutcomeFailedHalt},
		// A path error is not evidence of a sick worker: a source block whose
		// index object is missing or truncated fails with ENOENT or EINVAL from
		// the bucket's side, and that has to stay the halt the compactor raised,
		// or the group is silently replanned every pass instead of being seen.
		{name: "missing index", err: &os.PathError{Op: "open", Path: "index", Err: syscall.ENOENT}, want: OutcomeFailedHalt},
		{name: "invalid argument", err: syscall.EINVAL, want: OutcomeFailedHalt},
		{name: "truncated index", err: &os.PathError{Op: "mmap", Path: "index", Err: syscall.EINVAL}, want: OutcomeFailedHalt},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outcome, _ := ClassifyError(compact.NewHaltError(tc.err))
			testutil.Equals(t, tc.want, outcome)
		})
	}
}

// TestLeaseExpiredDuringPersistLeavesNoDuplicateQueueEntry pins down that a
// lease requeued by expiry while its own journal write is still in flight ends
// up in the queue exactly once. The Lease call releases the state lock for the
// write, so expiry can run in between; a second copy of the id would never be
// removed and would inflate the queue metrics forever.
func TestLeaseExpiredDuringPersistLeavesNoDuplicateQueueEntry(t *testing.T) {
	bkt := compacttest.NewHookBucket(objstore.NewInMemBucket())
	sched, err := NewScheduler(t.Context(), log.NewNopLogger(), bkt, prometheus.NewRegistry(), ManagerConfig{
		JournalID:   "shard-dup",
		LeaseTTL:    20 * time.Millisecond,
		MaxAttempts: 3,
	})
	testutil.Ok(t, err)
	_, err = sched.Submit(t.Context(), Task{ID: "t-dup", Type: TaskCompaction})
	testutil.Ok(t, err)

	uploadStarted := make(chan struct{}, 1)
	release := make(chan struct{})
	bkt.SetOnUpload(func(_ context.Context, name string) error {
		if strings.HasPrefix(name, JournalPrefix) {
			select {
			case uploadStarted <- struct{}{}:
			default:
			}
			<-release
		}
		return nil
	})

	leased := make(chan error, 1)
	go func() {
		_, leaseErr := sched.Lease(t.Context(), LeaseRequest{WorkerID: "w1", Accepts: []TaskType{TaskCompaction}})
		leased <- leaseErr
	}()
	<-uploadStarted

	// The lease lapses while its journal write hangs; the next maintenance
	// pass requeues the task behind the writer's back.
	time.Sleep(50 * time.Millisecond)
	maintained := make(chan error, 1)
	go func() { maintained <- sched.Maintain() }()
	// Maintain's own journal write queues up behind the hanging one.
	time.Sleep(20 * time.Millisecond)
	close(release)
	testutil.Ok(t, <-maintained)

	err = <-leased
	testutil.NotOk(t, err)
	testutil.Assert(t, compact.IsRetryError(err), "a lease that changed underneath its persist must be retryable, got %v", err)

	sched.mtx.Lock()
	defer sched.mtx.Unlock()
	testutil.Equals(t, []string{"t-dup"}, sched.queue)
	testutil.Equals(t, StatePending, sched.tasks["t-dup"].entry.State)
}

// TestWorkerShutdownAbortsAreNotChargedOrBackedOff pins down that a graceful
// worker shutdown neither eats into the abort budget nor delays the requeue:
// the operator restarted the worker, the task did nothing wrong, and a
// long-running task must survive an arbitrary number of rolling restarts.
func TestWorkerShutdownAbortsAreNotChargedOrBackedOff(t *testing.T) {
	sched, err := NewScheduler(t.Context(), log.NewNopLogger(), objstore.NewInMemBucket(), prometheus.NewRegistry(), ManagerConfig{
		JournalID:   "shard-restart",
		LeaseTTL:    time.Hour, // Any backoff would be visible as an unleaseable task.
		MaxAttempts: 3,
	})
	testutil.Ok(t, err)
	resultCh, err := sched.Submit(t.Context(), Task{ID: "t-restart", Type: TaskCompaction})
	testutil.Ok(t, err)

	for range abortCapFor(3) + 1 {
		task, leaseErr := sched.Lease(t.Context(), LeaseRequest{WorkerID: "w1", Accepts: []TaskType{TaskCompaction}})
		testutil.Ok(t, leaseErr)
		testutil.Assert(t, task != nil, "the task must be leaseable again immediately after a shutdown abort")
		testutil.Ok(t, sched.Report(t.Context(), Result{
			TaskID: task.ID, LeaseToken: task.LeaseToken, Generation: task.Generation,
			Outcome: OutcomeAbortedWorkerShutdown, ErrorMessage: "rolling restart",
		}))
	}
	select {
	case res := <-resultCh:
		t.Fatalf("shutdown aborts must not fail the task, got %v", res)
	default:
	}
	sched.mtx.Lock()
	entry := sched.tasks["t-restart"].entry
	testutil.Equals(t, 0, entry.Aborts)
	testutil.Equals(t, 0, entry.Attempts)
	testutil.Equals(t, StatePending, entry.State)
	sched.mtx.Unlock()

	// Every other abort kind still counts and backs off.
	task, err := sched.Lease(t.Context(), LeaseRequest{WorkerID: "w1", Accepts: []TaskType{TaskCompaction}})
	testutil.Ok(t, err)
	testutil.Ok(t, sched.Report(t.Context(), Result{
		TaskID: task.ID, LeaseToken: task.LeaseToken, Generation: task.Generation,
		Outcome: OutcomeAbortedStoreUnreachable, ErrorMessage: "journal unreadable",
	}))
	task, err = sched.Lease(t.Context(), LeaseRequest{WorkerID: "w1", Accepts: []TaskType{TaskCompaction}})
	testutil.Ok(t, err)
	testutil.Assert(t, task == nil, "a store-unreachable abort must back the task off")
	sched.mtx.Lock()
	testutil.Equals(t, 1, sched.tasks["t-restart"].entry.Aborts)
	sched.mtx.Unlock()
}

func TestParkedSourcesRemainParkedWhenPlanGrows(t *testing.T) {
	s := &Scheduler{journal: NewJournal("j", "")}
	s.journal.Tasks["poison"] = &TaskEntry{State: StateAbandoned, Task: Task{SourceBlocks: []string{"a", "b"}}}
	testutil.Equals(t, true, s.SourcesParked([]string{"a", "b", "new"}))
	testutil.Equals(t, true, s.SourcesParked([]string{"b", "new"}))
	testutil.Equals(t, false, s.SourcesParked([]string{"healthy", "new"}))
}

func TestFinalizeRetriesTransientMetadataReads(t *testing.T) {
	if testing.Short() {
		t.Skip("runs real TSDB compactions")
	}
	for _, persistent := range []bool{false, true} {
		t.Run(map[bool]string{false: "recovers", true: "bounded failure"}[persistent], func(t *testing.T) {
			// Nothing renews the lease here, so it must outlast the task on a
			// slow runner, or the ownership check aborts it.
			c := newTestClusterConf(t, ManagerConfig{LeaseTTL: time.Minute})
			cg, metas := c.makeGroup(labels.FromStrings("tenant", "retry"))
			comp, err := tsdb.NewLeveledCompactor(t.Context(), nil, logutil.GoKitLogToSlog(c.logger), []int64{1000, 3000}, downsample.NewPool(), nil)
			testutil.Ok(t, err)
			w, err := NewWorker(c.logger, c.shared, nil, comp, prometheus.NewRegistry(), WorkerConfig{JournalID: journalID, DataDir: t.TempDir()})
			testutil.Ok(t, err)
			task, err := CompactionTask(cg, compact.Plan{Sources: metas})
			testutil.Ok(t, err)
			_, err = c.sched.Submit(t.Context(), task)
			testutil.Ok(t, err)
			leased, err := c.sched.Lease(t.Context(), LeaseRequest{WorkerID: "w"})
			testutil.Ok(t, err)
			res := w.execute(t.Context(), *leased, testAtomicBool(true))
			testutil.Equals(t, OutcomeCompleted, res.Outcome)
			var reads int
			bkt := compacttest.NewHookBucket(c.shared)
			bkt.SetOnGet(func(_ context.Context, name string) error {
				if name == res.OutputBlocks[0]+"/"+block.MetaFilename {
					reads++
					if persistent || reads == 1 {
						return errors.New("temporary metadata read failure")
					}
				}
				return nil
			})
			e := NewRemotePlanExecutor(c.logger, bkt, c.sched, nil, 1, nil)
			ids, err := e.verifyAndFinalize(t.Context(), cg, compact.Plan{Sources: metas}, res)
			if persistent {
				testutil.NotOk(t, err)
				testutil.Equals(t, 3, reads)
			} else {
				testutil.Ok(t, err)
				testutil.Equals(t, 2, reads)
				testutil.Equals(t, 1, len(ids))
			}
			for _, m := range metas {
				marked, existsErr := c.shared.Exists(t.Context(), m.ULID.String()+"/deletion-mark.json")
				testutil.Ok(t, existsErr)
				testutil.Equals(t, !persistent, marked)
			}
		})
	}
}

func TestMaintenanceWaitingForJournalDoesNotBlockHeartbeats(t *testing.T) {
	bkt := compacttest.NewHookBucket(objstore.NewInMemBucket())
	s := testScheduler(t, bkt, ManagerConfig{LeaseTTL: time.Minute})
	_, err := s.Submit(t.Context(), Task{ID: "first", Type: TaskCompaction})
	testutil.Ok(t, err)
	task, err := s.Lease(t.Context(), LeaseRequest{WorkerID: "w"})
	testutil.Ok(t, err)
	started, release := make(chan struct{}, 1), make(chan struct{})
	var released atomic.Bool
	unblock := func() {
		if released.CompareAndSwap(false, true) {
			close(release)
		}
	}
	defer unblock()
	bkt.SetOnUpload(func(ctx context.Context, name string) error {
		if strings.HasPrefix(name, JournalPrefix) {
			select {
			case started <- struct{}{}:
			default:
			}
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	done := make(chan struct{})
	go func() { defer close(done); _, _ = s.Submit(t.Context(), Task{ID: "second", Type: TaskCompaction}) }()
	<-started
	s.mtx.Lock()
	s.lastPersist = time.Now().Add(-time.Hour)
	seq := s.persistSeq
	s.mtx.Unlock()
	maintenance := make(chan error, 1)
	go func() { maintenance <- s.Maintain() }()
	deadline := time.Now().Add(time.Second)
	for {
		s.mtx.Lock()
		queued := s.persistSeq > seq
		s.mtx.Unlock()
		if queued {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("maintenance never prepared its journal snapshot")
		}
		time.Sleep(time.Millisecond)
	}
	response := make(chan HeartbeatResponse, 1)
	go func() {
		response <- s.Heartbeat(HeartbeatRequest{TaskID: task.ID, LeaseToken: task.LeaseToken, Generation: task.Generation})
	}()
	select {
	case r := <-response:
		testutil.Equals(t, true, r.Acknowledged)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("maintenance held the state lock while waiting for journal I/O")
	}
	unblock()
	<-done
	testutil.Ok(t, <-maintenance)
}

// TestHaltFreezesTheFleet pins down that a halted manager stops its fleet the
// way a halt stops a standalone compactor: leases are revoked so heartbeats
// and ownership checks fail, queued work is failed in the journal with the
// halt as its reason, and nothing is leased or accepted afterwards.
func TestHaltFreezesTheFleet(t *testing.T) {
	ctx := t.Context()
	bkt := objstore.NewInMemBucket()
	sched, err := NewScheduler(ctx, log.NewNopLogger(), bkt, prometheus.NewRegistry(), ManagerConfig{JournalID: "shard-halt"})
	testutil.Ok(t, err)

	leasedCh, err := sched.Submit(ctx, Task{ID: "t-leased", Type: TaskCompaction})
	testutil.Ok(t, err)
	queuedCh, err := sched.Submit(ctx, Task{ID: "t-queued", Type: TaskCompaction})
	testutil.Ok(t, err)
	leased, err := sched.Lease(ctx, LeaseRequest{WorkerID: "w1", Accepts: []TaskType{TaskCompaction}})
	testutil.Ok(t, err)
	testutil.Equals(t, "t-leased", leased.ID)

	sched.Halt(errors.New("pre compaction overlap check"))
	sched.Halt(errors.New("again")) // Idempotent.

	// The worker holding the lease is cut off, in memory and in the journal.
	testutil.Equals(t, false, sched.Heartbeat(HeartbeatRequest{TaskID: leased.ID, LeaseToken: leased.LeaseToken, Generation: leased.Generation}).Acknowledged)
	status, err := CheckOwnership(ctx, bkt, "shard-halt", leased.ID, leased.LeaseToken, leased.Generation, sched.conf.LeaseTTL)
	testutil.NotOk(t, err)
	testutil.Equals(t, OwnershipLost, status)
	testutil.Ok(t, sched.Report(ctx, Result{TaskID: leased.ID, LeaseToken: leased.LeaseToken, Generation: leased.Generation, Outcome: OutcomeCompleted}))

	// Both tasks are failed with the halt as the reason, and their submitters
	// hear about it.
	for _, ch := range []<-chan Result{leasedCh, queuedCh} {
		select {
		case res := <-ch:
			testutil.Equals(t, OutcomeFailedHalt, res.Outcome)
			testutil.Assert(t, strings.Contains(res.ErrorMessage, "manager halted: pre compaction overlap check"), res.ErrorMessage)
		default:
			t.Fatal("the halt must be delivered to every submitter")
		}
	}
	j, err := ReadJournal(ctx, bkt, "shard-halt")
	testutil.Ok(t, err)
	for _, id := range []string{"t-leased", "t-queued"} {
		testutil.Equals(t, StateFailed, j.Tasks[id].State)
		testutil.Assert(t, j.Tasks[id].Lease == nil, "leases must be revoked in the journal")
		testutil.Equals(t, OutcomeFailedHalt, j.Tasks[id].LastError.Outcome)
	}

	// Nothing goes out or comes in until a restart.
	task, err := sched.Lease(ctx, LeaseRequest{WorkerID: "w2", Accepts: []TaskType{TaskCompaction}})
	testutil.Ok(t, err)
	testutil.Assert(t, task == nil, "a halted manager hands out nothing")
	_, err = sched.Submit(ctx, Task{ID: "t-late", Type: TaskCompaction})
	testutil.Assert(t, compact.IsHaltError(err), "a halted manager accepts nothing, got %v", err)
	sched.mtx.Lock()
	testutil.Equals(t, 0, len(sched.queue))
	testutil.Equals(t, 0, len(sched.tasks))
	sched.mtx.Unlock()
}
