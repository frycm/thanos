// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"context"
	"io"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
)

// journalFaultBucket refuses to write the journal while fail is set.
type journalFaultBucket struct {
	objstore.Bucket
	fail bool
}

func (b *journalFaultBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	if b.fail && path.Base(name) == path.Base(JournalPath("x")) {
		return errors.New("injected: journal write failed")
	}
	return b.Bucket.Upload(ctx, name, r, opts...)
}

// TestVerificationCountsOnlyOnceRecorded pins down that the sources of a plan
// are retired only once the journal holds the verdict on its result. A
// verdict held in memory alone would not survive a restart: the successor
// would find the task unverified and delete the outputs, while the sources,
// marked on the strength of that verdict, were on their way out.
func TestVerificationCountsOnlyOnceRecorded(t *testing.T) {
	ctx := context.Background()
	e, bkt, cg, toCompact := provenanceFixture(t)
	jb := &journalFaultBucket{Bucket: bkt}
	sched := testScheduler(t, jb, ManagerConfig{JournalID: "shard-test", JournalUnavailableTimeout: time.Hour})
	e.sched = sched

	out := resultMeta(ulid.MustNew(99, nil), 200, 0, map[string]string{"ext": "1"}, toCompact[0].ULID, toCompact[1].ULID)
	id, sum := uploadResultMeta(t, bkt, out)
	sched.journal.Tasks["t1"] = &TaskEntry{Task: Task{ID: "t1", Type: TaskCompaction}, State: StateCompleted, Outputs: []string{id}}
	res := Result{TaskID: "t1", Outcome: OutcomeCompleted, OutputBlocks: []string{id}, OutputChecksums: map[string]string{id: sum}}

	jb.fail = true
	_, err := e.verifyAndFinalize(ctx, cg, compact.Plan{Sources: toCompact}, res)
	testutil.NotOk(t, err)
	testutil.Equals(t, true, compact.IsRetryError(err), "an unwritable journal is retried: %v", err)
	testutil.Equals(t, false, sched.journal.Tasks["t1"].Verified, "the verdict is taken back")
	testutil.Equals(t, false, sched.OutputPublished("t1", id))
	for _, m := range toCompact {
		testutil.Equals(t, false, deletionMarked(t, bkt, m.ULID), "source %s must not be retired", m.ULID)
	}

	jb.fail = false
	_, err = e.verifyAndFinalize(ctx, cg, compact.Plan{Sources: toCompact}, res)
	testutil.Ok(t, err)
	testutil.Equals(t, true, sched.journal.Tasks["t1"].Verified)
	j, err := ReadJournal(ctx, bkt, "shard-test")
	testutil.Ok(t, err)
	testutil.Equals(t, true, j.Tasks["t1"].Verified, "recorded before the sources are retired")
	for _, m := range toCompact {
		testutil.Equals(t, true, deletionMarked(t, bkt, m.ULID))
	}
}

// blockingDeleteBucket holds the first delete until released.
type blockingDeleteBucket struct {
	objstore.Bucket
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func (b *blockingDeleteBucket) Delete(ctx context.Context, name string) error {
	b.once.Do(func() {
		close(b.started)
		<-b.release
	})
	return b.Bucket.Delete(ctx, name)
}

// TestMaintenanceDeletesWithoutTheStateLock pins down that deleting rejected
// outputs does not hold the scheduler's state lock: a slow delete would
// otherwise stall every heartbeat, lease and report, and workers would lose
// their leases.
func TestMaintenanceDeletesWithoutTheStateLock(t *testing.T) {
	ctx := context.Background()
	inner := objstore.NewInMemBucket()
	bkt := &blockingDeleteBucket{Bucket: inner, started: make(chan struct{}), release: make(chan struct{})}
	sched := testScheduler(t, bkt, ManagerConfig{})

	id := ulid.MustNew(5, nil)
	m := resultMeta(id, 200, 0, map[string]string{"ext": "1"})
	m.Version, m.Thanos.Version = metadata.TSDBVersion1, metadata.ThanosVersion1
	ext, err := (Provenance{TaskID: "t1", TaskType: TaskCompaction, JournalID: sched.conf.JournalID}).For(id, nil).Stamp(nil)
	testutil.Ok(t, err)
	m.Thanos.Extensions = ext
	uploadResultMeta(t, inner, m)
	sched.journal.Tasks["t1"] = &TaskEntry{Task: Task{ID: "t1"}, State: StateCompleted, RejectedOutputs: []string{id.String()}}

	maintained := make(chan error, 1)
	go func() { maintained <- sched.Maintain() }()
	<-bkt.started

	leased := make(chan error, 1)
	go func() {
		_, err := sched.Lease(ctx, LeaseRequest{WorkerID: "w1"})
		leased <- err
	}()
	select {
	case err := <-leased:
		testutil.Ok(t, err)
	case <-time.After(10 * time.Second):
		close(bkt.release)
		t.Fatal("a lease waited for maintenance to finish deleting a block")
	}

	close(bkt.release)
	testutil.Ok(t, <-maintained)
	exists, err := inner.Exists(ctx, path.Join(id.String(), block.MetaFilename))
	testutil.Ok(t, err)
	testutil.Equals(t, false, exists)
	testutil.Equals(t, 0, len(sched.journal.Tasks["t1"].RejectedOutputs))
}

// TestStoppedSchedulerWritesNothing pins down that once the manager's
// lifetime is over the scheduler writes nothing more to the journal, also
// from the paths that have no context of their own: maintenance, lease
// expiry, oversized refusals and halts. A successor may own the journal by
// then.
func TestStoppedSchedulerWritesNothing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	bkt := objstore.NewInMemBucket()
	sched, err := NewScheduler(ctx, log.NewNopLogger(), bkt, prometheus.NewRegistry(), ManagerConfig{JournalID: "shard-a", LeaseTTL: time.Millisecond})
	testutil.Ok(t, err)
	_, err = sched.Submit(context.Background(), Task{ID: "t1", Type: TaskCompaction})
	testutil.Ok(t, err)
	_, err = sched.Lease(context.Background(), LeaseRequest{WorkerID: "w1"})
	testutil.Ok(t, err)
	before, err := ReadJournal(context.Background(), bkt, "shard-a")
	testutil.Ok(t, err)

	cancel()
	time.Sleep(5 * time.Millisecond) // Let the lease expire, so maintenance has something to write.
	_ = sched.Maintain()
	sched.MarkOversized(Task{ID: "t2", Type: TaskCompaction}, "too big")
	sched.Halt(errors.New("stop"))

	after, err := ReadJournal(context.Background(), bkt, "shard-a")
	testutil.Ok(t, err)
	testutil.Equals(t, before.UpdatedAt, after.UpdatedAt, "the journal must not be written after the stop")
	testutil.Equals(t, len(before.Tasks), len(after.Tasks))
}

// TestClaimOutputsRefusesASetOnAPlainPlan pins down that the one block a plan
// without outputs or siblings produces records no set. One that did would be
// judged by it, and a set naming blocks that never appear would keep it
// unpublished for good while the plan's sources are retired.
func TestClaimOutputsRefusesASetOnAPlainPlan(t *testing.T) {
	_, _, cg, toCompact := provenanceFixture(t)
	id := ulid.MustNew(99, nil)
	m := resultMeta(id, 200, 0, map[string]string{"ext": "1"}, toCompact[0].ULID, toCompact[1].ULID)
	outMetas := map[ulid.ULID]metadata.Meta{id: m}
	testutil.Ok(t, claimOutputs(cg, compact.Plan{Sources: toCompact}, Result{}, outMetas))

	m.Thanos.Output = &metadata.ThanosOutput{Index: 0, Count: 1, Blocks: []ulid.ULID{id, ulid.MustNew(100, nil)}}
	outMetas[id] = m
	testutil.NotOk(t, claimOutputs(cg, compact.Plan{Sources: toCompact}, Result{}, outMetas))
}
