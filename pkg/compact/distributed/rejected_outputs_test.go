// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"context"
	"path"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
)

// stubbornBucket refuses to delete the objects of one block.
type stubbornBucket struct {
	objstore.Bucket
	keep string
}

func (b stubbornBucket) Delete(ctx context.Context, name string) error {
	if path.Dir(name) == b.keep || name == b.keep {
		return errors.New("injected: delete refused")
	}
	return b.Bucket.Delete(ctx, name)
}

// syncAndCollect runs the compactor's own metadata sync and garbage collection
// over the bucket, judging worker outputs by the manager's rule, and reports
// which of the given blocks ended up marked for deletion.
func syncAndCollect(t *testing.T, bkt objstore.Bucket, published func(*metadata.Meta) bool, ids ...ulid.ULID) map[ulid.ULID]bool {
	t.Helper()
	ctx := t.Context()
	logger := log.NewNopLogger()
	dedup := block.NewDeduplicateFilter(1)
	if published != nil {
		dedup.SetPublishedFunc(published)
	}
	instr := objstore.WithNoopInstr(bkt)
	ignore := block.NewIgnoreDeletionMarkFilter(logger, instr, 0, 1)
	fetcher, err := block.NewMetaFetcher(logger, 1, instr, block.NewConcurrentLister(logger, instr), "", prometheus.NewRegistry(), []block.MetadataFilter{ignore, dedup})
	testutil.Ok(t, err)
	counter := func() prometheus.Counter { return promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "test"}) }
	syncer, err := compact.NewMetaSyncer(logger, prometheus.NewRegistry(), bkt, fetcher, dedup, ignore, counter(), counter(), 0)
	testutil.Ok(t, err)
	testutil.Ok(t, syncer.SyncMetas(ctx))
	testutil.Ok(t, syncer.GarbageCollect(ctx, nil))
	marked := map[ulid.ULID]bool{}
	for _, id := range ids {
		marked[id] = deletionMarked(t, bkt, id)
	}
	return marked
}

// TestRejectedOutputsNeverRetireTheSources: a result the manager rejects is
// retried, and its sources stay. The block the worker uploaded lists those
// sources, so left alone it would let the next sync hide them as duplicates
// and garbage collection retire them - on the strength of a block that
// failed verification. The rejected block is deleted; when it cannot be, it
// is recorded as rejected, and the manager's deduplication filter treats it
// as unpublished, through sync and garbage collection.
func TestRejectedOutputsNeverRetireTheSources(t *testing.T) {
	ctx := t.Context()
	versioned := func(m metadata.Meta) metadata.Meta {
		m.Version = metadata.TSDBVersion1
		m.Thanos.Version = metadata.ThanosVersion1
		return m
	}
	partial := func(t *testing.T, bkt objstore.Bucket, toCompact []*metadata.Meta) (string, string) {
		t.Helper()
		// Correct labels, both sources, provenance and checksum, but half the
		// plan's time range: the second half would be deleted with the sources.
		return uploadResultMeta(t, bkt, versioned(resultMeta(t, ulid.MustNew(99, nil), 100, 0, map[string]string{"ext": "1"}, toCompact[0].ULID, toCompact[1].ULID)))
	}
	uploadSources := func(t *testing.T, bkt objstore.Bucket, toCompact []*metadata.Meta) {
		t.Helper()
		for _, m := range toCompact {
			uploadResultMeta(t, bkt, versioned(*m))
		}
	}

	t.Run("the rejected block is deleted", func(t *testing.T) {
		e, bkt, cg, toCompact := provenanceFixture(t)
		uploadSources(t, bkt, toCompact)
		id, sum := partial(t, bkt, toCompact)

		_, err := e.verifyAndFinalize(ctx, cg, compact.Plan{Sources: toCompact}, Result{
			TaskID: "t1", Outcome: OutcomeCompleted, OutputBlocks: []string{id}, OutputChecksums: map[string]string{id: sum},
		})
		testutil.NotOk(t, err)
		testutil.Equals(t, true, compact.IsRetryError(err))
		exists, err := bkt.Exists(ctx, path.Join(id, block.MetaFilename))
		testutil.Ok(t, err)
		testutil.Equals(t, false, exists, "the rejected block must be gone")

		marked := syncAndCollect(t, bkt, nil, toCompact[0].ULID, toCompact[1].ULID)
		testutil.Equals(t, map[ulid.ULID]bool{toCompact[0].ULID: false, toCompact[1].ULID: false}, marked)
	})

	t.Run("a rejected block that cannot be deleted is unpublished", func(t *testing.T) {
		e, inner, cg, toCompact := provenanceFixture(t)
		uploadSources(t, inner, toCompact)
		id, sum := partial(t, inner, toCompact)
		bkt := stubbornBucket{Bucket: inner, keep: id}
		sched := testScheduler(t, inner, ManagerConfig{JournalID: "shard-test"})
		sched.journal.Tasks["t1"] = &TaskEntry{Task: Task{ID: "t1", Type: TaskCompaction}, State: StateCompleted, Outputs: []string{id}}
		e.bkt, e.sched = bkt, sched

		_, err := e.verifyAndFinalize(ctx, cg, compact.Plan{Sources: toCompact}, Result{
			TaskID: "t1", Outcome: OutcomeCompleted, OutputBlocks: []string{id}, OutputChecksums: map[string]string{id: sum},
		})
		testutil.NotOk(t, err)
		testutil.Equals(t, true, compact.IsRetryError(err), "recorded as rejected, so a retry is safe: %v", err)
		exists, err := inner.Exists(ctx, path.Join(id, block.MetaFilename))
		testutil.Ok(t, err)
		testutil.Equals(t, true, exists, "the delete was refused")
		testutil.Equals(t, []string{id}, sched.journal.Tasks["t1"].RejectedOutputs)
		testutil.Equals(t, false, sched.OutputPublished("t1", id))

		// Without the manager's rule the block would retire the sources.
		marked := syncAndCollect(t, inner, nil, toCompact[0].ULID, toCompact[1].ULID)
		testutil.Equals(t, map[ulid.ULID]bool{toCompact[0].ULID: true, toCompact[1].ULID: true}, marked, "the test would not prove anything otherwise")
		// Undo the marks and judge by the rule.
		for _, m := range toCompact {
			testutil.Ok(t, inner.Delete(ctx, path.Join(m.ULID.String(), metadata.DeletionMarkFilename)))
		}
		marked = syncAndCollect(t, inner, sched.PublishedFunc(), toCompact[0].ULID, toCompact[1].ULID)
		testutil.Equals(t, map[ulid.ULID]bool{toCompact[0].ULID: false, toCompact[1].ULID: false}, marked)

		// The entry outlives the retention while the block does, and
		// maintenance finishes the deletion once the bucket allows it.
		testutil.Equals(t, 0, sched.journal.Prune(0, sched.journal.Tasks["t1"].UpdatedAt.Add(1e12)))
		testutil.Ok(t, sched.deleteRejectedOutputs(ctx))
		exists, err = inner.Exists(ctx, path.Join(id, block.MetaFilename))
		testutil.Ok(t, err)
		testutil.Equals(t, false, exists)
		testutil.Equals(t, 0, len(sched.journal.Tasks["t1"].RejectedOutputs))
	})

	t.Run("a rejected block that can be neither deleted nor recorded halts the manager", func(t *testing.T) {
		e, inner, cg, toCompact := provenanceFixture(t)
		uploadSources(t, inner, toCompact)
		id, sum := partial(t, inner, toCompact)
		e.bkt = stubbornBucket{Bucket: inner, keep: id}

		_, err := e.verifyAndFinalize(ctx, cg, compact.Plan{Sources: toCompact}, Result{
			TaskID: "t1", Outcome: OutcomeCompleted, OutputBlocks: []string{id}, OutputChecksums: map[string]string{id: sum},
		})
		testutil.NotOk(t, err)
		testutil.Equals(t, true, compact.IsHaltError(err), "nothing protects the sources: %v", err)
	})
}

// TestOutputPublished pins down when a worker's block may supersede the
// blocks it was made from: its task completed, was verified, lists it and
// did not reject it. An unknown task is not judged.
func TestOutputPublished(t *testing.T) {
	sched := testScheduler(t, objstore.NewInMemBucket(), ManagerConfig{})
	a, b, c, d := ulid.MustNew(1, nil), ulid.MustNew(2, nil), ulid.MustNew(3, nil), ulid.MustNew(4, nil)
	sched.journal.Tasks["done"] = &TaskEntry{State: StateCompleted, Verified: true, Outputs: []string{a.String(), b.String()}, RejectedOutputs: []string{c.String()}}
	sched.journal.Tasks["reported"] = &TaskEntry{State: StateCompleted, Outputs: []string{d.String()}}
	sched.journal.Tasks["running"] = &TaskEntry{State: StateLeased}

	testutil.Equals(t, true, sched.OutputPublished("done", a.String()))
	testutil.Equals(t, false, sched.OutputPublished("done", c.String()), "rejected")
	testutil.Equals(t, false, sched.OutputPublished("done", "z"), "not an output of the task")
	testutil.Equals(t, false, sched.OutputPublished("reported", d.String()), "reported but not verified")
	testutil.Equals(t, false, sched.OutputPublished("running", "e"))
	testutil.Equals(t, true, sched.OutputPublished("forgotten", "f"), "aged out of the journal")

	stamped := func(taskID string, blockID ulid.ULID, journalID string) *metadata.Meta {
		m := &metadata.Meta{}
		m.ULID = blockID
		ext, err := (Provenance{TaskID: taskID, JournalID: journalID, BlockID: blockID.String()}).Stamp(nil)
		testutil.Ok(t, err)
		m.Thanos.Extensions = ext
		return m
	}
	published := sched.PublishedFunc()
	testutil.Equals(t, true, published(&metadata.Meta{}), "not a worker's block")
	testutil.Equals(t, true, published(stamped("reported", d, "another-journal")), "another manager's block")
	testutil.Equals(t, false, published(stamped("reported", d, sched.conf.JournalID)))
	testutil.Equals(t, true, published(stamped("done", a, sched.conf.JournalID)))
	testutil.Equals(t, false, published(stamped("done", c, sched.conf.JournalID)))
}

// TestMaintenanceRejectsUnverifiedOutputs: a task reported completed whose
// outputs were never verified - the manager stopped in between - has them
// rejected once no verification can still come, so that the plan is redone
// and the stale outputs never supersede the sources. Only blocks whose own
// metadata says they were made for the task are deleted: the list of outputs
// is the worker's word, and a block ID it named could be a source of the plan.
func TestMaintenanceRejectsUnverifiedOutputs(t *testing.T) {
	ctx := t.Context()
	bkt := objstore.NewInMemBucket()
	sched := testScheduler(t, bkt, ManagerConfig{})
	id, source := ulid.MustNew(5, nil), ulid.MustNew(6, nil)
	m := resultMeta(t, id, 200, 0, map[string]string{"ext": "1"})
	m.Version, m.Thanos.Version = metadata.TSDBVersion1, metadata.ThanosVersion1
	ext, err := (Provenance{TaskID: "t1", TaskType: TaskCompaction, JournalID: sched.conf.JournalID}).For(id, nil).Stamp(nil)
	testutil.Ok(t, err)
	m.Thanos.Extensions = ext
	uploadResultMeta(t, bkt, m)
	src := resultMeta(t, source, 200, 0, map[string]string{"ext": "1"})
	src.Version, src.Thanos.Version = metadata.TSDBVersion1, metadata.ThanosVersion1
	uploadResultMeta(t, bkt, src)
	sched.journal.Tasks["t1"] = &TaskEntry{State: StateCompleted, Outputs: []string{id.String(), source.String()}}

	exists := func(blockID ulid.ULID) bool {
		ok, existsErr := bkt.Exists(ctx, path.Join(blockID.String(), block.MetaFilename))
		testutil.Ok(t, existsErr)
		return ok
	}

	// While the plan that submitted the task is verifying the result, the
	// outputs are its business alone, however long it takes.
	sched.verifying["t1"] = struct{}{}
	testutil.Ok(t, sched.Maintain())
	testutil.Equals(t, true, exists(id))
	testutil.Equals(t, 2, len(sched.journal.Tasks["t1"].Outputs))

	// Nobody verifying - the manager that received the report is gone.
	sched.VerificationDone("t1")
	testutil.Ok(t, sched.Maintain())
	e := sched.journal.Tasks["t1"]
	testutil.Equals(t, 0, len(e.Outputs))
	testutil.Equals(t, 0, len(e.RejectedOutputs), "settled right away")
	testutil.Equals(t, false, exists(id), "the task's own block is deleted")
	testutil.Equals(t, true, exists(source), "a block the worker named but that is not the task's is left alone")
}
