// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"context"
	"encoding/json"
	"path"
	"strings"
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

type rollbackFixture struct {
	bkt objstore.InstrumentedBucket

	producedA ulid.ULID // made by a worker of journal A from consumedA and sourceA
	producedB ulid.ULID // made by a worker of journal B from sourceB
	sourceA   ulid.ULID // marked for deletion by manager A
	sourceB   ulid.ULID // marked for deletion by manager B
	retention ulid.ULID // marked for deletion by retention, not ours
	untouched ulid.ULID // an ordinary block
	consumedA ulid.ULID // made by a worker of A from rawA, later consumed by another A task and marked by A
	rawA      ulid.ULID // the source of consumedA, marked for deletion by manager A

	next uint64
}

func newRollbackFixture(t *testing.T) rollbackFixture {
	t.Helper()
	f := rollbackFixture{bkt: objstore.WithNoopInstr(objstore.NewInMemBucket()), next: 1}

	f.producedA = f.upload(t, nil)
	f.producedB = f.upload(t, nil)
	f.sourceA = f.upload(t, nil)
	f.mark(t, f.sourceA, DeletionDetails("A", "ta1"))
	f.sourceB = f.upload(t, nil)
	f.mark(t, f.sourceB, DeletionDetails("B", "tb1"))
	f.retention = f.upload(t, nil)
	f.mark(t, f.retention, "retention")
	f.untouched = f.upload(t, nil)
	f.consumedA = f.upload(t, nil)
	f.mark(t, f.consumedA, DeletionDetails("A", "ta1"))
	f.rawA = f.upload(t, nil)
	f.mark(t, f.rawA, DeletionDetails("A", "ta0"))

	// The produced blocks record what they were made from, which the IDs
	// above had to exist for.
	f.stamp(t, f.producedA, Provenance{TaskID: "ta1", TaskType: TaskCompaction, WorkerID: "w", JournalID: "A"}, f.consumedA, f.sourceA)
	f.stamp(t, f.producedB, Provenance{TaskID: "tb1", TaskType: TaskCompaction, WorkerID: "w", JournalID: "B"}, f.sourceB)
	f.stamp(t, f.consumedA, Provenance{TaskID: "ta0", TaskType: TaskCompaction, WorkerID: "w", JournalID: "A"}, f.rawA)
	return f
}

// upload puts a block with the given provenance, if any, in the bucket.
func (f *rollbackFixture) upload(t *testing.T, prov *Provenance, sources ...ulid.ULID) ulid.ULID {
	t.Helper()
	id := ulid.MustNew(f.next, nil)
	f.next++
	m := metadata.Meta{}
	m.ULID = id
	m.MinTime, m.MaxTime = 0, 1000
	m.Version = metadata.TSDBVersion1
	m.Thanos.Version = metadata.ThanosVersion1
	m.Thanos.Labels = map[string]string{"ext": "1"}
	m.Compaction.Sources = []ulid.ULID{id}
	f.writeMeta(t, m)
	// A block needs something besides meta.json for Delete to have work to do.
	testutil.Ok(t, f.bkt.Upload(context.Background(), path.Join(id.String(), block.IndexFilename), strings.NewReader("index")))
	if prov != nil {
		f.stamp(t, id, *prov, sources...)
	}
	return id
}

// stamp records on a block that a worker made it from the given sources.
func (f *rollbackFixture) stamp(t *testing.T, id ulid.ULID, prov Provenance, sources ...ulid.ULID) {
	t.Helper()
	m, err := block.DownloadMeta(context.Background(), log.NewNopLogger(), f.bkt, id)
	testutil.Ok(t, err)
	srcs := make([]string, 0, len(sources))
	for _, s := range sources {
		srcs = append(srcs, s.String())
	}
	m.Thanos.Extensions, err = prov.For(id, srcs).Stamp(m.Thanos.Extensions)
	testutil.Ok(t, err)
	f.writeMeta(t, m)
}

func (f *rollbackFixture) writeMeta(t *testing.T, m metadata.Meta) {
	t.Helper()
	raw, err := json.Marshal(m)
	testutil.Ok(t, err)
	testutil.Ok(t, f.bkt.Upload(context.Background(), path.Join(m.ULID.String(), block.MetaFilename), strings.NewReader(string(raw))))
}

func (f *rollbackFixture) mark(t *testing.T, id ulid.ULID, details string) {
	t.Helper()
	testutil.Ok(t, block.MarkForDeletion(context.Background(), log.NewNopLogger(), f.bkt, id, details,
		prometheus.NewCounter(prometheus.CounterOpts{Name: "test"})))
}

func (f *rollbackFixture) remove(t *testing.T, id ulid.ULID) {
	t.Helper()
	testutil.Ok(t, block.Delete(context.Background(), log.NewNopLogger(), f.bkt, id))
}

func exists(t *testing.T, bkt objstore.Bucket, name string) bool {
	t.Helper()
	ok, err := bkt.Exists(context.Background(), name)
	testutil.Ok(t, err)
	return ok
}

// TestRollbackPlanScopedToJournal asserts the plan selects exactly what one
// manager did: its workers' blocks to delete, the blocks it marked to restore,
// and nothing that belongs to another journal, to retention, or to nobody.
func TestRollbackPlanScopedToJournal(t *testing.T) {
	f := newRollbackFixture(t)

	r, err := PlanRollback(context.Background(), log.NewNopLogger(), f.bkt, RollbackOptions{JournalID: "A"})
	testutil.Ok(t, err)

	// consumedA was produced by A and is gone with the rest, not restored;
	// what it was made from comes back.
	testutil.Equals(t, []ulid.ULID{f.producedA, f.consumedA}, r.Produced)
	testutil.Equals(t, []ulid.ULID{f.sourceA, f.rawA}, r.Restore)
}

// TestRollbackPlanForEveryJournal asserts an unscoped plan covers every
// manager's work while still leaving foreign marks and blocks alone.
func TestRollbackPlanForEveryJournal(t *testing.T) {
	f := newRollbackFixture(t)

	r, err := PlanRollback(context.Background(), log.NewNopLogger(), f.bkt, RollbackOptions{AllJournals: true})
	testutil.Ok(t, err)

	testutil.Equals(t, []ulid.ULID{f.producedA, f.producedB, f.consumedA}, r.Produced)
	testutil.Equals(t, []ulid.ULID{f.sourceA, f.sourceB, f.rawA}, r.Restore)
}

// TestRollbackApply asserts applying the plan leaves the bucket as the
// standalone compactor left it: produced blocks gone, their sources back
// without marks, everything else untouched.
func TestRollbackApply(t *testing.T) {
	f := newRollbackFixture(t)
	ctx := context.Background()

	r, err := PlanRollback(ctx, log.NewNopLogger(), f.bkt, RollbackOptions{JournalID: "A"})
	testutil.Ok(t, err)
	testutil.Ok(t, r.Apply(ctx, log.NewNopLogger(), f.bkt))

	// A's produced blocks are gone.
	testutil.Equals(t, false, exists(t, f.bkt, path.Join(f.producedA.String(), block.MetaFilename)))
	testutil.Equals(t, false, exists(t, f.bkt, path.Join(f.consumedA.String(), block.MetaFilename)))
	// A's sources are back: present and no longer marked.
	for _, id := range []ulid.ULID{f.sourceA, f.rawA} {
		testutil.Equals(t, true, exists(t, f.bkt, path.Join(id.String(), block.MetaFilename)))
		testutil.Equals(t, false, exists(t, f.bkt, path.Join(id.String(), metadata.DeletionMarkFilename)))
	}

	// B's work, retention's mark and the ordinary block are exactly as they were.
	testutil.Equals(t, true, exists(t, f.bkt, path.Join(f.producedB.String(), block.MetaFilename)))
	testutil.Equals(t, true, exists(t, f.bkt, path.Join(f.sourceB.String(), metadata.DeletionMarkFilename)))
	testutil.Equals(t, true, exists(t, f.bkt, path.Join(f.retention.String(), metadata.DeletionMarkFilename)))
	testutil.Equals(t, true, exists(t, f.bkt, path.Join(f.untouched.String(), block.MetaFilename)))

	// Running it again finds nothing left to do.
	again, err := PlanRollback(ctx, log.NewNopLogger(), f.bkt, RollbackOptions{JournalID: "A"})
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(again.Produced))
	testutil.Equals(t, 0, len(again.Restore))
}

// TestRollbackRequiresAScope asserts the destructive, bucket-wide form has to
// be asked for by name: no journal and no AllJournals is refused, and so is
// both at once.
func TestRollbackRequiresAScope(t *testing.T) {
	f := newRollbackFixture(t)

	_, err := PlanRollback(context.Background(), log.NewNopLogger(), f.bkt, RollbackOptions{})
	testutil.NotOk(t, err)

	_, err = PlanRollback(context.Background(), log.NewNopLogger(), f.bkt, RollbackOptions{JournalID: "A", AllJournals: true})
	testutil.NotOk(t, err)
}

// TestRollbackRefusesUnreadableBlocks asserts planning fails closed when a
// block's metadata cannot be read: that block might be a marked source, and a
// plan built without it would delete the replacement while leaving the source
// marked. With the explicit allowance the block is left out and named.
func TestRollbackRefusesUnreadableBlocks(t *testing.T) {
	f := newRollbackFixture(t)
	ctx := context.Background()

	// A block directory with no meta.json - what an aborted upload leaves.
	broken := ulid.MustNew(500, nil)
	testutil.Ok(t, f.bkt.Upload(ctx, path.Join(broken.String(), block.IndexFilename), strings.NewReader("index")))

	_, err := PlanRollback(ctx, log.NewNopLogger(), f.bkt, RollbackOptions{JournalID: "A"})
	testutil.NotOk(t, err)
	testutil.Assert(t, strings.Contains(err.Error(), broken.String()), "the refusal must name the block, got: %v", err)

	r, err := PlanRollback(ctx, log.NewNopLogger(), f.bkt, RollbackOptions{JournalID: "A", AllowUnreadableBlocks: true})
	testutil.Ok(t, err)
	testutil.Equals(t, []ulid.ULID{broken}, r.Unreadable)
	testutil.Equals(t, []ulid.ULID{f.producedA, f.consumedA}, r.Produced)
}

// TestRollbackSeesEveryJournalInScope asserts liveness is judged on every
// journal the rollback would touch: just the named one when scoped, all of
// them for AllJournals - since undoing every manager's work means every
// manager has to be stopped.
func TestRollbackSeesEveryJournalInScope(t *testing.T) {
	f := newRollbackFixture(t)
	ctx := context.Background()

	// Two managers wrote journals; B's is fresh, A's is old.
	old := NewJournal("A", "")
	testutil.Ok(t, WriteJournal(ctx, f.bkt, old))
	aged, err := ReadJournal(ctx, f.bkt, "A")
	testutil.Ok(t, err)
	aged.UpdatedAt = time.Now().Add(-2 * time.Hour)
	raw, err := json.Marshal(aged)
	testutil.Ok(t, err)
	testutil.Ok(t, f.bkt.Upload(ctx, JournalPath("A"), strings.NewReader(string(raw))))
	testutil.Ok(t, WriteJournal(ctx, f.bkt, NewJournal("B", "")))

	scoped, err := PlanRollback(ctx, log.NewNopLogger(), f.bkt, RollbackOptions{JournalID: "A"})
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(scoped.JournalsUpdatedAt))
	testutil.Equals(t, 0, len(scoped.RecentlyActive(15*time.Minute, time.Now())))

	all, err := PlanRollback(ctx, log.NewNopLogger(), f.bkt, RollbackOptions{AllJournals: true})
	testutil.Ok(t, err)
	testutil.Equals(t, 2, len(all.JournalsUpdatedAt))
	testutil.Equals(t, []string{"B"}, all.RecentlyActive(15*time.Minute, time.Now()))
}

// TestRollbackDetectsRunningManager asserts a manager that is merely idle
// still shows up as alive: its maintenance loop keeps the journal fresh.
func TestRollbackDetectsRunningManager(t *testing.T) {
	f := newRollbackFixture(t)
	ctx := context.Background()

	s, err := NewScheduler(ctx, log.NewNopLogger(), f.bkt, prometheus.NewRegistry(), ManagerConfig{JournalID: "A", LeaseTTL: 10 * time.Millisecond})
	testutil.Ok(t, err)
	time.Sleep(20 * time.Millisecond)
	testutil.Ok(t, s.Maintain()) // idle, but alive

	r, err := PlanRollback(ctx, log.NewNopLogger(), f.bkt, RollbackOptions{JournalID: "A"})
	testutil.Ok(t, err)
	testutil.Equals(t, []string{"A"}, r.RecentlyActive(15*time.Minute, time.Now()))
}

// TestRollbackRestoresSourcesGarbageCollectionMarked asserts a source the
// standalone compactor's garbage collection marked as covered - which happens
// when the manager is interrupted between a result becoming visible and
// marking its sources - is restored along with the sources the manager
// marked. Deleting the produced block without restoring it would lose the
// data: the mark names nobody, and nothing else would ever remove it.
func TestRollbackRestoresSourcesGarbageCollectionMarked(t *testing.T) {
	f := newRollbackFixture(t)
	ctx := context.Background()

	testutil.Ok(t, block.RemoveMark(ctx, log.NewNopLogger(), f.bkt, f.sourceA,
		prometheus.NewCounter(prometheus.CounterOpts{Name: "test"}), metadata.DeletionMarkFilename))
	f.mark(t, f.sourceA, outdatedBlockDetails)

	r, err := PlanRollback(ctx, log.NewNopLogger(), f.bkt, RollbackOptions{JournalID: "A"})
	testutil.Ok(t, err)
	testutil.Equals(t, []ulid.ULID{f.producedA, f.consumedA}, r.Produced)
	testutil.Equals(t, []ulid.ULID{f.sourceA, f.rawA}, r.Restore)

	testutil.Ok(t, r.Apply(ctx, log.NewNopLogger(), f.bkt))
	testutil.Equals(t, false, exists(t, f.bkt, path.Join(f.sourceA.String(), metadata.DeletionMarkFilename)))
}

// A standalone output can inherit a worker's extensions. The stamped block ID
// must prevent that descendant from being selected for rollback.
func TestRollbackIgnoresInheritedProvenance(t *testing.T) {
	f := newRollbackFixture(t)
	ctx := context.Background()

	// A 5m downsample of producedA made by the standalone downsampler: the
	// meta is a copy of producedA's, stamp included, under a new ULID.
	inheritedMeta, err := block.DownloadMeta(ctx, log.NewNopLogger(), f.bkt, f.producedA)
	testutil.Ok(t, err)
	inherited := ulid.MustNew(f.next, nil)
	inheritedMeta.ULID = inherited
	inheritedMeta.Thanos.Downsample.Resolution = 5 * 60 * 1000
	f.writeMeta(t, inheritedMeta)

	r, err := PlanRollback(ctx, log.NewNopLogger(), f.bkt, RollbackOptions{JournalID: "A"})
	testutil.Ok(t, err)
	testutil.Equals(t, []ulid.ULID{f.producedA, f.consumedA}, r.Produced)
}

func TestRollbackRefusesUnrecoverableSources(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*testing.T, *rollbackFixture)
	}{
		{"missing direct source", func(t *testing.T, f *rollbackFixture) { f.remove(t, f.sourceA) }},
		{"missing ancestor", func(t *testing.T, f *rollbackFixture) { f.remove(t, f.rawA) }},
		{"foreign mark", func(t *testing.T, f *rollbackFixture) {
			testutil.Ok(t, block.RemoveMark(t.Context(), log.NewNopLogger(), f.bkt, f.rawA, prometheus.NewCounter(prometheus.CounterOpts{Name: "test"}), metadata.DeletionMarkFilename))
			f.mark(t, f.rawA, "retention")
		}},
		{"cycle", func(t *testing.T, f *rollbackFixture) {
			f.stamp(t, f.consumedA, Provenance{TaskID: "ta0", JournalID: "A"}, f.producedA)
		}},
		{"missing provenance sources", func(t *testing.T, f *rollbackFixture) {
			f.stamp(t, f.consumedA, Provenance{TaskID: "ta0", JournalID: "A"})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRollbackFixture(t)
			tc.change(t, &f)
			_, err := PlanRollback(t.Context(), log.NewNopLogger(), f.bkt, RollbackOptions{JournalID: "A"})
			testutil.NotOk(t, err)
			testutil.Assert(t, strings.Contains(err.Error(), "refusing rollback"), "unexpected refusal: %v", err)
			testutil.Equals(t, true, exists(t, f.bkt, path.Join(f.producedA.String(), block.MetaFilename)))
		})
	}
}

type rollbackDeleteFailureBucket struct {
	objstore.Bucket
	failAt, deletes int
}

func (b *rollbackDeleteFailureBucket) Delete(ctx context.Context, name string) error {
	b.deletes++
	if b.deletes == b.failAt {
		return errors.New("interrupted rollback")
	}
	return b.Bucket.Delete(ctx, name)
}

// Each delete operation can be the last one before a process crash. Replanning
// must still recover the originals after either source restoration or partial
// output deletion, including GC marks that carry no journal information.
func TestRollbackCanResumeAfterInterruption(t *testing.T) {
	for failAt := 1; failAt <= 8; failAt++ {
		f := newRollbackFixture(t)
		testutil.Ok(t, block.RemoveMark(t.Context(), log.NewNopLogger(), f.bkt, f.sourceA, prometheus.NewCounter(prometheus.CounterOpts{Name: "test"}), metadata.DeletionMarkFilename))
		f.mark(t, f.sourceA, outdatedBlockDetails)
		r, err := PlanRollback(t.Context(), log.NewNopLogger(), f.bkt, RollbackOptions{JournalID: "A"})
		testutil.Ok(t, err)
		failing := &rollbackDeleteFailureBucket{Bucket: f.bkt, failAt: failAt}
		_ = r.Apply(t.Context(), log.NewNopLogger(), failing)
		// A block whose metadata was deleted but whose index remains is an
		// explicitly allowed remnant of this interrupted offline rollback.
		again, err := PlanRollback(t.Context(), log.NewNopLogger(), f.bkt, RollbackOptions{JournalID: "A", AllowUnreadableBlocks: true})
		testutil.Ok(t, err)
		testutil.Ok(t, again.Apply(t.Context(), log.NewNopLogger(), f.bkt))
		for _, id := range []ulid.ULID{f.sourceA, f.rawA} {
			testutil.Equals(t, true, exists(t, f.bkt, path.Join(id.String(), block.MetaFilename)))
			testutil.Equals(t, false, exists(t, f.bkt, path.Join(id.String(), metadata.DeletionMarkFilename)))
		}
	}
}

func TestRollbackRestoresSourcesAfterManagerCrashAndGC(t *testing.T) {
	ctx := t.Context()
	logger := log.NewNopLogger()
	bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())
	sourceID, outID := ulid.MustNew(1, nil), ulid.MustNew(2, nil)
	source := metadata.Meta{}
	source.ULID = sourceID
	source.Version = metadata.TSDBVersion1
	source.MinTime, source.MaxTime = 0, 1000
	source.Compaction.Sources = []ulid.ULID{sourceID}
	source.Thanos.Version = metadata.ThanosVersion1
	source.Thanos.Labels = map[string]string{"ext": "1"}
	out := source
	out.ULID = outID
	out.Compaction.Level = 2
	source2 := source
	source2.ULID = ulid.MustNew(3, nil)
	source2.MinTime, source2.MaxTime = 1000, 2000
	source2.Compaction.Sources = []ulid.ULID{source2.ULID}
	out.MaxTime = 2000
	out.Compaction.Sources = []ulid.ULID{sourceID, source2.ULID}
	var err error
	out.Thanos.Extensions, err = (Provenance{TaskID: "task", TaskType: TaskCompaction, JournalID: "review"}).For(outID, []string{sourceID.String(), source2.ULID.String()}).Stamp(nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range []metadata.Meta{source, source2, out} {
		raw, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		if err := bkt.Upload(ctx, path.Join(m.ULID.String(), block.MetaFilename), strings.NewReader(string(raw))); err != nil {
			t.Fatal(err)
		}
	}
	dedupFilter := block.NewDeduplicateFilter(1)
	ignoreFilter := block.NewIgnoreDeletionMarkFilter(logger, bkt, 0, 1)
	fetcher, err := block.NewMetaFetcher(logger, 1, bkt, block.NewConcurrentLister(logger, bkt), "", prometheus.NewRegistry(), []block.MetadataFilter{ignoreFilter, dedupFilter})
	if err != nil {
		t.Fatal(err)
	}
	counter := func() prometheus.Counter { return prometheus.NewCounter(prometheus.CounterOpts{Name: "review"}) }
	syncer, err := compact.NewMetaSyncer(logger, prometheus.NewRegistry(), bkt, fetcher, dedupFilter, ignoreFilter, counter(), counter(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := syncer.SyncMetas(ctx); err != nil {
		t.Fatal(err)
	}
	if err := syncer.GarbageCollect(ctx, nil); err != nil {
		t.Fatal(err)
	}
	var before metadata.DeletionMark
	if err := metadata.ReadMarker(ctx, logger, bkt, sourceID.String(), &before); err != nil {
		t.Fatalf("expected GC source mark: %v", err)
	}
	r, err := PlanRollback(ctx, logger, bkt, RollbackOptions{JournalID: "review"})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Apply(ctx, logger, bkt); err != nil {
		t.Fatal(err)
	}
	var mark metadata.DeletionMark
	if err := metadata.ReadMarker(ctx, logger, bkt, sourceID.String(), &mark); err == nil {
		t.Fatalf("rollback deleted output but left only source marked for deletion: %q; restore=%v", mark.Details, r.Restore)
	}
}
