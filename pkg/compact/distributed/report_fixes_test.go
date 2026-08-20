// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"context"
	"encoding/json"
	"io"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
)

// TestSchedulerDetectsPeerWithSameGeneration covers the startup race generation
// numbers alone cannot see: two managers starting at the same time both read
// generation N and both write N+1. The owner ID is what tells them apart, so a
// journal holding our generation but a foreign owner must halt us.
func TestSchedulerDetectsPeerWithSameGeneration(t *testing.T) {
	ctx := context.Background()
	bkt := objstore.NewInMemBucket()
	s := testScheduler(t, bkt, ManagerConfig{})

	// Simulate the concurrent manager whose takeover write landed after ours:
	// same generation, different owner.
	j, err := ReadJournal(ctx, bkt, "shard-a")
	testutil.Ok(t, err)
	j.Owner = "somebody-else"
	testutil.Ok(t, WriteJournal(ctx, bkt, j))

	_, err = s.Submit(ctx, Task{ID: "t1", Type: TaskCompaction})
	testutil.NotOk(t, err)
	testutil.Equals(t, true, compact.IsHaltError(err))
}

// failingUploadBucket makes journal writes fail on demand.
type failingUploadBucket struct {
	objstore.Bucket
	fail bool
}

func (b *failingUploadBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	if b.fail {
		return errors.New("journal write failed")
	}
	return b.Bucket.Upload(ctx, name, r, opts...)
}

// TestSchedulerDeliversResultDespiteJournalWriteFailure asserts a terminal
// result reaches the submitter even when the journal cannot be written. The
// bucket, not the journal, is the source of truth for what a completed task
// produced, and a submitter blocked on a lost result can never be woken again:
// a re-report finds the task no longer leased and is ignored.
func TestSchedulerDeliversResultDespiteJournalWriteFailure(t *testing.T) {
	ctx := context.Background()
	bkt := &failingUploadBucket{Bucket: objstore.NewInMemBucket()}
	s := testScheduler(t, bkt, ManagerConfig{})

	resultCh := submitTask(t, s)
	task, err := s.Lease(ctx, LeaseRequest{WorkerID: "w1"})
	testutil.Ok(t, err)

	// The journal becomes unwritable exactly when the result comes in.
	bkt.fail = true
	reportErr := s.Report(ctx, Result{
		TaskID: "t1", LeaseToken: task.LeaseToken, Generation: task.Generation,
		Outcome: OutcomeCompleted, OutputBlocks: []string{"out"},
	})
	testutil.NotOk(t, reportErr)

	select {
	case res := <-resultCh:
		testutil.Equals(t, OutcomeCompleted, res.Outcome)
	case <-time.After(time.Second):
		t.Fatal("the submitter must receive the result even though the journal write failed")
	}

	// A worker retrying the report must not wedge or double-deliver.
	bkt.fail = false
	testutil.Ok(t, s.Report(ctx, Result{
		TaskID: "t1", LeaseToken: task.LeaseToken, Generation: task.Generation,
		Outcome: OutcomeCompleted, OutputBlocks: []string{"out"},
	}))
}

// TestSchedulerDeliversTerminalClassesImmediately asserts halt, issue347 and
// out-of-order-chunks failures are not retried: each demands a specific
// reaction from the compactor's control loop, and repeating a potentially
// hours-long compaction to reach the same conclusion delays that reaction.
func TestSchedulerDeliversTerminalClassesImmediately(t *testing.T) {
	ctx := context.Background()

	for _, outcome := range []Outcome{OutcomeFailedHalt, OutcomeFailedIssue347, OutcomeFailedOOOChunks} {
		t.Run(string(outcome), func(t *testing.T) {
			s := testScheduler(t, objstore.NewInMemBucket(), ManagerConfig{MaxAttempts: 3})

			resultCh := submitTask(t, s)
			task, err := s.Lease(ctx, LeaseRequest{WorkerID: "w1"})
			testutil.Ok(t, err)

			testutil.Ok(t, s.Report(ctx, Result{
				TaskID: "t1", LeaseToken: task.LeaseToken, Generation: task.Generation,
				Outcome: outcome, OffendingBlock: "01ARZ3NDEKTSV4RRFFQ69G5FAV", ErrorMessage: "boom",
			}))

			select {
			case res := <-resultCh:
				testutil.Equals(t, outcome, res.Outcome)
			case <-time.After(time.Second):
				t.Fatalf("a %s failure must be delivered on the first report", outcome)
			}

			none, err := s.Lease(ctx, LeaseRequest{WorkerID: "w1"})
			testutil.Ok(t, err)
			testutil.Assert(t, none == nil, "a %s failure must not be retried", outcome)
		})
	}
}

// TestSchedulerTakeoverDropsUnfinishedTasks asserts a restarting manager drops
// pending and leased journal entries instead of carrying them: the new
// scheduler's queue starts empty, so carried entries could never be leased and,
// being non-terminal, would never be pruned either. Terminal entries stay for
// observability until their retention expires.
func TestSchedulerTakeoverDropsUnfinishedTasks(t *testing.T) {
	ctx := context.Background()
	bkt := objstore.NewInMemBucket()

	j := NewJournal("shard-a", "")
	j.Generation = 4
	now := time.Now()
	j.Tasks["leased"] = &TaskEntry{Task: Task{ID: "leased"}, State: StateLeased,
		Lease: &Lease{WorkerID: "w1", Token: "tok", Generation: 4, ExpiresAt: now.Add(time.Minute)}, UpdatedAt: now}
	j.Tasks["pending"] = &TaskEntry{Task: Task{ID: "pending"}, State: StatePending, UpdatedAt: now}
	j.Tasks["done"] = &TaskEntry{Task: Task{ID: "done"}, State: StateCompleted, UpdatedAt: now}
	testutil.Ok(t, WriteJournal(ctx, bkt, j))

	_ = testScheduler(t, bkt, ManagerConfig{})

	got, err := ReadJournal(ctx, bkt, "shard-a")
	testutil.Ok(t, err)
	_, ok := got.Tasks["leased"]
	testutil.Assert(t, !ok, "a leased task must be dropped at takeover")
	_, ok = got.Tasks["pending"]
	testutil.Assert(t, !ok, "a pending task must be dropped at takeover")
	_, ok = got.Tasks["done"]
	testutil.Assert(t, ok, "a completed task must survive until its retention expires")
}

// --- verifyAndFinalize provenance ---

func provenanceFixture(t *testing.T) (*RemotePlanExecutor, objstore.Bucket, *compact.Group, []*metadata.Meta) {
	t.Helper()
	bkt := objstore.NewInMemBucket()

	cnt := func() prometheus.Counter { return prometheus.NewCounter(prometheus.CounterOpts{Name: "test"}) }
	cg, err := compact.NewGroup(log.NewNopLogger(), bkt, "0@test", labels.FromStrings("ext", "1"), 0,
		false, false, cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), metadata.NoneFunc, 1, 1)
	testutil.Ok(t, err)

	src := func(i uint64, mint, maxt int64, samples uint64) *metadata.Meta {
		m := &metadata.Meta{}
		m.ULID = ulid.MustNew(i, nil)
		m.MinTime, m.MaxTime = mint, maxt
		m.Compaction.Level = 1
		m.Compaction.Sources = []ulid.ULID{m.ULID}
		m.Stats.NumSamples = samples
		m.Thanos.Labels = map[string]string{"ext": "1"}
		return m
	}
	toCompact := []*metadata.Meta{src(1, 0, 100, 10), src(2, 100, 200, 10)}

	return &RemotePlanExecutor{logger: log.NewNopLogger(), bkt: bkt, journalID: "shard-test"}, bkt, cg, toCompact
}

// fixtureProvenance is what a worker of the fixture's manager stamps on a
// result of task t1; tests that want a mismatch override fields.
var fixtureProvenance = Provenance{TaskID: "t1", TaskType: TaskCompaction, WorkerID: "w1", JournalID: "shard-test", Generation: 0}

func uploadResultMeta(t *testing.T, bkt objstore.Bucket, m metadata.Meta) (string, string) {
	t.Helper()
	raw, err := json.Marshal(m)
	testutil.Ok(t, err)
	testutil.Ok(t, bkt.Upload(context.Background(), path.Join(m.ULID.String(), "meta.json"), strings.NewReader(string(raw))))
	return m.ULID.String(), checksumOf(raw)
}

func resultMeta(id ulid.ULID, maxt int64, resolution int64, lbls map[string]string, sources ...ulid.ULID) metadata.Meta {
	m := metadata.Meta{}
	m.ULID = id
	m.MinTime, m.MaxTime = 0, maxt
	m.Compaction.Level = 2
	m.Compaction.Sources = sources
	m.Thanos.Labels = lbls
	m.Thanos.Downsample.Resolution = resolution
	ext, err := fixtureProvenance.For(id, nil).Stamp(nil)
	if err != nil {
		panic(err)
	}
	m.Thanos.Extensions = ext
	return m
}

func deletionMarked(t *testing.T, bkt objstore.Bucket, id ulid.ULID) bool {
	t.Helper()
	ok, err := bkt.Exists(context.Background(), path.Join(id.String(), metadata.DeletionMarkFilename))
	testutil.Ok(t, err)
	return ok
}

// TestVerifyAndFinalizeAcceptsTheRealResult is the golden path: an output built
// from exactly the plan's sources passes verification and the sources are
// marked for deletion.
func TestVerifyAndFinalizeAcceptsTheRealResult(t *testing.T) {
	e, bkt, cg, toCompact := provenanceFixture(t)

	out := resultMeta(ulid.MustNew(99, nil), 200, 0, map[string]string{"ext": "1"},
		toCompact[0].ULID, toCompact[1].ULID)
	id, sum := uploadResultMeta(t, bkt, out)

	compIDs, err := e.verifyAndFinalize(context.Background(), cg, toCompact, Result{
		TaskID: "t1", Outcome: OutcomeCompleted,
		OutputBlocks: []string{id}, OutputChecksums: map[string]string{id: sum},
	})
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(compIDs))
	testutil.Equals(t, true, deletionMarked(t, bkt, toCompact[0].ULID))
	testutil.Equals(t, true, deletionMarked(t, bkt, toCompact[1].ULID))
}

// TestVerifyAndFinalizeRefusesForeignBlocks asserts no source is ever deleted
// on the strength of a block that exists but is not the result of this plan.
// This is what stands between a confused worker and data loss.
func TestVerifyAndFinalizeRefusesForeignBlocks(t *testing.T) {
	stranger := ulid.MustNew(77, nil)

	for _, tc := range []struct {
		name string
		out  func(toCompact []*metadata.Meta) metadata.Meta
	}{
		{"unrelated sources", func(tc []*metadata.Meta) metadata.Meta {
			return resultMeta(ulid.MustNew(99, nil), 200, 0, map[string]string{"ext": "1"}, stranger)
		}},
		{"foreign labels", func(tc []*metadata.Meta) metadata.Meta {
			return resultMeta(ulid.MustNew(99, nil), 200, 0, map[string]string{"ext": "other"}, tc[0].ULID, tc[1].ULID)
		}},
		{"wrong resolution", func(tc []*metadata.Meta) metadata.Meta {
			return resultMeta(ulid.MustNew(99, nil), 200, 300000, map[string]string{"ext": "1"}, tc[0].ULID, tc[1].ULID)
		}},
		{"outside the plan's time range", func(tc []*metadata.Meta) metadata.Meta {
			return resultMeta(ulid.MustNew(99, nil), 5000, 0, map[string]string{"ext": "1"}, tc[0].ULID, tc[1].ULID)
		}},
		{"covers only part of the plan", func(tc []*metadata.Meta) metadata.Meta {
			return resultMeta(ulid.MustNew(99, nil), 100, 0, map[string]string{"ext": "1"}, tc[0].ULID)
		}},
		{"claims every source but spans less time", func(tc []*metadata.Meta) metadata.Meta {
			// The block sits inside the plan's range and accounts for every
			// source, but half the planned range is missing: accepting it would
			// delete that half with the sources.
			return resultMeta(ulid.MustNew(99, nil), 100, 0, map[string]string{"ext": "1"}, tc[0].ULID, tc[1].ULID)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, bkt, cg, toCompact := provenanceFixture(t)
			id, sum := uploadResultMeta(t, bkt, tc.out(toCompact))

			_, err := e.verifyAndFinalize(context.Background(), cg, toCompact, Result{
				TaskID: "t1", Outcome: OutcomeCompleted,
				OutputBlocks: []string{id}, OutputChecksums: map[string]string{id: sum},
			})
			testutil.NotOk(t, err)
			testutil.Equals(t, true, compact.IsRetryError(err))
			testutil.Equals(t, false, deletionMarked(t, bkt, toCompact[0].ULID))
			testutil.Equals(t, false, deletionMarked(t, bkt, toCompact[1].ULID))
		})
	}
}

// TestVerifyAndFinalizeRefusesChecksumMismatch asserts a result whose metadata
// does not match what the worker reported uploading is not trusted.
func TestVerifyAndFinalizeRefusesChecksumMismatch(t *testing.T) {
	e, bkt, cg, toCompact := provenanceFixture(t)

	out := resultMeta(ulid.MustNew(99, nil), 200, 0, map[string]string{"ext": "1"},
		toCompact[0].ULID, toCompact[1].ULID)
	id, _ := uploadResultMeta(t, bkt, out)

	_, err := e.verifyAndFinalize(context.Background(), cg, toCompact, Result{
		TaskID: "t1", Outcome: OutcomeCompleted,
		OutputBlocks: []string{id}, OutputChecksums: map[string]string{id: "sha256:not-what-was-uploaded"},
	})
	testutil.NotOk(t, err)
	testutil.Equals(t, false, deletionMarked(t, bkt, toCompact[0].ULID))
}

// TestVerifyAndFinalizeEmptyResult asserts a task that legitimately produced
// nothing, because every source was empty, gets its sources marked for deletion
// exactly as the in-process compactor would; leaving them would have the same
// empty blocks planned, downloaded and compacted forever. If a source does hold
// samples, an empty result is a worker bug and nothing may be deleted.
func TestVerifyAndFinalizeEmptyResult(t *testing.T) {
	t.Run("all sources empty", func(t *testing.T) {
		e, bkt, cg, toCompact := provenanceFixture(t)
		for _, m := range toCompact {
			m.Stats.NumSamples = 0
		}

		compIDs, err := e.verifyAndFinalize(context.Background(), cg, toCompact, Result{TaskID: "t1", Outcome: OutcomeCompleted})
		testutil.Ok(t, err)
		testutil.Equals(t, 0, len(compIDs))
		testutil.Equals(t, true, deletionMarked(t, bkt, toCompact[0].ULID))
		testutil.Equals(t, true, deletionMarked(t, bkt, toCompact[1].ULID))
	})

	t.Run("a source holds samples", func(t *testing.T) {
		e, bkt, cg, toCompact := provenanceFixture(t)
		toCompact[0].Stats.NumSamples = 0

		_, err := e.verifyAndFinalize(context.Background(), cg, toCompact, Result{TaskID: "t1", Outcome: OutcomeCompleted})
		testutil.NotOk(t, err)
		testutil.Equals(t, true, compact.IsRetryError(err))
		testutil.Equals(t, false, deletionMarked(t, bkt, toCompact[0].ULID))
		testutil.Equals(t, false, deletionMarked(t, bkt, toCompact[1].ULID))
	})
}

// flakyClient fails the first reports, then accepts.
type flakyClient struct {
	failures int
	reported []Result
}

func (c *flakyClient) Lease(context.Context, LeaseRequest) (*Task, error) { return nil, nil }
func (c *flakyClient) Heartbeat(context.Context, HeartbeatRequest) (HeartbeatResponse, error) {
	return HeartbeatResponse{Acknowledged: true}, nil
}
func (c *flakyClient) Report(_ context.Context, res Result) error {
	if c.failures > 0 {
		c.failures--
		return errors.New("network blip")
	}
	c.reported = append(c.reported, res)
	return nil
}

// TestReportWithRetrySurvivesBlips asserts a finished task's report is not
// thrown away over transient failures; re-executing a report that represents
// hours of work is far more expensive than a few retries.
func TestReportWithRetrySurvivesBlips(t *testing.T) {
	c := &flakyClient{failures: 2}
	testutil.Ok(t, reportWithBackoff(context.Background(), log.NewNopLogger(), c, Result{TaskID: "t1"}, time.Millisecond))
	testutil.Equals(t, 1, len(c.reported))
}

// TestReportWithRetryGivesUpOnContext asserts the retry loop respects its
// deadline, since the manager's lease expiry is the fallback anyway.
func TestReportWithRetryGivesUpOnContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	c := &flakyClient{failures: 1 << 30}
	err := reportWithBackoff(ctx, log.NewNopLogger(), c, Result{TaskID: "t1"}, time.Millisecond)
	testutil.NotOk(t, err)
	testutil.Equals(t, 0, len(c.reported))
}

// TestVerifyAndFinalizeRefusesMissingChecksum asserts a result reported without
// its metadata checksum is not trusted: the checksum is what binds the report
// to the metadata the worker observed after upload, and workers always send it.
func TestVerifyAndFinalizeRefusesMissingChecksum(t *testing.T) {
	e, bkt, cg, toCompact := provenanceFixture(t)

	out := resultMeta(ulid.MustNew(99, nil), 200, 0, map[string]string{"ext": "1"},
		toCompact[0].ULID, toCompact[1].ULID)
	id, _ := uploadResultMeta(t, bkt, out)

	_, err := e.verifyAndFinalize(context.Background(), cg, toCompact, Result{
		TaskID: "t1", Outcome: OutcomeCompleted,
		OutputBlocks: []string{id},
	})
	testutil.NotOk(t, err)
	testutil.Equals(t, true, compact.IsRetryError(err))
	testutil.Equals(t, false, deletionMarked(t, bkt, toCompact[0].ULID))
	testutil.Equals(t, false, deletionMarked(t, bkt, toCompact[1].ULID))
}

// TestSchedulerTakeoverHaltReachesSubmitter asserts that when recording a
// result reveals another manager owns the journal, the halt is what reaches the
// submitter - not the worker's outcome. Otherwise the old manager's control
// loop would verify the result and keep compacting a shard somebody else now
// manages, with only the worker ever seeing the error.
func TestSchedulerTakeoverHaltReachesSubmitter(t *testing.T) {
	ctx := context.Background()
	bkt := objstore.NewInMemBucket()
	s := testScheduler(t, bkt, ManagerConfig{})

	resultCh := submitTask(t, s)
	task, err := s.Lease(ctx, LeaseRequest{WorkerID: "w1"})
	testutil.Ok(t, err)

	// Another manager takes the journal over while the worker is compacting.
	j, err := ReadJournal(ctx, bkt, "shard-a")
	testutil.Ok(t, err)
	j.Owner = "somebody-else"
	testutil.Ok(t, WriteJournal(ctx, bkt, j))

	reportErr := s.Report(ctx, Result{
		TaskID: "t1", LeaseToken: task.LeaseToken, Generation: task.Generation,
		Outcome: OutcomeCompleted, OutputBlocks: []string{"out"},
	})
	testutil.NotOk(t, reportErr)
	testutil.Equals(t, true, compact.IsHaltError(reportErr))

	select {
	case res := <-resultCh:
		testutil.Equals(t, OutcomeFailedHalt, res.Outcome)
		testutil.Equals(t, true, compact.IsHaltError(ReconstructError(res)))
	case <-time.After(time.Second):
		t.Fatal("the submitter must receive the halt")
	}
}

// TestVerifyAndFinalizeRequiresProvenance asserts a result is only accepted
// when it records exactly the task it is reported for. The rollback finds the
// distributed compactor's work by this record, so a block accepted without it
// would replace the sources and yet be invisible to a rollback.
func TestVerifyAndFinalizeRequiresProvenance(t *testing.T) {
	for _, tc := range []struct {
		name string
		prov *Provenance // nil = none at all
	}{
		{"no provenance", nil},
		{"another task", &Provenance{TaskID: "t2", TaskType: TaskCompaction, JournalID: "shard-test"}},
		{"a downsample task", &Provenance{TaskID: "t1", TaskType: TaskDownsample, JournalID: "shard-test"}},
		{"another journal", &Provenance{TaskID: "t1", TaskType: TaskCompaction, JournalID: "other-shard"}},
		{"another generation", &Provenance{TaskID: "t1", TaskType: TaskCompaction, JournalID: "shard-test", Generation: 7}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, bkt, cg, toCompact := provenanceFixture(t)

			out := resultMeta(ulid.MustNew(99, nil), 200, 0, map[string]string{"ext": "1"}, toCompact[0].ULID, toCompact[1].ULID)
			out.Thanos.Extensions = nil
			if tc.prov != nil {
				ext, err := tc.prov.For(out.ULID, nil).Stamp(nil)
				testutil.Ok(t, err)
				out.Thanos.Extensions = ext
			}
			id, sum := uploadResultMeta(t, bkt, out)

			_, err := e.verifyAndFinalize(context.Background(), cg, toCompact, Result{
				TaskID: "t1", Outcome: OutcomeCompleted,
				OutputBlocks: []string{id}, OutputChecksums: map[string]string{id: sum},
			})
			testutil.NotOk(t, err)
			testutil.Equals(t, true, compact.IsRetryError(err))
			testutil.Equals(t, false, deletionMarked(t, bkt, toCompact[0].ULID))
		})
	}
}

// TestVerifyTimeCoverage pins the rule for several outputs of one plan: they
// must follow each other exactly or span exactly the same range. Anything in
// between would replace the sources with overlapping blocks.
func TestVerifyTimeCoverage(t *testing.T) {
	span := func(mint, maxt int64) metadata.Meta {
		m := metadata.Meta{}
		m.MinTime, m.MaxTime = mint, maxt
		return m
	}
	for _, tc := range []struct {
		name   string
		blocks []metadata.Meta
		ok     bool
	}{
		{"single exact", []metadata.Meta{span(0, 200)}, true},
		{"contiguous", []metadata.Meta{span(0, 100), span(100, 200)}, true},
		{"same range, as series sharding produces", []metadata.Meta{span(0, 200), span(0, 200), span(0, 200)}, true},
		{"partial overlap", []metadata.Meta{span(0, 150), span(100, 200)}, false},
		{"gap", []metadata.Meta{span(0, 80), span(120, 200)}, false},
		{"starts late", []metadata.Meta{span(50, 200)}, false},
		{"ends early", []metadata.Meta{span(0, 150)}, false},
		{"nested inside another", []metadata.Meta{span(0, 200), span(50, 100)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := verifyTimeCoverage(tc.blocks, 0, 200)
			testutil.Equals(t, tc.ok, err == nil, "unexpected result: %v", err)
		})
	}
}

// TestSchedulerRefusesMismatchedWorkers asserts a worker whose configuration
// would produce wrong or unverifiable blocks is never handed work, and the
// refusal names the flag to fix. A worker on another journal would abort every
// task at its ownership check; a worker with another merge configuration would
// produce blocks that carry no trace of the difference.
func TestSchedulerRefusesMismatchedWorkers(t *testing.T) {
	ctx := context.Background()
	s := testScheduler(t, objstore.NewInMemBucket(), ManagerConfig{
		DedupFunc:          "penalty",
		DedupReplicaLabels: []string{"replica"},
	})
	submitTask(t, s)

	ok := LeaseRequest{WorkerID: "w", JournalID: "shard-a", DedupFunc: "penalty", DedupReplicaLabels: []string{"replica"}}

	for _, tc := range []struct {
		name   string
		mutate func(r LeaseRequest) LeaseRequest
		flag   string
	}{
		{"another journal", func(r LeaseRequest) LeaseRequest { r.JournalID = "other-shard"; return r }, "--compact.manager.journal-id"},
		{"another merge function", func(r LeaseRequest) LeaseRequest { r.DedupFunc = ""; return r }, "--deduplication.func"},
		{"other replica labels", func(r LeaseRequest) LeaseRequest { r.DedupReplicaLabels = nil; return r }, "--deduplication.replica-label"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.Lease(ctx, tc.mutate(ok))
			testutil.NotOk(t, err)
			testutil.Assert(t, strings.Contains(err.Error(), tc.flag), "the refusal must name the flag, got: %v", err)
		})
	}

	// A matching worker gets the task; an empty journal ID is tolerated for
	// compatibility with workers that do not state one.
	task, err := s.Lease(ctx, ok)
	testutil.Ok(t, err)
	testutil.Assert(t, task != nil, "a matching worker must be served")
}

// TestSchedulerParksOversizedTasks asserts a plan over the configured worker
// capacity is refused before any worker touches it, recorded in the journal,
// and its source blocks withheld from planning until an operator unparks them.
func TestSchedulerParksOversizedTasks(t *testing.T) {
	ctx := context.Background()
	bkt := objstore.NewInMemBucket()
	s := testScheduler(t, bkt, ManagerConfig{MaxTaskSeries: 100, LeaseTTL: 10 * time.Millisecond})

	task := Task{ID: "big", Type: TaskCompaction, SourceBlocks: []string{"a", "b"}, ExpectedSeries: 1000}
	reason := oversizedReason(task, s.conf)
	testutil.Assert(t, reason != "", "1000 series over a limit of 100 must be oversized")
	s.MarkOversized(task, reason)

	// Parked: the journal records why, and the block set is withheld.
	j, err := ReadJournal(ctx, bkt, "shard-a")
	testutil.Ok(t, err)
	entry := j.Tasks["big"]
	testutil.Assert(t, entry != nil && entry.State == StateOversized, "the refusal must be in the journal")
	testutil.Equals(t, OutcomeOversized, entry.LastError.Outcome)
	testutil.Equals(t, true, s.SourcesParked([]string{"b", "a"}))
	testutil.Equals(t, false, s.SourcesParked([]string{"a"}))

	// Parked entries survive a manager restart: they are terminal, and what
	// they say - do not plan these blocks - must outlive the process.
	restarted := testScheduler(t, bkt, ManagerConfig{MaxTaskSeries: 100, LeaseTTL: 10 * time.Millisecond})
	testutil.Equals(t, true, restarted.SourcesParked([]string{"a", "b"}))

	// The operator releases it: one empty object, applied by maintenance.
	testutil.Ok(t, bkt.Upload(ctx, UnparkPath("shard-a", "big"), strings.NewReader("")))
	time.Sleep(15 * time.Millisecond)
	testutil.Ok(t, restarted.Maintain())
	testutil.Equals(t, false, restarted.SourcesParked([]string{"a", "b"}))

	// The marker is consumed, and the journal no longer holds the entry.
	exists, err := bkt.Exists(ctx, UnparkPath("shard-a", "big"))
	testutil.Ok(t, err)
	testutil.Equals(t, false, exists)
	j, err = ReadJournal(ctx, bkt, "shard-a")
	testutil.Ok(t, err)
	_, held := j.Tasks["big"]
	testutil.Equals(t, false, held)
}

// TestSchedulerAbandonedTaskParksItsSources asserts the poison-task protection
// actually protects: once a task burned its attempts without a report, its
// exact source set is parked, and the executor defers rather than re-dispatch.
func TestSchedulerAbandonedTaskParksItsSources(t *testing.T) {
	ctx := context.Background()
	s := testScheduler(t, objstore.NewInMemBucket(), ManagerConfig{LeaseTTL: time.Millisecond, MaxAttempts: 2})

	resultCh, err := s.Submit(ctx, Task{ID: "t1", Type: TaskCompaction, SourceBlocks: []string{"a", "b"}})
	testutil.Ok(t, err)

	for range 2 {
		task, err := s.Lease(ctx, LeaseRequest{WorkerID: "crashy", JournalID: "shard-a"})
		testutil.Ok(t, err)
		testutil.Assert(t, task != nil, "expected a lease")
		time.Sleep(5 * time.Millisecond)
		testutil.Ok(t, s.Maintain()) // expire without a report
	}

	select {
	case res := <-resultCh:
		testutil.Equals(t, OutcomeAbandoned, res.Outcome)
	case <-time.After(time.Second):
		t.Fatal("the submitter must learn the task was abandoned")
	}
	testutil.Equals(t, true, s.SourcesParked([]string{"a", "b"}))
}
