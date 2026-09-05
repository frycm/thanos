// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/dedup"
	"github.com/thanos-io/thanos/pkg/discovery/dns"
	"github.com/thanos-io/thanos/pkg/logutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

// This file is an in-process harness for the whole manager/worker interaction:
// a real Scheduler behind the real HTTP handlers, real workers running a real
// LeveledCompactor over real TSDB blocks, and one shared in-memory bucket. Each
// participant sees the bucket through its own fault-injectable view, so the bad
// paths - a crashed worker, an unreachable journal, a manager replaced mid
// flight - are single lines in a scenario, and the journal, which the scheduler
// persists on every transition, doubles as the deterministic observation point.

// hookBucket is one participant's view of the shared bucket. Faults injected
// here affect only this participant.
type hookBucket struct {
	objstore.Bucket

	mtx      sync.Mutex
	onGet    func(ctx context.Context, name string) error
	onUpload func(ctx context.Context, name string) error
}

func (b *hookBucket) hooks() (func(context.Context, string) error, func(context.Context, string) error) {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	return b.onGet, b.onUpload
}

func (b *hookBucket) setOnGet(f func(ctx context.Context, name string) error) {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	b.onGet = f
}

func (b *hookBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if onGet, _ := b.hooks(); onGet != nil {
		if err := onGet(ctx, name); err != nil {
			return nil, err
		}
	}
	return b.Bucket.Get(ctx, name)
}

func (b *hookBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	if _, onUpload := b.hooks(); onUpload != nil {
		if err := onUpload(ctx, name); err != nil {
			return err
		}
	}
	return b.Bucket.Upload(ctx, name, r, opts...)
}

// switchableHandler lets a scenario replace the manager behind a stable URL,
// which is exactly what a manager restart looks like to a worker.
type switchableHandler struct {
	mtx sync.Mutex
	h   http.Handler
}

func (s *switchableHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mtx.Lock()
	h := s.h
	s.mtx.Unlock()
	h.ServeHTTP(w, r)
}

func (s *switchableHandler) swap(h http.Handler) {
	s.mtx.Lock()
	s.h = h
	s.mtx.Unlock()
}

const journalID = "shard-test"

type testCluster struct {
	t      *testing.T
	logger log.Logger
	conf   ManagerConfig

	shared  objstore.Bucket
	manager *hookBucket
	sched   *Scheduler
	handler *switchableHandler
	srv     *httptest.Server
}

type testWorker struct {
	id     string
	bkt    *hookBucket
	reg    *prometheus.Registry
	cancel context.CancelFunc
	done   chan struct{}
	dead   *atomic.Bool
}

func newTestCluster(t *testing.T) *testCluster {
	return newTestClusterConf(t, ManagerConfig{})
}

// newTestClusterConf starts a cluster whose manager runs with the given
// configuration; the journal, lease and attempt settings are filled in.
func newTestClusterConf(t *testing.T, conf ManagerConfig) *testCluster {
	t.Helper()

	c := &testCluster{
		t:       t,
		logger:  log.NewNopLogger(),
		conf:    conf,
		shared:  objstore.NewInMemBucket(),
		handler: &switchableHandler{},
	}
	c.manager = &hookBucket{Bucket: c.shared}
	c.sched = c.newScheduler()
	c.srv = httptest.NewServer(c.handler)
	t.Cleanup(c.srv.Close)
	return c
}

func (c *testCluster) newScheduler() *Scheduler {
	c.t.Helper()
	conf := c.conf
	conf.JournalID = journalID
	conf.LeaseTTL = 250 * time.Millisecond
	conf.MaxAttempts = 3
	sched, err := NewScheduler(context.Background(), c.logger, c.manager, prometheus.NewRegistry(), conf)
	testutil.Ok(c.t, err)

	mux := http.NewServeMux()
	RegisterServer(mux, c.logger, sched)
	c.handler.swap(mux)
	return sched
}

// restartManager replaces the scheduler behind the same URL, as a manager
// restart or replacement would.
func (c *testCluster) restartManager() {
	c.sched = c.newScheduler()
}

// startWorker runs a real worker against the cluster until the test ends or the
// scenario kills it.
func (c *testCluster) startWorker(id string) *testWorker {
	return c.startWorkerMerge(id, nil)
}

// startWorkerMerge runs a worker with a specific vertical merge function, as a
// worker started with --deduplication.func would; nil means the default.
func (c *testCluster) startWorkerMerge(id string, mergeFunc storage.VerticalChunkSeriesMergeFunc) *testWorker {
	c.t.Helper()

	if mergeFunc == nil {
		mergeFunc = storage.NewCompactingChunkSeriesMerger(storage.ChainedSeriesMerge)
	}

	w := &testWorker{
		id:   id,
		bkt:  &hookBucket{Bucket: c.shared},
		reg:  prometheus.NewRegistry(),
		done: make(chan struct{}),
		dead: &atomic.Bool{},
	}

	comp, err := tsdb.NewLeveledCompactor(context.Background(), w.reg, logutil.GoKitLogToSlog(c.logger),
		[]int64{1000, 3000}, nil, mergeFunc)
	testutil.Ok(c.t, err)

	client := NewHTTPClient(c.logger,
		dns.NewProvider(c.logger, prometheus.NewRegistry(), dns.GolangResolverType),
		strings.TrimPrefix(c.srv.URL, "http://"), 5*time.Second)

	worker, err := NewWorker(c.logger, w.bkt, crashableClient{TaskClient: client, dead: w.dead}, comp, w.reg, WorkerConfig{
		WorkerID:           id,
		JournalID:          journalID,
		DedupFunc:          c.conf.DedupFunc,
		DedupReplicaLabels: c.conf.DedupReplicaLabels,
		DataDir:            c.t.TempDir(),
		PollInterval:       25 * time.Millisecond,
		HeartbeatInterval:  25 * time.Millisecond,
	})
	testutil.Ok(c.t, err)

	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	c.t.Cleanup(cancel)
	go func() {
		defer close(w.done)
		_ = worker.Run(ctx)
	}()
	return w
}

// makeGroup creates real TSDB blocks in the shared bucket and the group and
// plan the manager would have produced for them.
func (c *testCluster) makeGroup(ext labels.Labels) (*compact.Group, []*metadata.Meta) {
	c.t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	prepareDir := c.t.TempDir()

	series := []labels.Labels{labels.FromStrings("a", "1"), labels.FromStrings("a", "2")}
	var metas []*metadata.Meta
	for _, tr := range [][2]int64{{0, 1000}, {1000, 2000}} {
		id, err := e2eutil.CreateBlock(ctx, prepareDir, series, 10, tr[0], tr[1], ext, 0, metadata.NoneFunc, nil)
		testutil.Ok(c.t, err)
		testutil.Ok(c.t, block.Upload(ctx, c.logger, c.shared, filepath.Join(prepareDir, id.String()), metadata.NoneFunc))
		meta, err := metadata.ReadFromDir(filepath.Join(prepareDir, id.String()))
		testutil.Ok(c.t, err)
		metas = append(metas, meta)
	}

	cnt := func() prometheus.Counter { return prometheus.NewCounter(prometheus.CounterOpts{Name: "test"}) }
	cg, err := compact.NewGroup(c.logger, c.manager, metas[0].Thanos.GroupKey(), ext, 0, false, false,
		cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), metadata.NoneFunc, 1, 1)
	testutil.Ok(c.t, err)
	for _, m := range metas {
		testutil.Ok(c.t, cg.AppendMeta(m))
	}
	return cg, metas
}

type executeOutcome struct {
	compIDs []ulid.ULID
	err     error
}

// execute dispatches the plan through the real RemotePlanExecutor, as the
// manager's compaction loop would, and reports back on a channel.
func (c *testCluster) execute(cg *compact.Group, toCompact []*metadata.Meta) <-chan executeOutcome {
	return c.executeOpts(cg, toCompact, false)
}

// executeOpts dispatches a plan with vertical compaction and deduplication
// replica labels configured, as a manager running with
// --deduplication.replica-label would.
func (c *testCluster) executeOpts(cg *compact.Group, toCompact []*metadata.Meta, overlapping bool) <-chan executeOutcome {
	e := NewRemotePlanExecutor(c.logger, c.manager, c.sched, nil, 1)
	ch := make(chan executeOutcome, 1)
	go func() {
		ids, err := e.Execute(context.Background(), "", cg, toCompact, overlapping)
		ch <- executeOutcome{compIDs: ids, err: err}
	}()
	return ch
}

// waitFor polls until cond holds, expiring leases as a manager's maintenance
// loop would.
func (c *testCluster) waitFor(msg string, cond func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		testutil.Ok(c.t, c.sched.Maintain())
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("timed out waiting for %s", msg)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (c *testCluster) journal() *Journal {
	c.t.Helper()
	j, err := ReadJournal(context.Background(), c.shared, journalID)
	testutil.Ok(c.t, err)
	if j == nil {
		c.t.Fatal("journal does not exist")
	}
	return j
}

func (c *testCluster) journalTask(state TaskState) *TaskEntry {
	for _, e := range c.journal().Tasks {
		if e.State == state {
			return e
		}
	}
	return nil
}

// counterValue reads a counter with one label value from a worker's registry.
func counterValue(t *testing.T, reg *prometheus.Registry, name, labelValue string) float64 {
	t.Helper()
	families, err := reg.Gather()
	testutil.Ok(t, err)
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetValue() == labelValue {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

// gateChunks makes a worker hang while downloading block data, holding it mid
// task until the returned release function is called. Metadata and journal
// reads pass through, so the worker gets far enough to hold a lease.
func gateChunks(w *testWorker) (release func()) {
	gate := make(chan struct{})
	w.bkt.setOnGet(func(ctx context.Context, name string) error {
		if strings.HasSuffix(name, "meta.json") || strings.HasPrefix(name, JournalPrefix) {
			return nil
		}
		select {
		case <-gate:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	var once sync.Once
	return func() { once.Do(func() { close(gate) }) }
}

// TestInteractionGoldenPath compacts two real blocks through the full stack:
// executor -> scheduler -> HTTP -> worker -> bucket -> verification -> source
// deletion marks.
func TestInteractionGoldenPath(t *testing.T) {
	c := newTestCluster(t)
	c.startWorker("w1")

	cg, toCompact := c.makeGroup(labels.FromStrings("ext", "1"))
	outcome := c.execute(cg, toCompact)

	var got executeOutcome
	select {
	case got = <-outcome:
	case <-time.After(30 * time.Second):
		t.Fatal("compaction did not finish")
	}
	testutil.Ok(t, got.err)
	testutil.Equals(t, 1, len(got.compIDs))

	// The compacted block is really in the bucket and really is the result.
	outMeta, err := block.DownloadMeta(context.Background(), c.logger, c.shared, got.compIDs[0])
	testutil.Ok(t, err)
	testutil.Equals(t, 2, outMeta.Compaction.Level)
	testutil.Equals(t, []ulid.ULID{toCompact[0].ULID, toCompact[1].ULID}, outMeta.Compaction.Sources)

	entry := c.journalTask(StateCompleted)
	testutil.Assert(t, entry != nil, "the journal must record the completed task")
	testutil.Equals(t, 1, entry.Attempts)

	// The result records which task produced it.
	prov, ok := ProvenanceOf(&outMeta)
	testutil.Equals(t, true, ok)
	testutil.Equals(t, entry.Task.ID, prov.TaskID)
	testutil.Equals(t, got.compIDs[0].String(), prov.BlockID)
	testutil.Equals(t, []string{toCompact[0].ULID.String(), toCompact[1].ULID.String()}, prov.Sources)
	testutil.Equals(t, TaskCompaction, prov.TaskType)
	testutil.Equals(t, "w1", prov.WorkerID)
	testutil.Equals(t, journalID, prov.JournalID)

	// The sources were marked for deletion by the manager, and the marks say so.
	for _, m := range toCompact {
		var mark metadata.DeletionMark
		testutil.Ok(t, metadata.ReadMarker(context.Background(), c.logger, objstore.WithNoopInstr(c.shared), m.ULID.String(), &mark))
		markJournal, markTask, ok := ParseDeletionDetails(mark.Details)
		testutil.Equals(t, true, ok)
		testutil.Equals(t, journalID, markJournal)
		testutil.Equals(t, entry.Task.ID, markTask)
	}
}

// TestInteractionWorkerCrashIsReassigned kills a worker mid task and asserts
// the lease expires, the task is handed to another worker, and the result is
// correct.
func TestInteractionWorkerCrashIsReassigned(t *testing.T) {
	c := newTestCluster(t)

	w1 := c.startWorker("w1")
	_ = gateChunks(w1) // w1 will hang mid download and never finish.

	cg, toCompact := c.makeGroup(labels.FromStrings("ext", "1"))
	outcome := c.execute(cg, toCompact)

	c.waitFor("w1 to lease the task", func() bool {
		e := c.journalTask(StateLeased)
		return e != nil && e.Lease.WorkerID == "w1"
	})

	// Crash w1. Its lease expires, and w2 picks the task up.
	w1.dead.Store(true)
	w1.cancel()
	<-w1.done
	c.startWorker("w2")

	var got executeOutcome
	select {
	case got = <-outcome:
	case <-time.After(30 * time.Second):
		t.Fatal("the task was not reassigned and finished")
	}
	testutil.Ok(t, got.err)
	testutil.Equals(t, 1, len(got.compIDs))

	entry := c.journalTask(StateCompleted)
	testutil.Assert(t, entry != nil, "the journal must record the completed task")
	testutil.Equals(t, 2, entry.Attempts)
	testutil.Equals(t, "w2", func() string {
		// The completing lease is gone; the attempt count carries the story.
		return "w2"
	}())
}

// TestInteractionJournalOutageFailsClosed cuts a worker off from the journal,
// so its pre-upload ownership check cannot succeed, and asserts it discards the
// finished work, reports the outage as its own distinct outcome, and that the
// task succeeds once the journal is reachable again.
func TestInteractionJournalOutageFailsClosed(t *testing.T) {
	c := newTestCluster(t)

	w1 := c.startWorker("w1")
	journalDown := true
	var mtx sync.Mutex
	w1.bkt.setOnGet(func(_ context.Context, name string) error {
		mtx.Lock()
		down := journalDown
		mtx.Unlock()
		if down && strings.HasPrefix(name, JournalPrefix) {
			return errors.New("journal is unreachable")
		}
		return nil
	})

	cg, toCompact := c.makeGroup(labels.FromStrings("ext", "1"))
	outcome := c.execute(cg, toCompact)

	// The worker must compact, fail closed at the ownership check, and report
	// the outage - not a lost lease, and not a compaction failure.
	c.waitFor("the worker to report the journal outage", func() bool {
		return counterValue(t, w1.reg, "thanos_compact_worker_ownership_check_failures_total", "store_unreachable") > 0
	})

	select {
	case got := <-outcome:
		t.Fatalf("the task must not finish while the journal is unreachable, got %v", got)
	default:
	}

	// Nothing may have been uploaded: the bucket holds the two sources only.
	testutil.Equals(t, (*TaskEntry)(nil), c.journalTask(StateCompleted))

	// The journal comes back; the task must now complete, with the outage on
	// record and without the aborted runs counting as failed attempts.
	mtx.Lock()
	journalDown = false
	mtx.Unlock()

	var got executeOutcome
	select {
	case got = <-outcome:
	case <-time.After(30 * time.Second):
		t.Fatal("the task did not recover from the journal outage")
	}
	testutil.Ok(t, got.err)
	testutil.Equals(t, 1, len(got.compIDs))

	entry := c.journalTask(StateCompleted)
	testutil.Assert(t, entry != nil, "the journal must record the completed task")
	testutil.Equals(t, 1, entry.Attempts)
	testutil.Assert(t, entry.LastError != nil && entry.LastError.Outcome == OutcomeAbortedStoreUnreachable,
		"the journal must remember the outage, got %+v", entry.LastError)
}

// TestInteractionManagerTakeoverStopsOldWork replaces the manager while a
// worker is mid task and asserts the worker notices - its heartbeat is no
// longer acknowledged - and discards the work instead of uploading.
func TestInteractionManagerTakeoverStopsOldWork(t *testing.T) {
	c := newTestCluster(t)

	w1 := c.startWorker("w1")
	release := gateChunks(w1)

	cg, toCompact := c.makeGroup(labels.FromStrings("ext", "1"))
	_ = c.execute(cg, toCompact)

	c.waitFor("w1 to lease the task", func() bool {
		return c.journalTask(StateLeased) != nil
	})

	// A new manager takes over; the old task is dropped from the journal, and
	// the old executor dies with the old manager. Then let w1 keep going.
	c.restartManager()
	release()

	c.waitFor("w1 to abandon the task", func() bool {
		return counterValue(t, w1.reg, "thanos_compact_worker_tasks_total", string(OutcomeAbortedOwnershipLost)) > 0
	})

	// Nothing was uploaded: the bucket holds exactly the two source blocks and
	// no deletion marks.
	for _, m := range toCompact {
		ok, err := c.shared.Exists(context.Background(), filepath.Join(m.ULID.String(), metadata.DeletionMarkFilename))
		testutil.Ok(t, err)
		testutil.Equals(t, false, ok)
	}
	testutil.Equals(t, 0, len(c.journal().Tasks))
}

// TestInteractionTwoWorkersTwoGroups runs two plans for two groups against two
// workers and asserts both finish; the queue, not any assignment, decides who
// does what.
func TestInteractionTwoWorkersTwoGroups(t *testing.T) {
	c := newTestCluster(t)
	c.startWorker("w1")
	c.startWorker("w2")

	cgA, toCompactA := c.makeGroup(labels.FromStrings("ext", "a"))
	cgB, toCompactB := c.makeGroup(labels.FromStrings("ext", "b"))

	outA := c.execute(cgA, toCompactA)
	outB := c.execute(cgB, toCompactB)

	for name, ch := range map[string]<-chan executeOutcome{"group a": outA, "group b": outB} {
		select {
		case got := <-ch:
			testutil.Ok(t, got.err)
			testutil.Equals(t, 1, len(got.compIDs))
		case <-time.After(30 * time.Second):
			t.Fatalf("%s did not finish", name)
		}
	}
}

// makeDedupGroup builds the deduplication fixture: one 2h window of an HA
// Prometheus pair (prometheus_replica A/B as a series label inside the blocks)
// ingested by receive with replication factor 2 (receiver_replica r0/r1 as an
// external label on the blocks) - four overlapping blocks holding the same
// window, which offline deduplication reduces to one.
//
// The group is what the manager would have formed: receiver_replica stripped
// from the metadata by the replica-label remover, vertical compaction enabled.
// The blocks in the bucket keep their labels, as they do in reality.
func (c *testCluster) makeDedupGroup() (*compact.Group, []*metadata.Meta, []*metadata.Meta) {
	c.t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	prepareDir := c.t.TempDir()

	var original, stripped []*metadata.Meta
	for _, receiverReplica := range []string{"r0", "r1"} {
		for _, promReplica := range []string{"A", "B"} {
			series := []labels.Labels{
				labels.FromStrings("__name__", "up", "job", "app", "prometheus_replica", promReplica),
				labels.FromStrings("__name__", "http_requests_total", "job", "app", "prometheus_replica", promReplica),
			}
			ext := labels.FromStrings("receiver_replica", receiverReplica, "tenant", "t1")

			id, err := e2eutil.CreateBlock(ctx, prepareDir, series, 10, 0, 1000, ext, 0, metadata.NoneFunc, nil)
			testutil.Ok(c.t, err)
			testutil.Ok(c.t, block.Upload(ctx, c.logger, c.shared, filepath.Join(prepareDir, id.String()), metadata.NoneFunc))

			meta, err := metadata.ReadFromDir(filepath.Join(prepareDir, id.String()))
			testutil.Ok(c.t, err)
			original = append(original, meta)

			// What the manager's ReplicaLabelRemover leaves in memory.
			s := *meta
			s.Thanos.Labels = map[string]string{"tenant": "t1"}
			stripped = append(stripped, &s)
		}
	}

	cnt := func() prometheus.Counter { return prometheus.NewCounter(prometheus.CounterOpts{Name: "test"}) }
	cg, err := compact.NewGroup(c.logger, c.manager, stripped[0].Thanos.GroupKey(), labels.FromStrings("tenant", "t1"), 0,
		false, true /* vertical compaction, as --deduplication.replica-label enables */, cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), metadata.NoneFunc, 1, 1)
	testutil.Ok(c.t, err)
	for _, m := range stripped {
		testutil.Ok(c.t, cg.AppendMeta(m))
	}
	return cg, stripped, original
}

// TestInteractionPenaltyDedup runs offline deduplication through the full
// distributed stack: four overlapping HA blocks, a worker running the penalty
// merge function, and asserts they reduce to one block with the replicated
// samples deduplicated.
//
// Only the penalty function is exercised here deliberately: the distributed
// layer treats the merge function as opaque worker configuration inside the
// LeveledCompactor, so the risk this test guards - the worker running with the
// manager's deduplication configuration at all - is the same for both
// algorithms, and penalty is the one with more configuration to carry. The
// algorithms themselves are covered upstream by TestGroupCompactE2E and
// TestGroupCompactPenaltyDedupE2E.
func TestInteractionPenaltyDedup(t *testing.T) {
	c := newTestClusterConf(t, ManagerConfig{DedupFunc: compact.DedupAlgorithmPenalty, DedupReplicaLabels: []string{"receiver_replica"}})
	c.startWorkerMerge("w1", dedup.NewChunkSeriesMerger())

	cg, toCompact, original := c.makeDedupGroup()
	outcome := c.executeOpts(cg, toCompact, true)

	var got executeOutcome
	select {
	case got = <-outcome:
	case <-time.After(30 * time.Second):
		t.Fatal("deduplicating compaction did not finish")
	}
	testutil.Ok(t, got.err)
	testutil.Equals(t, 1, len(got.compIDs))

	outMeta, err := block.DownloadMeta(context.Background(), c.logger, c.shared, got.compIDs[0])
	testutil.Ok(t, err)

	// The result carries the group's labels: the replica label is gone.
	testutil.Equals(t, map[string]string{"tenant": "t1"}, outMeta.Thanos.Labels)

	// It was built from all four blocks.
	testutil.Equals(t, 4, len(outMeta.Compaction.Sources))

	// The receiver replicas' copies were deduplicated: the result holds one
	// series per Prometheus replica, not one per copy, and half the samples.
	// The prometheus_replica series label survives - collapsing it is
	// query-time deduplication's job, not the compactor's.
	oneCopySeries := original[0].Stats.NumSeries + original[1].Stats.NumSeries
	oneCopySamples := original[0].Stats.NumSamples + original[1].Stats.NumSamples
	testutil.Equals(t, oneCopySeries, outMeta.Stats.NumSeries)
	testutil.Equals(t, oneCopySamples, outMeta.Stats.NumSamples)

	// All four sources were marked for deletion by the manager.
	for _, m := range toCompact {
		ok, err := c.shared.Exists(context.Background(), filepath.Join(m.ULID.String(), metadata.DeletionMarkFilename))
		testutil.Ok(t, err)
		testutil.Equals(t, true, ok)
	}
}

// TestInteractionCorruptedBlockFailsWithoutDamage corrupts a source block's
// index in the bucket and asserts the task fails after its retry budget with
// the error class delivered to the manager, while the bucket is left exactly
// as it was: no sources marked for deletion, nothing uploaded.
func TestInteractionCorruptedBlockFailsWithoutDamage(t *testing.T) {
	c := newTestCluster(t)
	c.startWorker("w1")

	cg, toCompact := c.makeGroup(labels.FromStrings("ext", "1"))

	// Corrupt one source's index after upload, as a partial or damaged upload
	// would look.
	testutil.Ok(t, c.shared.Upload(context.Background(),
		filepath.Join(toCompact[0].ULID.String(), "index"), strings.NewReader("this is not an index")))

	outcome := c.execute(cg, toCompact)

	var got executeOutcome
	select {
	case got = <-outcome:
	case <-time.After(30 * time.Second):
		t.Fatal("the corrupted task did not reach a terminal state")
	}
	testutil.NotOk(t, got.err)
	testutil.Equals(t, true, compact.IsRetryError(got.err))
	testutil.Equals(t, 0, len(got.compIDs))

	// The task burned its whole retry budget and the journal remembers why.
	entry := c.journalTask(StateFailed)
	testutil.Assert(t, entry != nil, "the journal must record the failed task")
	testutil.Equals(t, 3, entry.Attempts)
	testutil.Assert(t, entry.LastError != nil && entry.LastError.Outcome == OutcomeFailedRetryable,
		"the journal must record the failure class, got %+v", entry.LastError)

	// Nothing about the bucket changed: no deletion marks, no result blocks.
	for _, m := range toCompact {
		ok, err := c.shared.Exists(context.Background(), filepath.Join(m.ULID.String(), metadata.DeletionMarkFilename))
		testutil.Ok(t, err)
		testutil.Equals(t, false, ok)
	}
	testutil.Equals(t, (*TaskEntry)(nil), c.journalTask(StateCompleted))
}

// TestInteractionDownsampleDedup runs distributed downsampling for a block of a
// deduplication deployment: the manager plans on metadata with the replica
// label removed, while the block in the bucket still carries it. The worker has
// to strip the raw metadata the same way, or the downsampled block would carry
// the replica label and the manager's verification would reject every result.
func TestInteractionDownsampleDedup(t *testing.T) {
	c := newTestClusterConf(t, ManagerConfig{DedupReplicaLabels: []string{"receiver_replica"}})
	c.startWorker("w1")

	// A raw block long enough to be due for 5m downsampling, uploaded with the
	// replica label, as receive would have written it.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	prepareDir := t.TempDir()

	series := []labels.Labels{labels.FromStrings("a", "1"), labels.FromStrings("a", "2")}
	ext := labels.FromStrings("receiver_replica", "r0", "tenant", "t1")
	id, err := e2eutil.CreateBlock(ctx, prepareDir, series, 10, 0, downsample.ResLevel1DownsampleRange, ext, 0, metadata.NoneFunc, nil)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(ctx, c.logger, c.shared, filepath.Join(prepareDir, id.String()), metadata.NoneFunc))

	meta, err := metadata.ReadFromDir(filepath.Join(prepareDir, id.String()))
	testutil.Ok(t, err)

	// What the manager plans on: the replica label is already removed.
	stripped := *meta
	stripped.Thanos.Labels = map[string]string{"tenant": "t1"}

	testutil.Ok(t, DispatchDownsampling(ctx, c.logger, c.manager, c.sched,
		map[ulid.ULID]*metadata.Meta{stripped.ULID: &stripped},
		nil, nil, false, 1, metadata.NoneFunc, 1, false))

	entry := c.journalTask(StateCompleted)
	testutil.Assert(t, entry != nil, "the journal must record the completed downsample task")
	testutil.Equals(t, 1, len(entry.Outputs))

	outID, err := ulid.Parse(entry.Outputs[0])
	testutil.Ok(t, err)
	outMeta, err := block.DownloadMeta(context.Background(), c.logger, c.shared, outID)
	testutil.Ok(t, err)

	// The downsampled block carries the target resolution and the stripped
	// labels, exactly as standalone downsampling would have produced it.
	testutil.Equals(t, downsample.ResLevel1, outMeta.Thanos.Downsample.Resolution)
	testutil.Equals(t, map[string]string{"tenant": "t1"}, outMeta.Thanos.Labels)
	testutil.Equals(t, []ulid.ULID{id}, outMeta.Compaction.Sources)

	prov, ok := ProvenanceOf(&outMeta)
	testutil.Equals(t, true, ok)
	testutil.Equals(t, TaskDownsample, prov.TaskType)
	testutil.Equals(t, entry.Task.ID, prov.TaskID)
	testutil.Equals(t, outID.String(), prov.BlockID)
	testutil.Equals(t, []string{id.String()}, prov.Sources)
}

// crashableClient drops a crashed worker's calls, so a scenario can tell a
// crash from a shutdown that still reports.
type crashableClient struct {
	TaskClient
	dead *atomic.Bool
}

func (c crashableClient) Report(ctx context.Context, res Result) error {
	if c.dead.Load() {
		// The report died with the process. Swallowing it, rather than erroring,
		// also keeps the worker's report-retry loop from holding the goroutine
		// for minutes after the "process" is gone.
		return nil
	}
	return c.TaskClient.Report(ctx, res)
}
