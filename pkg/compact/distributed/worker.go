// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/runutil"
)

// WorkerConfig configures a compaction worker.
type WorkerConfig struct {
	// WorkerID identifies this worker to the manager. Defaults to the hostname.
	WorkerID string
	// JournalID is the shard journal the worker verifies its ownership against.
	// It has to match the manager's.
	JournalID string
	// DedupFunc and DedupReplicaLabels are what the worker's compactor was built
	// with (--deduplication.func and --deduplication.replica-label). They are
	// stated when asking for work, and the manager refuses a mismatch.
	DedupFunc          string
	DedupReplicaLabels []string
	// DataDir is where blocks are downloaded and compacted.
	DataDir string

	// PollInterval is how long the worker waits after finding no work.
	PollInterval time.Duration
	// HeartbeatInterval is how often the worker extends its lease.
	HeartbeatInterval time.Duration
}

func (c *WorkerConfig) applyDefaults() {
	if c.WorkerID == "" {
		if host, err := os.Hostname(); err == nil {
			c.WorkerID = host
		} else {
			c.WorkerID = ulid.Make().String()
		}
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 5 * time.Second
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 30 * time.Second
	}
}

type workerMetrics struct {
	tasksTotal             *prometheus.CounterVec
	stageDuration          *prometheus.HistogramVec
	ownershipCheckFailures *prometheus.CounterVec
}

func newWorkerMetrics(reg prometheus.Registerer) *workerMetrics {
	factory := promauto.With(reg)
	return &workerMetrics{
		tasksTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_compact_worker_tasks_total",
			Help: "Total number of tasks this worker finished, by outcome.",
		}, []string{"outcome"}),
		stageDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "thanos_compact_worker_stage_duration_seconds",
			Help:    "Time spent in each stage of executing a task.",
			Buckets: prometheus.ExponentialBuckets(10, 2, 10),
		}, []string{"stage"}),
		ownershipCheckFailures: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_compact_worker_ownership_check_failures_total",
			Help: "Total number of times a worker discarded its work because it could not confirm ownership.",
		}, []string{"reason"}),
	}
}

// TaskClient is how a worker talks to the manager.
type TaskClient interface {
	Lease(ctx context.Context, req LeaseRequest) (*Task, error)
	Heartbeat(ctx context.Context, req HeartbeatRequest) (HeartbeatResponse, error)
	Report(ctx context.Context, res Result) error
}

// Worker leases one task at a time from the manager and executes it. Scale the
// throughput of a shard by running more workers, not by making one worker do
// more at once: a worker holds exactly one lease.
type Worker struct {
	logger log.Logger
	bkt    objstore.Bucket
	client TaskClient
	comp   compact.Compactor
	conf   WorkerConfig
	m      *workerMetrics
}

// NewWorker returns a worker ready to be run.
func NewWorker(logger log.Logger, bkt objstore.Bucket, client TaskClient, comp compact.Compactor, reg prometheus.Registerer, conf WorkerConfig) (*Worker, error) {
	conf.applyDefaults()
	if conf.JournalID == "" {
		return nil, errors.New("journal ID must be set")
	}
	if conf.DataDir == "" {
		return nil, errors.New("data directory must be set")
	}
	return &Worker{
		logger: logger,
		bkt:    bkt,
		client: client,
		comp:   comp,
		conf:   conf,
		m:      newWorkerMetrics(reg),
	}, nil
}

// Run leases and executes tasks until the context is canceled.
func (w *Worker) Run(ctx context.Context) error {
	level.Info(w.logger).Log("msg", "compaction worker started", "worker_id", w.conf.WorkerID, "journal_id", w.conf.JournalID)

	for ctx.Err() == nil {
		task, err := w.client.Lease(ctx, LeaseRequest{
			WorkerID:           w.conf.WorkerID,
			Accepts:            []TaskType{TaskCompaction, TaskDownsample},
			JournalID:          w.conf.JournalID,
			DedupFunc:          w.conf.DedupFunc,
			DedupReplicaLabels: w.conf.DedupReplicaLabels,
		})
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			level.Warn(w.logger).Log("msg", "could not lease a task", "err", err)
			if err := sleep(ctx, w.conf.PollInterval); err != nil {
				break
			}
			continue
		}
		if task == nil {
			if err := sleep(ctx, w.conf.PollInterval); err != nil {
				break
			}
			continue
		}

		w.runTask(ctx, *task)
	}
	return ctx.Err()
}

// runTask executes one leased task and reports the outcome.
func (w *Worker) runTask(ctx context.Context, task Task) {
	level.Info(w.logger).Log("msg", "leased task", "task", task.ID, "type", task.Type,
		"group", task.Group.Key, "blocks", len(task.SourceBlocks))

	taskCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Heartbeat until the task is done. A heartbeat the manager refuses means the
	// task was taken away, so stop working on it immediately.
	acknowledged := newAtomicBool(true)
	go w.heartbeat(taskCtx, task, acknowledged, cancel)

	start := time.Now()
	res := w.execute(taskCtx, task, acknowledged)
	w.m.stageDuration.WithLabelValues("task").Observe(time.Since(start).Seconds())
	w.m.tasksTotal.WithLabelValues(string(res.Outcome)).Inc()

	level.Info(w.logger).Log("msg", "finished task", "task", task.ID, "outcome", res.Outcome,
		"blocks", len(res.OutputBlocks), "duration", time.Since(start))

	// Report on a context that is not tied to the task, so a canceled task still
	// tells the manager what happened.
	reportCtx, reportCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer reportCancel()
	if err := reportWithRetry(reportCtx, w.logger, w.client, res); err != nil {
		// The manager will time the lease out and hand the task to somebody else.
		// Duplicate results are reconciled by block deduplication in the bucket.
		level.Warn(w.logger).Log("msg", "could not report the result; the manager will retry the task", "task", task.ID, "err", err)
	}
}

// reportWithRetry delivers a result to the manager, retrying with backoff. A
// completed task may represent hours of work, and losing its report to one
// network blip means all of it is executed again after the lease expires, so a
// few retries here are cheap insurance. Giving up remains safe: the manager
// requeues the task, and a duplicate result is reconciled by block
// deduplication in the bucket.
func reportWithRetry(ctx context.Context, logger log.Logger, client TaskClient, res Result) error {
	return reportWithBackoff(ctx, logger, client, res, 5*time.Second)
}

func reportWithBackoff(ctx context.Context, logger log.Logger, client TaskClient, res Result, initial time.Duration) error {
	backoff := initial

	var err error
	for {
		if err = client.Report(ctx, res); err == nil {
			return nil
		}
		level.Warn(logger).Log("msg", "could not report the result; retrying", "task", res.TaskID, "err", err)

		if sleepErr := sleep(ctx, backoff); sleepErr != nil {
			return err
		}
		if backoff < time.Minute {
			backoff *= 2
		}
	}
}

func (w *Worker) heartbeat(ctx context.Context, task Task, acknowledged *atomicBool, cancel context.CancelFunc) {
	ticker := time.NewTicker(w.conf.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		resp, err := w.client.Heartbeat(ctx, HeartbeatRequest{
			TaskID:     task.ID,
			LeaseToken: task.LeaseToken,
			Generation: task.Generation,
		})
		if err != nil {
			level.Warn(w.logger).Log("msg", "heartbeat failed", "task", task.ID, "err", err)
			continue
		}
		if !resp.Acknowledged {
			level.Warn(w.logger).Log("msg", "the manager no longer recognizes our lease; abandoning the task", "task", task.ID)
			acknowledged.set(false)
			cancel()
			return
		}
	}
}

// execute does the actual work for a task.
func (w *Worker) execute(ctx context.Context, task Task, acknowledged *atomicBool) Result {
	res := Result{
		TaskID:     task.ID,
		LeaseToken: task.LeaseToken,
		Generation: task.Generation,
	}

	if task.Type != TaskCompaction && task.Type != TaskDownsample {
		res.Outcome = OutcomeFailedRetryable
		res.ErrorMessage = "unsupported task type " + string(task.Type)
		return res
	}

	dir := filepath.Join(w.conf.DataDir, task.ID)
	if err := os.MkdirAll(dir, 0750); err != nil {
		res.Outcome = OutcomeFailedRetryable
		res.ErrorMessage = err.Error()
		return res
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			level.Warn(w.logger).Log("msg", "could not clean up the task work directory", "dir", dir, "err", err)
		}
	}()

	cg, toCompact, err := w.rebuildGroup(ctx, task)
	if err != nil {
		return w.triageExecutionError(ctx, res, err, "", acknowledged)
	}

	// The ownership check runs as late as possible, right before each result
	// block becomes visible, and fails closed.
	var aborted Outcome
	ownershipGate := func(ctx context.Context) error {
		if !acknowledged.get() {
			w.m.ownershipCheckFailures.WithLabelValues("lease_not_acknowledged").Inc()
			aborted = OutcomeAbortedOwnershipLost
			return errors.New("the manager no longer acknowledges our lease")
		}

		status, err := CheckOwnership(ctx, w.bkt, w.conf.JournalID, task.ID, task.LeaseToken, task.Generation)
		switch status {
		case OwnershipConfirmed:
			return nil
		case OwnershipLost:
			w.m.ownershipCheckFailures.WithLabelValues("lost").Inc()
			aborted = OutcomeAbortedOwnershipLost
			return errors.Wrap(err, "we no longer own this task")
		default:
			// The journal could not be read, so whether we still own the task is
			// unknown. Discard the work rather than risk uploading a block a
			// second worker is also uploading, and say so distinctly, because
			// this is a storage problem and not a lost race.
			w.m.ownershipCheckFailures.WithLabelValues("store_unreachable").Inc()
			aborted = OutcomeAbortedStoreUnreachable
			return errors.Wrap(err, "could not reach the journal to confirm ownership")
		}
	}

	if task.Type == TaskDownsample {
		outIDs, err := w.executeDownsample(ctx, task, dir, ownershipGate)
		if err != nil {
			return w.triageExecutionError(ctx, res, err, aborted, acknowledged)
		}
		res.Outcome = OutcomeCompleted
		res.OutputChecksums = map[string]string{}
		for _, id := range outIDs {
			res.OutputBlocks = append(res.OutputBlocks, id.String())
			if sum, err := metaChecksum(ctx, w.bkt, id); err == nil {
				res.OutputChecksums[id.String()] = sum
			} else if ctx.Err() != nil {
				return w.triageExecutionError(ctx, res, err, "", acknowledged)
			}
		}
		return res
	}

	executor := compact.LocalPlanExecutor{
		Comp:                  w.comp,
		BlockDeletableChecker: compact.DefaultBlockDeletableChecker{},
		Callback:              compact.DefaultCompactionLifecycleCallback{},
		// Only the manager touches source blocks.
		MarkSourcesForDeletion: false,
		PreUploadCheck: func(ctx context.Context, _ *compact.Group, compIDs []ulid.ULID) error {
			if err := w.stampOutputs(task, dir, compIDs); err != nil {
				return err
			}
			return ownershipGate(ctx)
		},
	}

	compIDs, err := executor.Execute(ctx, dir, cg, toCompact, task.OverlappingBlocks)
	if err != nil {
		return w.triageExecutionError(ctx, res, err, aborted, acknowledged)
	}

	res.Outcome = OutcomeCompleted
	res.OutputChecksums = map[string]string{}
	for _, id := range compIDs {
		res.OutputBlocks = append(res.OutputBlocks, id.String())
		sum, err := metaChecksum(ctx, w.bkt, id)
		if err != nil {
			if ctx.Err() != nil {
				return w.triageExecutionError(ctx, res, err, "", acknowledged)
			}
			level.Warn(w.logger).Log("msg", "could not checksum the result block metadata", "block", id, "err", err)
			continue
		}
		res.OutputChecksums[id.String()] = sum
	}
	return res
}

// triageExecutionError separates lifecycle cancellation from errors in the
// data. TSDB compaction can wrap context cancellation as a halt error.
func (w *Worker) triageExecutionError(ctx context.Context, res Result, err error, aborted Outcome, acknowledged *atomicBool) Result {
	res.ErrorMessage = err.Error()
	switch {
	case !acknowledged.get():
		res.Outcome = OutcomeAbortedOwnershipLost
	case ctx.Err() != nil:
		res.Outcome = OutcomeAbortedWorkerShutdown
	case aborted != "":
		res.Outcome = aborted
	default:
		res.Outcome, res.OffendingBlock = ClassifyError(err)
	}
	return res
}

// rebuildGroup reconstructs the compaction group from the task, reading the
// metadata of the source blocks from the bucket rather than trusting what came
// over the wire.
func (w *Worker) rebuildGroup(ctx context.Context, task Task) (*compact.Group, []*metadata.Meta, error) {
	noopCounter := func() prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{
			Name: "thanos_compact_worker_noop_total",
			Help: "Never exposed; the group needs counters and the worker's live elsewhere.",
		})
	}

	var extensions any
	if len(task.Group.Extensions) > 0 {
		if err := json.Unmarshal(task.Group.Extensions, &extensions); err != nil {
			return nil, nil, errors.Wrap(err, "unmarshal group extensions")
		}
	}

	cg, err := compact.NewGroup(
		w.logger,
		w.bkt,
		task.Group.Key,
		labels.FromMap(task.Group.Labels),
		task.Group.Resolution,
		task.Group.AcceptMalformedIndex,
		task.Group.EnableVerticalCompaction,
		noopCounter(), noopCounter(), noopCounter(), noopCounter(),
		noopCounter(), noopCounter(), noopCounter(), noopCounter(),
		metadata.HashFunc(task.Group.HashFunc),
		task.Group.BlockFilesConcurrency,
		task.Group.CompactBlocksFetchConcurrency,
	)
	if err != nil {
		return nil, nil, errors.Wrap(err, "rebuild compaction group")
	}

	// The result block inherits the group's extensions, so stamping them here
	// is what records on the block which task produced it.
	stamped, err := w.provenance(task).Stamp(extensions)
	if err != nil {
		return nil, nil, err
	}
	cg.SetExtensions(stamped)

	metas := make([]*metadata.Meta, 0, len(task.SourceBlocks))
	for _, raw := range task.SourceBlocks {
		id, err := ulid.Parse(raw)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "parse source block ID %q", raw)
		}
		meta, err := block.DownloadMeta(ctx, w.logger, w.bkt, id)
		if err != nil {
			return nil, nil, compact.NewRetryError(errors.Wrapf(err, "read metadata of source block %s", id))
		}
		m := meta
		stripDedupReplicaLabels(&m, task.Group.DedupReplicaLabels)
		if err := cg.AppendMeta(&m); err != nil {
			return nil, nil, errors.Wrapf(err, "add source block %s to the group", id)
		}
		metas = append(metas, &m)
	}

	if len(metas) == 0 {
		return nil, nil, errors.New("task has no source blocks")
	}

	// Sanity check what we fetched against what the manager planned.
	minTime, maxTime := metas[0].MinTime, metas[0].MaxTime
	for _, m := range metas {
		if m.MinTime < minTime {
			minTime = m.MinTime
		}
		if m.MaxTime > maxTime {
			maxTime = m.MaxTime
		}
	}
	if minTime != task.ExpectedMinTime || maxTime != task.ExpectedMaxTime {
		return nil, nil, errors.Errorf(
			"source blocks span [%d, %d] but the task was planned for [%d, %d]",
			minTime, maxTime, task.ExpectedMinTime, task.ExpectedMaxTime)
	}

	return cg, metas, nil
}

// stripDedupReplicaLabels removes the manager's deduplication replica labels
// from fetched block metadata, mirroring what ReplicaLabelRemover did on the
// manager before grouping - including the placeholder it leaves when a block
// has no labels left. Without this, blocks of a deduplication deployment could
// never validate against their group, whose labels are already stripped.
func stripDedupReplicaLabels(m *metadata.Meta, replicaLabels []string) {
	if len(replicaLabels) == 0 {
		return
	}
	stripped := make(map[string]string, len(m.Thanos.Labels))
	for k, v := range m.Thanos.Labels {
		stripped[k] = v
	}
	for _, l := range replicaLabels {
		delete(stripped, l)
	}
	if len(stripped) == 0 {
		stripped[replicaLabels[0]] = "deduped"
	}
	m.Thanos.Labels = stripped
}

// metaChecksum returns the checksum of a block's meta.json as it is in the
// bucket, so the manager can verify what the worker claims it uploaded.
func metaChecksum(ctx context.Context, bkt objstore.Bucket, id ulid.ULID) (string, error) {
	r, err := bkt.Get(ctx, filepath.Join(id.String(), block.MetaFilename))
	if err != nil {
		return "", err
	}
	defer runutil.CloseWithLogOnErr(log.NewNopLogger(), r, "close meta reader")

	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// provenance is the record of this task every block it produces carries,
// before it is completed for a block with For.
func (w *Worker) provenance(task Task) Provenance {
	return Provenance{
		TaskID:     task.ID,
		TaskType:   task.Type,
		WorkerID:   w.conf.WorkerID,
		JournalID:  w.conf.JournalID,
		Generation: task.Generation,
	}
}

// stampOutputs completes the provenance of the result blocks of a compaction
// on disk, right before they are uploaded. Until now the blocks carried the
// group's stamp, which does not name a block: the block's ID is only known
// once the block exists.
func (w *Worker) stampOutputs(task Task, dir string, compIDs []ulid.ULID) error {
	for _, id := range compIDs {
		bdir := filepath.Join(dir, id.String())
		m, err := metadata.ReadFromDir(bdir)
		if err != nil {
			return errors.Wrapf(err, "read metadata of result block %s", id)
		}
		if _, ok := ProvenanceOf(m); ok {
			// Already completed on an earlier pass: the check runs before
			// every upload, with every result block.
			continue
		}
		m.Thanos.Extensions, err = w.provenance(task).For(id, task.SourceBlocks).Stamp(m.Thanos.Extensions)
		if err != nil {
			return errors.Wrapf(err, "stamp result block %s", id)
		}
		if err := m.WriteToDir(w.logger, bdir); err != nil {
			return errors.Wrapf(err, "write metadata of result block %s", id)
		}
	}
	return nil
}
