// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"cmp"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/runutil"
)

// ManagerConfig configures the scheduling side of a distributed compactor.
type ManagerConfig struct {
	// JournalID identifies this shard's journal in the bucket. Two managers
	// working on different shards must not share one.
	JournalID string
	// SelectorHash fingerprints the manager's selector relabel config.
	SelectorHash string

	// DedupFunc and DedupReplicaLabels mirror --deduplication.func and
	// --deduplication.replica-label. Workers state theirs when they ask for
	// work, and a mismatch is refused: both settings shape the produced blocks
	// without leaving a trace in them, so nothing downstream would catch it.
	DedupFunc          string
	DedupReplicaLabels []string

	// MaxTaskSeries and MaxTaskIndexBytes bound how big a task the manager is
	// willing to hand out, in what the source blocks report about themselves.
	// A plan over either limit is recorded as oversized and never dispatched;
	// zero means unlimited.
	MaxTaskSeries     uint64
	MaxTaskIndexBytes int64

	// LeaseTTL is how long a lease survives without a heartbeat.
	LeaseTTL time.Duration
	// MaxAttempts bounds how often a task is retried before it is given up on.
	MaxAttempts int
	// JournalRetention is how long terminal tasks are kept in the journal.
	JournalRetention time.Duration
	// JournalUnavailableTimeout bounds how long the manager keeps going while it
	// cannot write the journal, before it halts.
	JournalUnavailableTimeout time.Duration
}

func (c *ManagerConfig) applyDefaults() {
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = 5 * time.Minute
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 3
	}
	if c.JournalRetention <= 0 {
		c.JournalRetention = 24 * time.Hour
	}
	if c.JournalUnavailableTimeout <= 0 {
		c.JournalUnavailableTimeout = 15 * time.Minute
	}
}

type managerMetrics struct {
	tasksTotal        *prometheus.CounterVec
	tasksInFlight     *prometheus.GaugeVec
	taskDuration      prometheus.Histogram
	taskAttempts      prometheus.Histogram
	leaseExpirations  prometheus.Counter
	abandonedTasks    prometheus.Counter
	oversizedTasks    prometheus.Counter
	parkedTasks       prometheus.Gauge
	journalWrites     prometheus.Counter
	journalWriteFails prometheus.Counter
	journalGeneration prometheus.Gauge
	pendingTasks      prometheus.Gauge
	oldestPendingSecs prometheus.Gauge
	connectedWorkers  prometheus.Gauge
}

func newManagerMetrics(reg prometheus.Registerer) *managerMetrics {
	factory := promauto.With(reg)
	return &managerMetrics{
		tasksTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_compact_manager_tasks_total",
			Help: "Total number of tasks that reached a terminal state, by type and outcome.",
		}, []string{"type", "outcome"}),
		tasksInFlight: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "thanos_compact_manager_tasks_in_flight",
			Help: "Number of tasks currently leased by a worker, by type.",
		}, []string{"type"}),
		taskDuration: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "thanos_compact_manager_task_duration_seconds",
			Help:    "Time from a task being leased until it reached a terminal state.",
			Buckets: prometheus.ExponentialBuckets(30, 2, 10),
		}),
		taskAttempts: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "thanos_compact_manager_task_attempts",
			Help:    "Number of attempts a task needed before reaching a terminal state.",
			Buckets: prometheus.LinearBuckets(1, 1, 6),
		}),
		leaseExpirations: factory.NewCounter(prometheus.CounterOpts{
			Name: "thanos_compact_manager_lease_expirations_total",
			Help: "Total number of leases that expired without the worker reporting.",
		}),
		abandonedTasks: factory.NewCounter(prometheus.CounterOpts{
			Name: "thanos_compact_manager_abandoned_tasks_total",
			Help: "Total number of tasks given up on after repeatedly losing their worker.",
		}),
		oversizedTasks: factory.NewCounter(prometheus.CounterOpts{
			Name: "thanos_compact_manager_oversized_tasks_total",
			Help: "Total number of tasks refused because they exceed the configured worker capacity.",
		}),
		parkedTasks: factory.NewGauge(prometheus.GaugeOpts{
			Name: "thanos_compact_manager_parked_tasks",
			Help: "Number of tasks whose source blocks are withheld from planning until an operator intervenes.",
		}),
		journalWrites: factory.NewCounter(prometheus.CounterOpts{
			Name: "thanos_compact_manager_journal_writes_total",
			Help: "Total number of journal writes.",
		}),
		journalWriteFails: factory.NewCounter(prometheus.CounterOpts{
			Name: "thanos_compact_manager_journal_write_failures_total",
			Help: "Total number of failed journal writes.",
		}),
		journalGeneration: factory.NewGauge(prometheus.GaugeOpts{
			Name: "thanos_compact_manager_journal_generation",
			Help: "Generation of the journal this manager owns.",
		}),
		pendingTasks: factory.NewGauge(prometheus.GaugeOpts{
			Name: "thanos_compact_manager_pending_tasks",
			Help: "Number of tasks waiting for a worker.",
		}),
		oldestPendingSecs: factory.NewGauge(prometheus.GaugeOpts{
			Name: "thanos_compact_manager_oldest_pending_task_seconds",
			Help: "Age of the oldest task waiting for a worker.",
		}),
		connectedWorkers: factory.NewGauge(prometheus.GaugeOpts{
			Name: "thanos_compact_manager_connected_workers",
			Help: "Number of workers that leased or heartbeated within the lease TTL.",
		}),
	}
}

// pendingTask is a task waiting for, or held by, a worker, plus the channel the
// executor blocks on.
type pendingTask struct {
	entry  *TaskEntry
	result chan Result

	leasedAt  time.Time
	queuedAt  time.Time
	notBefore time.Time
}

// Scheduler owns the task queue and the leases. It is the only writer of the
// journal.
type Scheduler struct {
	// lifetime is the context the manager runs under. Once it is done the
	// manager has stopped, and the scheduler writes nothing more to the
	// bucket: a successor may already own the journal.
	lifetime context.Context

	logger log.Logger
	bkt    objstore.Bucket
	conf   ManagerConfig
	m      *managerMetrics

	// ownerID identifies this manager instance in the journal, so that another
	// manager writing the same journal is detected even when both hold the same
	// generation.
	ownerID string

	mtx     sync.Mutex
	journal *Journal
	tasks   map[string]*pendingTask
	// verifying holds the tasks whose completed result this manager is
	// verifying right now, from the report to the verdict. Maintenance never
	// touches their outputs: only a completed task nobody in this process is
	// verifying - one reported before a restart - has its outputs rejected.
	verifying  map[string]struct{}
	queue      []string
	workerSeen map[string]time.Time
	// halted is the error the manager's control loop halted on, once it has.
	// A halted scheduler hands out nothing and accepts nothing: the shard is
	// frozen for investigation, as a standalone compactor would be.
	halted error

	// persistSeq and lastPersist are protected by mtx.
	persistSeq  uint64
	lastPersist time.Time
	// Bucket I/O is serialized separately, without holding mtx. These two
	// persistence bookkeeping fields are protected by persistMtx.
	persistMtx              sync.Mutex
	persistedSeq            uint64
	journalUnavailableSince time.Time
}

// NewScheduler takes ownership of the shard's journal, bumping its generation so
// that leases handed out by a previous manager are void, and returns a scheduler
// ready to hand tasks to workers. The context is the manager's lifetime: once
// it is done, the scheduler writes nothing more to the bucket.
func NewScheduler(ctx context.Context, logger log.Logger, bkt objstore.Bucket, reg prometheus.Registerer, conf ManagerConfig) (*Scheduler, error) {
	conf.applyDefaults()
	if conf.JournalID == "" {
		return nil, errors.New("journal ID must be set")
	}

	j, err := ReadJournal(ctx, bkt, conf.JournalID)
	if err != nil {
		return nil, errors.Wrap(err, "read journal")
	}
	if j == nil {
		j = NewJournal(conf.JournalID, conf.SelectorHash)
	}
	if j.SelectorHash != "" && conf.SelectorHash != "" && j.SelectorHash != conf.SelectorHash {
		// Sharing a journal between differently sharded managers means two
		// writers, which the journal cannot protect against.
		level.Warn(logger).Log("msg", "journal was written by a manager with a different selector relabel config; "+
			"make sure no other compact manager uses this journal ID",
			"journalID", conf.JournalID, "journalSelector", j.SelectorHash, "ourSelector", conf.SelectorHash)
	}
	j.SelectorHash = conf.SelectorHash

	ownerID, err := randomToken()
	if err != nil {
		return nil, err
	}

	// Taking ownership voids every lease from an older generation, and ends
	// every task that never finished. Planning is idempotent, so this manager
	// will replan any work that was in flight, and a worker still executing an
	// old task fails its ownership check and discards the work - unless it
	// passed the check just before the takeover and is still uploading. Its
	// blocks were never verified, so the task stays in the journal as a failed
	// tombstone: a block stamped with it is unpublished, maintenance deletes
	// it once the manager's deduplication filter comes across it, and the
	// entry ages out with the retention like any other finished task.
	j.Generation++
	j.Owner = ownerID
	now := time.Now()
	for _, e := range j.Tasks {
		if e.State.Terminal() {
			continue
		}
		e.State = StateFailed
		e.Lease = nil
		e.Outputs, e.OutputChecksums = nil, nil
		e.LastError = &TaskError{Outcome: OutcomeAbortedOwnershipLost, Message: fmt.Sprintf("unfinished when generation %d took over the journal", j.Generation)}
		e.UpdatedAt = now
	}
	j.Prune(conf.JournalRetention, now)

	if err := WriteJournal(ctx, bkt, j); err != nil {
		return nil, errors.Wrap(err, "take ownership of journal")
	}

	s := &Scheduler{
		lifetime:    ctx,
		logger:      logger,
		bkt:         bkt,
		conf:        conf,
		m:           newManagerMetrics(reg),
		ownerID:     ownerID,
		journal:     j,
		tasks:       map[string]*pendingTask{},
		workerSeen:  map[string]time.Time{},
		verifying:   map[string]struct{}{},
		lastPersist: time.Now(),
	}
	s.m.journalGeneration.Set(float64(j.Generation))

	level.Info(logger).Log("msg", "took ownership of compaction journal", "journalID", conf.JournalID, "generation", j.Generation)
	return s, nil
}

// Generation returns the journal generation this scheduler owns.
func (s *Scheduler) Generation() uint64 {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.journal.Generation
}

// Submit queues a task and returns a channel that receives its terminal result.
func (s *Scheduler) Submit(ctx context.Context, task Task) (<-chan Result, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.halted != nil {
		return nil, compact.NewHaltError(errors.Wrap(s.halted, "manager is halted"))
	}

	task.Generation = s.journal.Generation
	task.LeaseTTL = s.conf.LeaseTTL
	task.Group.DedupFunc = s.conf.DedupFunc

	now := time.Now()
	entry := &TaskEntry{
		Task:      task,
		State:     StatePending,
		CreatedAt: now,
		UpdatedAt: now,
	}
	pending := &pendingTask{
		entry:    entry,
		result:   make(chan Result, 1),
		queuedAt: now,
	}
	s.journal.Tasks[task.ID] = entry
	s.tasks[task.ID] = pending
	s.enqueueLocked(task.ID)

	// The state lock is released while the journal is written, so by the time
	// the write returns, the task may have been leased, and other tasks may
	// have been queued behind or removed ahead of it. Nothing about the queue
	// or the task can be assumed from before the write.
	if err := s.persistLocked(ctx); err != nil {
		if compact.IsHaltError(err) {
			return nil, err
		}
		if s.tasks[task.ID] != pending || entry.State != StatePending {
			// The task went out to a worker while the write was in flight,
			// and every journal write from here on carries it. What the
			// failed write could not record was the task waiting, which it
			// no longer is; the submitter has a live task to wait for.
			level.Warn(s.logger).Log("msg", "journal write failed while the task was being leased; the task stays", "task", task.ID, "err", err)
			s.updateQueueMetricsLocked()
			return pending.result, nil
		}
		delete(s.journal.Tasks, task.ID)
		delete(s.tasks, task.ID)
		if i := slices.Index(s.queue, task.ID); i >= 0 {
			s.queue = slices.Delete(s.queue, i, i+1)
		}
		// A journal blip inside the tolerance window must surface as
		// retryable: a plain error would fall through the compactor's wait
		// loop and exit the whole manager process, while the same failure in
		// Report or Maintain is tolerated by design.
		return nil, compact.NewRetryError(err)
	}
	s.updateQueueMetricsLocked()
	return pending.result, nil
}

// Lease hands a queued task to a worker, if there is one it accepts.
func (s *Scheduler) Lease(ctx context.Context, req LeaseRequest) (*Task, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.halted != nil {
		// Nothing to hand out while the shard is frozen; the workers idle
		// until an operator restarts the manager.
		s.workerSeen[req.WorkerID] = time.Now()
		return nil, nil
	}

	if req.JournalID != "" && req.JournalID != s.conf.JournalID {
		// A worker on the wrong journal would fail its ownership check against
		// that other journal on every task, abort, and be handed the next one:
		// an invisible livelock. Refuse loudly instead; this is an operator
		// misconfiguration.
		return nil, errors.Errorf(
			"worker %s is configured for journal %q but this manager schedules journal %q; "+
				"--compact.manager.journal-id has to match on both", req.WorkerID, req.JournalID, s.conf.JournalID)
	}
	if req.DedupFunc != s.conf.DedupFunc {
		// The merge function is baked into the worker's compactor and leaves
		// no trace in the blocks it produces, so a mismatch would go unnoticed
		// while the workers merge the sources differently than planned.
		return nil, errors.Errorf(
			"worker %s deduplicates with %q but this manager plans for %q; "+
				"--deduplication.func has to match on both", req.WorkerID, describeDedupFunc(req.DedupFunc), describeDedupFunc(s.conf.DedupFunc))
	}
	if !sameSet(req.DedupReplicaLabels, s.conf.DedupReplicaLabels) {
		return nil, errors.Errorf(
			"worker %s removes replica labels %v but this manager removes %v; "+
				"--deduplication.replica-label has to match on both", req.WorkerID, req.DedupReplicaLabels, s.conf.DedupReplicaLabels)
	}

	s.expireLeasesLocked()
	now := time.Now()
	s.workerSeen[req.WorkerID] = now

	accepts := make(map[TaskType]bool, len(req.Accepts))
	for _, t := range req.Accepts {
		accepts[t] = true
	}

	for _, id := range s.queue {
		p, ok := s.tasks[id]
		if !ok || p.entry.State != StatePending {
			continue
		}
		if len(accepts) > 0 && !accepts[p.entry.Task.Type] {
			continue
		}
		if now.Before(p.notBefore) {
			continue
		}

		token, err := randomToken()
		if err != nil {
			return nil, err
		}
		leasedAt := time.Now()
		p.entry.State = StateLeased
		p.entry.Attempts++
		p.entry.Lease = &Lease{
			WorkerID:   req.WorkerID,
			Token:      token,
			Generation: s.journal.Generation,
			ExpiresAt:  leasedAt.Add(s.conf.LeaseTTL),
		}
		p.entry.UpdatedAt = leasedAt
		p.leasedAt = leasedAt
		s.m.tasksInFlight.WithLabelValues(string(p.entry.Task.Type)).Inc()
		// Leave the queue before the lock is released: a lease that expires
		// while the journal is being written is requeued by expireLeasesLocked,
		// and that must not find a stale copy of the id still in the queue.
		if qi := slices.Index(s.queue, id); qi >= 0 {
			s.queue = slices.Delete(s.queue, qi, qi+1)
		}

		if err := s.persistLocked(ctx); err != nil {
			if s.tasks[id] == p && p.entry.State == StateLeased && p.entry.Lease != nil && p.entry.Lease.Token == token {
				p.entry.State = StatePending
				p.entry.Attempts--
				p.entry.Lease = nil
				s.m.tasksInFlight.WithLabelValues(string(p.entry.Task.Type)).Dec()
				s.enqueueLocked(id)
			}
			return nil, err
		}
		if s.tasks[id] != p || p.entry.State != StateLeased || p.entry.Lease == nil || p.entry.Lease.Token != token {
			return nil, compact.NewRetryError(errors.New("lease changed while persisting it"))
		}
		s.updateQueueMetricsLocked()

		task := p.entry.Task
		task.LeaseToken = token
		task.Generation = s.journal.Generation
		task.LeaseTTL = s.conf.LeaseTTL
		return &task, nil
	}
	return nil, nil
}

// Heartbeat extends a lease. A worker that is not acknowledged has lost the task
// and must abort without uploading anything.
func (s *Scheduler) Heartbeat(req HeartbeatRequest) HeartbeatResponse {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	p, ok := s.tasks[req.TaskID]
	if !ok || p.entry.State != StateLeased || p.entry.Lease == nil {
		return HeartbeatResponse{Acknowledged: false}
	}
	if p.entry.Lease.Token != req.LeaseToken || p.entry.Lease.Generation != req.Generation {
		return HeartbeatResponse{Acknowledged: false}
	}

	// Extending in memory only: the journal records who owns a task, and that has
	// not changed. Persisting every heartbeat would write the journal constantly.
	now := time.Now()
	p.entry.Lease.ExpiresAt = now.Add(s.conf.LeaseTTL)
	s.workerSeen[p.entry.Lease.WorkerID] = now
	return HeartbeatResponse{Acknowledged: true}
}

// Report records a worker's terminal result and wakes whoever submitted the task.
func (s *Scheduler) Report(ctx context.Context, res Result) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	p, ok := s.tasks[res.TaskID]
	if !ok {
		// The task is gone, most likely because its lease expired and it was
		// already retried. Nothing to do; the block dedup in the bucket makes the
		// duplicate result harmless.
		level.Debug(s.logger).Log("msg", "result reported for unknown task", "task", res.TaskID)
		return nil
	}
	if p.entry.State != StateLeased || p.entry.Lease == nil || p.entry.Lease.Token != res.LeaseToken {
		level.Warn(s.logger).Log("msg", "result reported by a worker that no longer holds the task",
			"task", res.TaskID, "outcome", res.Outcome)
		return nil
	}

	s.m.tasksInFlight.WithLabelValues(string(p.entry.Task.Type)).Dec()
	if !p.leasedAt.IsZero() {
		s.m.taskDuration.Observe(time.Since(p.leasedAt).Seconds())
	}

	now := time.Now()
	p.entry.UpdatedAt = now
	p.entry.Lease = nil

	switch {
	case res.Outcome == OutcomeCompleted:
		p.entry.State = StateCompleted
		p.entry.Outputs = res.OutputBlocks
		p.entry.OutputChecksums = res.OutputChecksums
		// The result is delivered to the plan waiting for it, which verifies
		// it next; until it says so the outputs are neither published nor
		// anybody else's to clean up.
		s.verifying[res.TaskID] = struct{}{}
	case res.Outcome == OutcomeAbortedWorkerShutdown:
		// The operator stopped the worker; the task itself did nothing wrong.
		// It goes straight back to the queue: neither the attempt nor the
		// abort budget is charged, and there is nothing to back off from. A
		// fleet that never lives long enough to finish a task is still caught
		// by lease expiry, which does charge attempts.
		p.entry.Attempts--
		p.entry.LastError = &TaskError{Outcome: res.Outcome, Message: res.ErrorMessage}
		p.entry.State = StatePending
		s.enqueueLocked(res.TaskID)
	case res.Outcome.Aborted():
		// The worker threw its work away, so this is not a failed attempt.
		p.entry.Attempts--
		p.entry.Aborts++
		p.entry.LastError = &TaskError{Outcome: res.Outcome, Message: res.ErrorMessage}
		if p.entry.Aborts >= abortCapFor(s.conf.MaxAttempts) {
			// Every abort is benign in isolation, but this many over the task's
			// lifetime means something structural - a journal the workers
			// cannot read, a lease that keeps being lost - and requeueing would
			// retry forever while the submitter waits with no timeout. Fail the
			// task so the pass completes and the pattern becomes visible.
			p.entry.State = StateAbandoned
			s.m.abandonedTasks.Inc()
			res = Result{
				TaskID:     res.TaskID,
				LeaseToken: res.LeaseToken,
				Generation: res.Generation,
				Outcome:    OutcomeFailedRetryable,
				ErrorMessage: errors.Errorf("task was aborted %d times without completing (last abort: %s: %s); "+
					"investigate why workers keep discarding it", p.entry.Aborts, p.entry.LastError.Outcome, p.entry.LastError.Message).Error(),
			}
			p.entry.LastError = &TaskError{Outcome: res.Outcome, Message: res.ErrorMessage}
		} else {
			p.entry.State = StatePending
			// Back off before handing it out again, so a task that is aborted
			// the moment it is leased does not churn through the fleet.
			p.notBefore = now.Add(abortRequeueBackoff(p.entry.Aborts, s.conf.LeaseTTL))
			s.enqueueLocked(res.TaskID)
		}
	case res.Outcome == OutcomeFailedRetryable:
		p.entry.LastError = &TaskError{Outcome: res.Outcome, Block: res.OffendingBlock, Message: res.ErrorMessage}
		if p.entry.Attempts >= s.conf.MaxAttempts {
			p.entry.State = StateFailed
		} else {
			p.entry.State = StatePending
			s.enqueueLocked(res.TaskID)
		}
	default:
		// Halt, issue347 and out-of-order-chunks are not transient: each demands
		// a specific reaction from the compactor's control loop, and retrying
		// them here would repeat a potentially hours-long compaction just to
		// reach the same conclusion. Deliver them immediately.
		p.entry.LastError = &TaskError{Outcome: res.Outcome, Block: res.OffendingBlock, Message: res.ErrorMessage}
		p.entry.State = StateFailed
	}

	// The journal is written best effort here: the submitter must learn the
	// outcome even when the journal cannot be written, because the bucket, not
	// the journal, is the source of truth for what a completed task produced.
	// Blocking the result on the journal would wedge the submitter forever - a
	// re-report cannot reach it, since the task is no longer leased. A stale
	// journal entry merely means this manager's successor drops it at takeover
	// and replans.
	terminal := p.entry.State.Terminal()
	persistErr := s.persistLocked(ctx)
	s.updateQueueMetricsLocked()

	if persistErr != nil && compact.IsHaltError(persistErr) {
		// Another manager owns the journal now. The worker's outcome no longer
		// matters: what the submitter has to learn is the halt, or this
		// manager's control loop would verify the result and keep compacting a
		// shard somebody else manages. Only ordinary write failures get the
		// best-effort delivery above.
		//
		// The lock was released during the persist: the task may have been
		// re-leased or finished meanwhile, so only touch it if it is still the
		// entry this report was for, and give back the in-flight slot a new
		// lease took.
		if s.tasks[res.TaskID] == p {
			if p.entry.State == StateLeased {
				s.m.tasksInFlight.WithLabelValues(string(p.entry.Task.Type)).Dec()
			}
			p.entry.State = StateFailed
			p.entry.Lease = nil
			s.m.tasksTotal.WithLabelValues(string(p.entry.Task.Type), string(OutcomeFailedHalt)).Inc()
		}
		s.finishLocked(res.TaskID, Result{
			TaskID:       res.TaskID,
			Outcome:      OutcomeFailedHalt,
			ErrorMessage: persistErr.Error(),
		})
		return persistErr
	}

	if terminal {
		s.m.tasksTotal.WithLabelValues(string(p.entry.Task.Type), string(res.Outcome)).Inc()
		s.m.taskAttempts.Observe(float64(p.entry.Attempts))
		s.finishLocked(res.TaskID, res)
	}
	return persistErr
}

// MarkOversized records a task the manager refuses to dispatch because its
// expected size exceeds the configured worker capacity. The entry is terminal
// and never queued: it exists so the refusal is visible in the journal and in
// metrics rather than inferred from workers dying, and so SourcesParked keeps
// the same plan from being re-refused every pass.
func (s *Scheduler) MarkOversized(task Task, reason string) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	now := time.Now()
	task.Generation = s.journal.Generation
	s.journal.Tasks[task.ID] = &TaskEntry{
		Task:      task,
		State:     StateOversized,
		LastError: &TaskError{Outcome: OutcomeOversized, Message: reason},
		CreatedAt: now,
		UpdatedAt: now,
	}
	s.m.oversizedTasks.Inc()
	s.m.tasksTotal.WithLabelValues(string(task.Type), string(OutcomeOversized)).Inc()
	s.updateQueueMetricsLocked()

	if err := s.persistLocked(context.Background()); err != nil {
		level.Warn(s.logger).Log("msg", "could not persist an oversized-task entry; the journal catches up on the next write", "err", err)
	}
}

// oversizedReason returns why a task exceeds the limits, or "" when it fits.
//
// The two limits bound different things. The series limit bounds what a
// worker has to build in memory at once, so a plan that names several
// partitioned outputs is judged per output: its sources' series are spread
// over as many blocks as the plan produces, and the worker builds them one
// after another. The index size limit bounds what a worker has to download
// and hold on disk, which is the whole input however many outputs the plan
// names, so it is judged per task.
func oversizedReason(task Task, conf ManagerConfig) string {
	series := task.ExpectedSeries
	perOutput := ""
	if n := uint64(len(task.Outputs)); n > 1 {
		series = series / n
		perOutput = fmt.Sprintf(" per output, over %d outputs,", n)
	}
	switch {
	case conf.MaxTaskSeries > 0 && series > conf.MaxTaskSeries:
		return fmt.Sprintf("task expects %d series%s from %d source blocks, over the configured --compact.manager.max-task-series of %d; "+
			"split the plan, raise worker capacity together with the limit, or no-compact-mark the blocks",
			series, perOutput, len(task.SourceBlocks), conf.MaxTaskSeries)
	case conf.MaxTaskIndexBytes > 0 && task.ExpectedIndexBytes > conf.MaxTaskIndexBytes:
		return fmt.Sprintf("task expects %d bytes of source index from %d source blocks, over the configured --compact.manager.max-task-index-size of %d; "+
			"split the plan, raise worker capacity together with the limit, or no-compact-mark the blocks",
			task.ExpectedIndexBytes, len(task.SourceBlocks), conf.MaxTaskIndexBytes)
	}
	return ""
}

// VerificationDone tells the scheduler that the plan waiting for the task's
// result is done with it - accepted, rejected, or given up on - so that
// maintenance may act on whatever the journal says about its outputs.
func (s *Scheduler) VerificationDone(taskID string) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	delete(s.verifying, taskID)
}

// RejectOutputs records blocks a worker uploaded for the task that failed
// verification. While recorded they are unpublished to the manager's
// deduplication filter, the entry is not pruned, and maintenance retries
// deleting them.
func (s *Scheduler) RejectOutputs(ctx context.Context, taskID string, blocks []ulid.ULID) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	e, ok := s.journal.Tasks[taskID]
	if !ok {
		return errors.Errorf("task %s is not in the journal", taskID)
	}
	for _, b := range blocks {
		if !slices.Contains(e.RejectedOutputs, b.String()) {
			e.RejectedOutputs = append(e.RejectedOutputs, b.String())
		}
	}
	e.UpdatedAt = time.Now()
	return s.persistLocked(ctx)
}

// AcceptOutputs records that the task's outputs passed verification and may
// supersede the blocks they were made from. The verdict counts only once the
// journal holds it: the sources are retired on its strength, and a successor
// finding the task completed but unverified would delete the outputs. When
// the journal cannot be written the verdict is taken back and an error
// returned; the outputs are then rejected like any unverified result, and
// the plan is redone.
func (s *Scheduler) AcceptOutputs(ctx context.Context, taskID string) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	e, ok := s.journal.Tasks[taskID]
	if !ok {
		return errors.Errorf("task %s is not in the journal", taskID)
	}
	e.Verified = true
	e.UpdatedAt = time.Now()
	if err := s.persistLocked(ctx); err != nil {
		if s.journal.Tasks[taskID] == e {
			e.Verified = false
		}
		return err
	}
	return nil
}

// OutputPublished reports whether a block a worker produced for the task may
// supersede the blocks it was made from: the task completed, the manager
// verified its outputs, the block is one of them and was not rejected. A task
// the journal no longer knows - aged out, or another manager's - is not
// judged here; its blocks were verified long ago or are not ours to doubt.
func (s *Scheduler) OutputPublished(taskID, blockID string) bool {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	e, ok := s.journal.Tasks[taskID]
	if !ok {
		return true
	}
	if slices.Contains(e.RejectedOutputs, blockID) {
		return false
	}
	return e.State == StateCompleted && e.Verified && slices.Contains(e.Outputs, blockID)
}

// PublishedFunc is the rule the manager's deduplication filter judges blocks
// by: a block stamped with this journal's provenance is published only when
// OutputPublished says so; every other block is judged by its metadata alone.
func (s *Scheduler) PublishedFunc() func(*metadata.Meta) bool {
	return func(m *metadata.Meta) bool {
		prov, ok := ProvenanceOf(m)
		if !ok || prov.JournalID != s.conf.JournalID {
			return true
		}
		if s.OutputPublished(prov.TaskID, prov.BlockID) {
			return true
		}
		s.noteUnaccounted(prov.TaskID, prov.BlockID)
		return false
	}
}

// noteUnaccounted records a block stamped with a finished task that the task
// does not account for - uploaded by an attempt whose report never arrived,
// or for a task a predecessor left unfinished at takeover - as rejected, so
// that maintenance deletes it. Left alone it would be unpublished only while
// the task's entry lasts: once the retention aged the entry out, nothing
// would tell it from a block verified long ago. A task still running, or one
// whose result is being verified, is left alone.
func (s *Scheduler) noteUnaccounted(taskID, blockID string) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	e, ok := s.journal.Tasks[taskID]
	if !ok || !e.State.Terminal() {
		return
	}
	if _, busy := s.verifying[taskID]; busy {
		return
	}
	// Reported outputs of a result nobody verified are rejected by
	// maintenance as a whole.
	if slices.Contains(e.Outputs, blockID) || slices.Contains(e.RejectedOutputs, blockID) {
		return
	}
	level.Warn(s.logger).Log("msg", "found a block the task it was made for does not account for; deleting it", "task", taskID, "block", blockID, "state", e.State)
	e.RejectedOutputs = append(e.RejectedOutputs, blockID)
	e.UpdatedAt = time.Now()
}

// SourcesParked reports whether any source belongs to an abandoned or oversized
// task. New uploads must not reset a poison plan's retry budget. The operator
// can release the sources with an unpark request or journal retention expiry.
func (s *Scheduler) SourcesParked(sources []string) bool {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	for _, e := range s.journal.Tasks {
		if e.State.Parked() && slices.ContainsFunc(sources, func(id string) bool { return slices.Contains(e.Task.SourceBlocks, id) }) {
			return true
		}
	}
	return false
}

// Halt freezes the shard after the manager's control loop halted on cause:
// every lease is revoked, so the workers holding them fail their next
// heartbeat or ownership check and discard their work; every queued task is
// failed with the halt as its reason; and no task is handed out or accepted
// until the manager is restarted. This mirrors a standalone compactor, which
// stops everything on a halt - a manager that kept feeding its fleet would
// leave a halted shard busy producing outputs nobody verifies.
//
// The revocation is written to the journal so a worker that has already
// stopped heartbeating still sees it at its ownership check before upload.
// Idempotent: a second halt changes nothing.
func (s *Scheduler) Halt(cause error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if s.halted != nil {
		return
	}
	s.halted = cause
	msg := errors.Wrap(cause, "manager halted").Error()

	now := time.Now()
	revoked, failed := 0, 0
	for id, p := range s.tasks {
		if p.entry.State == StateLeased {
			s.m.tasksInFlight.WithLabelValues(string(p.entry.Task.Type)).Dec()
			revoked++
		} else {
			failed++
		}
		p.entry.State = StateFailed
		p.entry.Lease = nil
		p.entry.UpdatedAt = now
		p.entry.LastError = &TaskError{Outcome: OutcomeFailedHalt, Message: msg}
		s.m.tasksTotal.WithLabelValues(string(p.entry.Task.Type), string(OutcomeFailedHalt)).Inc()
		s.finishLocked(id, Result{TaskID: id, Outcome: OutcomeFailedHalt, ErrorMessage: msg})
	}
	s.queue = nil
	s.updateQueueMetricsLocked()
	level.Error(s.logger).Log("msg", "manager halted; leases revoked and the queue failed, workers will idle until the manager is restarted",
		"revokedLeases", revoked, "failedQueued", failed, "cause", cause)

	if err := s.persistLocked(context.Background()); err != nil {
		// Best effort: the leases are void in memory either way, so the
		// workers' heartbeats are refused, and the next takeover rewrites
		// the journal. Only an ownership check racing this write could still
		// pass, and its output is a same-sources duplicate the dedup filter
		// reconciles.
		level.Warn(s.logger).Log("msg", "could not persist the halt to the journal; the leases stay revoked in memory", "err", err)
	}
}

// enqueueLocked appends a task to the queue unless it is already waiting. The
// state lock is released while the journal is written, so a task can be
// requeued by lease expiry while the Lease that took it out is still
// persisting; without this guard the id would sit in the queue twice and the
// second copy would never be removed.
func (s *Scheduler) enqueueLocked(id string) {
	if !slices.Contains(s.queue, id) {
		s.queue = append(s.queue, id)
	}
}

// finishLocked delivers a terminal result to the submitter and forgets the task.
func (s *Scheduler) finishLocked(taskID string, res Result) {
	p, ok := s.tasks[taskID]
	if !ok {
		return
	}
	select {
	case p.result <- res:
	default:
	}
	delete(s.tasks, taskID)
}

// expireLeasesLocked requeues tasks whose worker stopped heartbeating.
func (s *Scheduler) expireLeasesLocked() {
	now := time.Now()
	dirty := false
	for id, p := range s.tasks {
		if p.entry.State != StateLeased || p.entry.Lease == nil {
			continue
		}
		if now.Before(p.entry.Lease.ExpiresAt) {
			continue
		}

		s.m.leaseExpirations.Inc()
		s.m.tasksInFlight.WithLabelValues(string(p.entry.Task.Type)).Dec()
		level.Warn(s.logger).Log("msg", "lease expired without the worker reporting; requeueing task",
			"task", id, "worker", p.entry.Lease.WorkerID, "attempts", p.entry.Attempts)

		p.entry.Lease = nil
		p.entry.UpdatedAt = now
		dirty = true

		// A task that keeps losing its worker without ever reporting is treated as
		// poisonous: retrying it forever would take the whole fleet down with it.
		if p.entry.Attempts >= s.conf.MaxAttempts {
			p.entry.State = StateAbandoned
			p.entry.LastError = &TaskError{
				Outcome: OutcomeAbandoned,
				Message: "task abandoned after repeatedly losing its worker without a report; the source blocks are untouched",
			}
			s.m.abandonedTasks.Inc()
			s.m.tasksTotal.WithLabelValues(string(p.entry.Task.Type), string(OutcomeAbandoned)).Inc()
			level.Error(s.logger).Log("msg", "giving up on task after repeatedly losing its worker; "+
				"the source blocks are untouched, investigate before retrying", "task", id, "attempts", p.entry.Attempts)
			s.finishLocked(id, Result{TaskID: id, Outcome: OutcomeAbandoned, ErrorMessage: p.entry.LastError.Message})
			continue
		}

		p.entry.State = StatePending
		s.enqueueLocked(id)
	}

	if dirty {
		if err := s.persistLocked(context.Background()); err != nil {
			// Best effort: the transition is applied in memory either way, and
			// the journal catches up on the next write. A takeover surfaces
			// again, fatally, on the next regular persist.
			level.Warn(s.logger).Log("msg", "could not persist expired leases; the journal catches up on the next write", "err", err)
		}
	}
}

func (s *Scheduler) updateQueueMetricsLocked() {
	pending := 0
	oldest := time.Time{}
	for _, id := range s.queue {
		p, ok := s.tasks[id]
		if !ok || p.entry.State != StatePending {
			continue
		}
		pending++
		if oldest.IsZero() || p.queuedAt.Before(oldest) {
			oldest = p.queuedAt
		}
	}
	s.m.pendingTasks.Set(float64(pending))

	parked := 0
	for _, e := range s.journal.Tasks {
		if e.State.Parked() {
			parked++
		}
	}
	s.m.parkedTasks.Set(float64(parked))
	oldestPendingSecs := 0.0
	if !oldest.IsZero() {
		oldestPendingSecs = time.Since(oldest).Seconds()
	}
	s.m.oldestPendingSecs.Set(oldestPendingSecs)

	active := 0
	for id, seen := range s.workerSeen {
		if time.Since(seen) > 10*s.conf.LeaseTTL {
			delete(s.workerSeen, id)
			continue
		}
		if time.Since(seen) <= s.conf.LeaseTTL {
			active++
		}
	}
	s.m.connectedWorkers.Set(float64(active))
}

// Maintain expires stale leases, applies unpark requests, ages out terminal
// journal entries, refreshes queue metrics, and keeps the journal's timestamp
// fresh. It is meant to be called periodically.
//
// The journal is otherwise written only when a task changes state, so an idle
// manager would leave it untouched for hours and look exactly like a stopped
// one. Writing it at least once per lease TTL turns its timestamp into a
// liveness signal: a journal not written for a few TTLs belongs to no running
// manager. The rollback tool relies on that before it touches a bucket.
func (s *Scheduler) Maintain() error {
	ctx := s.lifetime

	// Operators release parked tasks by writing a marker per task; read them
	// before taking the state lock, listing the bucket is slow.
	unpark, err := s.unparkRequests(ctx)
	if err != nil {
		level.Warn(s.logger).Log("msg", "could not list unpark requests; retrying on the next tick", "err", err)
	}

	if err := s.maintainLocked(ctx, unpark); err != nil {
		return err
	}
	if err := s.deleteRejectedOutputs(ctx); err != nil {
		return err
	}

	// A marker is only removed once the journal without its entry is in the
	// bucket, otherwise a restart in between would read the entry back and the
	// task would be parked again with nothing left to say it should not be.
	for _, taskID := range unpark {
		if err := s.bkt.Delete(ctx, UnparkPath(s.conf.JournalID, taskID)); err != nil && !s.bkt.IsObjNotFoundErr(err) {
			level.Warn(s.logger).Log("msg", "could not remove an unpark request; it is applied again on the next tick", "task", taskID, "err", err)
		}
	}
	return nil
}

// unparkRequests lists the task IDs an operator asked to release.
func (s *Scheduler) unparkRequests(ctx context.Context) ([]string, error) {
	var ids []string
	err := s.bkt.Iter(ctx, UnparkPrefix(s.conf.JournalID), func(name string) error {
		if id := path.Base(name); id != "." && id != "/" {
			ids = append(ids, id)
		}
		return nil
	})
	return ids, err
}

// rejectUnverifiedLocked rejects the outputs of a task reported completed that
// nobody in this process is verifying - the manager stopped between the
// report and the verdict, and this is its successor, or the verdict could not
// be recorded - since the plan is redone and nobody will account for them. A
// task under verification is left alone whatever the clock says: its verdict
// is what decides. Reports true when the journal changed.
func (s *Scheduler) rejectUnverifiedLocked() bool {
	dirty := false
	for id, e := range s.journal.Tasks {
		if _, busy := s.verifying[id]; busy {
			continue
		}
		if e.State != StateCompleted || e.Verified || len(e.Outputs) == 0 {
			continue
		}
		level.Warn(s.logger).Log("msg", "a completed task was never verified; rejecting its outputs so that the plan is redone", "task", id, "blocks", len(e.Outputs))
		for _, b := range e.Outputs {
			if !slices.Contains(e.RejectedOutputs, b) {
				e.RejectedOutputs = append(e.RejectedOutputs, b)
			}
		}
		e.Outputs = nil
		e.UpdatedAt = time.Now()
		dirty = true
	}
	return dirty
}

// deleteRejectedOutputs deletes the blocks the journal lists as rejected. The
// state lock is held only to read the list and to apply the result: deleting
// a block is bucket I/O that can take long, and holding the lock across it
// would stall every heartbeat, lease and report until workers lose their
// leases.
func (s *Scheduler) deleteRejectedOutputs(ctx context.Context) error {
	s.mtx.Lock()
	todo := map[string][]string{}
	for id, e := range s.journal.Tasks {
		if _, busy := s.verifying[id]; busy {
			continue
		}
		if len(e.RejectedOutputs) > 0 {
			todo[id] = slices.Clone(e.RejectedOutputs)
		}
	}
	s.mtx.Unlock()
	if len(todo) == 0 {
		return nil
	}

	settled := map[string][]string{}
	for id, blocks := range todo {
		for _, b := range blocks {
			if s.deleteIfOurs(ctx, id, b) {
				settled[id] = append(settled[id], b)
			}
		}
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()
	dirty := false
	for id, blocks := range settled {
		e, ok := s.journal.Tasks[id]
		if !ok {
			continue
		}
		n := len(e.RejectedOutputs)
		e.RejectedOutputs = slices.DeleteFunc(e.RejectedOutputs, func(b string) bool { return slices.Contains(blocks, b) })
		if len(e.RejectedOutputs) != n {
			e.UpdatedAt = time.Now()
			dirty = true
		}
	}
	if !dirty {
		return nil
	}
	return s.persistLocked(ctx)
}

// deleteIfOurs deletes a block the journal lists as a rejected output of the
// task, but only once the block's own metadata says it was made for the task:
// the list comes from a worker's report, and a block ID a worker named could
// be anybody's - a source of the plan, say. It reports whether the ID is
// settled and can leave the journal: deleted, gone, or not the task's to
// delete. A block without metadata is left to the compactor's cleanup of
// partial uploads.
func (s *Scheduler) deleteIfOurs(ctx context.Context, taskID, blockID string) bool {
	id, err := ulid.Parse(blockID)
	if err != nil {
		return true
	}
	raw, err := readRawMeta(ctx, s.bkt, id)
	if err != nil {
		if s.bkt.IsObjNotFoundErr(err) {
			return true
		}
		level.Warn(s.logger).Log("msg", "could not read a rejected result block's metadata; retrying on the next tick", "task", taskID, "block", blockID, "err", err)
		return false
	}
	var m metadata.Meta
	if err := json.Unmarshal(raw, &m); err != nil {
		level.Warn(s.logger).Log("msg", "not deleting a block listed as a rejected result: its metadata is unreadable, so whose it is cannot be told", "task", taskID, "block", blockID, "err", err)
		return true
	}
	prov, ok := ProvenanceOf(&m)
	if !ok || prov.JournalID != s.conf.JournalID || prov.TaskID != taskID || prov.BlockID != blockID {
		level.Warn(s.logger).Log("msg", "not deleting a block listed as a rejected result: its metadata does not say it was made for the task", "task", taskID, "block", blockID)
		return true
	}
	if err := block.Delete(ctx, s.logger, s.bkt, id); err != nil {
		level.Warn(s.logger).Log("msg", "could not delete a rejected result block; retrying on the next tick", "task", taskID, "block", blockID, "err", err)
		return false
	}
	return true
}

func (s *Scheduler) maintainLocked(ctx context.Context, unpark []string) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.expireLeasesLocked()

	// Terminal entries age out here, while the manager runs, so the journal
	// stays bounded and a parked set is released after the retention as
	// promised, not only on the next restart.
	dirty := s.journal.Prune(s.conf.JournalRetention, time.Now()) > 0
	dirty = s.rejectUnverifiedLocked() || dirty

	// Any unpark request forces a journal write, its marker is only removed
	// after one: the entry it names may be gone from memory already because
	// an earlier tick dropped it and then failed to write the journal.
	dirty = dirty || len(unpark) > 0
	for _, taskID := range unpark {
		e, ok := s.journal.Tasks[taskID]
		if !ok {
			continue
		}
		if !e.State.Parked() {
			level.Warn(s.logger).Log("msg", "ignoring an unpark request for a task that is not parked", "task", taskID, "state", e.State)
			continue
		}
		level.Info(s.logger).Log("msg", "releasing a parked task on request; its source blocks are planned again",
			"task", taskID, "state", e.State, "sources", strings.Join(e.Task.SourceBlocks, ","))
		delete(s.journal.Tasks, taskID)
	}
	s.updateQueueMetricsLocked()

	if !dirty && time.Since(s.lastPersist) < s.conf.LeaseTTL {
		return nil
	}
	return s.persistLocked(ctx)
}

// describeDedupFunc names a merge function the way an operator configured it.
func describeDedupFunc(f string) string {
	if f == "" {
		return "the default chained merge"
	}
	return fmt.Sprintf("%q", f)
}

// sameSet reports whether two label lists name the same set.
func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[string]struct{}, len(a))
	for _, s := range a {
		seen[s] = struct{}{}
	}
	for _, s := range b {
		if _, ok := seen[s]; !ok {
			return false
		}
	}
	return true
}

func randomToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", errors.Wrap(err, "generate lease token")
	}
	return hex.EncodeToString(buf), nil
}

// RemotePlanExecutor implements compact.PlanExecutor by handing the plan to a
// worker and waiting for it to report back.
//
// The existing --compact.concurrency controls concurrent groups, and each group
// can keep maxInflightPerGroup independent plans running. The control loop
// retains its existing handling of halt and retry errors.
type RemotePlanExecutor struct {
	logger  log.Logger
	bkt     objstore.Bucket
	sched   *Scheduler
	planner compact.Planner

	// maxInflightPerGroup bounds how many plans for one group are worked on at
	// the same time.
	maxInflightPerGroup int
	deletableChecker    compact.BlockDeletableChecker

	// journalID names this manager in the deletion marks it writes.
	journalID string
}

// NewRemotePlanExecutor returns an executor that dispatches plans to workers.
//
// The planner is used to look for further, disjoint work in the same group while
// the first plan is still running. That is what lets a single block stream be
// compacted by several workers at once, which one process cannot do because a
// compaction job is single threaded.
func NewRemotePlanExecutor(logger log.Logger, bkt objstore.Bucket, sched *Scheduler, planner compact.Planner, maxInflightPerGroup int, deletableChecker compact.BlockDeletableChecker) *RemotePlanExecutor {
	if deletableChecker == nil {
		deletableChecker = compact.DefaultBlockDeletableChecker{}
	}
	if maxInflightPerGroup <= 0 {
		maxInflightPerGroup = 1
	}
	return &RemotePlanExecutor{
		logger:              logger,
		bkt:                 bkt,
		sched:               sched,
		planner:             planner,
		maxInflightPerGroup: maxInflightPerGroup,
		deletableChecker:    deletableChecker,
		journalID:           sched.conf.JournalID,
	}
}

// runPlan hands one plan to a worker and waits for it to finish.
func (e *RemotePlanExecutor) runPlan(ctx context.Context, cg *compact.Group, plan compact.Plan) ([]ulid.ULID, error) {
	task, err := CompactionTask(cg, plan)
	if err != nil {
		return nil, err
	}
	task.Group.DedupReplicaLabels = e.sched.conf.DedupReplicaLabels

	if e.sched.SourcesParked(task.SourceBlocks) {
		// These blocks are deliberately withheld: either they took a task
		// through its whole attempt budget without a single report, or they
		// were refused as oversized. Handing them out again would repeat the
		// cycle; they stay parked until an operator intervenes or the journal
		// entry ages out of the retention.
		level.Warn(e.logger).Log("msg", "not replanning the source blocks of a parked (abandoned or oversized) task; investigate the journal entry",
			"group", cg.Key(), "blocks", len(task.SourceBlocks))
		return nil, compact.ErrPlanDeferred
	}
	if reason := oversizedReason(task, e.sched.conf); reason != "" {
		// Refusing here, before any worker touches it, turns "three workers
		// died at minute 40" into an immediate, named refusal in the journal.
		level.Error(e.logger).Log("msg", "refusing to dispatch an oversized compaction task", "group", cg.Key(), "reason", reason)
		e.sched.MarkOversized(task, reason)
		return nil, compact.ErrPlanDeferred
	}

	resultCh, err := e.sched.Submit(ctx, task)
	if err != nil {
		return nil, errors.Wrap(err, "submit compaction task")
	}
	level.Info(e.logger).Log("msg", "dispatched compaction task", "task", task.ID,
		"group", cg.Key(), "blocks", len(task.SourceBlocks))

	var res Result
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res = <-resultCh:
	}
	defer e.sched.VerificationDone(task.ID)

	if err := ReconstructError(res); err != nil {
		return nil, err
	}

	return e.verifyAndFinalize(ctx, cg, plan, res)
}

// verifyAndFinalize checks that what a worker claims to have uploaded is really
// in the bucket and really is the result of this plan, then marks the source
// blocks for deletion. Workers never touch source blocks; only the manager does.
//
// The provenance checks are what stands between a confused worker and data
// loss: the sources are deleted below on the strength of this verification, so
// a block that merely exists is not good enough. It has to carry the labels of
// one of the plan's outputs and the group's resolution, sit inside the plan's
// time range, and together the outputs have to account for every source in
// the plan, for the plan's whole time span, and for every output the plan
// named - a shard that holds a fraction of the series lists every source and
// spans the whole range, so ancestry and time alone would not notice a
// missing sibling.
//
// A rejected result is retried as a whole, and the sources stay. Every output
// verified to be this task's is then a block nobody will ever account for, and
// worse than an orphan: it lists the plan's sources, so the deduplication
// filter would let it retire them on the next sync. Such blocks are recorded
// in the journal as rejected, which the manager's deduplication filter treats
// as unpublished from then on, and deleted. Only blocks known to be ours: one
// that failed the provenance check could be anybody's.
func (e *RemotePlanExecutor) verifyAndFinalize(ctx context.Context, cg *compact.Group, plan compact.Plan, res Result) ([]ulid.ULID, error) {
	toCompact := plan.Sources
	checker := e.deletableChecker
	if checker == nil {
		checker = compact.DefaultBlockDeletableChecker{}
	}

	// An output block records the union of its parents' sources, not the
	// parents' ULIDs, so that union is what the outputs must account for.
	expected := map[ulid.ULID]struct{}{}
	planMinTime, planMaxTime := toCompact[0].MinTime, toCompact[0].MaxTime
	for _, m := range toCompact {
		for _, s := range m.Compaction.Sources {
			expected[s] = struct{}{}
		}
		planMinTime = min(planMinTime, m.MinTime)
		planMaxTime = max(planMaxTime, m.MaxTime)
	}

	var ours []ulid.ULID
	reject := func(err error) ([]ulid.ULID, error) {
		return nil, e.rejectOutputs(ctx, res.TaskID, ours, err)
	}

	covered := map[ulid.ULID]struct{}{}
	outMetas := make(map[ulid.ULID]metadata.Meta, len(res.OutputBlocks))
	compIDs := make([]ulid.ULID, 0, len(res.OutputBlocks))
	for _, raw := range res.OutputBlocks {
		meta, err := fetchVerifiedMeta(ctx, e.bkt, raw, res.OutputChecksums)
		if err != nil {
			return reject(err)
		}
		id := meta.ULID
		if err := verifyProvenance(meta, Provenance{
			TaskID: res.TaskID, TaskType: TaskCompaction, JournalID: e.journalID, Generation: res.Generation,
		}); err != nil {
			return reject(errors.Wrapf(err, "result block %s", id))
		}
		// From here on the block is known to be this task's.
		ours = append(ours, id)

		if meta.Thanos.Downsample.Resolution != cg.Resolution() {
			return reject(errors.Errorf(
				"result block %s has resolution %d, the group has %d", id, meta.Thanos.Downsample.Resolution, cg.Resolution()))
		}
		if meta.MinTime < planMinTime || meta.MaxTime > planMaxTime {
			return reject(errors.Errorf(
				"result block %s spans [%d, %d], outside the plan's [%d, %d]", id, meta.MinTime, meta.MaxTime, planMinTime, planMaxTime))
		}
		for _, s := range meta.Compaction.Sources {
			if _, isSource := expected[s]; !isSource {
				return reject(errors.Errorf(
					"result block %s was compacted from %s, which is not a source of this plan", id, s))
			}
			covered[s] = struct{}{}
		}
		if _, dup := outMetas[id]; dup {
			return reject(errors.Errorf("result block %s was reported twice", id))
		}
		outMetas[id] = *meta
		compIDs = append(compIDs, id)
	}

	// Every output the plan named is accounted for, each block is the output
	// it claims to be, and no output is claimed twice.
	if err := claimOutputs(cg, plan, res, outMetas); err != nil {
		return reject(err)
	}

	if len(compIDs) == 0 {
		// A plan legitimately produces nothing when every source block holds no
		// samples. Mirror what the in-process compactor does: mark the empty
		// sources for deletion, so they are not planned, downloaded and compacted
		// again forever. No outputs from sources that do hold samples is a worker
		// bug; deleting or ignoring those sources would each be wrong, so refuse.
		for _, meta := range toCompact {
			if meta.Stats.NumSamples > 0 {
				return nil, compact.NewRetryError(errors.Errorf(
					"task %s produced no blocks although source %s holds %d samples",
					res.TaskID, meta.ULID, meta.Stats.NumSamples))
			}
		}
		level.Info(e.logger).Log("msg", "task produced no blocks, deleting empty source blocks", "task", res.TaskID, "group", cg.Key())
		for _, meta := range toCompact {
			if !checker.CanDelete(cg, meta.ULID) {
				continue
			}
			if err := block.MarkForDeletion(ctx, e.logger, e.bkt, meta.ULID, DeletionDetails(e.journalID, res.TaskID), cg.BlocksMarkedForDeletion()); err != nil {
				return nil, compact.NewRetryError(errors.Wrapf(err, "mark empty source block %s for deletion", meta.ULID))
			}
		}
		return nil, nil
	}

	// Nothing is deleted unless the outputs, together, account for every source
	// in the plan. A partial result means data would go missing with the sources.
	for s := range expected {
		if _, ok := covered[s]; !ok {
			return reject(errors.Errorf(
				"the reported result blocks do not account for source %s; refusing to delete the plan's sources", s))
		}
	}

	// The same goes for time: claiming every source is not enough if the
	// outputs span less than the plan does - the missing range would be deleted
	// with the sources. Compacted blocks inherit the union of their parents'
	// ranges, so the outputs have to cover the plan's span exactly, without
	// gaps between them.
	metas := make([]metadata.Meta, 0, len(compIDs))
	for _, id := range compIDs {
		metas = append(metas, outMetas[id])
	}
	if err := verifyTimeCoverage(metas, planMinTime, planMaxTime); err != nil {
		return reject(err)
	}

	// Verified: from now on the outputs may supersede the sources, in the
	// bucket-wide view as here. The sources are retired on the strength of
	// that verdict, so it has to be in the journal first: a successor that
	// found the task unverified would delete the outputs while the sources
	// were on their way out. Unrecorded, the verdict is void - the outputs
	// are rejected by maintenance and the plan is redone.
	if e.sched != nil {
		if err := e.sched.AcceptOutputs(ctx, res.TaskID); err != nil {
			if compact.IsHaltError(err) {
				return nil, err
			}
			return nil, compact.NewRetryError(errors.Wrapf(err, "record the verification of task %s", res.TaskID))
		}
	}
	cg.RecordCompaction(plan.OverlappingBlocks)

	// Mark the sources for deletion now that the result is known to be in the
	// bucket, so the next planning cycle does not pick them up again.
	for _, meta := range toCompact {
		if !checker.CanDelete(cg, meta.ULID) {
			continue
		}
		if err := block.MarkForDeletion(ctx, e.logger, e.bkt, meta.ULID, DeletionDetails(e.journalID, res.TaskID), cg.BlocksMarkedForDeletion()); err != nil {
			return nil, compact.NewRetryError(errors.Wrapf(err, "mark source block %s for deletion", meta.ULID))
		}
		cg.RecordSourceGarbageCollected()
	}
	return compIDs, nil
}

// rejectOutputs refuses a task's result: the blocks verified to be the task's
// are recorded as rejected in the journal and deleted, and the cause is
// returned as the error the control loop retries on. If a block can be
// neither deleted nor recorded, nothing keeps it from retiring the plan's
// sources on the next sync, and the manager halts instead.
func (e *RemotePlanExecutor) rejectOutputs(ctx context.Context, taskID string, ours []ulid.ULID, cause error) error {
	if len(ours) == 0 {
		return compact.NewRetryError(cause)
	}
	recorded := false
	if e.sched != nil {
		err := e.sched.RejectOutputs(ctx, taskID, ours)
		if err != nil {
			level.Error(e.logger).Log("msg", "could not record the rejected result blocks of a task in the journal", "task", taskID, "err", err)
		}
		recorded = err == nil
	}
	failed := 0
	for _, id := range ours {
		level.Warn(e.logger).Log("msg", "deleting a result block of a rejected task result", "task", taskID, "block", id, "reason", cause)
		if err := block.Delete(ctx, e.logger, e.bkt, id); err != nil {
			failed++
			level.Error(e.logger).Log("msg", "could not delete the result block of a rejected task result", "task", taskID, "block", id, "err", err)
		}
	}
	if failed > 0 && !recorded {
		return compact.NewHaltError(errors.Wrapf(cause,
			"%d rejected result block(s) of task %s could be neither deleted nor recorded as rejected; left alone they would retire the plan's sources on the next sync", failed, taskID))
	}
	return compact.NewRetryError(cause)
}

// claimOutputs checks the result blocks against the outputs the plan named.
//
// For a plan without outputs the result is the one block a compaction has
// always produced, carrying the group's labels; the worker reports no
// per-output account. For a plan with outputs the worker accounts for each
// of them: the block that is it, or none when it held no series. Every
// block then has to say which output it is and agree with the report on the
// whole set the compaction produced, and every output is claimed at most
// once. An output missing from the account is a missing block: without this
// a result could omit a shard and still pass the source and time checks,
// since every shard lists every source and spans the whole range.
func claimOutputs(cg *compact.Group, plan compact.Plan, res Result, outMetas map[ulid.ULID]metadata.Meta) error {
	// The set every result block must record: the uploaded blocks and the
	// siblings the plan named.
	wantSet := make(map[ulid.ULID]struct{}, len(outMetas)+len(plan.Siblings))
	for id := range outMetas {
		wantSet[id] = struct{}{}
	}
	for _, id := range plan.Siblings {
		wantSet[id] = struct{}{}
	}
	recordsSet := func(id ulid.ULID, set *metadata.ThanosOutput, index, count int) error {
		if set == nil {
			return errors.Errorf("result block %s does not record which output it is", id)
		}
		if set.Index != index || set.Count != count {
			return errors.Errorf("result block %s says it is output %d of %d, the report says output %d of %d", id, set.Index, set.Count, index, count)
		}
		if len(set.Blocks) != len(wantSet) {
			return errors.Errorf("result block %s records a set of %d blocks, the report and the plan's siblings make %d", id, len(set.Blocks), len(wantSet))
		}
		for _, member := range set.Blocks {
			if _, ok := wantSet[member]; !ok {
				return errors.Errorf("result block %s records %s in its set, which is neither reported nor a sibling the plan named", id, member)
			}
		}
		return nil
	}

	if len(plan.Outputs) == 0 {
		if len(res.Outputs) > 0 {
			return errors.Errorf("the report accounts for %d outputs, the plan named none", len(res.Outputs))
		}
		if len(outMetas) > 1 {
			return errors.Errorf("%d result blocks for a plan that produces one", len(outMetas))
		}
		for id, meta := range outMetas {
			if !labels.Equal(labels.FromMap(meta.Thanos.Labels), cg.Labels()) {
				return errors.Errorf("result block %s carries labels %v, the plan's output has %v", id, meta.Thanos.Labels, cg.Labels())
			}
			if len(plan.Siblings) > 0 {
				if err := recordsSet(id, meta.Thanos.Output, 0, 1); err != nil {
					return err
				}
				continue
			}
			// The block the compaction has always produced records no set. One
			// that does would be judged by it, and a set naming blocks that
			// never appear would keep it unpublished for good while the plan's
			// sources are retired.
			if meta.Thanos.Output != nil {
				return errors.Errorf("result block %s records an output set, the plan named neither outputs nor siblings", id)
			}
		}
		return nil
	}

	if len(res.Outputs) != len(plan.Outputs) {
		return errors.Errorf("the report accounts for %d outputs, the plan named %d", len(res.Outputs), len(plan.Outputs))
	}
	seen := make([]bool, len(plan.Outputs))
	claimed := make(map[ulid.ULID]int, len(res.Outputs))
	for _, o := range res.Outputs {
		if o.Index < 0 || o.Index >= len(plan.Outputs) {
			return errors.Errorf("the report accounts for output %d, which the plan did not name", o.Index)
		}
		if seen[o.Index] {
			return errors.Errorf("the report accounts for output %d twice", o.Index)
		}
		seen[o.Index] = true
		if o.Block == "" {
			continue
		}
		id, err := ulid.Parse(o.Block)
		if err != nil {
			return errors.Wrapf(err, "the report names an unparsable block %q for output %d", o.Block, o.Index)
		}
		meta, ok := outMetas[id]
		if !ok {
			return errors.Errorf("the report names block %s for output %d but does not report it as uploaded", id, o.Index)
		}
		if _, dup := claimed[id]; dup {
			return errors.Errorf("result block %s is claimed for two outputs", id)
		}
		claimed[id] = o.Index

		want := plan.Outputs[o.Index].Labels
		if want == nil {
			want = cg.Labels().Map()
		}
		if !labels.Equal(labels.FromMap(meta.Thanos.Labels), labels.FromMap(want)) {
			return errors.Errorf("result block %s carries labels %v, output %d of the plan has %v", id, meta.Thanos.Labels, o.Index, want)
		}
		if err := recordsSet(id, meta.Thanos.Output, o.Index, len(plan.Outputs)); err != nil {
			return err
		}
	}
	if len(claimed) != len(outMetas) {
		return errors.Errorf("the report uploads %d blocks but accounts for %d of them as outputs", len(outMetas), len(claimed))
	}
	return nil
}

// readRawMeta returns the raw bytes of a block's meta.json in the bucket.
func readRawMeta(ctx context.Context, bkt objstore.Bucket, id ulid.ULID) (_ []byte, err error) {
	r, err := bkt.Get(ctx, path.Join(id.String(), block.MetaFilename))
	if err != nil {
		return nil, err
	}
	defer runutil.ExhaustCloseWithErrCapture(&err, r, "close meta.json reader")
	return io.ReadAll(r)
}

// Retry only the read, before any verification or source retirement. A brief
// object-store failure should not discard a completed compaction pass.
func readRawMetaWithRetry(ctx context.Context, bkt objstore.Bucket, id ulid.ULID) ([]byte, error) {
	var err error
	for attempt := range 3 {
		if attempt > 0 {
			if err := sleep(ctx, time.Duration(attempt)*100*time.Millisecond); err != nil {
				return nil, err
			}
		}
		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		var raw []byte
		raw, err = readRawMeta(readCtx, bkt, id)
		cancel()
		if err == nil {
			return raw, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return nil, err
}

// checksumOf returns the checksum of meta.json bytes in the form workers report.
func checksumOf(raw []byte) string {
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// verifyTimeCoverage checks that the blocks, together, cover exactly the range
// [minTime, maxTime]: no gaps between them, and no partial overlaps either.
//
// Two shapes are legitimate for several outputs of one compaction: blocks that
// follow each other exactly, and blocks that span exactly the same range, as a
// compactor sharding by series produces. Anything in between - [0,150) next to
// [100,200) - would replace the sources with overlapping blocks.
func verifyTimeCoverage(metas []metadata.Meta, minTime, maxTime int64) error {
	slices.SortFunc(metas, func(a, b metadata.Meta) int {
		if c := cmp.Compare(a.MinTime, b.MinTime); c != 0 {
			return c
		}
		return cmp.Compare(a.MaxTime, b.MaxTime)
	})

	if metas[0].MinTime != minTime {
		return errors.Errorf("the result blocks start at %d, the plan at %d", metas[0].MinTime, minTime)
	}
	prev := metas[0]
	coveredUpTo := prev.MaxTime
	for _, m := range metas[1:] {
		sameRange := m.MinTime == prev.MinTime && m.MaxTime == prev.MaxTime
		contiguous := m.MinTime == coveredUpTo
		switch {
		case sameRange:
		case contiguous:
			coveredUpTo = m.MaxTime
		case m.MinTime > coveredUpTo:
			return errors.Errorf("the result blocks leave [%d, %d) uncovered", coveredUpTo, m.MinTime)
		default:
			return errors.Errorf("result blocks [%d, %d) and [%d, %d) overlap", prev.MinTime, prev.MaxTime, m.MinTime, m.MaxTime)
		}
		prev = m
	}
	if coveredUpTo != maxTime {
		return errors.Errorf("the result blocks cover up to %d, the plan up to %d", coveredUpTo, maxTime)
	}
	return nil
}

// verifyProvenance checks that a block records exactly the task it is being
// reported for. Without this a block the worker did not make for this task -
// or did not make at all - could replace the plan's sources, and because the
// rollback finds the distributed compactor's work by this very record, it
// could never undo that.
func verifyProvenance(m *metadata.Meta, want Provenance) error {
	got, ok := ProvenanceOf(m)
	if !ok {
		return errors.New("carries no provenance; every block a worker produces records the task that made it")
	}
	switch {
	case got.TaskID != want.TaskID:
		return errors.Errorf("was produced by task %s, not %s", got.TaskID, want.TaskID)
	case got.TaskType != want.TaskType:
		return errors.Errorf("was produced by a %s task, not a %s task", got.TaskType, want.TaskType)
	case got.JournalID != want.JournalID:
		return errors.Errorf("was produced for journal %q, not %q", got.JournalID, want.JournalID)
	case got.Generation != want.Generation:
		return errors.Errorf("was produced under journal generation %d, not %d", got.Generation, want.Generation)
	}
	return nil
}

// CompactionTask builds the task that asks a worker to execute a plan.
func CompactionTask(cg *compact.Group, plan compact.Plan) (Task, error) {
	toCompact := plan.Sources
	spec, err := GroupSpecOf(cg)
	if err != nil {
		return Task{}, err
	}

	sources := make([]string, 0, len(toCompact))
	minTime, maxTime := int64(0), int64(0)
	for i, m := range toCompact {
		sources = append(sources, m.ULID.String())
		if i == 0 || m.MinTime < minTime {
			minTime = m.MinTime
		}
		if i == 0 || m.MaxTime > maxTime {
			maxTime = m.MaxTime
		}
	}

	series, indexBytes := expectedTaskSize(toCompact)
	return Task{
		ID:                 ulid.Make().String(),
		Type:               TaskCompaction,
		Group:              spec,
		SourceBlocks:       sources,
		ExpectedMinTime:    minTime,
		ExpectedMaxTime:    maxTime,
		OverlappingBlocks:  plan.OverlappingBlocks,
		Outputs:            plan.Outputs,
		Siblings:           plan.Siblings,
		ExpectedSeries:     series,
		ExpectedIndexBytes: indexBytes,
	}, nil
}

// expectedTaskSize sums what the source blocks report about themselves. Series
// counts sum to an upper bound of the merge's footprint (deduplication only
// shrinks it), and the index sizes come from the file stats blocks carry since
// they are uploaded with them. Blocks that report nothing contribute zero, so
// a missing figure can never make a task look bigger than it is.
func expectedTaskSize(metas []*metadata.Meta) (series uint64, indexBytes int64) {
	for _, m := range metas {
		series += m.Stats.NumSeries
		for _, f := range m.Thanos.Files {
			if f.RelPath == block.IndexFilename {
				indexBytes += f.SizeBytes
			}
		}
	}
	return series, indexBytes
}

// GroupSpecOf describes a group in the form a worker can rebuild it from.
func GroupSpecOf(cg *compact.Group) (GroupSpec, error) {
	spec := GroupSpec{
		Key:                           cg.Key(),
		Labels:                        cg.Labels().Map(),
		Resolution:                    cg.Resolution(),
		AcceptMalformedIndex:          cg.AcceptMalformedIndex(),
		EnableVerticalCompaction:      cg.EnableVerticalCompaction(),
		HashFunc:                      string(cg.HashFunc()),
		BlockFilesConcurrency:         cg.BlockFilesConcurrency(),
		CompactBlocksFetchConcurrency: cg.CompactBlocksFetchConcurrency(),
	}
	if ext := cg.Extensions(); ext != nil {
		raw, err := json.Marshal(ext)
		if err != nil {
			return GroupSpec{}, errors.Wrap(err, "marshal group extensions")
		}
		spec.Extensions = raw
	}
	return spec, nil
}

// DispatchDownsampling hands every block that needs downsampling to a worker and
// waits for all of them to finish.
//
// It replaces downsampling in the manager process. Retention, garbage collection
// and every other mutation of the bucket stay with the manager; the only thing
// a worker does is produce the downsampled block.
func DispatchDownsampling(
	ctx context.Context,
	logger log.Logger,
	bkt objstore.Bucket,
	sched *Scheduler,
	metas map[ulid.ULID]*metadata.Meta,
	opts downsample.PlanOptions,
	concurrency int,
	hashFunc metadata.HashFunc,
	blockFilesConcurrency int,
	acceptMalformedIndex bool,
	downsamples *prometheus.CounterVec,
	downsampleFailures *prometheus.CounterVec,
) error {
	candidates, err := downsample.Plan(metas, opts)
	if err != nil {
		return err
	}
	if len(candidates) == 0 {
		return nil
	}
	if concurrency <= 0 {
		concurrency = 1
	}

	level.Info(logger).Log("msg", "dispatching downsampling to workers", "blocks", len(candidates))

	// The counters are the same per-resolution series the in-process
	// downsampler feeds, so alerts on downsample failures keep firing when a
	// deployment flips to manager mode. Nil vectors keep this callable from
	// tests without metrics.
	inc := func(vec *prometheus.CounterVec, resolution string) {
		if vec != nil {
			vec.WithLabelValues(resolution).Inc()
		}
	}

	var g errgroup.Group
	g.SetLimit(concurrency)
	var failed atomic.Bool

	for _, c := range candidates {
		if sched.SourcesParked([]string{c.Meta.ULID.String()}) {
			level.Warn(logger).Log("msg", "not re-dispatching the source block of a parked (abandoned or oversized) downsample task; investigate the journal entry",
				"block", c.Meta.ULID)
			continue
		}
		task := DownsampleTask(c.Meta, c.TargetResolution, hashFunc, blockFilesConcurrency, acceptMalformedIndex, sched.conf.DedupReplicaLabels)
		if reason := oversizedReason(task, sched.conf); reason != "" {
			// Blocks marked no-compact for index size are exactly the ones
			// downsampling must not assume a worker can hold.
			level.Error(logger).Log("msg", "refusing to dispatch an oversized downsample task", "block", c.Meta.ULID, "reason", reason)
			sched.MarkOversized(task, reason)
			continue
		}
		g.Go(func() error {
			// Fail fast: once anything failed, no further tasks are submitted.
			// What is already in flight is still awaited, so no task is left
			// behind in the scheduler without a submitter.
			if failed.Load() {
				return nil
			}
			resolution := c.Meta.Thanos.ResolutionString()

			resultCh, submitErr := sched.Submit(ctx, task)
			if submitErr != nil {
				failed.Store(true)
				return errors.Wrap(submitErr, "submit downsample task")
			}

			var res Result
			select {
			case <-ctx.Done():
				failed.Store(true)
				return ctx.Err()
			case res = <-resultCh:
			}
			// The result is under verification from here until this returns;
			// maintenance leaves it alone meanwhile, and rejects what was
			// never accepted afterwards.
			defer sched.VerificationDone(res.TaskID)

			// Aborted outcomes never reach the submitter: the scheduler requeues
			// them, and past the abort cap it fails the task.
			if err := ReconstructError(res); err != nil {
				failed.Store(true)
				inc(downsampleFailures, resolution)
				return errors.Wrapf(err, "downsample block %s", c.Meta.ULID)
			}

			// Confirm the downsampled block really is in the bucket, and really
			// is this block's downsample: same labels, same sources, the target
			// resolution. Downsampling deletes nothing, so a wrong block cannot
			// lose data the way it could for compaction, but accepting it would
			// leave this block silently never downsampled.
			if len(res.OutputBlocks) != 1 {
				failed.Store(true)
				inc(downsampleFailures, resolution)
				return compact.NewRetryError(errors.Errorf("downsample task %s reported %d output blocks; expected exactly one", res.TaskID, len(res.OutputBlocks)))
			}
			for _, raw := range res.OutputBlocks {
				outMeta, fetchErr := fetchVerifiedMeta(ctx, bkt, raw, res.OutputChecksums)
				if fetchErr != nil {
					failed.Store(true)
					inc(downsampleFailures, resolution)
					return compact.NewRetryError(errors.Wrapf(fetchErr, "downsample of %s", c.Meta.ULID))
				}
				if err := verifyDownsampledBlock(outMeta, c, Provenance{
					TaskID: res.TaskID, TaskType: TaskDownsample, JournalID: sched.conf.JournalID, Generation: res.Generation,
					Sources: []string{c.Meta.ULID.String()},
				}); err != nil {
					failed.Store(true)
					inc(downsampleFailures, resolution)
					return compact.NewRetryError(errors.Wrapf(err, "downsampled block %s reported for %s", outMeta.ULID, c.Meta.ULID))
				}
			}

			// Verified: the block is published to the manager's view. Until
			// then the deduplication filter withholds it, and without this
			// the manager would downsample the same block again every pass.
			if err := sched.AcceptOutputs(ctx, res.TaskID); err != nil {
				failed.Store(true)
				inc(downsampleFailures, resolution)
				return compact.NewRetryError(errors.Wrapf(err, "accept downsample of %s", c.Meta.ULID))
			}

			inc(downsamples, resolution)
			return nil
		})
	}
	return g.Wait()
}

// verifyDownsampledBlock checks that a block a worker reported is the
// downsample of the given candidate, produced by the task being reported.
func verifyDownsampledBlock(outMeta *metadata.Meta, c downsample.Candidate, want Provenance) error {
	if err := verifyProvenance(outMeta, want); err != nil {
		return err
	}
	if outMeta.Thanos.Downsample.Resolution != c.TargetResolution {
		return errors.Errorf("has resolution %d, expected %d", outMeta.Thanos.Downsample.Resolution, c.TargetResolution)
	}
	if !labels.Equal(labels.FromMap(outMeta.Thanos.Labels), labels.FromMap(c.Meta.Thanos.Labels)) {
		return errors.Errorf("carries labels %v, the source has %v", outMeta.Thanos.Labels, c.Meta.Thanos.Labels)
	}
	if outMeta.MinTime != c.Meta.MinTime || outMeta.MaxTime != c.Meta.MaxTime {
		return errors.Errorf("spans [%d, %d], the source spans [%d, %d]",
			outMeta.MinTime, outMeta.MaxTime, c.Meta.MinTime, c.Meta.MaxTime)
	}
	expected := make(map[ulid.ULID]struct{}, len(c.Meta.Compaction.Sources))
	for _, s := range c.Meta.Compaction.Sources {
		expected[s] = struct{}{}
	}
	if len(outMeta.Compaction.Sources) != len(expected) {
		return errors.Errorf("was built from %d sources, the source block has %d",
			len(outMeta.Compaction.Sources), len(expected))
	}
	for _, s := range outMeta.Compaction.Sources {
		if _, ok := expected[s]; !ok {
			return errors.Errorf("was built from %s, which is not a source of the block being downsampled", s)
		}
	}
	return nil
}

func abortCapFor(maxAttempts int) int {
	return 3 * maxAttempts
}

func abortRequeueBackoff(aborts int, leaseTTL time.Duration) time.Duration {
	return min(time.Duration(aborts)*30*time.Second, leaseTTL)
}

func fetchVerifiedMeta(ctx context.Context, bkt objstore.Bucket, raw string, checksums map[string]string) (*metadata.Meta, error) {
	id, err := ulid.Parse(raw)
	if err != nil {
		return nil, errors.Wrapf(err, "worker reported an unparsable block ID %q", raw)
	}
	rawMeta, err := readRawMetaWithRetry(ctx, bkt, id)
	if err != nil {
		return nil, errors.Wrapf(err, "verify result block %s reported by worker", id)
	}
	sum, ok := checksums[raw]
	if !ok || sum == "" {
		// The checksum is what binds the reported result to the metadata the
		// worker observed after its upload; without it the block in the
		// bucket could be anything. Workers always report it, so its absence
		// is a verification failure, not a matter of degree.
		return nil, errors.Errorf("worker reported no checksum for result block %s", id)
	}
	if got := checksumOf(rawMeta); got != sum {
		return nil, errors.Errorf(
			"result block %s metadata does not match the checksum the worker reported: got %s, reported %s", id, got, sum)
	}
	var meta metadata.Meta
	if err := json.Unmarshal(rawMeta, &meta); err != nil {
		return nil, errors.Wrapf(err, "unmarshal metadata of result block %s", id)
	}
	if meta.ULID.Compare(id) != 0 {
		return nil, errors.Errorf("result block %s holds metadata for %s", id, meta.ULID)
	}
	return &meta, nil
}
