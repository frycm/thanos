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

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
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

	leasedAt time.Time
	queuedAt time.Time
}

// Scheduler owns the task queue and the leases. It is the only writer of the
// journal.
type Scheduler struct {
	logger log.Logger
	bkt    objstore.Bucket
	conf   ManagerConfig
	m      *managerMetrics

	// ownerID identifies this manager instance in the journal, so that another
	// manager writing the same journal is detected even when both hold the same
	// generation.
	ownerID string

	mtx        sync.Mutex
	journal    *Journal
	tasks      map[string]*pendingTask
	queue      []string
	workerSeen map[string]time.Time

	journalUnavailableSince time.Time
	lastPersist             time.Time
}

// NewScheduler takes ownership of the shard's journal, bumping its generation so
// that leases handed out by a previous manager are void, and returns a scheduler
// ready to hand tasks to workers.
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
			"make sure no other compactor manager uses this journal ID",
			"journal_id", conf.JournalID, "journal_selector", j.SelectorHash, "our_selector", conf.SelectorHash)
	}
	j.SelectorHash = conf.SelectorHash

	ownerID, err := randomToken()
	if err != nil {
		return nil, err
	}

	// Taking ownership voids every lease from an older generation, and drops
	// every task that never finished. Planning is idempotent, so this manager
	// will replan any work that was in flight, and a worker still executing an
	// old task fails its ownership check and discards the work. Keeping the old
	// entries would only leak them: nothing would ever lease or prune them.
	j.Generation++
	j.Owner = ownerID
	for id, e := range j.Tasks {
		if !e.State.Terminal() {
			delete(j.Tasks, id)
		}
	}
	j.Prune(conf.JournalRetention, time.Now())

	if err := WriteJournal(ctx, bkt, j); err != nil {
		return nil, errors.Wrap(err, "take ownership of journal")
	}

	s := &Scheduler{
		logger:      logger,
		bkt:         bkt,
		conf:        conf,
		m:           newManagerMetrics(reg),
		ownerID:     ownerID,
		journal:     j,
		tasks:       map[string]*pendingTask{},
		workerSeen:  map[string]time.Time{},
		lastPersist: time.Now(),
	}
	s.m.journalGeneration.Set(float64(j.Generation))

	level.Info(logger).Log("msg", "took ownership of compaction journal", "journal_id", conf.JournalID, "generation", j.Generation)
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

	task.Generation = s.journal.Generation
	task.LeaseTTL = s.conf.LeaseTTL

	now := time.Now()
	entry := &TaskEntry{
		Task:      task,
		State:     StatePending,
		CreatedAt: now,
		UpdatedAt: now,
	}
	s.journal.Tasks[task.ID] = entry
	s.tasks[task.ID] = &pendingTask{
		entry:    entry,
		result:   make(chan Result, 1),
		queuedAt: now,
	}
	s.queue = append(s.queue, task.ID)

	if err := s.persistLocked(ctx); err != nil {
		delete(s.journal.Tasks, task.ID)
		delete(s.tasks, task.ID)
		s.queue = s.queue[:len(s.queue)-1]
		return nil, err
	}
	s.updateQueueMetricsLocked()
	return s.tasks[task.ID].result, nil
}

// Lease hands a queued task to a worker, if there is one it accepts.
func (s *Scheduler) Lease(ctx context.Context, req LeaseRequest) (*Task, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

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
			"worker %s deduplicates with %s but this manager plans for %s; "+
				"--deduplication.func has to match on both", req.WorkerID, describeDedupFunc(req.DedupFunc), describeDedupFunc(s.conf.DedupFunc))
	}
	if !sameSet(req.DedupReplicaLabels, s.conf.DedupReplicaLabels) {
		return nil, errors.Errorf(
			"worker %s removes replica labels %v but this manager removes %v; "+
				"--deduplication.replica-label has to match on both", req.WorkerID, req.DedupReplicaLabels, s.conf.DedupReplicaLabels)
	}

	s.expireLeasesLocked()
	s.workerSeen[req.WorkerID] = time.Now()

	accepts := map[TaskType]bool{}
	for _, t := range req.Accepts {
		accepts[t] = true
	}

	for i, id := range s.queue {
		p, ok := s.tasks[id]
		if !ok || p.entry.State != StatePending {
			continue
		}
		if len(accepts) > 0 && !accepts[p.entry.Task.Type] {
			continue
		}

		token, err := randomToken()
		if err != nil {
			return nil, err
		}
		now := time.Now()
		p.entry.State = StateLeased
		p.entry.Attempts++
		p.entry.Lease = &Lease{
			WorkerID:   req.WorkerID,
			Token:      token,
			Generation: s.journal.Generation,
			ExpiresAt:  now.Add(s.conf.LeaseTTL),
		}
		p.entry.UpdatedAt = now
		p.leasedAt = now

		if err := s.persistLocked(ctx); err != nil {
			p.entry.State = StatePending
			p.entry.Attempts--
			p.entry.Lease = nil
			return nil, err
		}

		s.queue = append(s.queue[:i], s.queue[i+1:]...)
		s.m.tasksInFlight.WithLabelValues(string(p.entry.Task.Type)).Inc()
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
	case res.Outcome.Aborted():
		// The worker threw its work away, so this is not a failed attempt.
		p.entry.State = StatePending
		p.entry.Attempts--
		p.entry.LastError = &TaskError{Outcome: res.Outcome, Message: res.ErrorMessage}
		s.queue = append(s.queue, res.TaskID)
	case res.Outcome == OutcomeFailedRetryable:
		p.entry.LastError = &TaskError{Outcome: res.Outcome, Block: res.OffendingBlock, Message: res.ErrorMessage}
		if p.entry.Attempts >= s.conf.MaxAttempts {
			p.entry.State = StateFailed
		} else {
			p.entry.State = StatePending
			s.queue = append(s.queue, res.TaskID)
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
	persistErr := s.persistLocked(ctx)
	s.updateQueueMetricsLocked()

	if persistErr != nil && compact.IsHaltError(persistErr) {
		// Another manager owns the journal now. The worker's outcome no longer
		// matters: what the submitter has to learn is the halt, or this
		// manager's control loop would verify the result and keep compacting a
		// shard somebody else manages. Only ordinary write failures get the
		// best-effort delivery above.
		p.entry.State = StateFailed
		s.m.tasksTotal.WithLabelValues(string(p.entry.Task.Type), string(OutcomeFailedHalt)).Inc()
		s.finishLocked(res.TaskID, Result{
			TaskID:       res.TaskID,
			Outcome:      OutcomeFailedHalt,
			ErrorMessage: persistErr.Error(),
		})
		return persistErr
	}

	if p.entry.State.Terminal() {
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
func oversizedReason(task Task, conf ManagerConfig) string {
	switch {
	case conf.MaxTaskSeries > 0 && task.ExpectedSeries > conf.MaxTaskSeries:
		return fmt.Sprintf("task expects %d series from %d source blocks, over the configured --compact.manager.max-task-series of %d; "+
			"split the plan, raise worker capacity together with the limit, or no-compact-mark the blocks",
			task.ExpectedSeries, len(task.SourceBlocks), conf.MaxTaskSeries)
	case conf.MaxTaskIndexBytes > 0 && task.ExpectedIndexBytes > conf.MaxTaskIndexBytes:
		return fmt.Sprintf("task expects %d bytes of source index from %d source blocks, over the configured --compact.manager.max-task-index-size of %d; "+
			"split the plan, raise worker capacity together with the limit, or no-compact-mark the blocks",
			task.ExpectedIndexBytes, len(task.SourceBlocks), conf.MaxTaskIndexBytes)
	}
	return ""
}

// SourcesParked reports whether the journal still holds a parked - abandoned
// or oversized - task over exactly these source blocks. Planning consults this
// so a parked task's blocks are not replanned into a fresh task, which for an
// abandoned set would repeat the worker-killing cycle forever and for an
// oversized one would spam the journal with a new refusal every pass. The
// block set stays parked until an operator releases it (see UnparkPath) or it
// ages out of the journal retention.
func (s *Scheduler) SourcesParked(sources []string) bool {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	want := make(map[string]struct{}, len(sources))
	for _, b := range sources {
		want[b] = struct{}{}
	}
	for _, e := range s.journal.Tasks {
		if !e.State.Parked() || len(e.Task.SourceBlocks) != len(want) {
			continue
		}
		match := true
		for _, b := range e.Task.SourceBlocks {
			if _, ok := want[b]; !ok {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
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

		// A task that keeps losing its worker without ever reporting is treated as
		// poisonous: retrying it forever would take the whole fleet down with it.
		if p.entry.Attempts >= s.conf.MaxAttempts {
			p.entry.State = StateAbandoned
			p.entry.LastError = &TaskError{Outcome: OutcomeAbandoned, Message: "task abandoned after repeatedly losing its worker"}
			s.m.abandonedTasks.Inc()
			s.m.tasksTotal.WithLabelValues(string(p.entry.Task.Type), string(OutcomeAbandoned)).Inc()
			level.Error(s.logger).Log("msg", "giving up on task after repeatedly losing its worker; "+
				"its source blocks are parked, investigate before unparking", "task", id, "attempts", p.entry.Attempts)
			s.finishLocked(id, Result{TaskID: id, Outcome: OutcomeAbandoned, ErrorMessage: "task abandoned after repeatedly losing its worker"})
			continue
		}

		p.entry.State = StatePending
		s.queue = append(s.queue, id)
	}
}

// persistLocked writes the journal, verifying first that no other manager has
// taken it over.
func (s *Scheduler) persistLocked(ctx context.Context) error {
	current, err := ReadJournal(ctx, s.bkt, s.conf.JournalID)
	if err == nil && current != nil && (current.Generation != s.journal.Generation || current.Owner != s.ownerID) {
		// Another manager owns this journal now. The owner ID matters as much as
		// the generation: two managers starting at the same time both bump the
		// same generation, so only the owner tells them apart. Two managers
		// writing the same journal is a misconfiguration this design cannot
		// recover from, so stop.
		return compact.NewHaltError(errors.Errorf(
			"journal %s was taken over by another manager (generation %d owner %q, ours is %d owner %q); "+
				"only one compactor manager may run per shard",
			s.conf.JournalID, current.Generation, current.Owner, s.journal.Generation, s.ownerID))
	}

	if err == nil {
		err = WriteJournal(ctx, s.bkt, s.journal)
	}
	if err != nil {
		s.m.journalWriteFails.Inc()
		if s.journalUnavailableSince.IsZero() {
			s.journalUnavailableSince = time.Now()
		}
		if time.Since(s.journalUnavailableSince) > s.conf.JournalUnavailableTimeout {
			return compact.NewHaltError(errors.Wrapf(err, "journal has been unwritable for %s", s.conf.JournalUnavailableTimeout))
		}
		return errors.Wrap(err, "write journal")
	}

	s.journalUnavailableSince = time.Time{}
	s.lastPersist = time.Now()
	s.m.journalWrites.Inc()
	return nil
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
	if oldest.IsZero() {
		s.m.oldestPendingSecs.Set(0)
	} else {
		s.m.oldestPendingSecs.Set(time.Since(oldest).Seconds())
	}

	active := 0
	for _, seen := range s.workerSeen {
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
	ctx := context.Background()

	// Operators release parked tasks by writing a marker per task; read them
	// before taking the state lock, listing the bucket is slow.
	unpark, err := s.unparkRequests(ctx)
	if err != nil {
		level.Warn(s.logger).Log("msg", "could not list unpark requests; retrying on the next tick", "err", err)
	}

	if err := s.maintainLocked(ctx, unpark); err != nil {
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

func (s *Scheduler) maintainLocked(ctx context.Context, unpark []string) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.expireLeasesLocked()

	// Terminal entries age out here, while the manager runs, so the journal
	// stays bounded and a parked set is released after the retention as
	// promised, not only on the next restart.
	dirty := s.journal.Prune(s.conf.JournalRetention, time.Now()) > 0

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
// Because Execute blocks until the task reaches a terminal state, the compactor's
// existing --compact.concurrency becomes the number of tasks the manager keeps in
// flight, and the whole control loop around it, including how it reacts to halt
// and retry errors, is reused unchanged.
type RemotePlanExecutor struct {
	logger  log.Logger
	bkt     objstore.Bucket
	sched   *Scheduler
	planner compact.Planner

	// maxInflightPerGroup bounds how many plans for one group are worked on at
	// the same time.
	maxInflightPerGroup int

	// journalID names this manager in the deletion marks it writes.
	journalID string
}

// NewRemotePlanExecutor returns an executor that dispatches plans to workers.
//
// The planner is used to look for further, disjoint work in the same group while
// the first plan is still running. That is what lets a single block stream be
// compacted by several workers at once, which one process cannot do because a
// compaction job is single threaded.
func NewRemotePlanExecutor(logger log.Logger, bkt objstore.Bucket, sched *Scheduler, planner compact.Planner, maxInflightPerGroup int) *RemotePlanExecutor {
	if maxInflightPerGroup <= 0 {
		maxInflightPerGroup = 1
	}
	return &RemotePlanExecutor{
		logger:              logger,
		bkt:                 bkt,
		sched:               sched,
		planner:             planner,
		maxInflightPerGroup: maxInflightPerGroup,
		journalID:           sched.conf.JournalID,
	}
}

// Execute implements compact.PlanExecutor.
func (e *RemotePlanExecutor) Execute(ctx context.Context, _ string, cg *compact.Group, toCompact []*metadata.Meta, overlappingBlocks bool) ([]ulid.ULID, error) {
	plans := e.planGroup(ctx, cg, toCompact)

	type outcome struct {
		ids []ulid.ULID
		err error
	}
	outcomes := make([]outcome, len(plans))

	var wg sync.WaitGroup
	for i, plan := range plans {
		wg.Add(1)
		go func(i int, plan []*metadata.Meta) {
			defer wg.Done()
			ids, err := e.runPlan(ctx, cg, plan, overlappingBlocks)
			outcomes[i] = outcome{ids: ids, err: err}
		}(i, plan)
	}
	wg.Wait()

	var (
		compIDs  []ulid.ULID
		firstErr error
		deferred int
	)
	for _, o := range outcomes {
		compIDs = append(compIDs, o.ids...)
		if o.err == nil {
			continue
		}
		if errors.Is(o.err, compact.ErrPlanDeferred) {
			deferred++
			continue
		}
		// A halt outweighs anything else: it means something is wrong that more
		// compaction would only make worse.
		if firstErr == nil || (compact.IsHaltError(o.err) && !compact.IsHaltError(firstErr)) {
			firstErr = o.err
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}
	if deferred == len(outcomes) {
		// Nothing ran at all: every plan sits parked. Tell the control loop so
		// it does not rerun the group into the same deferrals forever.
		return nil, compact.ErrPlanDeferred
	}
	return compIDs, nil
}

// planGroup returns the plan it was given plus any further plans for the same
// group that share no source blocks with it and do not overlap it in time.
//
// Disjoint sources alone are not enough: the planner selects range parts
// "potentially with gaps", so a later plan's blocks can bracket an earlier
// plan's range, and the two compacted outputs would overlap. The overlap halt
// guard the in-process compactor runs cannot catch this - each worker sees
// only its own task's sources - so time disjointness is enforced here, where
// every in-flight plan is known. A plan's output spans the envelope of its
// sources, so envelopes are what must not intersect.
func (e *RemotePlanExecutor) planGroup(ctx context.Context, cg *compact.Group, toCompact []*metadata.Meta) [][]*metadata.Meta {
	plans := [][]*metadata.Meta{toCompact}
	if e.planner == nil || e.maxInflightPerGroup <= 1 {
		return plans
	}

	type span struct{ min, max int64 }
	envelope := func(ms []*metadata.Meta) span {
		s := span{min: ms[0].MinTime, max: ms[0].MaxTime}
		for _, m := range ms[1:] {
			s.min = min(s.min, m.MinTime)
			s.max = max(s.max, m.MaxTime)
		}
		return s
	}
	taken := []span{envelope(toCompact)}

	inflight := map[ulid.ULID]struct{}{}
	for _, m := range toCompact {
		inflight[m.ULID] = struct{}{}
	}

	for len(plans) < e.maxInflightPerGroup {
		errChan := make(chan error, 1)
		next, _, err := cg.PlanExcluding(ctx, e.planner, inflight, errChan)
		if err != nil {
			// The plan we already have is good, so a failure to find more work is
			// not worth failing the group over; it will be looked for again on the
			// next pass.
			level.Warn(e.logger).Log("msg", "could not plan further work for the group", "group", cg.Key(), "err", err)
			break
		}
		if len(next) == 0 {
			break
		}
		ns := envelope(next)
		overlaps := false
		for _, t := range taken {
			if ns.min < t.max && t.min < ns.max {
				overlaps = true
				break
			}
		}
		if overlaps {
			// This plan runs on the next pass, once the work it brackets is done.
			level.Info(e.logger).Log("msg", "further plan overlaps in-flight work; deferring it to the next pass",
				"group", cg.Key(), "span_min", ns.min, "span_max", ns.max)
			break
		}
		taken = append(taken, ns)
		for _, m := range next {
			inflight[m.ULID] = struct{}{}
		}
		plans = append(plans, next)
	}

	if len(plans) > 1 {
		level.Info(e.logger).Log("msg", "dispatching several plans for one group",
			"group", cg.Key(), "plans", len(plans))
	}
	return plans
}

// runPlan hands one plan to a worker and waits for it to finish.
func (e *RemotePlanExecutor) runPlan(ctx context.Context, cg *compact.Group, toCompact []*metadata.Meta, overlappingBlocks bool) ([]ulid.ULID, error) {
	task, err := CompactionTask(cg, toCompact, overlappingBlocks)
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

	if err := ReconstructError(res); err != nil {
		return nil, err
	}
	if res.Outcome.Aborted() {
		// The worker discarded its work, so there is nothing to verify and nothing
		// went wrong with the compaction itself. Reporting no blocks and no error
		// lets the compactor move on and pick the work up again on the next pass.
		level.Info(e.logger).Log("msg", "worker discarded task without uploading", "task", task.ID, "outcome", res.Outcome)
		return nil, nil
	}

	return e.verifyAndFinalize(ctx, cg, toCompact, res)
}

// verifyAndFinalize checks that what a worker claims to have uploaded is really
// in the bucket and really is the result of this plan, then marks the source
// blocks for deletion. Workers never touch source blocks; only the manager does.
//
// The provenance checks are what stands between a confused worker and data
// loss: the sources are deleted below on the strength of this verification, so
// a block that merely exists is not good enough. It has to carry this group's
// labels and resolution, sit inside the plan's time range, and together the
// outputs have to account for every source in the plan.
func (e *RemotePlanExecutor) verifyAndFinalize(ctx context.Context, cg *compact.Group, toCompact []*metadata.Meta, res Result) ([]ulid.ULID, error) {
	// An output block records the union of its parents' sources, not the
	// parents' ULIDs, so that union is what the outputs must account for.
	expected := map[ulid.ULID]struct{}{}
	planMinTime, planMaxTime := toCompact[0].MinTime, toCompact[0].MaxTime
	for _, m := range toCompact {
		for _, s := range m.Compaction.Sources {
			expected[s] = struct{}{}
		}
		if m.MinTime < planMinTime {
			planMinTime = m.MinTime
		}
		if m.MaxTime > planMaxTime {
			planMaxTime = m.MaxTime
		}
	}

	covered := map[ulid.ULID]struct{}{}
	outMetas := make([]metadata.Meta, 0, len(res.OutputBlocks))
	compIDs := make([]ulid.ULID, 0, len(res.OutputBlocks))
	for _, raw := range res.OutputBlocks {
		id, err := ulid.Parse(raw)
		if err != nil {
			return nil, compact.NewRetryError(errors.Wrapf(err, "worker reported an unparseable block ID %q", raw))
		}

		rawMeta, err := readRawMeta(ctx, e.bkt, id)
		if err != nil {
			return nil, compact.NewRetryError(errors.Wrapf(err, "verify result block %s reported by worker", id))
		}
		sum, ok := res.OutputChecksums[raw]
		if !ok || sum == "" {
			// The checksum is what binds the reported result to the metadata the
			// worker observed after its upload; without it the block in the
			// bucket could be anything. Workers always report it, so its absence
			// is a verification failure, not a matter of degree.
			return nil, compact.NewRetryError(errors.Errorf(
				"worker reported no checksum for result block %s", id))
		}
		if got := checksumOf(rawMeta); got != sum {
			return nil, compact.NewRetryError(errors.Errorf(
				"result block %s metadata does not match the checksum the worker reported: got %s, reported %s", id, got, sum))
		}

		var meta metadata.Meta
		if err := json.Unmarshal(rawMeta, &meta); err != nil {
			return nil, compact.NewRetryError(errors.Wrapf(err, "unmarshal metadata of result block %s", id))
		}

		if meta.ULID.Compare(id) != 0 {
			return nil, compact.NewRetryError(errors.Errorf("result block %s holds metadata for %s", id, meta.ULID))
		}
		if err := verifyProvenance(&meta, Provenance{
			TaskID: res.TaskID, TaskType: TaskCompaction, JournalID: e.journalID, Generation: res.Generation,
		}); err != nil {
			return nil, compact.NewRetryError(errors.Wrapf(err, "result block %s", id))
		}
		if !labels.Equal(labels.FromMap(meta.Thanos.Labels), cg.Labels()) {
			return nil, compact.NewRetryError(errors.Errorf(
				"result block %s carries labels %v, the group has %v", id, meta.Thanos.Labels, cg.Labels()))
		}
		if meta.Thanos.Downsample.Resolution != cg.Resolution() {
			return nil, compact.NewRetryError(errors.Errorf(
				"result block %s has resolution %d, the group has %d", id, meta.Thanos.Downsample.Resolution, cg.Resolution()))
		}
		if meta.MinTime < planMinTime || meta.MaxTime > planMaxTime {
			return nil, compact.NewRetryError(errors.Errorf(
				"result block %s spans [%d, %d], outside the plan's [%d, %d]", id, meta.MinTime, meta.MaxTime, planMinTime, planMaxTime))
		}
		for _, s := range meta.Compaction.Sources {
			if _, ok := expected[s]; !ok {
				return nil, compact.NewRetryError(errors.Errorf(
					"result block %s was compacted from %s, which is not a source of this plan", id, s))
			}
			covered[s] = struct{}{}
		}

		outMetas = append(outMetas, meta)
		compIDs = append(compIDs, id)
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
			return nil, compact.NewRetryError(errors.Errorf(
				"the reported result blocks do not account for source %s; refusing to delete the plan's sources", s))
		}
	}

	// The same goes for time: claiming every source is not enough if the
	// outputs span less than the plan does - the missing range would be deleted
	// with the sources. Compacted blocks inherit the union of their parents'
	// ranges, so the outputs have to cover the plan's span exactly, without
	// gaps between them.
	if err := verifyTimeCoverage(outMetas, planMinTime, planMaxTime); err != nil {
		return nil, compact.NewRetryError(err)
	}

	// Mark the sources for deletion now that the result is known to be in the
	// bucket, so the next planning cycle does not pick them up again.
	for _, meta := range toCompact {
		if err := block.MarkForDeletion(ctx, e.logger, e.bkt, meta.ULID, DeletionDetails(e.journalID, res.TaskID), cg.BlocksMarkedForDeletion()); err != nil {
			return nil, compact.NewRetryError(errors.Wrapf(err, "mark source block %s for deletion", meta.ULID))
		}
	}
	return compIDs, nil
}

// readRawMeta returns the raw bytes of a block's meta.json in the bucket.
func readRawMeta(ctx context.Context, bkt objstore.Bucket, id ulid.ULID) ([]byte, error) {
	r, err := bkt.Get(ctx, path.Join(id.String(), block.MetaFilename))
	if err != nil {
		return nil, err
	}
	defer func() { _ = r.Close() }()
	return io.ReadAll(r)
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
func CompactionTask(cg *compact.Group, toCompact []*metadata.Meta, overlappingBlocks bool) (Task, error) {
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
		OverlappingBlocks:  overlappingBlocks,
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
