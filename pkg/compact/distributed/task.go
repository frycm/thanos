// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Package distributed splits Thanos compaction into a manager, which plans work
// and owns the bucket, and workers, which execute one plan at a time.
//
// The manager runs the regular compactor loop but hands every plan it produces
// to a RemotePlanExecutor instead of compacting in process. Workers lease a task,
// compact it, and report the result back. Ownership of in-flight work is tracked
// in a journal in object storage, written only by the manager.
package distributed

import (
	"encoding/json"
	"time"

	"github.com/thanos-io/thanos/pkg/compact"
)

// TaskType describes the unit of work a task represents.
type TaskType string

const (
	// TaskCompaction compacts a set of source blocks into one or more blocks.
	TaskCompaction TaskType = "compaction"
	// TaskDownsample downsamples a single source block to a lower resolution.
	TaskDownsample TaskType = "downsample"
)

// GroupSpec carries everything a worker needs to reconstruct a compaction group.
// Everything else a group holds (metrics, logger, bucket client) is process local.
type GroupSpec struct {
	Key                           string            `json:"key"`
	Labels                        map[string]string `json:"labels"`
	Resolution                    int64             `json:"resolution"`
	AcceptMalformedIndex          bool              `json:"accept_malformed_index"`
	EnableVerticalCompaction      bool              `json:"enable_vertical_compaction"`
	HashFunc                      string            `json:"hash_func"`
	BlockFilesConcurrency         int               `json:"block_files_concurrency"`
	CompactBlocksFetchConcurrency int               `json:"compact_blocks_fetch_concurrency"`
	Extensions                    json.RawMessage   `json:"extensions,omitempty"`

	// DedupReplicaLabels are the external labels the manager removed from block
	// metadata before grouping (--deduplication.replica-label). The group's
	// labels have them removed, but the metadata a worker fetches from the
	// bucket still carries them, so the worker has to remove them the same way
	// before it can validate the blocks against the group.
	DedupReplicaLabels []string `json:"dedup_replica_labels,omitempty"`
	// DedupFunc is the vertical merge function the manager was configured with
	// (--deduplication.func). Which one compacted the block is invisible in the
	// output, so a worker checks it against its own before doing the work.
	DedupFunc string `json:"dedup_func,omitempty"`
}

// Task is one atomic unit of work handed to a worker.
type Task struct {
	ID         string   `json:"id"`
	Generation uint64   `json:"generation"`
	Type       TaskType `json:"type"`

	Group GroupSpec `json:"group"`

	// SourceBlocks are the blocks to operate on. The worker re-reads their
	// metadata from the bucket rather than trusting metadata sent over the wire.
	SourceBlocks []string `json:"source_blocks"`

	// ExpectedMinTime and ExpectedMaxTime bound the source blocks and are used by
	// the worker as a sanity check on what it fetched.
	ExpectedMinTime int64 `json:"expected_min_time"`
	ExpectedMaxTime int64 `json:"expected_max_time"`

	// OverlappingBlocks reports whether the group contained overlapping blocks at
	// planning time. Only possible with vertical compaction enabled.
	OverlappingBlocks bool `json:"overlapping_blocks"`

	// Outputs are the blocks the plan produces, as the manager's planner
	// decided them: each with its external labels and, optionally, the
	// partition of the series it holds. Empty means the one block a
	// compaction has always produced, carrying the group's labels. The
	// worker produces exactly these; it decides nothing about them, since
	// only the manager sees the whole bucket.
	Outputs []compact.PlanOutput `json:"outputs,omitempty"`

	// TargetResolution is set for TaskDownsample only.
	TargetResolution int64 `json:"target_resolution,omitzero"`

	// ExpectedSeries and ExpectedIndexBytes estimate how big executing this
	// task is, summed from the source blocks' own metadata. They exist so the
	// manager can refuse a task no worker could survive, and so an operator
	// reading the journal sees how big a task was. Zero means the sources did
	// not report the figure; an absent figure is never held against a task.
	ExpectedSeries     uint64 `json:"expected_series,omitzero"`
	ExpectedIndexBytes int64  `json:"expected_index_bytes,omitzero"`

	LeaseToken string        `json:"lease_token"`
	LeaseTTL   time.Duration `json:"lease_ttl"`
}

// Outcome is the terminal state a worker reports for a task.
type Outcome string

const (
	OutcomeCompleted Outcome = "completed"

	// The task failed in a way that maps onto one of the compact package's error
	// classes. The manager reconstructs the class so its control loop can react
	// exactly as it would for an in-process failure.
	OutcomeFailedRetryable Outcome = "failed_retryable"
	OutcomeFailedHalt      Outcome = "failed_halt"
	OutcomeFailedIssue347  Outcome = "failed_issue347"
	OutcomeFailedOOOChunks Outcome = "failed_out_of_order_chunks"

	// The worker discarded its work before making it visible. None of these is
	// a compaction failure: the task simply has to be executed again.
	OutcomeAbortedOwnershipLost    Outcome = "aborted_ownership_lost"
	OutcomeAbortedStoreUnreachable Outcome = "aborted_store_unreachable"
	// OutcomeAbortedWorkerShutdown means the worker was asked to shut down
	// (SIGTERM, rolling restart) while executing the task. Reporting it
	// distinctly matters: the underlying error is a context cancellation that
	// would otherwise be classified through the compaction error taxonomy and
	// halt the manager.
	OutcomeAbortedWorkerShutdown Outcome = "aborted_worker_shutdown"

	// OutcomeAbandoned is not reported by workers: the manager synthesizes it
	// when a task repeatedly lost its worker without ever reporting.
	OutcomeAbandoned Outcome = "abandoned"
	// OutcomeOversized is not reported by workers either: the manager
	// synthesizes it for a task it refused to dispatch because its expected
	// size exceeds the configured worker capacity.
	OutcomeOversized Outcome = "oversized"
)

// Aborted reports whether the outcome means the worker threw its work away, or
// at least never confirmed that anything became visible.
func (o Outcome) Aborted() bool {
	return o == OutcomeAbortedOwnershipLost || o == OutcomeAbortedStoreUnreachable || o == OutcomeAbortedWorkerShutdown
}

// Result is what a worker reports back once it reaches a terminal state.
type Result struct {
	TaskID     string `json:"task_id"`
	LeaseToken string `json:"lease_token"`
	Generation uint64 `json:"generation"`

	Outcome Outcome `json:"outcome"`

	// OutputBlocks are the blocks the worker uploaded, with the checksum of each
	// block's meta.json so the manager can verify what landed in the bucket.
	OutputBlocks    []string          `json:"output_blocks,omitempty"`
	OutputChecksums map[string]string `json:"output_checksums,omitempty"`

	// OffendingBlock is set for the issue347 and out-of-order-chunks outcomes.
	OffendingBlock string `json:"offending_block,omitempty"`
	ErrorMessage   string `json:"error_message,omitempty"`
}

// LeaseRequest asks the manager for a task to work on.
type LeaseRequest struct {
	WorkerID string     `json:"worker_id"`
	Accepts  []TaskType `json:"accepts"`
	// JournalID is the journal the worker was configured for. The manager
	// refuses the lease on a mismatch: nothing else ties the two flags
	// together, and a worker verifying its ownership against a different
	// journal than the one scheduling it would abort every task forever.
	JournalID string `json:"journal_id,omitempty"`
	// DedupFunc and DedupReplicaLabels are the worker's merge configuration
	// (--deduplication.func and --deduplication.replica-label). The manager
	// refuses the lease when they differ from its own: the worker's compactor
	// is built from them and would silently merge the sources differently
	// than the manager planned for.
	DedupFunc          string   `json:"dedup_func,omitempty"`
	DedupReplicaLabels []string `json:"dedup_replica_labels,omitempty"`
}

// LeaseResponse carries the leased task, if any was available.
type LeaseResponse struct {
	Task *Task `json:"task,omitempty"`
}

// HeartbeatRequest extends the lease on a task the worker is still working on.
type HeartbeatRequest struct {
	TaskID     string `json:"task_id"`
	LeaseToken string `json:"lease_token"`
	Generation uint64 `json:"generation"`
}

// HeartbeatResponse tells the worker whether it still owns the task. A worker
// that is not acknowledged has to abort without uploading anything.
type HeartbeatResponse struct {
	Acknowledged bool `json:"acknowledged"`
}
