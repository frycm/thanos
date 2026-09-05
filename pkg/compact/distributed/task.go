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

	// ExpectedSeries and ExpectedIndexBytes estimate how big executing this
	// task is, summed from what the source blocks report about themselves.
	// The manager refuses tasks over its configured worker capacity instead
	// of watching workers die on them.
	ExpectedSeries     uint64 `json:"expected_series,omitempty"`
	ExpectedIndexBytes int64  `json:"expected_index_bytes,omitempty"`

	// TargetResolution is set for TaskDownsample only.
	TargetResolution int64 `json:"target_resolution,omitempty"`

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

	// The worker could not finish because of lease, storage, or lifecycle
	// events. These are not failures of the data being compacted.
	OutcomeAbortedOwnershipLost    Outcome = "aborted_ownership_lost"
	OutcomeAbortedStoreUnreachable Outcome = "aborted_store_unreachable"
	OutcomeAbortedWorkerShutdown   Outcome = "aborted_worker_shutdown"

	// OutcomeAbandoned is not reported by workers: the manager synthesizes it
	// for a task that burned its whole attempt budget without a single report,
	// and parks the task's source blocks.
	OutcomeAbandoned Outcome = "abandoned"

	// OutcomeOversized is recorded by the manager itself for a task it refused
	// to dispatch because it exceeds the configured worker capacity. No worker
	// ever sees such a task.
	OutcomeOversized Outcome = "oversized"
)

// Aborted reports whether execution was interrupted without a data failure.
// Partially uploaded outputs may still be discovered by metadata synchronization.
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
//
// It carries the worker's configuration alongside its identity: the parts
// that must match the manager's for the produced blocks to be correct, and
// that leave no trace in the blocks when they do not. The manager refuses to
// lease to a worker whose configuration does not match, loudly, instead of
// letting the mismatch run.
type LeaseRequest struct {
	WorkerID string     `json:"worker_id"`
	Accepts  []TaskType `json:"accepts"`

	// JournalID is the journal the worker verifies its ownership against. A
	// worker on another journal would abort every task at its ownership check:
	// an invisible livelock, refused here instead.
	JournalID string `json:"journal_id,omitempty"`
	// DedupFunc and DedupReplicaLabels are the worker's merge configuration
	// (--deduplication.func and --deduplication.replica-label). The merge
	// function is baked into the worker's compactor and invisible in its
	// output, so a mismatch has to be caught before any task is handed out.
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
	Stage      string `json:"stage,omitempty"`
}

// HeartbeatResponse tells the worker whether it still owns the task. A worker
// that is not acknowledged has to abort without uploading anything.
type HeartbeatResponse struct {
	Acknowledged bool `json:"acknowledged"`
}
