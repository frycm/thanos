// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"time"

	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"
)

// JournalVersion is the schema version of the journal. It is bumped whenever the
// on-disk representation changes in a way older managers cannot read.
const JournalVersion = 1

// JournalPrefix is the top level directory the journals of all shards live in.
const JournalPrefix = "compact-manager"

// TaskState is where a task sits in its lifecycle.
type TaskState string

const (
	// StatePending is queued and waiting for a worker.
	StatePending TaskState = "pending"
	// StateLeased is currently held by a worker.
	StateLeased TaskState = "leased"
	// StateCompleted finished and its worker reported the outputs. Whether the
	// manager has verified them is TaskEntry.Verified.
	StateCompleted TaskState = "completed"
	// StateFailed exhausted its attempts with a reported failure, or was
	// still unfinished when another manager took the journal over.
	StateFailed TaskState = "failed"
	// StateAbandoned repeatedly lost workers or exhausted its abort budget. It is not
	// retried automatically, so that a task that kills workers cannot spin
	// forever; an operator decides what to do with it.
	StateAbandoned TaskState = "abandoned"
	// StateOversized was never dispatched: its expected size exceeds the
	// configured worker capacity. Recorded so an operator can see exactly which
	// plan cannot be executed, instead of discovering it through workers dying.
	StateOversized TaskState = "oversized"
)

// Terminal reports whether no further work will happen for a task in this state.
func (s TaskState) Terminal() bool {
	return s == StateCompleted || s == StateFailed || s == StateAbandoned || s == StateOversized
}

// Parked reports whether the task's source blocks are deliberately withheld
// from planning until an operator intervenes or the entry ages out of the
// journal retention.
func (s TaskState) Parked() bool {
	return s == StateAbandoned || s == StateOversized
}

// Lease records which worker currently holds a task.
type Lease struct {
	WorkerID   string    `json:"worker_id"`
	Token      string    `json:"token"`
	Generation uint64    `json:"generation"`
	ExpiresAt  time.Time `json:"expires_at"`
}

// TaskError records why a task last failed.
type TaskError struct {
	Outcome Outcome `json:"outcome"`
	Block   string  `json:"block,omitempty"`
	Message string  `json:"message,omitempty"`
}

// TaskEntry is the journal's record of one task.
type TaskEntry struct {
	Task Task `json:"task"`

	State TaskState `json:"state"`
	Lease *Lease    `json:"lease,omitempty"`

	Outputs         []string          `json:"outputs,omitempty"`
	OutputChecksums map[string]string `json:"output_checksums,omitempty"`
	// Verified is set once the manager has checked the outputs against the
	// plan and retired the sources. Until then the outputs supersede nothing,
	// however complete they look: the manager's deduplication filter treats
	// a worker's block as published only when its task says so here.
	Verified bool `json:"verified,omitzero"`
	// RejectedOutputs are blocks the worker uploaded for the task that failed
	// verification. They are deleted, here or by maintenance if the delete
	// failed; while listed they are unpublished, and the entry is not pruned.
	RejectedOutputs []string `json:"rejected_outputs,omitempty"`

	Attempts int `json:"attempts"`
	// Aborts counts, separately from Attempts, how often a worker discarded the
	// task without completing it. Aborts are benign in isolation - a lost
	// lease, a store blip, a shutdown - so they do not consume attempts, but an
	// unbounded streak of them would retry the task forever.
	Aborts    int        `json:"aborts,omitzero"`
	LastError *TaskError `json:"last_error,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Journal is the manager's durable record of in-flight and recent work.
//
// It has exactly one writer, the manager that owns the shard. Object storage
// offers no compare-and-swap this design could rely on, so the journal does not
// use it for mutual exclusion: correctness against a task being executed twice
// comes from block deduplication in the bucket, where two results built from the
// same sources reduce to one. Generation exists to detect, not prevent, a second
// manager writing to the same journal.
type Journal struct {
	Version   int    `json:"version"`
	JournalID string `json:"journal_id"`

	// SelectorHash fingerprints the manager's selector relabel config. A journal
	// found with a different hash was written by a differently sharded manager.
	SelectorHash string `json:"selector_hash"`

	// Generation is bumped every time a manager takes ownership of the journal.
	// Leases from an older generation are void.
	Generation uint64 `json:"generation"`

	// Owner identifies the manager instance that currently owns the journal.
	// Generation alone cannot tell two managers apart: two of them starting at
	// the same time both read generation N and both write N+1, and neither would
	// ever notice the other. The owner ID is unique per manager instance, so
	// whichever of them writes last is detected by the other on its next write.
	Owner string `json:"owner"`

	// UpdatedAt is when the owning manager last wrote the journal. A running
	// manager writes it at least once per lease TTL even when idle, so a
	// journal that has not been written for a few TTLs belongs to no running
	// manager.
	UpdatedAt time.Time             `json:"updated_at"`
	Tasks     map[string]*TaskEntry `json:"tasks"`
}

// NewJournal returns an empty journal for a shard.
func NewJournal(journalID, selectorHash string) *Journal {
	return &Journal{
		Version:      JournalVersion,
		JournalID:    journalID,
		SelectorHash: selectorHash,
		Tasks:        map[string]*TaskEntry{},
	}
}

// JournalPath returns the object storage path of a shard's journal.
func JournalPath(journalID string) string {
	return path.Join(JournalPrefix, journalID, "journal.json")
}

// UnparkPrefix returns the object storage directory under which an operator
// asks a shard's manager to release parked tasks, one empty object named after
// each task ID. The manager drops the parked entry, so its source blocks are
// planned again, and removes the object.
func UnparkPrefix(journalID string) string {
	return path.Join(JournalPrefix, journalID, "unpark") + "/"
}

// UnparkPath returns the object an operator writes to release one parked task.
func UnparkPath(journalID, taskID string) string {
	return UnparkPrefix(journalID) + taskID
}

// ReadJournal loads a shard's journal. It returns a nil journal and no error
// when the shard has no journal yet.
func ReadJournal(ctx context.Context, bkt objstore.Bucket, journalID string) (*Journal, error) {
	r, err := bkt.Get(ctx, JournalPath(journalID))
	if err != nil {
		if bkt.IsObjNotFoundErr(err) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "get journal")
	}
	defer func() { _ = r.Close() }()

	body, err := io.ReadAll(r)
	if err != nil {
		return nil, errors.Wrap(err, "read journal")
	}

	var j Journal
	if err := json.Unmarshal(body, &j); err != nil {
		// A partially written journal is not something to guess about.
		return nil, errors.Wrap(err, "unmarshal journal")
	}
	if j.Version > JournalVersion {
		return nil, errors.Errorf("journal version %d is newer than supported version %d", j.Version, JournalVersion)
	}
	if j.JournalID != journalID {
		// A journal copied under another shard's path - by an operator moving
		// a trial, say - would otherwise be taken over as that shard's, and
		// every write would then go to the path its body names.
		return nil, errors.Errorf("journal at %s says it is journal %q", JournalPath(journalID), j.JournalID)
	}
	if j.Tasks == nil {
		j.Tasks = map[string]*TaskEntry{}
	}
	return &j, nil
}

// WriteJournal replaces a shard's journal. Only the manager may call it.
func WriteJournal(ctx context.Context, bkt objstore.Bucket, j *Journal) error {
	j.Version = JournalVersion
	j.UpdatedAt = time.Now()

	body, err := json.Marshal(j)
	if err != nil {
		return errors.Wrap(err, "marshal journal")
	}
	if err := bkt.Upload(ctx, JournalPath(j.JournalID), bytes.NewReader(body)); err != nil {
		return errors.Wrap(err, "upload journal")
	}
	return nil
}

// Prune drops terminal tasks that finished longer ago than retention, to keep
// the journal from growing without bound.
func (j *Journal) Prune(retention time.Duration, now time.Time) int {
	pruned := 0
	for id, e := range j.Tasks {
		// An entry still naming rejected outputs, or outputs nobody verified,
		// guards the bucket against them: pruned, they would count as
		// published. Maintenance rejects unverified outputs first, but a
		// takeover prunes before any maintenance has run.
		unverified := e.State == StateCompleted && !e.Verified && len(e.Outputs) > 0
		if e.State.Terminal() && !unverified && len(e.RejectedOutputs) == 0 && now.Sub(e.UpdatedAt) > retention {
			delete(j.Tasks, id)
			pruned++
		}
	}
	return pruned
}

// SelectorHash fingerprints a selector relabel config so a manager can notice it
// is reusing the journal of a differently sharded manager.
func SelectorHash(relabelConfigYAML []byte) string {
	sum := sha256.Sum256(relabelConfigYAML)
	return fmt.Sprintf("sha256:%x", sum)
}

// Ownership is the result of a worker asking whether it still owns a task.
type Ownership int

const (
	// OwnershipConfirmed means the journal still records this worker's lease.
	OwnershipConfirmed Ownership = iota
	// OwnershipLost means the journal no longer records this worker's lease, so
	// somebody else may already be doing this work.
	OwnershipLost
	// OwnershipUnknown means the journal could not be read. It is deliberately
	// distinct from OwnershipLost: the worker has to fail closed, but the manager
	// should not treat it as evidence that the task was reassigned.
	OwnershipUnknown
)

func (o Ownership) String() string {
	switch o {
	case OwnershipConfirmed:
		return "confirmed"
	case OwnershipLost:
		return "lost"
	default:
		return "unknown"
	}
}

// CheckOwnership re-reads the journal and reports whether the given lease is
// still the one recorded for the task. Workers call this immediately before
// making their work visible, and fail closed on anything but a confirmation.
//
// grace bounds how stale the recorded lease expiry may be. The manager extends
// leases in memory and persists at least once per lease TTL, so for a healthy
// worker the recorded expiry is at most one TTL behind; a recorded expiry more
// than grace in the past means the worker has been partitioned from the manager
// (whose in-memory revocations it cannot see) or the manager itself is gone.
// Pass the lease TTL as grace; zero disables the check.
func CheckOwnership(ctx context.Context, bkt objstore.Bucket, journalID, taskID, token string, generation uint64, grace time.Duration) (Ownership, error) {
	j, err := ReadJournal(ctx, bkt, journalID)
	if err != nil {
		return OwnershipUnknown, errors.Wrap(err, "read journal for ownership check")
	}
	if j == nil {
		return OwnershipLost, errors.New("journal does not exist")
	}
	if j.Generation != generation {
		return OwnershipLost, errors.Errorf("journal generation is %d, lease was issued for %d", j.Generation, generation)
	}

	e, ok := j.Tasks[taskID]
	if !ok {
		return OwnershipLost, errors.Errorf("task %s is no longer in the journal", taskID)
	}
	if e.State != StateLeased {
		return OwnershipLost, errors.Errorf("task %s is in state %q, not leased", taskID, e.State)
	}
	if e.Lease == nil || e.Lease.Token != token {
		return OwnershipLost, errors.Errorf("task %s is leased to somebody else", taskID)
	}
	if grace > 0 && time.Now().After(e.Lease.ExpiresAt.Add(grace)) {
		return OwnershipLost, errors.Errorf(
			"the journal's lease on task %s expired at %s and was never refreshed; the manager no longer vouches for us",
			taskID, e.Lease.ExpiresAt.Format(time.RFC3339))
	}
	return OwnershipConfirmed, nil
}
