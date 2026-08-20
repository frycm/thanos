// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"
)

func TestJournalRoundTrip(t *testing.T) {
	ctx := context.Background()
	bkt := objstore.NewInMemBucket()

	// A shard with no journal yet is not an error.
	j, err := ReadJournal(ctx, bkt, "shard-a")
	testutil.Ok(t, err)
	testutil.Equals(t, true, j == nil)

	j = NewJournal("shard-a", "sha256:abc")
	j.Generation = 7
	j.Tasks["task-1"] = &TaskEntry{
		Task:      Task{ID: "task-1", Type: TaskCompaction},
		State:     StateLeased,
		Lease:     &Lease{WorkerID: "w1", Token: "tok", Generation: 7, ExpiresAt: time.Now().Add(time.Minute)},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	testutil.Ok(t, WriteJournal(ctx, bkt, j))

	got, err := ReadJournal(ctx, bkt, "shard-a")
	testutil.Ok(t, err)
	testutil.Equals(t, uint64(7), got.Generation)
	testutil.Equals(t, "sha256:abc", got.SelectorHash)
	testutil.Equals(t, StateLeased, got.Tasks["task-1"].State)
	testutil.Equals(t, "tok", got.Tasks["task-1"].Lease.Token)
}

func TestJournalPrunesOnlyOldTerminalTasks(t *testing.T) {
	now := time.Now()
	j := NewJournal("shard-a", "")
	j.Tasks["old-done"] = &TaskEntry{State: StateCompleted, UpdatedAt: now.Add(-48 * time.Hour)}
	j.Tasks["new-done"] = &TaskEntry{State: StateCompleted, UpdatedAt: now}
	j.Tasks["old-leased"] = &TaskEntry{State: StateLeased, UpdatedAt: now.Add(-48 * time.Hour)}

	testutil.Equals(t, 1, j.Prune(24*time.Hour, now))
	_, ok := j.Tasks["old-done"]
	testutil.Equals(t, false, ok)
	// In-flight work is never pruned, however old it is.
	_, ok = j.Tasks["old-leased"]
	testutil.Equals(t, true, ok)
	_, ok = j.Tasks["new-done"]
	testutil.Equals(t, true, ok)
}

// TestCheckOwnershipFailsClosed asserts a worker only gets a confirmation when
// the journal really still records its lease, and that an unreadable journal is
// reported distinctly from a lost lease.
func TestCheckOwnershipFailsClosed(t *testing.T) {
	ctx := context.Background()
	bkt := objstore.NewInMemBucket()

	j := NewJournal("shard-a", "")
	j.Generation = 3
	j.Tasks["t1"] = &TaskEntry{
		Task:  Task{ID: "t1"},
		State: StateLeased,
		Lease: &Lease{WorkerID: "w1", Token: "tok", Generation: 3, ExpiresAt: time.Now().Add(time.Minute)},
	}
	testutil.Ok(t, WriteJournal(ctx, bkt, j))

	for _, tc := range []struct {
		name       string
		taskID     string
		token      string
		generation uint64
		expect     Ownership
	}{
		{"still ours", "t1", "tok", 3, OwnershipConfirmed},
		{"someone else holds it", "t1", "other-token", 3, OwnershipLost},
		{"a new manager took over", "t1", "tok", 2, OwnershipLost},
		{"task is gone", "gone", "tok", 3, OwnershipLost},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := CheckOwnership(ctx, bkt, "shard-a", tc.taskID, tc.token, tc.generation)
			testutil.Equals(t, tc.expect, got)
		})
	}

	t.Run("journal unreachable is not a lost lease", func(t *testing.T) {
		got, err := CheckOwnership(ctx, failingBucket{bkt}, "shard-a", "t1", "tok", 3)
		testutil.NotOk(t, err)
		testutil.Equals(t, OwnershipUnknown, got)
	})

	t.Run("task no longer leased", func(t *testing.T) {
		j.Tasks["t1"].State = StateCompleted
		testutil.Ok(t, WriteJournal(ctx, bkt, j))
		got, _ := CheckOwnership(ctx, bkt, "shard-a", "t1", "tok", 3)
		testutil.Equals(t, OwnershipLost, got)
	})
}

// failingBucket makes reads fail, to stand in for object storage being
// unreachable.
type failingBucket struct {
	objstore.Bucket
}

func (b failingBucket) Get(_ context.Context, _ string) (io.ReadCloser, error) {
	return nil, errors.New("storage is unreachable")
}
