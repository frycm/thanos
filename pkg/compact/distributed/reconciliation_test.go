// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/logutil"
)

type heartbeatFailureClient struct {
	TaskClient
	calls atomic.Int64
	block bool
}

func (c *heartbeatFailureClient) Heartbeat(ctx context.Context, _ HeartbeatRequest) (HeartbeatResponse, error) {
	if c.calls.Add(1) == 1 {
		return HeartbeatResponse{Acknowledged: true}, nil
	}
	if c.block {
		<-ctx.Done()
		return HeartbeatResponse{}, ctx.Err()
	}
	return HeartbeatResponse{}, errors.New("manager unreachable")
}

func TestWorkerStopsAfterUnacknowledgedLeaseTTL(t *testing.T) {
	for _, blocked := range []bool{false, true} {
		t.Run(map[bool]string{false: "failed requests", true: "blocked request"}[blocked], func(t *testing.T) {
			client := &heartbeatFailureClient{block: blocked}
			w := &Worker{logger: log.NewNopLogger(), client: client, conf: WorkerConfig{HeartbeatInterval: time.Hour}}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			acknowledged := testAtomicBool(true)
			done := make(chan struct{})
			go func() {
				defer close(done)
				w.heartbeat(ctx, Task{ID: ulid.Make().String(), LeaseTTL: 90 * time.Millisecond}, acknowledged, cancel)
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("worker kept running past an unacknowledged lease TTL")
			}
			testutil.Equals(t, false, acknowledged.Load())
			testutil.NotOk(t, ctx.Err())
			testutil.Assert(t, client.calls.Load() >= 2, "must heartbeat immediately and retry within the TTL")
		})
	}
}

func TestWorkerRejectsTaskPathBeforeTouchingDisk(t *testing.T) {
	root := t.TempDir()
	victim := filepath.Join(root, "victim", "keep")
	testutil.Ok(t, os.MkdirAll(filepath.Dir(victim), 0750))
	testutil.Ok(t, os.WriteFile(victim, []byte("keep"), 0600))
	w := &Worker{conf: WorkerConfig{DataDir: filepath.Join(root, "worker")}}
	for _, id := range []string{"../victim", "..", filepath.Dir(victim), "", "not-a-ulid", strings.ToLower(ulid.Make().String())} {
		res := w.execute(t.Context(), Task{ID: id, Type: TaskCompaction}, testAtomicBool(true))
		testutil.Equals(t, OutcomeFailedRetryable, res.Outcome)
		testutil.Assert(t, strings.Contains(res.ErrorMessage, "ULID"), "must reject the ID before executing: %s", res.ErrorMessage)
		b, err := os.ReadFile(victim)
		testutil.Ok(t, err)
		testutil.Equals(t, "keep", string(b))
	}
	_, err := os.Stat(w.conf.DataDir)
	testutil.Assert(t, os.IsNotExist(err), "invalid tasks must not create directories")
}

func TestWorkerStartupCleansAbandonedTaskDirectories(t *testing.T) {
	dir := t.TempDir()
	abandoned := filepath.Join(dir, ulid.Make().String())
	unrelated := filepath.Join(dir, "operator-files")
	outside := t.TempDir()
	for _, d := range []string{abandoned, unrelated, outside} {
		testutil.Ok(t, os.MkdirAll(d, 0750))
		testutil.Ok(t, os.WriteFile(filepath.Join(d, "keep"), []byte("content"), 0600))
	}
	link := filepath.Join(dir, ulid.Make().String())
	testutil.Ok(t, os.Symlink(outside, link))
	w, err := NewWorker(log.NewNopLogger(), nil, nil, nil, prometheus.NewRegistry(), WorkerConfig{JournalID: "j", DataDir: dir})
	testutil.Ok(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	testutil.Assert(t, errors.Is(w.Run(ctx), context.Canceled), "startup cleanup precedes the lease loop")
	_, err = os.Stat(abandoned)
	testutil.Assert(t, os.IsNotExist(err), "old task downloads must be removed")
	for _, d := range []string{unrelated, outside, link} {
		_, err = os.Stat(filepath.Join(d, "keep"))
		testutil.Ok(t, err)
	}
}

func TestWorkerResourceErrorsDoNotHaltManager(t *testing.T) {
	for _, err := range []error{syscall.ENOSPC, syscall.EMFILE, &os.PathError{Op: "write", Path: "chunks", Err: syscall.ENOSPC}} {
		outcome, _ := ClassifyError(compact.NewHaltError(err))
		testutil.Equals(t, OutcomeFailedRetryable, outcome)
	}
	outcome, _ := ClassifyError(compact.NewHaltError(errors.New("invalid index")))
	testutil.Equals(t, OutcomeFailedHalt, outcome)
}

func TestParkedSourcesRemainParkedWhenPlanGrows(t *testing.T) {
	s := &Scheduler{journal: NewJournal("j", "")}
	s.journal.Tasks["poison"] = &TaskEntry{State: StateAbandoned, Task: Task{SourceBlocks: []string{"a", "b"}}}
	testutil.Equals(t, true, s.SourcesParked([]string{"a", "b", "new"}))
	testutil.Equals(t, true, s.SourcesParked([]string{"b", "new"}))
	testutil.Equals(t, false, s.SourcesParked([]string{"healthy", "new"}))
}

func TestFinalizeRetriesTransientMetadataReads(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		t.Run(map[bool]string{false: "recovers", true: "bounded failure"}[persistent], func(t *testing.T) {
			c := newTestCluster(t)
			cg, metas := c.makeGroup(labels.FromStrings("tenant", "retry"))
			comp, err := tsdb.NewLeveledCompactor(t.Context(), nil, logutil.GoKitLogToSlog(c.logger), []int64{1000, 3000}, downsample.NewPool(), nil)
			testutil.Ok(t, err)
			w, err := NewWorker(c.logger, c.shared, nil, comp, prometheus.NewRegistry(), WorkerConfig{JournalID: journalID, DataDir: t.TempDir()})
			testutil.Ok(t, err)
			task, err := CompactionTask(cg, metas, false)
			testutil.Ok(t, err)
			_, err = c.sched.Submit(t.Context(), task)
			testutil.Ok(t, err)
			leased, err := c.sched.Lease(t.Context(), LeaseRequest{WorkerID: "w"})
			testutil.Ok(t, err)
			res := w.execute(t.Context(), *leased, testAtomicBool(true))
			testutil.Equals(t, OutcomeCompleted, res.Outcome)
			var reads int
			bkt := &hookBucket{Bucket: c.shared, onGet: func(_ context.Context, name string) error {
				if name == res.OutputBlocks[0]+"/"+block.MetaFilename {
					reads++
					if persistent || reads == 1 {
						return errors.New("temporary metadata read failure")
					}
				}
				return nil
			}}
			e := NewRemotePlanExecutor(c.logger, bkt, c.sched, nil, 1, nil)
			ids, err := e.verifyAndFinalize(t.Context(), cg, metas, res, false)
			if persistent {
				testutil.NotOk(t, err)
				testutil.Equals(t, 3, reads)
			} else {
				testutil.Ok(t, err)
				testutil.Equals(t, 2, reads)
				testutil.Equals(t, 1, len(ids))
			}
			for _, m := range metas {
				marked, err := c.shared.Exists(t.Context(), m.ULID.String()+"/deletion-mark.json")
				testutil.Ok(t, err)
				testutil.Equals(t, !persistent, marked)
			}
		})
	}
}

func TestMaintenanceWaitingForJournalDoesNotBlockHeartbeats(t *testing.T) {
	bkt := &hookBucket{Bucket: objstore.NewInMemBucket()}
	s := testScheduler(t, bkt, ManagerConfig{LeaseTTL: time.Minute})
	_, err := s.Submit(t.Context(), Task{ID: "first", Type: TaskCompaction})
	testutil.Ok(t, err)
	task, err := s.Lease(t.Context(), LeaseRequest{WorkerID: "w"})
	testutil.Ok(t, err)
	started, release := make(chan struct{}, 1), make(chan struct{})
	var released atomic.Bool
	unblock := func() {
		if released.CompareAndSwap(false, true) {
			close(release)
		}
	}
	defer unblock()
	bkt.mtx.Lock()
	bkt.onUpload = func(ctx context.Context, name string) error {
		if strings.HasPrefix(name, JournalPrefix) {
			select {
			case started <- struct{}{}:
			default:
			}
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	bkt.mtx.Unlock()
	done := make(chan struct{})
	go func() { defer close(done); _, _ = s.Submit(t.Context(), Task{ID: "second", Type: TaskCompaction}) }()
	<-started
	s.mtx.Lock()
	s.lastPersist = time.Now().Add(-time.Hour)
	seq := s.persistSeq
	s.mtx.Unlock()
	maintenance := make(chan error, 1)
	go func() { maintenance <- s.Maintain() }()
	deadline := time.Now().Add(time.Second)
	for {
		s.mtx.Lock()
		queued := s.persistSeq > seq
		s.mtx.Unlock()
		if queued {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("maintenance never prepared its journal snapshot")
		}
		time.Sleep(time.Millisecond)
	}
	response := make(chan HeartbeatResponse, 1)
	go func() {
		response <- s.Heartbeat(HeartbeatRequest{TaskID: task.ID, LeaseToken: task.LeaseToken, Generation: task.Generation})
	}()
	select {
	case r := <-response:
		testutil.Equals(t, true, r.Acknowledged)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("maintenance held the state lock while waiting for journal I/O")
	}
	unblock()
	<-done
	testutil.Ok(t, <-maintenance)
}
