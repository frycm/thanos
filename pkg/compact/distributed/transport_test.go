// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/discovery/dns"
)

// TestHTTPTransportRoundTrip drives a worker's whole conversation with a manager
// over the real HTTP handlers.
func TestHTTPTransportRoundTrip(t *testing.T) {
	ctx := t.Context()
	bkt := objstore.NewInMemBucket()
	sched := testScheduler(t, bkt, ManagerConfig{})

	mux := http.NewServeMux()
	RegisterServer(mux, log.NewNopLogger(), sched)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := NewHTTPClient(
		log.NewNopLogger(),
		dns.NewProvider(log.NewNopLogger(), prometheus.NewRegistry(), dns.GolangResolverType),
		srv.Listener.Addr().String(),
		5*time.Second,
	)

	// Nothing queued yet.
	task, err := client.Lease(ctx, LeaseRequest{WorkerID: "w1"})
	testutil.Ok(t, err)
	testutil.Assert(t, task == nil, "expected no work")

	resultCh := submitTask(t, sched)

	task, err = client.Lease(ctx, LeaseRequest{WorkerID: "w1", Accepts: []TaskType{TaskCompaction}})
	testutil.Ok(t, err)
	testutil.Assert(t, task != nil, "expected a task")
	testutil.Equals(t, "t1", task.ID)

	hb, err := client.Heartbeat(ctx, HeartbeatRequest{TaskID: task.ID, LeaseToken: task.LeaseToken, Generation: task.Generation})
	testutil.Ok(t, err)
	testutil.Equals(t, true, hb.Acknowledged)

	testutil.Ok(t, client.Report(ctx, Result{
		TaskID: task.ID, LeaseToken: task.LeaseToken, Generation: task.Generation,
		Outcome: OutcomeCompleted, OutputBlocks: []string{"out"},
	}))

	select {
	case res := <-resultCh:
		testutil.Equals(t, OutcomeCompleted, res.Outcome)
		testutil.Equals(t, []string{"out"}, res.OutputBlocks)
	case <-time.After(2 * time.Second):
		t.Fatal("expected the result to reach the submitter")
	}
}

// TestHTTPTransportRejectsTaskTypeWorkerDoesNotAccept asserts a worker is never
// handed a kind of task it did not ask for.
// TestHTTPTransportAcceptsOnlyPost: the task API takes a POST on every
// endpoint and answers any other method with 405 and an Allow header,
// before a handler reads anything.
func TestHTTPTransportAcceptsOnlyPost(t *testing.T) {
	mux := http.NewServeMux()
	RegisterServer(mux, log.NewNopLogger(), testScheduler(t, objstore.NewInMemBucket(), ManagerConfig{}))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	for _, path := range []string{LeasePath, HeartbeatPath, ResultPath} {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+path, nil)
		testutil.Ok(t, err)
		resp, err := srv.Client().Do(req)
		testutil.Ok(t, err)
		testutil.Ok(t, resp.Body.Close())
		testutil.Equals(t, http.StatusMethodNotAllowed, resp.StatusCode, path)
		testutil.Equals(t, http.MethodPost, resp.Header.Get("Allow"), path)
	}
}

func TestHTTPTransportRejectsTaskTypeWorkerDoesNotAccept(t *testing.T) {
	ctx := context.Background()
	sched := testScheduler(t, objstore.NewInMemBucket(), ManagerConfig{})

	_, err := sched.Submit(ctx, Task{ID: "d1", Type: TaskDownsample})
	testutil.Ok(t, err)

	task, err := sched.Lease(ctx, LeaseRequest{WorkerID: "w1", Accepts: []TaskType{TaskCompaction}})
	testutil.Ok(t, err)
	testutil.Assert(t, task == nil, "a compaction-only worker must not get a downsample task")

	task, err = sched.Lease(ctx, LeaseRequest{WorkerID: "w2", Accepts: []TaskType{TaskDownsample}})
	testutil.Ok(t, err)
	testutil.Assert(t, task != nil, "a downsample worker should get it")
}
