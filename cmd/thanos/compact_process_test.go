// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
)

// TestCompactorManagerWorkerProcesses runs the real binary as a standalone
// compactor over one bucket and as a manager with two workers over an
// identical bucket, and requires both to serve the same samples. The
// subprocess entry point and the fixture are shared with the standalone
// process test.
func TestCompactorManagerWorkerProcesses(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	f := newCompactorProcessFixture(t, ctx, 2)

	// Omitting compact.mode must keep the production standalone behavior.
	f.Wait(f.Start(f.Args(f.Dirs[0], "127.0.0.1:0")))

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	testutil.Ok(t, err)
	address := listener.Addr().String()
	testutil.Ok(t, listener.Close())
	managerArgs := append(f.Args(f.Dirs[1], address), "--compact.mode=manager", "--compact.manager.journal-id=process-test")
	manager := f.Start(managerArgs)
	for _, worker := range []string{"one", "two"} {
		f.Start(append(f.Args(f.Dirs[1], "127.0.0.1:0"), "--compact.mode=worker", "--compact.manager.journal-id=process-test", "--compact.worker.id="+worker,
			"--compact.worker.manager-address="+address, "--compact.worker.poll-interval=25ms", "--compact.worker.heartbeat-interval=25ms"))
	}
	f.Wait(manager)
	want := processBucketSamples(t, f.Buckets[0])
	got := processBucketSamples(t, f.Buckets[1])
	testutil.Assert(t, len(want) > 0, "the standalone reference must not be empty")
	testutil.Equals(t, want, got, "real manager/worker processes must preserve every standalone sample")
}
