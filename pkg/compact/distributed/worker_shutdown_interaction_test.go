// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/logutil"
)

type shutdownCompactor struct {
	compact.Compactor
	cancel context.CancelFunc
}

func (c shutdownCompactor) CompactWithBlockPopulator(dest string, dirs []string, open []*tsdb.Block, populator tsdb.BlockPopulator) ([]ulid.ULID, error) {
	c.cancel()
	return c.Compactor.CompactWithBlockPopulator(dest, dirs, open, populator)
}

func TestWorkerShutdownAtExecutionStages(t *testing.T) {
	for _, taskType := range []TaskType{TaskCompaction, TaskDownsample} {
		for _, stage := range []string{"metadata", "download", "compaction", "ownership check", "upload", "checksum"} {
			if taskType == TaskDownsample && stage == "compaction" {
				continue
			}
			t.Run(string(taskType)+"/"+stage, func(t *testing.T) {
				c := newTestCluster(t)
				cg, metas := c.makeGroup(labels.FromStrings("ext", "1"))
				task, err := CompactionTask(cg, metas, false)
				testutil.Ok(t, err)
				if taskType == TaskDownsample {
					task = DownsampleTask(metas[0], downsample.ResLevel1, metadata.NoneFunc, 1, false, nil)
				}
				_, err = c.sched.Submit(t.Context(), task)
				testutil.Ok(t, err)
				leased, err := c.sched.Lease(t.Context(), LeaseRequest{WorkerID: "w"})
				testutil.Ok(t, err)
				testutil.Assert(t, leased != nil, "task must be leased")
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				comp, err := tsdb.NewLeveledCompactor(ctx, nil, logutil.GoKitLogToSlog(c.logger), []int64{1000, 3000}, nil, nil)
				testutil.Ok(t, err)
				var executor compact.Compactor = comp
				if stage == "compaction" {
					executor = shutdownCompactor{Compactor: comp, cancel: cancel}
				}
				bkt := &hookBucket{Bucket: c.shared}
				bkt.onGet = func(_ context.Context, name string) error {
					if stage == "checksum" && strings.HasSuffix(name, "meta.json") {
						isSource := false
						for _, m := range metas {
							isSource = isSource || strings.HasPrefix(name, m.ULID.String()+"/")
						}
						if !isSource {
							cancel()
							return ctx.Err()
						}
					}
					if stage == "metadata" && strings.HasSuffix(name, "meta.json") || stage == "download" && strings.Contains(name, "/chunks/") || stage == "ownership check" && strings.HasPrefix(name, JournalPrefix) {
						cancel()
						return ctx.Err()
					}
					return nil
				}
				bkt.onUpload = func(_ context.Context, _ string) error {
					if stage == "upload" {
						cancel()
						return ctx.Err()
					}
					return nil
				}
				w, err := NewWorker(c.logger, bkt, nil, executor, prometheus.NewRegistry(), WorkerConfig{JournalID: journalID, DataDir: t.TempDir()})
				testutil.Ok(t, err)
				result := w.execute(ctx, *leased, newAtomicBool(true))
				testutil.Equals(t, OutcomeAbortedWorkerShutdown, result.Outcome)
				testutil.Ok(t, ReconstructError(result))
				testutil.Ok(t, c.sched.Report(t.Context(), result))
				entry := c.journalTask(StatePending)
				testutil.Assert(t, entry != nil, "shutdown must requeue the task")
				testutil.Equals(t, 0, entry.Attempts)
			})
		}
	}
}

// TestWorkerShutdownReportsAbortNotHalt pins down that a worker being asked to
// shut down mid-task reports an abort, not a halt. The compaction seam wraps a
// cancelled compaction in a halt error, and before the shutdown triage that
// halt travelled to the manager and stopped the whole shard.
func TestWorkerShutdownReportsAbortNotHalt(t *testing.T) {
	c := newTestCluster(t)

	w1 := c.startWorker("w1")
	_ = gateChunks(w1) // Hold w1 mid-download so the shutdown arrives mid-task.

	cg, toCompact := c.makeGroup(labels.FromStrings("ext", "1"))
	outcome := c.execute(cg, toCompact)

	c.waitFor("w1 to lease the task", func() bool {
		e := c.journalTask(StateLeased)
		return e != nil && e.Lease.WorkerID == "w1"
	})

	// Shut w1 down. Its report still goes out on a background context.
	w1.cancel()
	<-w1.done

	c.waitFor("the shutdown abort to be reported and the task requeued", func() bool {
		e := c.journalTask(StatePending)
		return e != nil && e.LastError != nil && e.LastError.Outcome == OutcomeAbortedWorkerShutdown
	})
	testutil.Equals(t, 1.0, counterValue(t, w1.reg, "thanos_compact_worker_tasks_total", string(OutcomeAbortedWorkerShutdown)))

	// The shard is not halted: another worker picks the task up and finishes.
	c.startWorker("w2")
	var got executeOutcome
	select {
	case got = <-outcome:
	case <-time.After(30 * time.Second):
		t.Fatal("compaction did not finish after the worker restart")
	}
	testutil.Ok(t, got.err)
	testutil.Equals(t, 1, len(got.compIDs))
}
