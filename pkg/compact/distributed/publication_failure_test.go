// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"bytes"
	"context"
	"io"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compact/compacttest"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/logutil"
	"github.com/thanos-io/thanos/pkg/runutil"
)

// publicationFault can lose the acknowledgement after a successful write,
// which is different from rejecting a write before it reaches object storage.
type publicationFault struct {
	objstore.Bucket
	stage string
	after bool
	hits  int
}

func (b *publicationFault) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	if !strings.HasPrefix(name, JournalPrefix) && (strings.Contains(name, "/"+b.stage+"/") || strings.HasSuffix(name, "/"+b.stage)) {
		b.hits++
		if b.after {
			if err := b.Bucket.Upload(ctx, name, r, opts...); err != nil {
				return err
			}
		}
		return errors.New("injected publication failure")
	}
	return b.Bucket.Upload(ctx, name, r, opts...)
}

func sourceObjects(t *testing.T, bkt objstore.Bucket, sources []string) map[string][]byte {
	t.Helper()
	objects := map[string][]byte{}
	for _, id := range sources {
		testutil.Ok(t, bkt.Iter(t.Context(), id+"/", func(name string) (err error) {
			r, err := bkt.Get(t.Context(), name)
			if err != nil {
				return err
			}
			defer runutil.CloseWithErrCapture(&err, r, "close %s", name)
			objects[name], err = io.ReadAll(r)
			return err
		}, objstore.WithRecursiveIter()))
	}
	return objects
}

func resultContent(t *testing.T, bkt objstore.Bucket, result Result) *compacttest.BucketDump {
	t.Helper()
	d := compacttest.NewBucketDump()
	for _, raw := range result.OutputBlocks {
		m, err := block.DownloadMeta(t.Context(), nil, bkt, ulid.MustParse(raw))
		testutil.Ok(t, err)
		d.ReadBlock(t, t.Context(), bkt, t.TempDir(), &m)
	}
	d.Sort()
	return d
}

// TestWorkerPublicationFailuresPreserveData covers compaction and BOTH
// downsampling transitions with real indexes/chunks. Every failed publication
// must leave the sources byte-for-byte intact, and retrying must produce the
// same samples and aggregates as a clean execution of the task.
func TestWorkerPublicationFailuresPreserveData(t *testing.T) {
	if testing.Short() {
		t.Skip("runs real TSDB compactions and downsamplings")
	}
	for _, kind := range []string{"compaction", "raw-to-5m", "5m-to-1h"} {
		for _, stage := range []string{"chunks", "index", "meta.json"} {
			for _, after := range []bool{false, true} {
				name := kind + "/" + stage + "/before-write"
				if after {
					name = kind + "/" + stage + "/lost-acknowledgement"
				}
				t.Run(name, func(t *testing.T) {
					// These tests execute directly, without the worker heartbeat loop.
					c := newTestClusterConf(t, ManagerConfig{LeaseTTL: time.Minute})
					cg, metas := c.makeGroup(labels.FromStrings("tenant", "one"))
					comp, err := tsdb.NewLeveledCompactor(t.Context(), nil, logutil.GoKitLogToSlog(c.logger), []int64{1000, 3000}, downsample.NewPool(), nil)
					testutil.Ok(t, err)
					w, err := NewWorker(c.logger, c.shared, nil, comp, prometheus.NewRegistry(), WorkerConfig{JournalID: journalID, DataDir: t.TempDir()})
					testutil.Ok(t, err)
					lease := func(task Task) Task {
						_, submitErr := c.sched.Submit(t.Context(), task)
						testutil.Ok(t, submitErr)
						leased, leaseErr := c.sched.Lease(t.Context(), LeaseRequest{WorkerID: "w"})
						testutil.Ok(t, leaseErr)
						testutil.Assert(t, leased != nil, "task must be leased")
						return *leased
					}
					task, err := CompactionTask(cg, compact.Plan{Sources: metas})
					testutil.Ok(t, err)
					if kind != "compaction" {
						m := metas[0]
						if kind == "5m-to-1h" {
							prepared := w.execute(t.Context(), lease(DownsampleTask(m, downsample.ResLevel1, metadata.NoneFunc, 1, false, nil)), testAtomicBool(true))
							testutil.Equals(t, OutcomeCompleted, prepared.Outcome)
							testutil.Ok(t, c.sched.Report(t.Context(), prepared))
							meta, downloadErr := block.DownloadMeta(t.Context(), c.logger, c.shared, ulid.MustParse(prepared.OutputBlocks[0]))
							testutil.Ok(t, downloadErr)
							m = &meta
						}
						target := downsample.ResLevel1
						if kind == "5m-to-1h" {
							target = downsample.ResLevel2
						}
						task = DownsampleTask(m, target, metadata.NoneFunc, 1, false, nil)
					}
					task = lease(task)
					before := sourceObjects(t, c.shared, task.SourceBlocks)
					testutil.Assert(t, len(before) >= 3, "the source snapshot must include metadata, index and chunks")

					// Run the same task in a separate bucket as the content oracle.
					clean := objstore.NewInMemBucket()
					for name, body := range before {
						testutil.Ok(t, clean.Upload(t.Context(), name, bytes.NewReader(body)))
					}
					j, err := ReadJournal(t.Context(), c.shared, journalID)
					testutil.Ok(t, err)
					testutil.Ok(t, WriteJournal(t.Context(), clean, j))
					w.bkt = clean
					golden := w.execute(t.Context(), task, testAtomicBool(true))
					testutil.Equals(t, OutcomeCompleted, golden.Outcome)
					want := resultContent(t, clean, golden)

					fault := &publicationFault{Bucket: c.shared, stage: stage, after: after}
					w.bkt = fault
					failed := w.execute(t.Context(), task, testAtomicBool(true))
					testutil.Assert(t, fault.hits > 0, "failure injection must fire")
					testutil.Assert(t, failed.Outcome != OutcomeCompleted, "failed publication was accepted")
					testutil.Equals(t, before, sourceObjects(t, c.shared, task.SourceBlocks))
					for _, id := range task.SourceBlocks {
						exists, existsErr := c.shared.Exists(t.Context(), path.Join(id, metadata.DeletionMarkFilename))
						testutil.Ok(t, existsErr)
						testutil.Assert(t, !exists, "worker must never delete a source")
					}
					w.bkt = c.shared
					retried := w.execute(t.Context(), task, testAtomicBool(true))
					testutil.Equals(t, OutcomeCompleted, retried.Outcome)
					testutil.Equals(t, before, sourceObjects(t, c.shared, task.SourceBlocks))
					compacttest.AssertSameContent(t, want, resultContent(t, c.shared, retried), "publication retry")
				})
			}
		}
	}
}

func TestSourceDeletionFailureCanBeRetried(t *testing.T) {
	if testing.Short() {
		t.Skip("runs real TSDB compactions")
	}
	for _, after := range []bool{false, true} {
		t.Run(map[bool]string{false: "before write", true: "lost acknowledgement"}[after], func(t *testing.T) {
			// These tests execute directly, without the worker heartbeat loop.
			c := newTestClusterConf(t, ManagerConfig{LeaseTTL: time.Minute})
			cg, metas := c.makeGroup(labels.FromStrings("tenant", "one"))
			task, err := CompactionTask(cg, compact.Plan{Sources: metas})
			testutil.Ok(t, err)
			_, err = c.sched.Submit(t.Context(), task)
			testutil.Ok(t, err)
			leased, err := c.sched.Lease(t.Context(), LeaseRequest{WorkerID: "w"})
			testutil.Ok(t, err)
			comp, err := tsdb.NewLeveledCompactor(t.Context(), nil, logutil.GoKitLogToSlog(c.logger), []int64{1000, 3000}, nil, nil)
			testutil.Ok(t, err)
			w, err := NewWorker(c.logger, c.shared, nil, comp, prometheus.NewRegistry(), WorkerConfig{JournalID: journalID, DataDir: t.TempDir()})
			testutil.Ok(t, err)
			before := sourceObjects(t, c.shared, task.SourceBlocks)
			result := w.execute(t.Context(), *leased, testAtomicBool(true))
			testutil.Equals(t, OutcomeCompleted, result.Outcome)
			want := resultContent(t, c.shared, result)

			fault := &publicationFault{Bucket: c.shared, stage: metadata.DeletionMarkFilename, after: after}
			executor := NewRemotePlanExecutor(c.logger, fault, c.sched, nil, 1, nil)
			_, err = executor.verifyAndFinalize(t.Context(), cg, compact.Plan{Sources: metas}, result)
			testutil.NotOk(t, err)
			testutil.Assert(t, fault.hits > 0, "deletion failure must be reached after verification")
			afterFailure := sourceObjects(t, c.shared, task.SourceBlocks)
			for name, body := range before {
				testutil.Equals(t, body, afterFailure[name], "source data must remain immutable: %s", name)
			}
			compacttest.AssertSameContent(t, want, resultContent(t, c.shared, result), "source deletion failure cannot damage the replacement")
			executor.bkt = c.shared
			ids, err := executor.verifyAndFinalize(t.Context(), cg, compact.Plan{Sources: metas}, result)
			testutil.Ok(t, err)
			testutil.Equals(t, 1, len(ids))
			for _, m := range metas {
				testutil.Assert(t, deletionMarked(t, c.shared, m.ULID), "retry must finish marking every source")
			}
			// A lost acknowledgement may cause the same successful finalization
			// to run again; it must be idempotent.
			_, err = executor.verifyAndFinalize(t.Context(), cg, compact.Plan{Sources: metas}, result)
			testutil.Ok(t, err)
			compacttest.AssertSameContent(t, want, resultContent(t, c.shared, result), "repeated finalization")
		})
	}
}
