// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"bytes"
	"context"
	"encoding/json"
	"path"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
)

func TestDispatchDownsamplingRejectsInvalidResults(t *testing.T) {
	for _, bad := range []string{"empty result", "duplicate result", "wrong block ID", "wrong labels", "wrong sources", "wrong time range", "wrong resolution", "wrong provenance", "missing checksum", "wrong checksum", "missing metadata"} {
		t.Run(bad, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			bkt := objstore.NewInMemBucket()
			s := testScheduler(t, bkt, ManagerConfig{})
			source := &metadata.Meta{}
			source.ULID = ulid.MustNew(1, nil)
			source.MaxTime = downsample.ResLevel1DownsampleRange
			source.Compaction.Sources = []ulid.ULID{source.ULID}
			source.Thanos.Labels = map[string]string{"tenant": "one"}
			done := make(chan error, 1)
			go func() {
				done <- DispatchDownsampling(ctx, log.NewNopLogger(), bkt, s, map[ulid.ULID]*metadata.Meta{source.ULID: source}, 1, metadata.NoneFunc, 1, false)
			}()
			var task *Task
			for task == nil && ctx.Err() == nil {
				var err error
				task, err = s.Lease(ctx, LeaseRequest{WorkerID: "w"})
				testutil.Ok(t, err)
				if task == nil {
					time.Sleep(time.Millisecond)
				}
			}
			testutil.Assert(t, task != nil, "downsampling must dispatch a task")
			id := ulid.MustNew(2, nil)
			out := *source
			out.ULID = id
			out.Thanos.Downsample.Resolution = downsample.ResLevel1
			prov := Provenance{TaskID: task.ID, TaskType: TaskDownsample, JournalID: s.conf.JournalID, Generation: task.Generation, WorkerID: "w"}.For(id, task.SourceBlocks)
			var err error
			out.Thanos.Extensions, err = prov.Stamp(nil)
			testutil.Ok(t, err)
			result := Result{TaskID: task.ID, Generation: task.Generation, LeaseToken: task.LeaseToken, Outcome: OutcomeCompleted, OutputBlocks: []string{id.String()}}
			switch bad {
			case "empty result":
				result.OutputBlocks = nil
			case "duplicate result":
				result.OutputBlocks = append(result.OutputBlocks, id.String())
			case "wrong block ID":
				out.ULID = ulid.MustNew(3, nil)
				out.Thanos.Extensions, err = prov.For(out.ULID, task.SourceBlocks).Stamp(nil)
				testutil.Ok(t, err)
			case "wrong labels":
				out.Thanos.Labels = map[string]string{"tenant": "another"}
			case "wrong sources":
				out.Compaction.Sources = []ulid.ULID{ulid.MustNew(3, nil)}
			case "wrong time range":
				out.MaxTime--
			case "wrong resolution":
				out.Thanos.Downsample.Resolution = downsample.ResLevel2
			case "wrong provenance":
				out.Thanos.Extensions = nil
			}
			raw, err := json.Marshal(out)
			testutil.Ok(t, err)
			if bad != "missing metadata" {
				testutil.Ok(t, bkt.Upload(ctx, path.Join(id.String(), "meta.json"), bytes.NewReader(raw)))
			}
			result.OutputChecksums = map[string]string{id.String(): checksumOf(raw)}
			if bad == "missing checksum" {
				result.OutputChecksums = nil
			}
			if bad == "wrong checksum" {
				result.OutputChecksums[id.String()] = "sha256:wrong"
			}
			testutil.Ok(t, s.Report(ctx, result))
			select {
			case err := <-done:
				testutil.NotOk(t, err)
			case <-ctx.Done():
				t.Fatal("manager did not return the verification failure")
			}
			testutil.Assert(t, !deletionMarked(t, bkt, source.ULID), "a downsample result must never delete its source")
		})
	}
}
