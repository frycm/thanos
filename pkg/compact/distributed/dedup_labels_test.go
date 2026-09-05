// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"context"
	"encoding/json"
	"path"
	"strings"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block/metadata"
)

// TestRebuildGroupStripsDedupReplicaLabels asserts a worker can validate the
// blocks of a deduplication deployment against their group.
//
// With --deduplication.replica-label the manager strips the replica labels from
// block metadata before grouping, so the group's labels don't carry them - but
// the metadata a worker fetches from the bucket still does. The worker has to
// strip fetched metadata the same way, placeholder included, or every block of
// such a deployment fails the group's label validation.
func TestRebuildGroupStripsDedupReplicaLabels(t *testing.T) {
	ctx := context.Background()

	uploadMeta := func(t *testing.T, bkt objstore.Bucket, id ulid.ULID, lbls map[string]string) *metadata.Meta {
		m := &metadata.Meta{}
		m.ULID = id
		m.MinTime, m.MaxTime = 0, 1000
		m.Compaction.Sources = []ulid.ULID{id}
		m.Thanos.Labels = lbls
		raw, err := json.Marshal(m)
		testutil.Ok(t, err)
		testutil.Ok(t, bkt.Upload(ctx, path.Join(id.String(), "meta.json"), strings.NewReader(string(raw))))
		return m
	}

	newWorker := func(t *testing.T, bkt objstore.Bucket) *Worker {
		w, err := NewWorker(log.NewNopLogger(), bkt, nil, nil, prometheus.NewRegistry(), WorkerConfig{
			WorkerID: "w1", JournalID: "shard-a", DataDir: t.TempDir(),
		})
		testutil.Ok(t, err)
		return w
	}

	task := func(groupLabels map[string]string, dedupLabels []string, sources ...ulid.ULID) Task {
		ids := make([]string, 0, len(sources))
		for _, s := range sources {
			ids = append(ids, s.String())
		}
		return Task{
			ID: "t1", Type: TaskCompaction,
			Group: GroupSpec{
				Key:                   "0@test",
				Labels:                groupLabels,
				DedupReplicaLabels:    dedupLabels,
				BlockFilesConcurrency: 1, CompactBlocksFetchConcurrency: 1,
			},
			SourceBlocks:    ids,
			ExpectedMinTime: 0, ExpectedMaxTime: 1000,
		}
	}

	t.Run("replica labels are stripped before validation", func(t *testing.T) {
		bkt := objstore.NewInMemBucket()
		id1, id2 := ulid.MustNew(1, nil), ulid.MustNew(2, nil)
		uploadMeta(t, bkt, id1, map[string]string{"tenant": "t", "receiver_replica": "r0"})
		uploadMeta(t, bkt, id2, map[string]string{"tenant": "t", "receiver_replica": "r1"})

		cg, metas, err := newWorker(t, bkt).rebuildGroup(ctx,
			task(map[string]string{"tenant": "t"}, []string{"receiver_replica"}, id1, id2))
		testutil.Ok(t, err)
		testutil.Equals(t, 2, len(metas))
		testutil.Equals(t, 2, len(cg.IDs()))
	})

	t.Run("without the carried labels validation fails", func(t *testing.T) {
		bkt := objstore.NewInMemBucket()
		id1 := ulid.MustNew(1, nil)
		uploadMeta(t, bkt, id1, map[string]string{"tenant": "t", "receiver_replica": "r0"})

		_, _, err := newWorker(t, bkt).rebuildGroup(ctx,
			task(map[string]string{"tenant": "t"}, nil, id1))
		testutil.NotOk(t, err)
	})

	t.Run("the deduped placeholder is mirrored", func(t *testing.T) {
		// A block whose only labels are replica labels: the manager's remover
		// leaves {<first replica label>: "deduped"}, and the group carries that.
		bkt := objstore.NewInMemBucket()
		id1 := ulid.MustNew(1, nil)
		uploadMeta(t, bkt, id1, map[string]string{"receiver_replica": "r0"})

		_, metas, err := newWorker(t, bkt).rebuildGroup(ctx,
			task(map[string]string{"receiver_replica": "deduped"}, []string{"receiver_replica"}, id1))
		testutil.Ok(t, err)
		testutil.Equals(t, "deduped", metas[0].Thanos.Labels["receiver_replica"])
	})
}
