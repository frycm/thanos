// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

// TestLocalPlanExecutorPublishesOutputsAsSet runs a plan with two partitioned
// outputs through the local executor against a bucket and checks that every
// result block records the whole set it belongs to, so that readers can tell
// a complete replacement of the sources from a partial one.
func TestLocalPlanExecutorPublishesOutputsAsSet(t *testing.T) {
	ctx := context.Background()
	bkt := objstore.NewInMemBucket()
	logger := log.NewNopLogger()
	dir := t.TempDir()

	var series []labels.Labels
	for i := range 20 {
		series = append(series, labels.FromStrings("__name__", "metric", "instance", fmt.Sprintf("host-%d", i)))
	}
	ext := labels.FromStrings("ext", "1")
	var sources []*metadata.Meta
	for _, tr := range [][2]int64{{0, time.Hour.Milliseconds()}, {time.Hour.Milliseconds(), 2 * time.Hour.Milliseconds()}} {
		id, err := e2eutil.CreateBlock(ctx, dir, series, 10, tr[0], tr[1], ext, 0, metadata.NoneFunc, nil)
		testutil.Ok(t, err)
		testutil.Ok(t, block.Upload(ctx, logger, bkt, filepath.Join(dir, id.String()), metadata.NoneFunc))
		m, err := metadata.ReadFromDir(filepath.Join(dir, id.String()))
		testutil.Ok(t, err)
		sources = append(sources, m)
	}

	cnt := func() prometheus.Counter { return promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "test"}) }
	cg, err := NewGroup(logger, bkt, "0@test", ext, 0, false, false, cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), metadata.NoneFunc, 1, 1)
	testutil.Ok(t, err)
	for _, m := range sources {
		testutil.Ok(t, cg.AppendMeta(m))
	}
	comp, err := tsdb.NewLeveledCompactor(ctx, nil, slog.Default(), []int64{time.Hour.Milliseconds(), 2 * time.Hour.Milliseconds()}, chunkenc.NewPool(),
		storage.NewCompactingChunkSeriesMerger(storage.ChainedSeriesMerge))
	testutil.Ok(t, err)

	plan := Plan{Sources: sources, Outputs: []PlanOutput{
		{Labels: map[string]string{"ext": "1", "part": "a"}, Series: &SeriesPartition{Index: 0, Count: 2}},
		{Labels: map[string]string{"ext": "1", "part": "b"}, Series: &SeriesPartition{Index: 1, Count: 2}},
	}}
	ex := LocalPlanExecutor{Comp: comp, BlockDeletableChecker: DefaultBlockDeletableChecker{}, Callback: DefaultCompactionLifecycleCallback{}, MarkSourcesForDeletion: true}
	compIDs, err := ex.Execute(ctx, t.TempDir(), cg, plan)
	testutil.Ok(t, err)
	testutil.Equals(t, 2, len(compIDs), "twenty series never all hash to one of two partitions")

	want := slices.Clone(compIDs)
	slices.SortFunc(want, func(a, b ulid.ULID) int { return a.Compare(b) })
	seen := map[int]bool{}
	for _, id := range compIDs {
		m, err := block.DownloadMeta(ctx, logger, bkt, id)
		testutil.Ok(t, err)
		testutil.Assert(t, m.Thanos.Output != nil, "result block %s records no output set", id)
		testutil.Equals(t, 2, m.Thanos.Output.Count)
		got := slices.Clone(m.Thanos.Output.Blocks)
		slices.SortFunc(got, func(a, b ulid.ULID) int { return a.Compare(b) })
		testutil.Equals(t, want, got, "result block %s does not record the whole set", id)
		testutil.Equals(t, plan.Outputs[m.Thanos.Output.Index].Labels, m.Thanos.Labels)
		seen[m.Thanos.Output.Index] = true
		testutil.Equals(t, true, m.Thanos.Published(m.ULID, func(id ulid.ULID) bool { return slices.Contains(compIDs, id) }))
		testutil.Equals(t, false, m.Thanos.Published(m.ULID, func(id ulid.ULID) bool { return id == m.ULID }), "a set with a missing sibling is not published")
		testutil.Equals(t, true, m.Thanos.Published(ulid.MustNew(7, nil), func(ulid.ULID) bool { return false }), "a set that does not name the block is not the block's own")
	}
	testutil.Equals(t, map[int]bool{0: true, 1: true}, seen)

	// The sources were retired only after the whole set was uploaded.
	for _, s := range sources {
		ok, err := bkt.Exists(ctx, filepath.Join(s.ULID.String(), metadata.DeletionMarkFilename))
		testutil.Ok(t, err)
		testutil.Equals(t, true, ok)
	}
}

// TestLocalPlanExecutorRefusesPartitionsThatLoseSeries: a plan whose
// partitions do not together hold every series of the sources - here one
// half of a two-way partition on its own, as a shard re-partitioned under
// another hash would look - is refused before anything is uploaded, since
// its outputs would replace the sources and lose the rest.
func TestLocalPlanExecutorRefusesPartitionsThatLoseSeries(t *testing.T) {
	ctx := context.Background()
	bkt := objstore.NewInMemBucket()
	logger := log.NewNopLogger()
	dir := t.TempDir()

	var series []labels.Labels
	for i := range 20 {
		series = append(series, labels.FromStrings("__name__", "metric", "instance", fmt.Sprintf("host-%d", i)))
	}
	ext := labels.FromStrings("ext", "1")
	var sources []*metadata.Meta
	for _, tr := range [][2]int64{{0, time.Hour.Milliseconds()}, {time.Hour.Milliseconds(), 2 * time.Hour.Milliseconds()}} {
		id, err := e2eutil.CreateBlock(ctx, dir, series, 10, tr[0], tr[1], ext, 0, metadata.NoneFunc, nil)
		testutil.Ok(t, err)
		testutil.Ok(t, block.Upload(ctx, logger, bkt, filepath.Join(dir, id.String()), metadata.NoneFunc))
		m, err := metadata.ReadFromDir(filepath.Join(dir, id.String()))
		testutil.Ok(t, err)
		sources = append(sources, m)
	}
	cnt := func() prometheus.Counter { return promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "test"}) }
	cg, err := NewGroup(logger, bkt, "0@test", ext, 0, false, false, cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), metadata.NoneFunc, 1, 1)
	testutil.Ok(t, err)
	for _, m := range sources {
		testutil.Ok(t, cg.AppendMeta(m))
	}
	comp, err := tsdb.NewLeveledCompactor(ctx, nil, slog.Default(), []int64{time.Hour.Milliseconds(), 2 * time.Hour.Milliseconds()}, chunkenc.NewPool(),
		storage.NewCompactingChunkSeriesMerger(storage.ChainedSeriesMerge))
	testutil.Ok(t, err)
	ex := LocalPlanExecutor{Comp: comp, BlockDeletableChecker: DefaultBlockDeletableChecker{}, Callback: DefaultCompactionLifecycleCallback{}, MarkSourcesForDeletion: true}

	plan := Plan{Sources: sources, Outputs: []PlanOutput{
		{Labels: map[string]string{"ext": "1", "part": "a"}, Series: &SeriesPartition{Index: 0, Count: 2}},
	}}
	_, err = ex.Execute(ctx, t.TempDir(), cg, plan)
	testutil.NotOk(t, err)
	testutil.Assert(t, IsHaltError(err), "losing series is not retried: %v", err)
	testutil.Assert(t, strings.Contains(err.Error(), "would lose the rest"), "unexpected error: %v", err)

	var uploaded []string
	testutil.Ok(t, bkt.Iter(ctx, "", func(name string) error { uploaded = append(uploaded, name); return nil }))
	testutil.Equals(t, 2, len(uploaded), "nothing but the sources is in the bucket: %v", uploaded)
	for _, s := range sources {
		ok, err := bkt.Exists(ctx, filepath.Join(s.ULID.String(), metadata.DeletionMarkFilename))
		testutil.Ok(t, err)
		testutil.Equals(t, false, ok, "the sources must not be retired")
	}

	// The same plan with both halves is fine.
	plan.Outputs = append(plan.Outputs, PlanOutput{Labels: map[string]string{"ext": "1", "part": "b"}, Series: &SeriesPartition{Index: 1, Count: 2}})
	_, err = ex.Execute(ctx, t.TempDir(), cg, plan)
	testutil.Ok(t, err)
}
