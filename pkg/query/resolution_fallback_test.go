// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"context"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/promql"
	"github.com/thanos-io/objstore"
	"go.uber.org/atomic"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/dedup"
	"github.com/thanos-io/thanos/pkg/logutil"
	"github.com/thanos-io/thanos/pkg/store"
)

// TestResolutionFilteredStore_UncoveredRawBlocks runs the source coverage
// scenarios through a store gateway with --min-block-resolution=5m. A raw
// block whose sources the 5m blocks do not all hold is not hidden, so data
// unique to either resolution stays visible at both, and shared data is not
// counted twice.
func TestResolutionFilteredStore_UncoveredRawBlocks(t *testing.T) {
	source := func(n uint64) ulid.ULID { return ulid.MustNew(n, nil) }
	t.Run("partial lineage", func(t *testing.T) {
		bkt := objstore.NewInMemBucket()
		w := sourceCoverageBlocks{t: t, logger: log.NewNopLogger(), dir: t.TempDir(), bkt: bkt}
		ext := map[string]string{"tenant": "1"}
		w.uploadDownsampled(w.write("", 0, []ulid.ULID{source(1)}, nil, ext))
		w.upload(w.write("late_series", 2, []ulid.ULID{source(1), source(2)}, nil, ext).ULID)

		runSourceCoverageQueriesAbove(t, bkt, downsample.ResLevel1, true, nil, []sourceCoverageQuery{
			{expr: `sum({__name__=~"shared|late_series"})`, raw: 3, downsampled: 3},
			{expr: `count({__name__="late_series"})`, raw: 1, downsampled: 1},
			{expr: `rate(shared_total[30m])`, raw: 1, downsampled: 1},
		})
	})
	replicaLabels := []string{"prometheus_replica", "receiver_replica"}
	for _, replicaLabel := range replicaLabels {
		t.Run("distinct lineage/"+replicaLabel, func(t *testing.T) {
			bkt := objstore.NewInMemBucket()
			w := sourceCoverageBlocks{t: t, logger: log.NewNopLogger(), dir: t.TempDir(), bkt: bkt}
			write := func(name string, value float64, sources []ulid.ULID, replica string) *metadata.Meta {
				ext := map[string]string{"tenant": "1"}
				var seriesLabels map[string]string
				if replicaLabel == "receiver_replica" {
					ext[replicaLabel] = replica
				} else {
					seriesLabels = map[string]string{replicaLabel: replica}
				}
				return w.write(name, value, sources, seriesLabels, ext)
			}
			w.upload(write("raw_only", 2, []ulid.ULID{source(1), source(2)}, "A").ULID)
			w.upload(write("raw_only", 2, []ulid.ULID{source(4), source(5)}, "B").ULID)
			w.uploadDownsampled(write("coarse_only", 3, []ulid.ULID{source(1), source(3)}, "B"))

			runSourceCoverageQueriesAbove(t, bkt, downsample.ResLevel1, true, replicaLabels, []sourceCoverageQuery{
				{expr: `sum({__name__=~"shared|raw_only|coarse_only"})`, raw: 3, downsampled: 6},
				{expr: `count(shared)`, raw: 1, downsampled: 1},
				{expr: `rate(shared_total[30m])`, raw: 1, downsampled: 1},
			})
		})
	}
}

type chunkFailureBucket struct {
	objstore.Bucket
	failChunks atomic.Bool
}

func (b *chunkFailureBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if b.failChunks.Load() && strings.Contains(name, "/chunks/") {
		return nil, errors.New("injected chunk read failure")
	}
	return b.Bucket.GetRange(ctx, name, off, length)
}

// TestResolutionFilteredStore_FallbackReadFailureFailsStrictQuery: a raw
// block the filter keeps is read like any other block, so a failed read of it
// fails a strict query rather than silently dropping its series.
func TestResolutionFilteredStore_FallbackReadFailureFailsStrictQuery(t *testing.T) {
	ctx := t.Context()
	logger := log.NewNopLogger()
	fault := &chunkFailureBucket{Bucket: objstore.NewInMemBucket()}
	w := sourceCoverageBlocks{t: t, logger: logger, dir: t.TempDir(), bkt: fault}
	source := func(n uint64) ulid.ULID { return ulid.MustNew(n, nil) }
	ext := map[string]string{"tenant": "1"}
	w.uploadDownsampled(w.write("", 0, []ulid.ULID{source(1)}, nil, ext))
	w.upload(w.write("late_series", 2, []ulid.ULID{source(1), source(2)}, nil, ext).ULID)

	bkt := objstore.WithNoopInstr(fault)
	filter := block.NewResolutionMetaFilter(logger, downsample.ResLevel1, downsample.ResLevel2, nil)
	fetcher, err := block.NewMetaFetcher(logger, 1, bkt, block.NewConcurrentLister(logger, bkt), "", nil, []block.MetadataFilter{filter})
	testutil.Ok(t, err)
	bs, err := store.NewBucketStore(bkt, fetcher, t.TempDir(), store.NewChunksLimiterFactory(10000), store.NewSeriesLimiterFactory(0), store.NewBytesLimiterFactory(0), store.NewGapBasedPartitioner(store.PartitionerMaxGapSize), 1, false, store.DefaultPostingOffsetInMemorySampling, false, false, 0, store.WithLogger(logger), store.WithResolutionFilter(filter))
	testutil.Ok(t, err)
	t.Cleanup(func() { testutil.Ok(t, bs.Close()) })
	testutil.Ok(t, bs.SyncBlocks(ctx))

	creator := NewQueryableCreator(logger, nil, newProxyStore(bs), 2, 10*time.Second, dedup.AlgorithmPenalty, 1)
	engine := promql.NewEngine(promql.EngineOpts{Logger: logutil.GoKitLogToSlog(logger), MaxSamples: math.MaxInt32, Timeout: 10 * time.Second})
	t.Cleanup(func() { testutil.Ok(t, engine.Close()) })
	fault.failChunks.Store(true)
	for _, resolution := range []int64{downsample.ResLevel0, downsample.ResLevel1} {
		qable := creator(false, nil, nil, resolution, false, false, nil, NoopSeriesStatsReporter)
		q, err := engine.NewInstantQuery(ctx, qable, promql.NewPrometheusQueryOpts(false, 5*time.Minute), `sum(late_series)`, time.Unix(60*60, 0))
		testutil.Ok(t, err)
		result := q.Exec(ctx)
		testutil.NotOk(t, result.Err, "resolution=%d", resolution)
		q.Close()
	}
}
