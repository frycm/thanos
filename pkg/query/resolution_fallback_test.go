// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/dedup"
	"github.com/thanos-io/thanos/pkg/logutil"
	"github.com/thanos-io/thanos/pkg/store"
	storetestutil "github.com/thanos-io/thanos/pkg/store/storepb/testutil"
)

// Exercise the complete path: actual TSDB and aggregate chunks, the metadata
// filter, bucket selection, strict StoreAPI proxying and PromQL evaluation.
// Shared sources must not double-count, while data unique to either resolution
// must remain visible even when both blocks occupy the same time range.
func TestResolutionFilteredStoreRawFallback(t *testing.T) {
	for _, withDownsample := range []bool{false, true} {
		t.Run(map[bool]string{false: "downsampling disabled", true: "partially overlapping genealogy"}[withDownsample], func(t *testing.T) {
			ctx := t.Context()
			logger := log.NewNopLogger()
			dir := t.TempDir()
			bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())
			source := func(n uint64) ulid.ULID { return ulid.MustNew(n, nil) }
			makeBlock := func(name string, value float64, sources []ulid.ULID) *metadata.Meta {
				headOpts := tsdb.DefaultHeadOptions()
				headOpts.ChunkDirRoot = t.TempDir()
				h, err := tsdb.NewHead(nil, nil, nil, nil, headOpts, nil)
				testutil.Ok(t, err)
				defer func() { testutil.Ok(t, h.Close()) }()
				app := h.Appender(ctx)
				for i := range 60 {
					ts := int64(i) * 60000
					for _, s := range []struct {
						name  string
						value float64
					}{{"shared", 1}, {name, value}, {"shared_total", float64(i) * 60}} {
						_, err := app.Append(0, labels.FromStrings("__name__", s.name), ts, s.value)
						testutil.Ok(t, err)
					}
				}
				testutil.Ok(t, app.Commit())
				id := storetestutil.CreateBlockFromHead(t, dir, h)
				m, err := metadata.InjectThanos(logger, filepath.Join(dir, id.String()), metadata.Thanos{Labels: map[string]string{"tenant": "1"}, Source: metadata.TestSource}, nil)
				testutil.Ok(t, err)
				m.Compaction.Sources = sources
				testutil.Ok(t, m.WriteToDir(logger, filepath.Join(dir, id.String())))
				return m
			}
			raw := makeBlock("raw_only", 2, []ulid.ULID{source(1), source(2)})
			testutil.Ok(t, block.Upload(ctx, logger, bkt, filepath.Join(dir, raw.ULID.String()), metadata.NoneFunc))
			if withDownsample {
				coarseSource := makeBlock("coarse_only", 3, []ulid.ULID{source(1), source(3)})
				input, err := tsdb.OpenBlock(logutil.GoKitLogToSlog(logger), filepath.Join(dir, coarseSource.ULID.String()), nil, nil)
				testutil.Ok(t, err)
				id, err := downsample.Downsample(ctx, logger, coarseSource, input, dir, downsample.ResLevel1)
				testutil.Ok(t, err)
				testutil.Ok(t, input.Close())
				testutil.Ok(t, block.Upload(ctx, logger, bkt, filepath.Join(dir, id.String()), metadata.NoneFunc))
			}
			filter := block.NewResolutionMetaFilter(logger, downsample.ResLevel1, downsample.ResLevel2, nil)
			fetcher, err := block.NewMetaFetcher(logger, 1, bkt, block.NewConcurrentLister(logger, bkt), "", nil, []block.MetadataFilter{filter, filter.Reporter()})
			testutil.Ok(t, err)
			bs, err := store.NewBucketStore(bkt, fetcher, t.TempDir(), store.NewChunksLimiterFactory(10000), store.NewSeriesLimiterFactory(0), store.NewBytesLimiterFactory(0), store.NewGapBasedPartitioner(store.PartitionerMaxGapSize), 1, false, store.DefaultPostingOffsetInMemorySampling, false, false, 0, store.WithLogger(logger), store.WithSourceCoverage())
			testutil.Ok(t, err)
			t.Cleanup(func() { testutil.Ok(t, bs.Close()) })
			testutil.Ok(t, bs.SyncBlocks(ctx))
			creator := NewQueryableCreator(logger, nil, newProxyStore(bs), 2, 10*time.Second, dedup.AlgorithmPenalty, 1)
			engine := promql.NewEngine(promql.EngineOpts{Logger: logutil.GoKitLogToSlog(logger), MaxSamples: math.MaxInt32, Timeout: 10 * time.Second})
			t.Cleanup(func() { testutil.Ok(t, engine.Close()) })
			for _, resolution := range []int64{0, downsample.ResLevel1} {
				qable := creator(true, []string{"prometheus_replica", "receiver_replica", "otelcol_replica", "ruler_replica"}, nil, resolution, false, false, nil, NoopSeriesStatsReporter)
				wantSum := float64(3)
				if withDownsample && resolution > 0 {
					wantSum = 6
				}
				for _, tc := range []struct {
					expr string
					want float64
				}{
					{`sum({__name__=~"shared|raw_only|coarse_only"})`, wantSum},
					{`count(shared)`, 1},
					{`rate(shared_total[30m])`, 1},
				} {
					q, err := engine.NewInstantQuery(ctx, qable, promql.NewPrometheusQueryOpts(false, 5*time.Minute), tc.expr, time.Unix(60*60, 0))
					testutil.Ok(t, err)
					result := q.Exec(ctx)
					testutil.Ok(t, result.Err)
					testutil.Assert(t, len(result.Warnings) == 0, "unexpected query warnings: %v", result.Warnings)
					vector, err := result.Vector()
					testutil.Ok(t, err)
					testutil.Equals(t, 1, len(vector))
					testutil.Assert(t, math.Abs(vector[0].F-tc.want) < 1e-9, "resolution=%d expression=%s: expected %v, got %v", resolution, tc.expr, tc.want, vector[0].F)
					q.Close()
				}
			}
		})
	}
}
