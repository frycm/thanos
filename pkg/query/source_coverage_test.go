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

// sourceCoverageBlocks writes TSDB blocks holding an hour of one sample per
// minute for "shared" (1), "shared_total" (a counter growing by 1/s) and
// the given series, with the given compaction sources.
type sourceCoverageBlocks struct {
	t      *testing.T
	logger log.Logger
	dir    string
	bkt    objstore.Bucket
}

func (w sourceCoverageBlocks) write(name string, value float64, sources []ulid.ULID, seriesLabels, extLabels map[string]string) *metadata.Meta {
	t := w.t
	headOpts := tsdb.DefaultHeadOptions()
	headOpts.ChunkDirRoot = t.TempDir()
	h, err := tsdb.NewHead(nil, nil, nil, nil, headOpts, nil)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, h.Close()) }()

	app := h.Appender(t.Context())
	for i := range 60 {
		ts := int64(i) * 60000
		for _, s := range []struct {
			name  string
			value float64
		}{{"shared", 1}, {"shared_total", float64(i) * 60}, {name, value}} {
			if s.name == "" {
				continue
			}
			b := labels.NewBuilder(labels.FromMap(seriesLabels)).Set(labels.MetricName, s.name)
			_, err = app.Append(0, b.Labels(), ts, s.value)
			testutil.Ok(t, err)
		}
	}
	testutil.Ok(t, app.Commit())

	id := storetestutil.CreateBlockFromHead(t, w.dir, h)
	m, err := metadata.InjectThanos(w.logger, filepath.Join(w.dir, id.String()), metadata.Thanos{Labels: extLabels, Source: metadata.TestSource}, nil)
	testutil.Ok(t, err)
	m.Compaction.Sources = sources
	testutil.Ok(t, m.WriteToDir(w.logger, filepath.Join(w.dir, id.String())))
	return m
}

func (w sourceCoverageBlocks) upload(id ulid.ULID) {
	testutil.Ok(w.t, block.Upload(w.t.Context(), w.logger, w.bkt, filepath.Join(w.dir, id.String()), metadata.NoneFunc))
}

// uploadDownsampled uploads the 5m block made from m, not m itself.
func (w sourceCoverageBlocks) uploadDownsampled(m *metadata.Meta) {
	t := w.t
	input, err := tsdb.OpenBlock(logutil.GoKitLogToSlog(w.logger), filepath.Join(w.dir, m.ULID.String()), nil, nil)
	testutil.Ok(t, err)
	id, err := downsample.Downsample(t.Context(), w.logger, m, input, w.dir, downsample.ResLevel1)
	testutil.Ok(t, err)
	testutil.Ok(t, input.Close())
	w.upload(id)
}

type sourceCoverageQuery struct {
	expr string
	// expected value at max_source_resolution=0 and =5m.
	raw, downsampled float64
}

// runSourceCoverageQueries syncs a bucket store over bkt and evaluates the
// queries through the strict StoreAPI proxy and the PromQL engine, at the raw
// and the 5m resolution.
func runSourceCoverageQueries(t *testing.T, bkt objstore.Bucket, deduplicate bool, replicaLabels []string, queries []sourceCoverageQuery) {
	runSourceCoverageQueriesAbove(t, bkt, 0, deduplicate, replicaLabels, queries)
}

// runSourceCoverageQueriesAbove is runSourceCoverageQueries with a store
// whose resolution filter hides finer blocks below minResolution where blocks
// at minResolution cover them; 0 installs no filter.
func runSourceCoverageQueriesAbove(t *testing.T, bkt objstore.Bucket, minResolution int64, deduplicate bool, replicaLabels []string, queries []sourceCoverageQuery) {
	ctx := t.Context()
	logger := log.NewNopLogger()
	instrBkt := objstore.WithNoopInstr(bkt)
	var filters []block.MetadataFilter
	opts := []store.BucketStoreOption{store.WithLogger(logger)}
	if minResolution > 0 {
		filter := block.NewResolutionMetaFilter(logger, minResolution, downsample.ResLevel2, nil)
		filters = append(filters, filter)
		opts = append(opts, store.WithResolutionFilter(filter))
	}
	fetcher, err := block.NewMetaFetcher(logger, 1, instrBkt, block.NewConcurrentLister(logger, instrBkt), "", nil, filters)
	testutil.Ok(t, err)
	bs, err := store.NewBucketStore(instrBkt, fetcher, t.TempDir(), store.NewChunksLimiterFactory(10000), store.NewSeriesLimiterFactory(0), store.NewBytesLimiterFactory(0), store.NewGapBasedPartitioner(store.PartitionerMaxGapSize), 1, false, store.DefaultPostingOffsetInMemorySampling, false, false, 0, opts...)
	testutil.Ok(t, err)
	t.Cleanup(func() { testutil.Ok(t, bs.Close()) })
	testutil.Ok(t, bs.SyncBlocks(ctx))

	creator := NewQueryableCreator(logger, nil, newProxyStore(bs), 2, 10*time.Second, dedup.AlgorithmPenalty, 1)
	engine := promql.NewEngine(promql.EngineOpts{Logger: logutil.GoKitLogToSlog(logger), MaxSamples: math.MaxInt32, Timeout: 10 * time.Second})
	t.Cleanup(func() { testutil.Ok(t, engine.Close()) })
	for _, resolution := range []int64{downsample.ResLevel0, downsample.ResLevel1} {
		qable := creator(deduplicate, replicaLabels, nil, resolution, false, false, nil, NoopSeriesStatsReporter)
		for _, tc := range queries {
			want := tc.raw
			if resolution > 0 {
				want = tc.downsampled
			}
			q, err := engine.NewInstantQuery(ctx, qable, promql.NewPrometheusQueryOpts(false, 5*time.Minute), tc.expr, time.Unix(60*60, 0))
			testutil.Ok(t, err)
			result := q.Exec(ctx)
			testutil.Ok(t, result.Err)
			testutil.Assert(t, len(result.Warnings) == 0, "unexpected query warnings: %v", result.Warnings)
			vector, err := result.Vector()
			testutil.Ok(t, err)
			testutil.Equals(t, 1, len(vector), "resolution=%d expression=%s", resolution, tc.expr)
			testutil.Assert(t, math.Abs(vector[0].F-want) < 1e-9, "resolution=%d expression=%s: expected %v, got %v", resolution, tc.expr, want, vector[0].F)
			q.Close()
		}
	}
}

// TestBucketStoreSourceCoverage_PartialLineage covers a raw block that
// gained a source after its 5m block was made, e.g. by a later vertical
// compaction or backfill. The 5m block spans the raw block's whole range, so
// selecting by time alone answers 5m queries from it and loses the series of
// the new source. The raw block must be read too, and the series both blocks
// hold, as identical labels with overlapping raw and aggregated chunks, must
// not count twice.
func TestBucketStoreSourceCoverage_PartialLineage(t *testing.T) {
	for _, deduplicate := range []bool{false, true} {
		t.Run(map[bool]string{false: "deduplication disabled", true: "deduplication enabled"}[deduplicate], func(t *testing.T) {
			bkt := objstore.NewInMemBucket()
			w := sourceCoverageBlocks{t: t, logger: log.NewNopLogger(), dir: t.TempDir(), bkt: bkt}
			ext := map[string]string{"tenant": "1"}
			source := func(n uint64) ulid.ULID { return ulid.MustNew(n, nil) }

			// What source 1 alone produced: downsampled, then deleted.
			w.uploadDownsampled(w.write("", 0, []ulid.ULID{source(1)}, nil, ext))
			// The raw block of sources 1 and 2, the latter adding "late_series".
			w.upload(w.write("late_series", 2, []ulid.ULID{source(1), source(2)}, nil, ext).ULID)

			runSourceCoverageQueries(t, bkt, deduplicate, nil, []sourceCoverageQuery{
				{expr: `sum({__name__=~"shared|late_series"})`, raw: 3, downsampled: 3},
				{expr: `count({__name__="late_series"})`, raw: 1, downsampled: 1},
				{expr: `count(shared)`, raw: 1, downsampled: 1},
				{expr: `rate(shared_total[30m])`, raw: 1, downsampled: 1},
			})
		})
	}
}

// TestBucketStoreSourceCoverage_DistinctLineage covers a raw block and a 5m
// block of the same time range made from different sources, with the replica
// as a series label (one block set) and as an external label (one block set
// per replica). Data unique to either resolution must stay visible, while
// replicas of the same data must deduplicate.
func TestBucketStoreSourceCoverage_DistinctLineage(t *testing.T) {
	replicaLabels := []string{"prometheus_replica", "receiver_replica"}
	for _, replicaLabel := range replicaLabels {
		for _, withDownsample := range []bool{false, true} {
			t.Run(replicaLabel+"/"+map[bool]string{false: "raw blocks only", true: "5m block of other sources"}[withDownsample], func(t *testing.T) {
				bkt := objstore.NewInMemBucket()
				w := sourceCoverageBlocks{t: t, logger: log.NewNopLogger(), dir: t.TempDir(), bkt: bkt}
				source := func(n uint64) ulid.ULID { return ulid.MustNew(n, nil) }
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
				if withDownsample {
					w.uploadDownsampled(write("coarse_only", 3, []ulid.ULID{source(1), source(3)}, "B"))
				}

				downsampledSum := float64(3)
				if withDownsample {
					downsampledSum = 6
				}
				runSourceCoverageQueries(t, bkt, true, replicaLabels, []sourceCoverageQuery{
					{expr: `sum({__name__=~"shared|raw_only|coarse_only"})`, raw: 3, downsampled: downsampledSum},
					{expr: `count(shared)`, raw: 1, downsampled: 1},
					{expr: `rate(shared_total[30m])`, raw: 1, downsampled: 1},
				})
			})
		}
	}
}
