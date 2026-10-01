// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/logutil"
	"github.com/thanos-io/thanos/pkg/runutil"
)

type splitTestSample struct {
	t int64
	f float64
}

func (s splitTestSample) T() int64                      { return s.t }
func (s splitTestSample) F() float64                    { return s.f }
func (s splitTestSample) H() *histogram.Histogram       { return nil }
func (s splitTestSample) FH() *histogram.FloatHistogram { return nil }
func (s splitTestSample) Type() chunkenc.ValueType      { return chunkenc.ValFloat }
func (s splitTestSample) Copy() chunks.Sample           { return s }

// uploadSplitTestBlock uploads a raw block of [mint, maxt) with a sample of each series every step from mint+offset.
func uploadSplitTestBlock(t *testing.T, bkt objstore.Bucket, series []labels.Labels, mint, maxt, step, offset int64, ext labels.Labels) {
	t.Helper()
	var ss []storage.Series
	for _, lset := range series {
		var samples []chunks.Sample
		for ts := mint + offset; ts < maxt; ts += step {
			samples = append(samples, splitTestSample{t: ts, f: float64(len(lset.String())) + float64(ts%100000)/1000})
		}
		ss = append(ss, storage.NewListSeries(lset, samples))
	}
	bdir, err := tsdb.CreateBlock(ss, t.TempDir(), 0, logutil.GoKitLogToSlog(log.NewNopLogger()))
	testutil.Ok(t, err)
	meta, err := metadata.ReadFromDir(bdir)
	testutil.Ok(t, err)
	meta.MinTime, meta.MaxTime = mint, maxt
	meta.Thanos = metadata.Thanos{Labels: ext.Map(), Downsample: metadata.ThanosDownsample{Resolution: 0}, Source: metadata.TestSource}
	testutil.Ok(t, meta.WriteToDir(log.NewNopLogger(), bdir))
	dir := filepath.Join(t.TempDir(), meta.ULID.String())
	testutil.Ok(t, os.Rename(bdir, dir))
	testutil.Ok(t, block.Upload(context.Background(), log.NewNopLogger(), bkt, dir, metadata.NoneFunc))
}

// splitTestBlockInfo is a block of the bucket not marked for deletion: its labels, resolution, range, and per series the
// number of raw samples it holds (raw blocks) or the sum of its count aggregates (downsampled blocks).
type splitTestBlockInfo struct {
	meta   *metadata.Meta
	counts map[string]int
}

func (b splitTestBlockInfo) String() string {
	shard := b.meta.Thanos.Labels[metadata.CompactorShardIDLabel]
	if shard == "" {
		shard = "unsplit"
	}
	return fmt.Sprintf("%s res=%d [%dh,%dh)", shard, b.meta.Thanos.Downsample.Resolution, b.meta.MinTime/time.Hour.Milliseconds(), b.meta.MaxTime/time.Hour.Milliseconds())
}

func readSplitTestBucket(t *testing.T, bkt objstore.Bucket) []splitTestBlockInfo {
	t.Helper()
	ctx := context.Background()
	var res []splitTestBlockInfo
	testutil.Ok(t, bkt.Iter(ctx, "", func(name string) error {
		id, ok := block.IsBlockDir(name)
		if !ok {
			return nil
		}
		if deleted, err := bkt.Exists(ctx, path.Join(id.String(), metadata.DeletionMarkFilename)); err != nil || deleted {
			return err
		}
		bdir := filepath.Join(t.TempDir(), id.String())
		if err := block.Download(ctx, log.NewNopLogger(), bkt, id, bdir); err != nil {
			return err
		}
		meta, err := metadata.ReadFromDir(bdir)
		if err != nil {
			return err
		}
		counts, err := countSamples(bdir, meta.Thanos.Downsample.Resolution)
		if err != nil {
			return err
		}
		res = append(res, splitTestBlockInfo{meta: meta, counts: counts})
		return nil
	}))
	sort.Slice(res, func(i, j int) bool { return res[i].String() < res[j].String() })
	return res
}

// countSamples returns, per series, the number of raw samples of a raw block, or the sum of the count aggregates of
// a downsampled block.
func countSamples(bdir string, resolution int64) (_ map[string]int, err error) {
	b, err := tsdb.OpenBlock(logutil.GoKitLogToSlog(log.NewNopLogger()), bdir, downsample.NewPool(), nil)
	if err != nil {
		return nil, err
	}
	defer runutil.CloseWithErrCapture(&err, b, "close block")
	ir, err := b.Index()
	if err != nil {
		return nil, err
	}
	defer runutil.CloseWithErrCapture(&err, ir, "close index")
	cr, err := b.Chunks()
	if err != nil {
		return nil, err
	}
	defer runutil.CloseWithErrCapture(&err, cr, "close chunks")

	res := map[string]int{}
	k, v := index.AllPostingsKey()
	p, err := ir.Postings(context.Background(), k, v)
	if err != nil {
		return nil, err
	}
	for p.Next() {
		var (
			builder labels.ScratchBuilder
			chks    []chunks.Meta
		)
		if err := ir.Series(p.At(), &builder, &chks); err != nil {
			return nil, err
		}
		key := builder.Labels().String()
		for _, c := range chks {
			chk, _, err := cr.ChunkOrIterable(c)
			if err != nil {
				return nil, err
			}
			if resolution == 0 {
				res[key] += chk.NumSamples()
				continue
			}
			count, err := chk.(*downsample.AggrChunk).Get(downsample.AggrCount)
			if err != nil {
				return nil, err
			}
			it := count.Iterator(nil)
			for it.Next() != chunkenc.ValNone {
				_, v := it.At()
				res[key] += int(v)
			}
			if err := it.Err(); err != nil {
				return nil, err
			}
		}
	}
	return res, p.Err()
}

// TestCompactSplitDownsampling runs the compactor's compaction and downsampling over two replicas of a stream split
// in two shards: every shard is downsampled to 5m and 1h on its own, the downsampled shards hold every raw sample, and
// the unsplit sources, retired once their shards exist, are never downsampled.
func TestCompactSplitDownsampling(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	logger := log.NewNopLogger()
	reg := prometheus.NewRegistry()
	bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())

	// Fresh blocks of 2d and a largest range of 14d, so that a test with few blocks reaches both downsampling levels:
	// raw blocks of 40h or more are downsampled to 5m, 5m blocks of 10d or more to 1h.
	var (
		day    = 24 * time.Hour.Milliseconds()
		twoD   = 2 * day
		step   = 30 * time.Minute.Milliseconds()
		ranges = []int64{twoD, 14 * day}
		series []labels.Labels
	)
	for i := range 12 {
		series = append(series, labels.FromStrings("__name__", "up", "a", fmt.Sprint(i)))
	}
	// Eight full windows, [0, 16d), the last in the second 14d range; then a 2h block, the newest window, not split.
	for w := range int64(8) {
		uploadSplitTestBlock(t, bkt, series, w*twoD, (w+1)*twoD, step, 0, labels.FromStrings("tenant", "big", "replica", "A"))
		uploadSplitTestBlock(t, bkt, series, w*twoD, (w+1)*twoD, step, step/2, labels.FromStrings("tenant", "big", "replica", "B"))
	}
	uploadSplitTestBlock(t, bkt, series, 8*twoD, 8*twoD+2*time.Hour.Milliseconds(), step, 0, labels.FromStrings("tenant", "big", "replica", "A"))
	// A stream with a single series: one of its shards is empty at every level.
	for w := range int64(8) {
		uploadSplitTestBlock(t, bkt, series[:1], w*twoD, (w+1)*twoD, step, 0, labels.FromStrings("tenant", "sparse"))
	}
	uploadSplitTestBlock(t, bkt, series[:1], 8*twoD, 8*twoD+2*time.Hour.Milliseconds(), step, 0, labels.FromStrings("tenant", "sparse"))

	var conf compactConfig
	conf.compactionConcurrency = 4
	conf.enableConcurrentJobs = true
	conf.maxBlockIndexSize = math.MaxInt64
	conf.split = *parseSplitFlags(t, "--compact.split.shards=2")
	splitConf, err := conf.split.splitConfig([]string{"replica"}, true)
	testutil.Ok(t, err)

	// The compactor command's syncer filters.
	ignoreDeletionMarkFilter := block.NewIgnoreDeletionMarkFilter(logger, bkt, 24*time.Hour, 32)
	duplicateBlocksFilter := block.NewDeduplicateFilter(32)
	noCompactMarkerFilter := compact.NewGatherNoCompactionMarkFilter(logger, bkt, 2)
	fetcher, err := block.NewMetaFetcher(logger, 32, bkt, block.NewConcurrentLister(logger, bkt), "", nil, []block.MetadataFilter{
		ignoreDeletionMarkFilter,
		block.NewReplicaLabelRemover(logger, []string{"replica"}),
		duplicateBlocksFilter,
		noCompactMarkerFilter,
	})
	testutil.Ok(t, err)
	compactMetrics := newCompactMetrics(reg, 48*time.Hour)
	sy, err := compact.NewMetaSyncer(logger, reg, bkt, fetcher, duplicateBlocksFilter, ignoreDeletionMarkFilter, compactMetrics.blocksMarked.WithLabelValues(metadata.DeletionMarkFilename, ""), compactMetrics.garbageCollectedBlocks, 0)
	testutil.Ok(t, err)
	comp, err := tsdb.NewLeveledCompactor(ctx, nil, logutil.GoKitLogToSlog(logger), ranges, downsample.NewPool(), storage.NewCompactingChunkSeriesMerger(storage.ChainedSeriesMerge))
	testutil.Ok(t, err)
	grouper := compact.NewDefaultGrouper(logger, bkt, false, true, reg, compactMetrics.blocksMarked.WithLabelValues(metadata.DeletionMarkFilename, ""), compactMetrics.garbageCollectedBlocks, compactMetrics.blocksMarked.WithLabelValues(metadata.NoCompactMarkFilename, metadata.OutOfOrderChunksNoCompactReason), metadata.NoneFunc, 1, 1)
	compactor, _, err := newBucketCompactor(logger, reg, bkt, sy, grouper, noCompactMarkerFilter, comp, ranges, splitConf, conf, true, compactMetrics.blocksMarked, t.TempDir(), nil)
	testutil.Ok(t, err)
	downsampleMetrics := newDownsampleMetrics(reg)

	// One iteration of the compactor: compaction, then two passes of downsampling.
	iteration := func() {
		testutil.Ok(t, compactor.Compact(ctx))
		for range 2 {
			testutil.Ok(t, sy.SyncMetas(ctx))
			testutil.Ok(t, downsampleBucket(ctx, logger, downsampleMetrics, bkt, sy.Metas(), t.TempDir(), 1, 1, metadata.NoneFunc, false))
		}
	}
	iteration()
	all := readSplitTestBucket(t, bkt)
	var (
		blocks, sparse         []splitTestBlockInfo
		summary, sparseSummary []string
	)
	for _, b := range all {
		switch b.meta.Thanos.Labels["tenant"] {
		case "big":
			blocks = append(blocks, b)
			summary = append(summary, b.String())
		case "sparse":
			sparse = append(sparse, b)
			sparseSummary = append(sparseSummary, fmt.Sprintf("%s: %d series", b, len(b.counts)))
		}
	}
	testutil.Equals(t, []string{
		"1_of_2 res=0 [0h,336h)", "1_of_2 res=0 [336h,384h)",
		"1_of_2 res=300000 [0h,336h)", "1_of_2 res=300000 [336h,384h)",
		"1_of_2 res=3600000 [0h,336h)",
		"2_of_2 res=0 [0h,336h)", "2_of_2 res=0 [336h,384h)",
		"2_of_2 res=300000 [0h,336h)", "2_of_2 res=300000 [336h,384h)",
		"2_of_2 res=3600000 [0h,336h)",
		"unsplit res=0 [384h,386h)",
	}, summary)

	// Empty shards compact and downsample like the others.
	testutil.Equals(t, []string{
		"1_of_2 res=0 [0h,336h): 0 series", "1_of_2 res=0 [336h,384h): 0 series",
		"1_of_2 res=300000 [0h,336h): 0 series", "1_of_2 res=300000 [336h,384h): 0 series",
		"1_of_2 res=3600000 [0h,336h): 0 series",
		"2_of_2 res=0 [0h,336h): 1 series", "2_of_2 res=0 [336h,384h): 1 series",
		"2_of_2 res=300000 [0h,336h): 1 series", "2_of_2 res=300000 [336h,384h): 1 series",
		"2_of_2 res=3600000 [0h,336h): 1 series",
		"unsplit res=0 [384h,386h): 1 series",
	}, sparseSummary)
	for _, b := range sparse {
		for _, n := range b.counts {
			if b.meta.Thanos.Downsample.Resolution > 0 || b.meta.MaxTime-b.meta.MinTime > 2*time.Hour.Milliseconds() {
				testutil.Equals(t, int((b.meta.MaxTime-b.meta.MinTime)/step), n, "samples of %s", b)
			}
		}
	}

	// Per range and resolution, the shards together hold every series once and all of its raw samples.
	type key struct {
		mint, maxt, res int64
	}
	totals := map[key]map[string]int{}
	for _, b := range blocks {
		if _, ok := b.meta.Thanos.ShardID(); !ok {
			continue
		}
		k := key{b.meta.MinTime, b.meta.MaxTime, b.meta.Thanos.Downsample.Resolution}
		if totals[k] == nil {
			totals[k] = map[string]int{}
		}
		for s, n := range b.counts {
			_, dup := totals[k][s]
			testutil.Assert(t, !dup, "series %s in two shards of %v", s, k)
			totals[k][s] = n
		}
	}
	for k, counts := range totals {
		testutil.Equals(t, len(series), len(counts), "series of %v", k)
		raw := totals[key{k.mint, k.maxt, 0}]
		testutil.Equals(t, raw, counts, "samples of %v", k)
		for _, n := range raw {
			// Both replicas, deduplicated: two samples per step.
			testutil.Equals(t, int((k.maxt-k.mint)/step*2), n)
		}
	}
	for _, k := range []key{{0, 14 * day, downsample.ResLevel1}, {0, 14 * day, downsample.ResLevel2}, {14 * day, 16 * day, downsample.ResLevel1}} {
		_, ok := totals[k]
		testutil.Assert(t, ok, "no downsampled shards for %v", k)
	}

	// Nothing is left to compact or downsample: another iteration changes nothing, and in particular does not
	// downsample the retired unsplit sources.
	testutil.Ok(t, sy.SyncMetas(ctx))
	groups, err := grouper.Groups(sy.Metas())
	testutil.Ok(t, err)
	ds := compact.NewDownsampleProgressCalculator(prometheus.NewRegistry())
	testutil.Ok(t, ds.ProgressCalculate(ctx, groups))
	testutil.Equals(t, 0.0, promtest.ToFloat64(ds.NumberOfBlocksDownsampled))
	iteration()
	var again []string
	for _, b := range readSplitTestBucket(t, bkt) {
		again = append(again, b.String())
	}
	var before []string
	for _, b := range all {
		before = append(before, b.String())
	}
	testutil.Equals(t, before, again)
	for _, b := range blocks {
		if b.meta.Thanos.Downsample.Resolution > 0 {
			testutil.Assert(t, strings.Contains(b.String(), "_of_2"), "unsplit block downsampled: %s", b)
		}
	}
}

// parseSplitFlags registers the split flags and parses the given arguments.
func parseSplitFlags(t *testing.T, args ...string) *splitFlags {
	t.Helper()
	app := kingpin.New("test", "")
	sf := &splitFlags{}
	sf.registerFlag(app)
	_, err := app.Parse(args)
	testutil.Ok(t, err)
	return sf
}

func TestSplitFlags(t *testing.T) {
	t.Parallel()

	file := filepath.Join(t.TempDir(), "split.yaml")
	testutil.Ok(t, os.WriteFile(file, []byte("- match: '{tenant_id=\"big\"}'\n  shards: 8\n"), 0600))
	sf := parseSplitFlags(t, "--compact.split.config-file="+file, "--compact.split.ignore-labels=replica,prometheus_replica", "--compact.split.ignore-labels=replica")
	conf, err := sf.splitConfig([]string{"rule_replica"}, true)
	testutil.Ok(t, err)
	testutil.Equals(t, 1, conf.Shards)
	testutil.Equals(t, []string{"prometheus_replica", "replica"}, conf.IgnoreLabels)
	testutil.Equals(t, 8, conf.ShardsFor(labels.FromStrings("tenant_id", "big")))
	testutil.Equals(t, 1, conf.ShardsFor(labels.FromStrings("tenant_id", "small")))

	// Splitting needs vertical compaction.
	_, err = sf.splitConfig(nil, false)
	testutil.NotOk(t, err)
	testutil.Assert(t, strings.Contains(err.Error(), "requires vertical compaction"), "unexpected error: %v", err)
	_, err = parseSplitFlags(t, "--compact.split.shards=4").splitConfig(nil, false)
	testutil.NotOk(t, err)

	// Not splitting needs nothing.
	conf, err = parseSplitFlags(t).splitConfig(nil, false)
	testutil.Ok(t, err)
	testutil.Assert(t, !conf.Enabled())
	conf, err = parseSplitFlags(t, "--compact.split.config=- match: '{tenant_id=\"big\"}'\n  shards: 1").splitConfig(nil, false)
	testutil.Ok(t, err)
	testutil.Assert(t, !conf.Enabled())

	// The shard label can never be a replica label.
	_, err = parseSplitFlags(t).splitConfig([]string{metadata.CompactorShardIDLabel}, true)
	testutil.NotOk(t, err)

	for _, args := range [][]string{
		{"--compact.split.shards=3"},
		{"--compact.split.shards=0"},
		{"--compact.split.config=- match: '{tenant_id=\"big\"}'\n  shards: 6"},
		{"--compact.split.config=- match: 'up'\n  shards: 2"},
		{"--compact.split.ignore-labels=a,,b"},
	} {
		_, err = parseSplitFlags(t, args...).splitConfig(nil, true)
		testutil.NotOk(t, err, "args %v", args)
	}
	// An empty flag value is no label.
	conf, err = parseSplitFlags(t, "--compact.split.ignore-labels=").splitConfig(nil, true)
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(conf.IgnoreLabels))
}
