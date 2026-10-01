// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"context"
	"fmt"
	"math"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/logutil"
	"github.com/thanos-io/thanos/pkg/runutil"
)

// concurrencyTrackingCompactor records how many compactions run at the same time. The first compactions wait (up to
// a timeout) until `wait` of them run together, so that the test does not depend on scheduling.
type concurrencyTrackingCompactor struct {
	Compactor

	wait        int
	mtx         sync.Mutex
	inFlight    int
	maxInFlight int
	release     chan struct{}
	released    bool
}

func newConcurrencyTrackingCompactor(comp Compactor, wait int) *concurrencyTrackingCompactor {
	return &concurrencyTrackingCompactor{Compactor: comp, wait: wait, release: make(chan struct{})}
}

func (c *concurrencyTrackingCompactor) CompactWithBlockPopulator(dest string, dirs []string, open []*tsdb.Block, blockPopulator tsdb.BlockPopulator) ([]ulid.ULID, error) {
	c.mtx.Lock()
	c.inFlight++
	c.maxInFlight = max(c.maxInFlight, c.inFlight)
	if c.inFlight >= c.wait && !c.released {
		c.released = true
		close(c.release)
	}
	c.mtx.Unlock()

	select {
	case <-c.release:
	case <-time.After(10 * time.Second):
	}
	defer func() {
		c.mtx.Lock()
		c.inFlight--
		c.mtx.Unlock()
	}()
	return c.Compactor.CompactWithBlockPopulator(dest, dirs, open, blockPopulator)
}

// countingGrouper counts the groups of every pass of BucketCompactor.
type countingGrouper struct {
	Grouper

	groupsPerPass []int
}

func (g *countingGrouper) Groups(blocks map[ulid.ULID]*metadata.Meta) ([]*Group, error) {
	groups, err := g.Grouper.Groups(blocks)
	g.groupsPerPass = append(g.groupsPerPass, len(groups))
	return groups, err
}

type bucketCompactionResult struct {
	groupsPerPass       []int
	compactions         float64
	verticalCompactions float64
	maxInFlight         int
	// blocks maps each block left in the bucket (time range, sources) to its content.
	blocks map[string]string
}

func compactBucket(t *testing.T, bkt objstore.InstrumentedBucket, ranges []int64, vertical, concurrentJobs bool, concurrency int) bucketCompactionResult {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	logger := log.NewNopLogger()
	reg := prometheus.NewRegistry()
	ignoreDeletionMarkFilter := block.NewIgnoreDeletionMarkFilter(logger, bkt, 48*time.Hour, fetcherConcurrency)
	duplicateBlocksFilter := block.NewDeduplicateFilter(fetcherConcurrency)
	noCompactMarkerFilter := NewGatherNoCompactionMarkFilter(logger, bkt, 2)
	metaFetcher, err := block.NewMetaFetcher(nil, 32, bkt, block.NewConcurrentLister(logger, bkt), "", nil, []block.MetadataFilter{
		ignoreDeletionMarkFilter,
		duplicateBlocksFilter,
		noCompactMarkerFilter,
	})
	testutil.Ok(t, err)

	blocksMarkedForDeletion := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
	garbageCollectedBlocks := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
	sy, err := NewMetaSyncer(nil, nil, bkt, metaFetcher, duplicateBlocksFilter, ignoreDeletionMarkFilter, blocksMarkedForDeletion, garbageCollectedBlocks, 0)
	testutil.Ok(t, err)

	leveledCompactor, err := tsdb.NewLeveledCompactor(ctx, nil, logutil.GoKitLogToSlog(logger), ranges, nil, nil)
	testutil.Ok(t, err)
	comp := newConcurrencyTrackingCompactor(leveledCompactor, concurrency)

	defaultGrouper := NewDefaultGrouper(logger, bkt, false, vertical, reg, blocksMarkedForDeletion, garbageCollectedBlocks,
		promauto.With(nil).NewCounter(prometheus.CounterOpts{}), metadata.NoneFunc, 1, 1)
	grouper := &countingGrouper{Grouper: defaultGrouper}
	indexSizeMarked := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
	largeIndexFilterPlanner := WithLargeTotalIndexSizeFilter(NewPlanner(logger, ranges, noCompactMarkerFilter), bkt, math.MaxInt64, indexSizeMarked)
	if concurrentJobs {
		grouper.Grouper = NewConcurrentJobsGrouper(defaultGrouper, ranges, noCompactMarkerFilter)
		largeIndexFilterPlanner = WithLargeTotalIndexSizeFilter(NewConcurrentJobsPlanner(logger, noCompactMarkerFilter), bkt, math.MaxInt64, indexSizeMarked)
	}
	var planner Planner = largeIndexFilterPlanner
	if vertical {
		planner = WithVerticalCompactionDownsampleFilter(largeIndexFilterPlanner, bkt, promauto.With(nil).NewCounter(prometheus.CounterOpts{}))
	}

	bComp, err := NewBucketCompactor(logger, sy, grouper, planner, comp, t.TempDir(), bkt, concurrency, false, nil)
	testutil.Ok(t, err)
	testutil.Ok(t, bComp.Compact(ctx))

	testutil.Equals(t, 1, MetricCount(defaultGrouper.compactions), "job groups must not add metric series")
	return bucketCompactionResult{
		groupsPerPass:       grouper.groupsPerPass,
		compactions:         promtest.ToFloat64(defaultGrouper.compactions.WithLabelValues("0")),
		verticalCompactions: promtest.ToFloat64(defaultGrouper.verticalCompactions.WithLabelValues("0")),
		maxInFlight:         comp.maxInFlight,
		blocks:              readBucketBlocks(t, ctx, bkt),
	}
}

// readBucketBlocks returns the series and samples of every block not marked for deletion, by time range and sources.
func readBucketBlocks(t *testing.T, ctx context.Context, bkt objstore.Bucket) map[string]string {
	t.Helper()

	dir := t.TempDir()
	res := map[string]string{}
	testutil.Ok(t, bkt.Iter(ctx, "", func(name string) error {
		id, ok := block.IsBlockDir(name)
		if !ok {
			return nil
		}
		deleted, err := bkt.Exists(ctx, path.Join(id.String(), metadata.DeletionMarkFilename))
		if err != nil || deleted {
			return err
		}

		bdir := filepath.Join(dir, id.String())
		if err := block.Download(ctx, log.NewNopLogger(), bkt, id, bdir); err != nil {
			return err
		}
		meta, err := metadata.ReadFromDir(bdir)
		if err != nil {
			return err
		}
		content, err := readBlockContent(ctx, bdir)
		if err != nil {
			return err
		}
		res[fmt.Sprintf("%s[%d,%d)%v", labels.FromMap(meta.Thanos.Labels), meta.MinTime, meta.MaxTime, meta.Compaction.Sources)] = content
		return nil
	}))
	return res
}

func readBlockContent(ctx context.Context, bdir string) (_ string, err error) {
	b, err := tsdb.OpenBlock(logutil.GoKitLogToSlog(log.NewNopLogger()), bdir, nil, nil)
	if err != nil {
		return "", err
	}
	defer runutil.CloseWithErrCapture(&err, b, "close block")

	q, err := tsdb.NewBlockQuerier(b, math.MinInt64, math.MaxInt64)
	if err != nil {
		return "", err
	}
	defer runutil.CloseWithErrCapture(&err, q, "close querier")

	var sb strings.Builder
	set := q.Select(ctx, true, nil, labels.MustNewMatcher(labels.MatchRegexp, "a", ".+"))
	for set.Next() {
		s := set.At()
		fmt.Fprintf(&sb, "%s:", s.Labels())
		it := s.Iterator(nil)
		for it.Next() == chunkenc.ValFloat {
			ts, v := it.At()
			fmt.Fprintf(&sb, " %v@%d", v, ts)
		}
		if err := it.Err(); err != nil {
			return "", err
		}
		sb.WriteString("\n")
	}
	return sb.String(), set.Err()
}

// TestConcurrentJobs_BucketCompactor compacts the same blocks with the default grouper and planner, one plan per
// stream and pass, and with concurrent jobs: the resulting blocks must be the same, with the same series and samples.
func TestConcurrentJobs_BucketCompactor(t *testing.T) {
	t.Parallel()

	// ranges[1:] are the ranges the planner compacts into.
	ranges := []int64{1000, 2000, 8000}
	streamA, streamB := labels.FromStrings("e1", "a"), labels.FromStrings("e1", "b")
	series := []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2", "b", "2"),
		labels.FromStrings("a", "3"),
	}

	// Stream A: eight 1s blocks fill four 2s ranges, then one 8s range; [8000, 9000) is the newest block.
	// Stream B: [0, 2000) is complete, [2000, 3000) is the newest block.
	var specs []blockgenSpec
	for start := int64(0); start <= 8000; start += 1000 {
		specs = append(specs, blockgenSpec{numSamples: 10, mint: start, maxt: start + 1000, extLset: streamA, series: series})
	}
	for start := int64(0); start <= 2000; start += 1000 {
		specs = append(specs, blockgenSpec{numSamples: 10, mint: start, maxt: start + 1000, extLset: streamB, series: series[1:]})
	}

	t.Run("no overlaps", func(t *testing.T) {
		t.Parallel()

		seq, conc := compactBucketsWithAndWithoutJobs(t, specs, ranges, false)

		// One plan per stream and pass: stream A needs 5 compactions, stream B one. The last pass has nothing to do.
		testutil.Equals(t, 6, len(seq.groupsPerPass))
		testutil.Equals(t, 6.0, seq.compactions)
		// First pass: the four 2s ranges of stream A and the 2s range of stream B, four at a time. Second pass: the
		// 8s range of stream A.
		testutil.Equals(t, []int{5, 1, 0}, conc.groupsPerPass)
		testutil.Equals(t, 6.0, conc.compactions)
		testutil.Equals(t, 4, conc.maxInFlight)

		testutil.Equals(t, []string{
			`{e1="a"}[0,8000): 3 series, 240 samples`,
			`{e1="a"}[8000,9000): 3 series, 30 samples`,
			`{e1="b"}[0,2000): 2 series, 40 samples`,
			`{e1="b"}[2000,3000): 2 series, 20 samples`,
		}, summarizeBlocks(conc.blocks))
	})

	t.Run("vertical compaction", func(t *testing.T) {
		t.Parallel()

		// Replicas of [2000, 3000) and [5000, 6000) of stream A, with another series.
		replicas := []blockgenSpec{
			{numSamples: 10, mint: 2000, maxt: 3000, extLset: streamA, series: []labels.Labels{labels.FromStrings("a", "4")}},
			{numSamples: 10, mint: 5000, maxt: 6000, extLset: streamA, series: []labels.Labels{labels.FromStrings("a", "4")}},
		}
		seq, conc := compactBucketsWithAndWithoutJobs(t, append(replicas, specs...), ranges, true)

		// Stream A: 2 vertical compactions first, then 5 range compactions. Stream B: one range compaction.
		testutil.Equals(t, 8, len(seq.groupsPerPass))
		testutil.Equals(t, 8.0, seq.compactions)
		testutil.Equals(t, 2.0, seq.verticalCompactions)
		// First pass: both sets of replicas, [0, 2000) and [6000, 8000) of stream A, [0, 2000) of stream B. Second
		// pass: [2000, 4000) and [4000, 6000) of stream A. Third pass: [0, 8000) of stream A.
		testutil.Equals(t, []int{5, 2, 1, 0}, conc.groupsPerPass)
		testutil.Equals(t, 8.0, conc.compactions)
		testutil.Equals(t, 2.0, conc.verticalCompactions)
		testutil.Equals(t, 4, conc.maxInFlight)

		testutil.Equals(t, []string{
			`{e1="a"}[0,8000): 4 series, 260 samples`,
			`{e1="a"}[8000,9000): 3 series, 30 samples`,
			`{e1="b"}[0,2000): 2 series, 40 samples`,
			`{e1="b"}[2000,3000): 2 series, 20 samples`,
		}, summarizeBlocks(conc.blocks))
	})
}

// compactBucketsWithAndWithoutJobs uploads the blocks to two buckets, compacts one sequentially and the other with
// concurrent jobs, and checks that both end with the same blocks, holding the same series and samples.
func compactBucketsWithAndWithoutJobs(t *testing.T, specs []blockgenSpec, ranges []int64, vertical bool) (seq, conc bucketCompactionResult) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	seqBkt, concBkt := objstore.WithNoopInstr(objstore.NewInMemBucket()), objstore.WithNoopInstr(objstore.NewInMemBucket())
	prepareDir := t.TempDir()
	for _, spec := range specs {
		id, _ := createBlock(t, ctx, prepareDir, spec)
		for _, bkt := range []objstore.Bucket{seqBkt, concBkt} {
			testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(prepareDir, id.String()), metadata.NoneFunc))
		}
	}

	seq = compactBucket(t, seqBkt, ranges, vertical, false, 1)
	conc = compactBucket(t, concBkt, ranges, vertical, true, 4)
	testutil.Equals(t, 1, seq.maxInFlight)
	testutil.Equals(t, seq.blocks, conc.blocks)
	return seq, conc
}

// summarizeBlocks returns the labels, time range and number of series and samples of each block.
func summarizeBlocks(blocks map[string]string) []string {
	var res []string
	for k, content := range blocks {
		res = append(res, fmt.Sprintf("%s: %d series, %d samples", k[:strings.Index(k, ")")+1], strings.Count(content, "\n"), strings.Count(content, "@")))
	}
	sort.Strings(res)
	return res
}
