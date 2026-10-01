// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/logutil"
	"github.com/thanos-io/thanos/pkg/runutil"
)

// splitTestStep is the scrape interval of the test blocks: replica B scrapes half a step after replica A.
const splitTestStep = 10 * 60 * 1000

// splitTestValue is the value of a series at a time, the same in every replica.
func splitTestValue(lset labels.Labels, t int64) float64 {
	return float64(len(lset.String())) + float64(t)/1000
}

// splitTestSample is a float sample.
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

// createSplitTestBlock writes a raw block of [mint, maxt) like Prometheus would, with the given series scraped every
// splitTestStep from mint+offset, and the given external labels.
func createSplitTestBlock(t *testing.T, dir string, series []labels.Labels, mint, maxt, offset int64, ext labels.Labels) ulid.ULID {
	t.Helper()

	var ss []storage.Series
	for _, lset := range series {
		var samples []chunks.Sample
		for ts := mint + offset; ts < maxt; ts += splitTestStep {
			samples = append(samples, splitTestSample{t: ts, f: splitTestValue(lset, ts)})
		}
		ss = append(ss, storage.NewListSeries(lset, samples))
	}
	tmp := t.TempDir()
	bdir, err := tsdb.CreateBlock(ss, tmp, 0, logutil.GoKitLogToSlog(log.NewNopLogger()))
	testutil.Ok(t, err)

	// Like a Prometheus head block, the block covers its whole window.
	meta, err := metadata.ReadFromDir(bdir)
	testutil.Ok(t, err)
	meta.MinTime, meta.MaxTime = mint, maxt
	meta.Thanos = metadata.Thanos{Labels: ext.Map(), Downsample: metadata.ThanosDownsample{Resolution: 0}, Source: metadata.TestSource}
	testutil.Ok(t, meta.WriteToDir(log.NewNopLogger(), bdir))
	testutil.Ok(t, os.Rename(bdir, filepath.Join(dir, meta.ULID.String())))
	return meta.ULID
}

// splitTestCompactor is a BucketCompactor set up like the compactor command with splitting enabled.
type splitTestCompactor struct {
	bkt      objstore.InstrumentedBucket
	sy       *Syncer
	grouper  *SplitGrouper
	compactr *splitTrackingCompactor
	bc       *BucketCompactor
}

func newSplitTestCompactor(t *testing.T, bkt objstore.InstrumentedBucket, ranges []int64, cfg SplitConfig, concurrentJobs bool, concurrency int) *splitTestCompactor {
	t.Helper()

	logger := log.NewNopLogger()
	reg := prometheus.NewRegistry()
	counter := func() prometheus.Counter { return promauto.With(nil).NewCounter(prometheus.CounterOpts{}) }
	ignoreDeletionMarkFilter := block.NewIgnoreDeletionMarkFilter(logger, bkt, 48*time.Hour, fetcherConcurrency)
	duplicateBlocksFilter := block.NewDeduplicateFilter(fetcherConcurrency)
	noCompactMarkerFilter := NewGatherNoCompactionMarkFilter(logger, bkt, 2)
	metaFetcher, err := block.NewMetaFetcher(nil, 32, bkt, block.NewConcurrentLister(logger, bkt), "", nil, []block.MetadataFilter{
		ignoreDeletionMarkFilter,
		block.NewReplicaLabelRemover(logger, []string{"replica"}),
		duplicateBlocksFilter,
		noCompactMarkerFilter,
	})
	testutil.Ok(t, err)
	sy, err := NewMetaSyncer(nil, nil, bkt, metaFetcher, duplicateBlocksFilter, ignoreDeletionMarkFilter, counter(), counter(), 0)
	testutil.Ok(t, err)

	leveled, err := tsdb.NewLeveledCompactor(context.Background(), nil, logutil.GoKitLogToSlog(logger), ranges, nil, storage.NewCompactingChunkSeriesMerger(storage.ChainedSeriesMerge))
	testutil.Ok(t, err)
	tracker := &splitTrackingCompactor{Compactor: leveled, laneWait: concurrency, laneRelease: make(chan struct{})}

	base := NewDefaultGrouper(logger, bkt, false, true, reg, counter(), counter(), counter(), metadata.NoneFunc, 1, 1)
	sg, err := NewSplitGrouper(logger, base, concurrentJobs, ranges, cfg, noCompactMarkerFilter, reg)
	testutil.Ok(t, err)
	var p noCompactAwarePlanner = NewPlanner(logger, ranges, sg)
	if concurrentJobs {
		p = NewConcurrentJobsPlanner(logger, sg)
	}
	planner := WithVerticalCompactionDownsampleFilter(WithLargeTotalIndexSizeFilter(p, bkt, math.MaxInt64, counter()), bkt, counter())

	bc, err := NewBucketCompactorWithCheckerAndCallback(logger, sy, sg, sg.Planner(planner), sg.Compactor(tracker),
		sg.BlockDeletableChecker(DefaultBlockDeletableChecker{}), sg.CompactionLifecycleCallback(DefaultCompactionLifecycleCallback{}),
		t.TempDir(), bkt, concurrency, false, nil)
	testutil.Ok(t, err)
	return &splitTestCompactor{bkt: bkt, sy: sy, grouper: sg, compactr: tracker, bc: bc}
}

// splitTrackingCompactor counts split jobs and records how many compactions of shard blocks run at the same time. The
// first compactions of shard blocks wait (up to a timeout) until laneWait of them run together, so that the test does
// not depend on scheduling.
type splitTrackingCompactor struct {
	Compactor

	splitJobs atomic.Int64

	mtx             sync.Mutex
	laneWait        int
	laneInFlight    int
	maxLaneInFlight int
	laneRelease     chan struct{}
	released        bool
}

func (c *splitTrackingCompactor) CompactWithBlockPopulator(dest string, dirs []string, open []*tsdb.Block, p tsdb.BlockPopulator) ([]ulid.ULID, error) {
	if _, ok := p.(PartitionedBlockPopulator); ok {
		c.splitJobs.Add(1)
		return c.Compactor.CompactWithBlockPopulator(dest, dirs, open, p)
	}
	if _, ok := p.(keepEmptyPopulator); !ok {
		return c.Compactor.CompactWithBlockPopulator(dest, dirs, open, p)
	}

	c.mtx.Lock()
	c.laneInFlight++
	c.maxLaneInFlight = max(c.maxLaneInFlight, c.laneInFlight)
	if c.laneInFlight >= c.laneWait && !c.released {
		c.released = true
		close(c.laneRelease)
	}
	c.mtx.Unlock()
	select {
	case <-c.laneRelease:
	case <-time.After(10 * time.Second):
	}
	defer func() {
		c.mtx.Lock()
		c.laneInFlight--
		c.mtx.Unlock()
	}()
	return c.Compactor.CompactWithBlockPopulator(dest, dirs, open, p)
}

// splitTestBlock is a block of the bucket, with its content: samples by series.
type splitTestBlock struct {
	meta    *metadata.Meta
	samples map[string][]string
}

// readSplitTestBlocks returns the blocks of the bucket that have a meta.json, whether marked for deletion or not.
func readSplitTestBlocks(t *testing.T, bkt objstore.Bucket) (live, deleted []splitTestBlock) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	testutil.Ok(t, bkt.Iter(ctx, "", func(name string) error {
		id, ok := block.IsBlockDir(name)
		if !ok {
			return nil
		}
		if ok, err := bkt.Exists(ctx, path.Join(id.String(), metadata.MetaFilename)); err != nil || !ok {
			return err // A partial upload.
		}
		isDeleted, err := bkt.Exists(ctx, path.Join(id.String(), metadata.DeletionMarkFilename))
		if err != nil {
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
		samples, err := readSplitTestSamples(bdir)
		if err != nil {
			return err
		}
		b := splitTestBlock{meta: meta, samples: samples}
		if isDeleted {
			deleted = append(deleted, b)
		} else {
			live = append(live, b)
		}
		return nil
	}))
	return live, deleted
}

func readSplitTestSamples(bdir string) (_ map[string][]string, err error) {
	b, err := tsdb.OpenBlock(logutil.GoKitLogToSlog(log.NewNopLogger()), bdir, nil, nil)
	if err != nil {
		return nil, err
	}
	defer runutil.CloseWithErrCapture(&err, b, "close block")
	q, err := tsdb.NewBlockQuerier(b, math.MinInt64, math.MaxInt64)
	if err != nil {
		return nil, err
	}
	defer runutil.CloseWithErrCapture(&err, q, "close querier")

	res := map[string][]string{}
	set := q.Select(context.Background(), true, nil, labels.MustNewMatcher(labels.MatchRegexp, "a", ".+"))
	for set.Next() {
		s := set.At()
		key := s.Labels().String()
		it := s.Iterator(nil)
		for it.Next() == chunkenc.ValFloat {
			ts, v := it.At()
			res[key] = append(res[key], fmt.Sprintf("%d=%v", ts, v))
		}
		if err := it.Err(); err != nil {
			return nil, err
		}
	}
	return res, set.Err()
}

// streamOf returns the blocks of a stream, by the value of its tenant label.
func streamOf(blocks []splitTestBlock, tenant string) []splitTestBlock {
	var res []splitTestBlock
	for _, b := range blocks {
		if b.meta.Thanos.Labels["tenant"] == tenant {
			res = append(res, b)
		}
	}
	return res
}

// unionSamples returns the samples of all blocks by series, and the samples that appear in more than one block.
func unionSamples(blocks []splitTestBlock) (union map[string][]string, duplicated []string) {
	union = map[string][]string{}
	seen := map[string]struct{}{}
	for _, b := range blocks {
		for s, samples := range b.samples {
			for _, smpl := range samples {
				k := s + " " + smpl
				if _, ok := seen[k]; ok {
					duplicated = append(duplicated, k)
					continue
				}
				seen[k] = struct{}{}
				union[s] = append(union[s], smpl)
			}
		}
	}
	for s := range union {
		sortSamples(union[s])
	}
	return union, duplicated
}

func sortSamples(samples []string) {
	sort.Slice(samples, func(i, j int) bool {
		var ti, tj int64
		_, _ = fmt.Sscanf(samples[i], "%d=", &ti)
		_, _ = fmt.Sscanf(samples[j], "%d=", &tj)
		return ti < tj
	})
}

// summarizeSplitBlocks returns "<shard> [mint,maxt) <n> series" of each block, in hours, sorted.
func summarizeSplitBlocks(blocks []splitTestBlock) []string {
	var res []string
	for _, b := range blocks {
		shard := b.meta.Thanos.Labels[metadata.CompactorShardIDLabel]
		if shard == "" {
			shard = "unsplit"
		}
		res = append(res, fmt.Sprintf("%s [%d,%d) %d series", shard, b.meta.MinTime/splitTestHour, b.meta.MaxTime/splitTestHour, len(b.samples)))
	}
	sort.Strings(res)
	return res
}

// splitTestData uploads, for stream {tenant="big"}, two replicas of each window [i*2h, (i+1)*2h) for i < windows, and
// returns their series and the dirs of the blocks of each window.
type splitTestData struct {
	dir     string
	windows [][]string
	big     []labels.Labels
}

func uploadSplitTestData(t *testing.T, bkt objstore.Bucket, windows int) splitTestData {
	t.Helper()
	ctx := context.Background()

	data := splitTestData{dir: t.TempDir()}
	var common []labels.Labels
	for i := range 24 {
		common = append(common, labels.FromStrings("__name__", fmt.Sprintf("metric_%d", i%3), "a", fmt.Sprintf("%d", i), "job", "api"))
	}
	// Each replica also has series the other one does not have.
	onlyA := []labels.Labels{labels.FromStrings("a", "only-a-1"), labels.FromStrings("a", "only-a-2")}
	onlyB := []labels.Labels{labels.FromStrings("a", "only-b-1")}
	data.big = slices.Concat(common, onlyA, onlyB)
	sparse := []labels.Labels{labels.FromStrings("a", "lonely")}
	small := common[:3]

	for w := range windows {
		mint, maxt := int64(w)*2*splitTestHour, int64(w+1)*2*splitTestHour
		var dirs []string
		for _, r := range []struct {
			name   string
			series []labels.Labels
			offset int64
		}{
			{name: "A", series: slices.Concat(common, onlyA), offset: 0},
			{name: "B", series: slices.Concat(common, onlyB), offset: splitTestStep / 2},
		} {
			id := createSplitTestBlock(t, data.dir, r.series, mint, maxt, r.offset, labels.FromStrings("tenant", "big", "replica", r.name))
			dirs = append(dirs, filepath.Join(data.dir, id.String()))
			testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(data.dir, id.String()), metadata.NoneFunc))
		}
		data.windows = append(data.windows, dirs)

		// A stream with a single series: most of its shards are empty.
		id := createSplitTestBlock(t, data.dir, sparse, mint, maxt, 0, labels.FromStrings("tenant", "sparse"))
		testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(data.dir, id.String()), metadata.NoneFunc))
		// A stream that is not split.
		id = createSplitTestBlock(t, data.dir, small, mint, maxt, 0, labels.FromStrings("tenant", "small"))
		testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(data.dir, id.String()), metadata.NoneFunc))
	}
	return data
}

// unsplitVerticalCompaction compacts the blocks of each window of the stream {tenant="big"} without splitting, like
// vertical compaction does, and returns the samples by series.
func (d splitTestData) unsplitVerticalCompaction(t *testing.T, windows int) map[string][]string {
	t.Helper()
	comp, err := tsdb.NewLeveledCompactor(context.Background(), nil, logutil.GoKitLogToSlog(log.NewNopLogger()), []int64{2 * splitTestHour}, nil, storage.NewCompactingChunkSeriesMerger(storage.ChainedSeriesMerge))
	testutil.Ok(t, err)
	out := t.TempDir()
	var blocks []splitTestBlock
	for _, dirs := range d.windows[:windows] {
		ids, err := comp.Compact(out, dirs, nil)
		testutil.Ok(t, err)
		testutil.Equals(t, 1, len(ids))
		samples, err := readSplitTestSamples(filepath.Join(out, ids[0].String()))
		testutil.Ok(t, err)
		blocks = append(blocks, splitTestBlock{samples: samples})
	}
	union, duplicated := unionSamples(blocks)
	testutil.Equals(t, 0, len(duplicated))
	return union
}

var splitTestConfig = SplitConfig{
	Shards: 1,
	Overrides: []SplitOverride{
		{Matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "tenant", "big|sparse")}, Shards: 4},
	},
}

// checkShards checks that each shard block records the scheme and that each of its series lies in the shard its hash
// says.
func checkShards(t *testing.T, blocks []splitTestBlock) {
	t.Helper()
	for _, b := range blocks {
		shardID, ok := b.meta.Thanos.ShardID()
		if !ok {
			continue
		}
		index, count, err := metadata.ParseShardID(shardID)
		testutil.Ok(t, err)
		scheme, err := b.meta.Thanos.SplitScheme()
		testutil.Ok(t, err)
		testutil.Assert(t, scheme != nil, "shard block %s records no scheme", b.meta.ULID)
		testutil.Equals(t, metadata.SplitScheme{Hash: metadata.SplitHashStable, Shards: 4}, *scheme)
		shard := SeriesShard{Index: index, Count: count}
		for s := range b.samples {
			lset, err := parseSeriesKey(s)
			testutil.Ok(t, err)
			testutil.Assert(t, shard.Contains(lset), "series %s in shard %s of block %s hashes elsewhere", s, shardID, b.meta.ULID)
		}
	}
}

func parseSeriesKey(s string) (labels.Labels, error) {
	var b labels.ScratchBuilder
	for _, kv := range strings.Split(strings.Trim(s, "{}"), ", ") {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			return labels.EmptyLabels(), errors.Errorf("bad label %q", kv)
		}
		b.Add(k, strings.Trim(v, `"`))
	}
	b.Sort()
	return b.Labels(), nil
}

// TestSplit_BucketCompactor splits two replicas of a stream over several windows into four shards, and compacts the
// shards further, with and without concurrent jobs.
func TestSplit_BucketCompactor(t *testing.T) {
	t.Parallel()

	ranges := []int64{2 * splitTestHour, 8 * splitTestHour, 2 * splitTestDay}
	const windows = 10
	for _, concurrentJobs := range []bool{false, true} {
		t.Run(fmt.Sprintf("concurrentJobs=%v", concurrentJobs), func(t *testing.T) {
			t.Parallel()

			bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())
			data := uploadSplitTestData(t, bkt, windows)
			c := newSplitTestCompactor(t, bkt, ranges, splitTestConfig, concurrentJobs, 4)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			testutil.Ok(t, c.bc.Compact(ctx))

			live, deleted := readSplitTestBlocks(t, bkt)
			big := streamOf(live, "big")

			// (a) Every window but the newest one was split in four shards; the shards then compacted to 8h where
			// a later block of the shard exists. The replicas of the newest window were vertically compacted.
			testutil.Equals(t, []string{
				"1_of_4 [0,8) 6 series", "1_of_4 [16,18) 6 series", "1_of_4 [8,16) 6 series",
				"2_of_4 [0,8) 7 series", "2_of_4 [16,18) 7 series", "2_of_4 [8,16) 7 series",
				"3_of_4 [0,8) 6 series", "3_of_4 [16,18) 6 series", "3_of_4 [8,16) 6 series",
				"4_of_4 [0,8) 8 series", "4_of_4 [16,18) 8 series", "4_of_4 [8,16) 8 series",
				"unsplit [18,20) 27 series",
			}, summarizeSplitBlocks(big))
			checkShards(t, big)

			// The shards together hold exactly what an unsplit vertical compaction of the same blocks holds: every
			// series once, every sample of both replicas once.
			union, duplicated := unionSamples(big)
			testutil.Equals(t, 0, len(duplicated), "samples held twice: %v", duplicated)
			testutil.Equals(t, data.unsplitVerticalCompaction(t, windows), union)

			// Every unsplit source of a split window was retired by garbage collection; split jobs mark nothing.
			var retired []string
			for _, b := range streamOf(deleted, "big") {
				if _, ok := b.meta.Thanos.ShardID(); !ok && b.meta.Compaction.Level == 1 {
					retired = append(retired, fmt.Sprintf("[%d,%d)", b.meta.MinTime/splitTestHour, b.meta.MaxTime/splitTestHour))
				}
			}
			testutil.Equals(t, 2*windows, len(retired), "retired sources: %v", retired)

			// The sparse stream: its single series sits in one shard, the other shards are empty blocks, which
			// compact further like any other.
			sparse := streamOf(live, "sparse")
			testutil.Equals(t, 13, len(sparse))
			var nonEmpty []string
			for _, b := range sparse {
				if len(b.samples) > 0 {
					nonEmpty = append(nonEmpty, b.meta.Thanos.Labels[metadata.CompactorShardIDLabel])
				}
			}
			testutil.Equals(t, 4, len(nonEmpty), "only one shard of each range holds the series: %v", nonEmpty)
			checkShards(t, sparse)

			// The stream that is not split compacted as usual.
			testutil.Equals(t, []string{"unsplit [0,8) 3 series", "unsplit [16,18) 3 series", "unsplit [18,20) 3 series", "unsplit [8,16) 3 series"}, summarizeSplitBlocks(streamOf(live, "small")))

			// (c) Each window was split by four jobs, and the shards compacted 2h to 8h independently, at the same
			// time.
			testutil.Equals(t, int64(2*4*(windows-1)), c.compactr.splitJobs.Load())
			testutil.Equals(t, 4, c.compactr.maxLaneInFlight)
		})
	}
}

// failingUploadBucket fails the upload of the meta.json of the blocks fail matches, so that they stay partial.
type failingUploadBucket struct {
	objstore.InstrumentedBucket

	fail   atomic.Pointer[func(*metadata.Meta) bool]
	failed atomic.Int64
}

func (b *failingUploadBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	if fail := b.fail.Load(); fail != nil && path.Base(name) == metadata.MetaFilename {
		content, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		var m metadata.Meta
		if err := json.Unmarshal(content, &m); err != nil {
			return err
		}
		if (*fail)(&m) {
			b.failed.Add(1)
			return errors.New("injected upload failure")
		}
		r = bytes.NewReader(content)
	}
	return b.InstrumentedBucket.Upload(ctx, name, r, opts...)
}

// TestSplit_PartialSplitNeverLosesData fails one shard of a split, and checks that the unsplit sources of its window
// are not retired, that no data is missing, and that the next run completes the split. A block uploaded late for a
// split window is then split alone and merged into the shards by vertical compaction.
func TestSplit_PartialSplitNeverLosesData(t *testing.T) {
	t.Parallel()

	ranges := []int64{2 * splitTestHour, 8 * splitTestHour, 2 * splitTestDay}
	const windows = 6
	bkt := &failingUploadBucket{InstrumentedBucket: objstore.WithNoopInstr(objstore.NewInMemBucket())}
	data := uploadSplitTestData(t, bkt, windows)
	expected := data.unsplitVerticalCompaction(t, windows)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Shard 3 of [2h, 4h) of the big stream cannot be uploaded.
	failShard := func(m *metadata.Meta) bool {
		return m.Thanos.Labels["tenant"] == "big" && m.Thanos.Labels[metadata.CompactorShardIDLabel] == "3_of_4" && m.MinTime == 2*splitTestHour
	}
	bkt.fail.Store(&failShard)
	c := newSplitTestCompactor(t, bkt, ranges, splitTestConfig, true, 1)
	err := c.bc.Compact(ctx)
	testutil.NotOk(t, err)
	testutil.Assert(t, IsRetryError(err), "an upload failure is retried: %v", err)
	testutil.Equals(t, int64(1), bkt.failed.Load())

	// A pass and a garbage collection later, the window with three shards out of four keeps its unsplit sources, and
	// its data is all there, in the unsplit blocks and the shards.
	testutil.Ok(t, c.sy.SyncMetas(ctx))
	testutil.Ok(t, c.sy.GarbageCollect(ctx, nil))
	live, deleted := readSplitTestBlocks(t, bkt)
	var keptW1, shardsW1 []string
	for _, b := range streamOf(live, "big") {
		if b.meta.MinTime != 2*splitTestHour {
			continue
		}
		if id, ok := b.meta.Thanos.ShardID(); ok {
			shardsW1 = append(shardsW1, id)
		} else {
			keptW1 = append(keptW1, b.meta.Thanos.Labels["replica"])
		}
	}
	slices.Sort(keptW1)
	slices.Sort(shardsW1)
	testutil.Equals(t, []string{"A", "B"}, keptW1)
	testutil.Equals(t, []string{"1_of_4", "2_of_4"}, shardsW1)
	for _, b := range streamOf(deleted, "big") {
		_, isShard := b.meta.Thanos.ShardID()
		testutil.Assert(t, isShard || b.meta.MinTime != 2*splitTestHour, "source %s of a window not split completely was retired", b.meta.ULID)
	}
	union, _ := unionSamples(streamOf(live, "big"))
	testutil.Equals(t, expected, union, "no data is lost by a partial split")

	// The next run splits the missing shards, without splitting again those that exist, and retires the sources.
	bkt.fail.Store(nil)
	c.compactr.splitJobs.Store(0)
	testutil.Ok(t, c.bc.Compact(ctx))
	// The split jobs of both split streams, less the shards of the big stream that exist.
	testutil.Equals(t, int64(2*4*(windows-1)-4-2), c.compactr.splitJobs.Load(), "the shards that exist are not split again")
	live, deleted = readSplitTestBlocks(t, bkt)
	big := streamOf(live, "big")
	for _, b := range big {
		_, isShard := b.meta.Thanos.ShardID()
		testutil.Assert(t, isShard || b.meta.MinTime == 10*splitTestHour, "unsplit block %s [%d, %d) left", b.meta.ULID, b.meta.MinTime, b.meta.MaxTime)
	}
	union, duplicated := unionSamples(big)
	testutil.Equals(t, 0, len(duplicated))
	testutil.Equals(t, expected, union)
	checkShards(t, big)
	// The sources of the split windows, and the replicas of the newest window, vertically compacted.
	testutil.Equals(t, 2*windows, countUnsplitSources(streamOf(deleted, "big")))

	// A replica uploaded late for [4h, 6h), with another series, is split alone, and vertical compaction merges its
	// shards into the shard blocks of that window.
	late := []labels.Labels{labels.FromStrings("a", "late"), data.big[0]}
	id := createSplitTestBlock(t, data.dir, late, 4*splitTestHour, 6*splitTestHour, splitTestStep/4, labels.FromStrings("tenant", "big", "replica", "C"))
	testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(data.dir, id.String()), metadata.NoneFunc))
	data.windows[2] = append(data.windows[2], filepath.Join(data.dir, id.String()))
	testutil.Ok(t, c.bc.Compact(ctx))

	live, deleted = readSplitTestBlocks(t, bkt)
	big = streamOf(live, "big")
	union, duplicated = unionSamples(big)
	testutil.Equals(t, 0, len(duplicated))
	testutil.Equals(t, data.unsplitVerticalCompaction(t, windows), union)
	checkShards(t, big)
	testutil.Equals(t, 2*windows+1, countUnsplitSources(streamOf(deleted, "big")))
	for _, b := range big {
		if b.meta.MinTime < 8*splitTestHour && b.meta.MaxTime > 4*splitTestHour {
			testutil.Assert(t, slices.Contains(b.meta.Compaction.Sources, id), "shard block %s of the late window does not hold the late block", b.meta.ULID)
		}
	}
}

func countUnsplitSources(blocks []splitTestBlock) int {
	n := 0
	for _, b := range blocks {
		if _, ok := b.meta.Thanos.ShardID(); !ok && b.meta.Compaction.Level == 1 {
			n++
		}
	}
	return n
}
