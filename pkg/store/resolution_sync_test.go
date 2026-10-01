// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/gogo/protobuf/types"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"
	"go.uber.org/atomic"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/indexheader"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/logutil"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/store/hintspb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

// faultyIndexBucket fails reads of the index of blocks whose ID starts with
// prefix while fail is set, and counts index reads and attribute lookups.
type faultyIndexBucket struct {
	objstore.Bucket
	prefix     string
	fail       atomic.Bool
	failures   atomic.Int64
	indexReads atomic.Int64
	attributes atomic.Int64
}

func (b *faultyIndexBucket) index(name string) error {
	if !strings.HasSuffix(name, "/"+block.IndexFilename) {
		return nil
	}
	b.indexReads.Inc()
	if b.fail.Load() && strings.HasPrefix(name, b.prefix) {
		b.failures.Inc()
		return errors.New("injected index read failure")
	}
	return nil
}

func (b *faultyIndexBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if err := b.index(name); err != nil {
		return nil, err
	}
	return b.Bucket.Get(ctx, name)
}

func (b *faultyIndexBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if err := b.index(name); err != nil {
		return nil, err
	}
	return b.Bucket.GetRange(ctx, name, off, length)
}

func (b *faultyIndexBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	b.attributes.Inc()
	return b.Bucket.Attributes(ctx, name)
}

// resolutionTestStore is a bucket store over bkt whose fetcher runs the
// resolution filter, with a minimum of 5m, ahead of the given filters.
type resolutionTestStore struct {
	*BucketStore
	filter *block.ResolutionMetaFilter
	reg    *prometheus.Registry
}

type resolutionTestStoreOpts struct {
	lazy        bool
	download    indexheader.LazyDownloadIndexHeaderFunc
	concurrency int
	following   []block.MetadataFilter
	gauge       prometheus.Gauge
	options     []BucketStoreOption
}

func newResolutionTestStore(t *testing.T, bkt objstore.InstrumentedBucket, o resolutionTestStoreOpts) resolutionTestStore {
	t.Helper()
	logger := log.NewNopLogger()
	if o.download == nil {
		o.download = indexheader.AlwaysEagerDownloadIndexHeader
	}
	if o.concurrency == 0 {
		o.concurrency = 1
	}
	filter := block.NewResolutionMetaFilter(logger, downsample.ResLevel1, downsample.ResLevel2, o.gauge)
	fetcher, err := block.NewMetaFetcher(logger, 1, bkt, block.NewConcurrentLister(logger, bkt), "", nil, append([]block.MetadataFilter{filter}, o.following...))
	testutil.Ok(t, err)
	reg := prometheus.NewRegistry()
	s, err := NewBucketStore(
		bkt,
		fetcher,
		t.TempDir(),
		NewChunksLimiterFactory(1000),
		NewSeriesLimiterFactory(0),
		NewBytesLimiterFactory(0),
		NewGapBasedPartitioner(PartitionerMaxGapSize),
		o.concurrency,
		false,
		DefaultPostingOffsetInMemorySampling,
		false,
		o.lazy,
		0,
		append([]BucketStoreOption{
			WithRegistry(reg),
			WithIndexHeaderLazyDownloadStrategy(o.download),
			WithResolutionFilter(filter, o.following...),
		}, o.options...)...,
	)
	testutil.Ok(t, err)
	t.Cleanup(func() { testutil.Ok(t, s.Close()) })
	return resolutionTestStore{BucketStore: s, filter: filter, reg: reg}
}

// writeRawAndDownsampled writes a raw block of one hour of "up" samples
// starting at mint, and the 5m block made from it, to dir.
func writeRawAndDownsampled(t *testing.T, dir string, mint int64) (raw, coarse ulid.ULID) {
	t.Helper()
	ctx := t.Context()
	logger := log.NewNopLogger()
	raw, err := e2eutil.CreateBlock(ctx, dir, []labels.Labels{labels.FromStrings("__name__", "up")}, 20, mint, mint+3600000, labels.FromStrings("tenant", "one"), 0, metadata.NoneFunc, nil)
	testutil.Ok(t, err)
	m, err := metadata.ReadFromDir(filepath.Join(dir, raw.String()))
	testutil.Ok(t, err)
	input, err := tsdb.OpenBlock(logutil.GoKitLogToSlog(logger), filepath.Join(dir, raw.String()), nil, nil)
	testutil.Ok(t, err)
	coarse, err = downsample.Downsample(ctx, logger, m, input, dir, downsample.ResLevel1)
	testutil.Ok(t, err)
	testutil.Ok(t, input.Close())
	return raw, coarse
}

func uploadBlock(t *testing.T, bkt objstore.Bucket, dir string, id ulid.ULID) {
	t.Helper()
	testutil.Ok(t, block.Upload(t.Context(), log.NewNopLogger(), bkt, filepath.Join(dir, id.String()), metadata.NoneFunc))
}

func uploadMeta(t *testing.T, bkt objstore.Bucket, m metadata.Meta) {
	t.Helper()
	body, err := json.Marshal(m)
	testutil.Ok(t, err)
	testutil.Ok(t, bkt.Upload(t.Context(), path.Join(m.ULID.String(), metadata.MetaFilename), bytes.NewReader(body)))
}

var indexReaderModes = []struct {
	name     string
	lazy     bool
	download indexheader.LazyDownloadIndexHeaderFunc
}{
	{"eager", false, indexheader.AlwaysEagerDownloadIndexHeader},
	{"lazy mmap", true, indexheader.AlwaysEagerDownloadIndexHeader},
	{"lazy download", true, indexheader.AlwaysLazyDownloadIndexHeader},
}

func TestResolutionFallbackRespectsTimePartition(t *testing.T) {
	bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())
	raw, coarse := ulid.MustNew(1, nil), ulid.MustNew(2, nil)
	for id, resolution := range map[ulid.ULID]int64{raw: 0, coarse: downsample.ResLevel1} {
		m := metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: id, Version: 1, MinTime: 0, MaxTime: 50,
			Compaction: tsdb.BlockMetaCompaction{Sources: []ulid.ULID{raw}}}}
		m.Thanos.Downsample.Resolution = resolution
		uploadMeta(t, bkt, m)
	}
	minTime, maxTime := time.UnixMilli(100), time.UnixMilli(200)
	partition := block.NewTimePartitionMetaFilter(model.TimeOrDurationValue{Time: &minTime}, model.TimeOrDurationValue{Time: &maxTime})
	s := newResolutionTestStore(t, bkt, resolutionTestStoreOpts{following: []block.MetadataFilter{partition}})
	// Neither block has an index: loading either out-of-partition block is an
	// error. A fallback must pass the same time filter as normal candidates.
	testutil.Ok(t, s.SyncBlocks(t.Context()))
	testutil.Equals(t, 0, len(s.blocks))
}

// TestResolutionFallbackSurvivesCoverLoadFailure: with an index header built
// when a block is added, a cover whose index cannot be read fails to load,
// and the finer block it would hide stays or comes back.
func TestResolutionFallbackSurvivesCoverLoadFailure(t *testing.T) {
	for _, reader := range indexReaderModes[:2] {
		t.Run(reader.name, func(t *testing.T) {
			for _, warm := range []bool{false, true} {
				t.Run(map[bool]string{false: "cold start", true: "already serving raw"}[warm], func(t *testing.T) {
					ctx := t.Context()
					dir := t.TempDir()
					fault := &faultyIndexBucket{Bucket: objstore.NewInMemBucket()}
					bkt := objstore.WithNoopInstr(fault)
					id, coarseID := writeRawAndDownsampled(t, dir, 0)
					uploadBlock(t, bkt, dir, id)
					fault.prefix = coarseID.String() + "/"
					fault.fail.Store(true)
					s := newResolutionTestStore(t, bkt, resolutionTestStoreOpts{lazy: reader.lazy, download: reader.download})
					if warm {
						testutil.Ok(t, s.SyncBlocks(ctx))
						testutil.Assert(t, s.getBlock(id) != nil, "raw must initially be served")
					}
					uploadBlock(t, bkt, dir, coarseID)
					testutil.Ok(t, s.SyncBlocks(ctx))
					testutil.Assert(t, fault.failures.Load() > 0, "the cover's index read must fail")
					testutil.Assert(t, s.getBlock(coarseID) == nil, "cover load must fail")
					fallback := s.getBlock(id)
					testutil.Assert(t, fallback != nil, "a failed cover must not hide available raw samples")
					selected := s.blockSets[fallback.extLset.Hash()].getFor(0, 3599999, downsample.ResLevel1, nil)
					testutil.Equals(t, 1, len(selected))
					testutil.Equals(t, id, selected[0].meta.ULID)

					fault.fail.Store(false)
					testutil.Ok(t, s.SyncBlocks(ctx))
					testutil.Assert(t, s.getBlock(coarseID) != nil, "cover must load after recovery")
					testutil.Assert(t, s.getBlock(id) == nil, "a loaded cover retires raw")
					// Losing the cover must bring the raw block back.
					testutil.Ok(t, block.Delete(ctx, log.NewNopLogger(), bkt, coarseID))
					testutil.Ok(t, s.SyncBlocks(ctx))
					testutil.Assert(t, s.getBlock(id) != nil, "raw must return when downsampled coverage disappears")
				})
			}
		})
	}
}

// TestResolutionFallbackSurvivesIncompleteCover: a cover whose objects are
// incomplete in the bucket hides nothing in any index reader mode, including
// lazy downloading, where adding it reads nothing.
func TestResolutionFallbackSurvivesIncompleteCover(t *testing.T) {
	for _, reader := range indexReaderModes {
		t.Run(reader.name, func(t *testing.T) {
			for _, tc := range []struct {
				name    string
				corrupt func(t *testing.T, bkt objstore.Bucket, dir string, id ulid.ULID)
				// fix makes the cover complete, if that can be done in place.
				fix func(t *testing.T, bkt objstore.Bucket, dir string, id ulid.ULID)
			}{
				{
					name: "missing index",
					corrupt: func(t *testing.T, bkt objstore.Bucket, _ string, id ulid.ULID) {
						testutil.Ok(t, bkt.Delete(t.Context(), path.Join(id.String(), block.IndexFilename)))
					},
					fix: func(t *testing.T, bkt objstore.Bucket, dir string, id ulid.ULID) {
						f, err := os.Open(filepath.Join(dir, id.String(), block.IndexFilename))
						testutil.Ok(t, err)
						defer f.Close()
						testutil.Ok(t, bkt.Upload(t.Context(), path.Join(id.String(), block.IndexFilename), f))
					},
				},
				{
					name: "index size differs from meta.json",
					corrupt: func(t *testing.T, bkt objstore.Bucket, _ string, id ulid.ULID) {
						m, err := block.DownloadMeta(t.Context(), log.NewNopLogger(), bkt, id)
						testutil.Ok(t, err)
						for i, f := range m.Thanos.Files {
							if f.RelPath == block.IndexFilename {
								m.Thanos.Files[i].SizeBytes++
							}
						}
						uploadMeta(t, bkt, m)
					},
					// The fetcher reads a block's meta.json once.
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					ctx := t.Context()
					dir := t.TempDir()
					inmem := objstore.NewInMemBucket()
					bkt := objstore.WithNoopInstr(inmem)
					id, coarseID := writeRawAndDownsampled(t, dir, 0)
					uploadBlock(t, bkt, dir, id)
					uploadBlock(t, bkt, dir, coarseID)
					tc.corrupt(t, inmem, dir, coarseID)
					s := newResolutionTestStore(t, bkt, resolutionTestStoreOpts{lazy: reader.lazy, download: reader.download})

					testutil.Ok(t, s.SyncBlocks(ctx))
					testutil.Assert(t, s.getBlock(id) != nil, "an incomplete cover must not hide raw samples")
					testutil.Assert(t, s.getBlock(coarseID) == nil, "an incomplete cover is not served")
					// A second sync re-adds and re-checks the cover, and still serves raw.
					testutil.Ok(t, s.SyncBlocks(ctx))
					testutil.Assert(t, s.getBlock(id) != nil, "an incomplete cover must not hide raw samples")

					if tc.fix == nil {
						return
					}
					tc.fix(t, inmem, dir, coarseID)
					testutil.Ok(t, s.SyncBlocks(ctx))
					testutil.Assert(t, s.getBlock(coarseID) != nil, "a complete cover is served")
					testutil.Assert(t, s.getBlock(id) == nil, "a complete cover retires raw")
				})
			}
		})
	}
}

// TestResolutionFallbackLoadFailureDoesNotFailSync pins down that a fallback
// that cannot be loaded is tolerated like any other unloadable block: the sync
// still succeeds, so the store becomes ready and keeps dropping outdated
// blocks, and the next sync retries.
func TestResolutionFallbackLoadFailureDoesNotFailSync(t *testing.T) {
	bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())
	raw, coarse := ulid.MustNew(1, nil), ulid.MustNew(2, nil)
	for id, resolution := range map[ulid.ULID]int64{raw: 0, coarse: downsample.ResLevel1} {
		m := metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: id, Version: 1, MinTime: 0, MaxTime: 50,
			Compaction: tsdb.BlockMetaCompaction{Sources: []ulid.ULID{raw}}}}
		m.Thanos.Downsample.Resolution = resolution
		uploadMeta(t, bkt, m)
	}
	s := newResolutionTestStore(t, bkt, resolutionTestStoreOpts{})
	// Neither block has an index, so the cover fails to load and so does the
	// raw fallback. The sync reports neither as an error.
	testutil.Ok(t, s.SyncBlocks(t.Context()))
	testutil.Equals(t, 0, len(s.blocks))
}

// TestResolutionStraddlingFallbackTrustsCoverBeyondPartition pins down the
// reason the resolution filter runs before the time partition: a raw block
// straddling the partition boundary, covered on the near side by a served
// block and on the far side by one this store gateway does not serve, stays
// hidden instead of being loaded and reported as uncovered at every shard
// boundary.
func TestResolutionStraddlingFallbackTrustsCoverBeyondPartition(t *testing.T) {
	ctx := t.Context()
	logger := log.NewNopLogger()
	bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())
	dir := t.TempDir()
	rawID, nearID := writeRawAndDownsampled(t, dir, 0)
	// The near cover serves the first half of the raw block's range; the far
	// cover, the second half, exists only as metadata on the other side of the
	// partition. Whether it could be loaded is the other store's concern.
	nearMeta, err := metadata.ReadFromDir(filepath.Join(dir, nearID.String()))
	testutil.Ok(t, err)
	nearMeta.MaxTime = 1800000
	testutil.Ok(t, nearMeta.WriteToDir(logger, filepath.Join(dir, nearID.String())))
	farMeta := *nearMeta
	farMeta.ULID, farMeta.MinTime, farMeta.MaxTime = ulid.MustNew(99, nil), 1800000, 3600000
	uploadBlock(t, bkt, dir, rawID)
	uploadBlock(t, bkt, dir, nearID)
	uploadMeta(t, bkt, farMeta)

	minTime, maxTime := time.UnixMilli(0), time.UnixMilli(1799999)
	partition := block.NewTimePartitionMetaFilter(model.TimeOrDurationValue{Time: &minTime}, model.TimeOrDurationValue{Time: &maxTime})
	gauge := promauto.With(nil).NewGauge(prometheus.GaugeOpts{Name: "test_uncovered"})
	s := newResolutionTestStore(t, bkt, resolutionTestStoreOpts{following: []block.MetadataFilter{partition}, gauge: gauge})

	testutil.Ok(t, s.SyncBlocks(ctx))
	testutil.Assert(t, s.getBlock(nearID) != nil, "the in-window cover is served")
	testutil.Assert(t, s.getBlock(rawID) == nil, "the straddling raw block is covered on both sides and stays hidden")
	testutil.Assert(t, s.getBlock(farMeta.ULID) == nil, "the far cover is not this store's to serve")
	testutil.Equals(t, 0.0, promtest.ToFloat64(gauge))

	// Without the far cover the second half is nobody's: the raw block is
	// served, and reported.
	testutil.Ok(t, block.Delete(ctx, logger, bkt, farMeta.ULID))
	testutil.Ok(t, s.SyncBlocks(ctx))
	testutil.Assert(t, s.getBlock(rawID) != nil, "an uncovered far side brings the raw block back")
	testutil.Equals(t, 1.0, promtest.ToFloat64(gauge))
}

// lazyIndexHeaderLoads returns how many index headers the store's lazy
// readers loaded.
func lazyIndexHeaderLoads(t *testing.T, reg *prometheus.Registry) float64 {
	t.Helper()
	families, err := reg.Gather()
	testutil.Ok(t, err)
	for _, f := range families {
		if f.GetName() == "thanos_bucket_store_indexheader_lazy_load_total" {
			return f.GetMetric()[0].GetCounter().GetValue()
		}
	}
	t.Fatal("lazy load counter not registered")
	return 0
}

// TestResolutionCoverVerificationNeverBuildsIndexHeader: with lazy index
// header downloading, retiring a finer block behind a cover reads neither the
// cover's index nor builds its index header: it looks up the cover's
// meta.json and index objects once, and caches the result.
func TestResolutionCoverVerificationNeverBuildsIndexHeader(t *testing.T) {
	for _, warm := range []bool{false, true} {
		t.Run(map[bool]string{false: "cold start", true: "already serving raw"}[warm], func(t *testing.T) {
			ctx := t.Context()
			dir := t.TempDir()
			counting := &faultyIndexBucket{Bucket: objstore.NewInMemBucket()}
			bkt := objstore.WithNoopInstr(counting)
			id, coarseID := writeRawAndDownsampled(t, dir, 0)
			uploadBlock(t, bkt, dir, id)
			s := newResolutionTestStore(t, bkt, resolutionTestStoreOpts{lazy: true, download: indexheader.AlwaysLazyDownloadIndexHeader})
			if warm {
				testutil.Ok(t, s.SyncBlocks(ctx))
				testutil.Assert(t, s.getBlock(id) != nil, "raw must initially be served")
			}
			uploadBlock(t, bkt, dir, coarseID)
			counting.indexReads.Store(0)
			counting.attributes.Store(0)

			testutil.Ok(t, s.SyncBlocks(ctx))
			testutil.Assert(t, s.getBlock(coarseID) != nil, "the cover is served")
			testutil.Assert(t, s.getBlock(id) == nil, "the verified cover retires raw")
			testutil.Equals(t, int64(0), counting.indexReads.Load())
			testutil.Equals(t, 0.0, lazyIndexHeaderLoads(t, s.reg))
			_, err := os.Stat(filepath.Join(s.dir, coarseID.String(), block.IndexHeaderFilename))
			testutil.Assert(t, os.IsNotExist(err), "no index header may be built for the cover, got %v", err)
			// meta.json and index, once.
			testutil.Equals(t, int64(2), counting.attributes.Load())

			testutil.Ok(t, s.SyncBlocks(ctx))
			testutil.Equals(t, int64(2), counting.attributes.Load())
			testutil.Equals(t, int64(0), counting.indexReads.Load())

			// The counters do count: a query loads the cover's index header.
			srv := newStoreSeriesServer(ctx)
			testutil.Ok(t, s.Series(&storepb.SeriesRequest{MinTime: 0, MaxTime: 3600000, MaxResolutionWindow: downsample.ResLevel1, Matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "up"}}}, srv))
			testutil.Equals(t, 1, len(srv.SeriesSet))
			testutil.Equals(t, 1.0, lazyIndexHeaderLoads(t, s.reg))
			testutil.Assert(t, counting.indexReads.Load() > 0, "the query reads the cover's index")
		})
	}
}

// TestResolutionVerificationIsScopedToCovers: only a block that hides another
// is checked, so a 5m block hiding nothing costs nothing extra.
func TestResolutionVerificationIsScopedToCovers(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	counting := &faultyIndexBucket{Bucket: objstore.NewInMemBucket()}
	bkt := objstore.WithNoopInstr(counting)
	id, coarseID := writeRawAndDownsampled(t, dir, 0)
	uploadBlock(t, bkt, dir, coarseID)
	s := newResolutionTestStore(t, bkt, resolutionTestStoreOpts{lazy: true, download: indexheader.AlwaysLazyDownloadIndexHeader})

	testutil.Ok(t, s.SyncBlocks(ctx))
	testutil.Assert(t, s.getBlock(coarseID) != nil, "the 5m block is served")
	testutil.Equals(t, int64(0), counting.attributes.Load())

	// Once it hides a raw block, it is checked, once.
	uploadBlock(t, bkt, dir, id)
	testutil.Ok(t, s.SyncBlocks(ctx))
	testutil.Equals(t, int64(2), counting.attributes.Load())
	testutil.Ok(t, s.SyncBlocks(ctx))
	testutil.Equals(t, int64(2), counting.attributes.Load())
	testutil.Assert(t, s.getBlock(id) == nil, "the raw block stays hidden")

	// A block no longer hiding anything is forgotten, and checked again once
	// it hides something again.
	testutil.Ok(t, block.Delete(ctx, log.NewNopLogger(), bkt, id))
	testutil.Ok(t, s.SyncBlocks(ctx))
	s.resolution.verifiedMtx.Lock()
	testutil.Equals(t, 0, len(s.resolution.verified))
	s.resolution.verifiedMtx.Unlock()
}

// barrierBucket holds the first read of each watched object until all of
// them are being read, or a timeout passes.
type barrierBucket struct {
	objstore.Bucket

	mtx      sync.Mutex
	watched  map[string]bool
	started  map[string]bool
	released chan struct{}
	timedOut atomic.Bool
}

func (b *barrierBucket) hold(name string) {
	b.mtx.Lock()
	if !b.watched[name] || b.started[name] {
		b.mtx.Unlock()
		return
	}
	b.started[name] = true
	if len(b.started) == len(b.watched) {
		close(b.released)
	}
	b.mtx.Unlock()
	select {
	case <-b.released:
	case <-time.After(10 * time.Second):
		b.timedOut.Store(true)
	}
}

func (b *barrierBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	b.hold(name)
	return b.Bucket.Get(ctx, name)
}

func (b *barrierBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	b.hold(name)
	return b.Bucket.GetRange(ctx, name, off, length)
}

// TestResolutionFallbacksRestoreConcurrently: restoring fallbacks loads them
// with the store's block sync concurrency, so a cold start whose covers all
// fail does not load the finer blocks one by one.
func TestResolutionFallbacksRestoreConcurrently(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	inmem := objstore.NewInMemBucket()
	barrier := &barrierBucket{Bucket: inmem, watched: map[string]bool{}, started: map[string]bool{}, released: make(chan struct{})}
	bkt := objstore.WithNoopInstr(barrier)
	var raws []ulid.ULID
	for i := range int64(3) {
		id, coarseID := writeRawAndDownsampled(t, dir, i*3600000)
		uploadBlock(t, bkt, dir, id)
		uploadBlock(t, bkt, dir, coarseID)
		// The covers cannot load.
		testutil.Ok(t, inmem.Delete(ctx, path.Join(coarseID.String(), block.IndexFilename)))
		raws = append(raws, id)
		barrier.watched[path.Join(id.String(), block.IndexFilename)] = true
	}
	s := newResolutionTestStore(t, bkt, resolutionTestStoreOpts{concurrency: 3})

	testutil.Ok(t, s.SyncBlocks(ctx))
	for _, id := range raws {
		testutil.Assert(t, s.getBlock(id) != nil, "fallback %s must be served", id)
	}
	testutil.Assert(t, !barrier.timedOut.Load(), "fallbacks must load concurrently")
}

// hiddenWarningSetup uploads a raw block covered by its 5m block, so the
// store hides it, and a raw block nobody downsampled, which it serves.
func hiddenWarningSetup(t *testing.T, options ...BucketStoreOption) (resolutionTestStore, ulid.ULID) {
	t.Helper()
	dir := t.TempDir()
	bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())
	id, coarseID := writeRawAndDownsampled(t, dir, 0)
	uploadBlock(t, bkt, dir, id)
	uploadBlock(t, bkt, dir, coarseID)
	uncovered, err := e2eutil.CreateBlock(t.Context(), dir, []labels.Labels{labels.FromStrings("__name__", "up")}, 20, 7200000, 10800000, labels.FromStrings("tenant", "one"), 0, metadata.NoneFunc, nil)
	testutil.Ok(t, err)
	uploadBlock(t, bkt, dir, uncovered)

	s := newResolutionTestStore(t, bkt, resolutionTestStoreOpts{options: options})
	testutil.Ok(t, s.SyncBlocks(t.Context()))
	testutil.Assert(t, s.getBlock(id) == nil, "the covered raw block is hidden")
	testutil.Assert(t, s.getBlock(uncovered) != nil, "the uncovered raw block is served")
	return s, id
}

func seriesWarnings(t *testing.T, s *BucketStore, req *storepb.SeriesRequest) []string {
	t.Helper()
	if len(req.Matchers) == 0 {
		req.Matchers = []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "up"}}
	}
	srv := newStoreSeriesServer(t.Context())
	testutil.Ok(t, s.Series(req, srv))
	return srv.Warnings
}

// TestResolutionHiddenWarningIsOptIn: by default a request for data finer than
// the minimum gets no warning about hidden blocks, so strict partial response
// queries, rulers' included, do not fail on it, and no snapshot is kept.
func TestResolutionHiddenWarningIsOptIn(t *testing.T) {
	s, _ := hiddenWarningSetup(t)
	testutil.Equals(t, 0, len(seriesWarnings(t, s.BucketStore, &storepb.SeriesRequest{MinTime: 0, MaxTime: 3600000, PartialResponseDisabled: true})))
	testutil.Assert(t, s.resolution.hidden.Load() == nil, "no snapshot without warnings")
	testutil.Equals(t, 0.0, promtest.ToFloat64(s.resolution.warnings))
}

// TestResolutionHiddenWarning: with warnings enabled, only a request finer
// than the minimum whose range and labels overlap a hidden block is warned,
// and counted.
func TestResolutionHiddenWarning(t *testing.T) {
	s, hidden := hiddenWarningSetup(t, WithHiddenResolutionWarning(true))
	blockHints := func(m storepb.LabelMatcher) *types.Any {
		hints, err := types.MarshalAny(&hintspb.SeriesRequestHints{BlockMatchers: []storepb.LabelMatcher{m}})
		testutil.Ok(t, err)
		return hints
	}
	for _, tc := range []struct {
		name string
		req  *storepb.SeriesRequest
		warn bool
	}{
		{name: "finer request over the hidden block", req: &storepb.SeriesRequest{MinTime: 0, MaxTime: 3600000}, warn: true},
		{name: "finer request touching the hidden block's last millisecond", req: &storepb.SeriesRequest{MinTime: 3599999, MaxTime: 4000000}, warn: true},
		{name: "finer request after the hidden block", req: &storepb.SeriesRequest{MinTime: 3600000, MaxTime: 10800000}},
		{name: "request at the minimum", req: &storepb.SeriesRequest{MinTime: 0, MaxTime: 3600000, MaxResolutionWindow: downsample.ResLevel1}},
		{name: "finer request for other external labels", req: &storepb.SeriesRequest{MinTime: 0, MaxTime: 3600000, Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "up"},
			{Type: storepb.LabelMatcher_EQ, Name: "tenant", Value: "two"},
		}}},
		{name: "finer request for the hidden block by ID", req: &storepb.SeriesRequest{MinTime: 0, MaxTime: 3600000, Hints: blockHints(storepb.LabelMatcher{Type: storepb.LabelMatcher_EQ, Name: block.BlockIDLabel, Value: hidden.String()})}, warn: true},
		{name: "finer request for other blocks by ID", req: &storepb.SeriesRequest{MinTime: 0, MaxTime: 3600000, Hints: blockHints(storepb.LabelMatcher{Type: storepb.LabelMatcher_NEQ, Name: block.BlockIDLabel, Value: hidden.String()})}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := promtest.ToFloat64(s.resolution.warnings)
			warnings := seriesWarnings(t, s.BucketStore, tc.req)
			if !tc.warn {
				testutil.Equals(t, 0, len(warnings))
				testutil.Equals(t, before, promtest.ToFloat64(s.resolution.warnings))
				return
			}
			testutil.Equals(t, 1, len(warnings))
			testutil.Assert(t, strings.Contains(warnings[0], "1 hidden block(s)"), "unexpected warning: %s", warnings[0])
			testutil.Equals(t, before+1, promtest.ToFloat64(s.resolution.warnings))
		})
	}
}

// TestResolutionHiddenSnapshot: the hidden blocks are published at sync time
// as an immutable snapshot cut to the store's time window, without the blocks
// the store serves after all.
func TestResolutionHiddenSnapshot(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	inmem := objstore.NewInMemBucket()
	bkt := objstore.WithNoopInstr(inmem)
	first, firstCover := writeRawAndDownsampled(t, dir, 0)
	second, secondCover := writeRawAndDownsampled(t, dir, 3600000)
	for _, id := range []ulid.ULID{first, firstCover, second, secondCover} {
		uploadBlock(t, bkt, dir, id)
	}

	// The store serves [0, 5400000]: all of the first hour, half the second.
	minTime, maxTime := time.UnixMilli(0), time.UnixMilli(5400000)
	window := &FilterConfig{MinTime: model.TimeOrDurationValue{Time: &minTime}, MaxTime: model.TimeOrDurationValue{Time: &maxTime}}
	partition := block.NewTimePartitionMetaFilter(window.MinTime, window.MaxTime)
	s := newResolutionTestStore(t, bkt, resolutionTestStoreOpts{
		following: []block.MetadataFilter{partition},
		options:   []BucketStoreOption{WithFilterConfig(window), WithHiddenResolutionWarning(true)},
	})

	testutil.Ok(t, s.SyncBlocks(ctx))
	snapshot := s.resolution.hidden.Load()
	testutil.Equals(t, downsample.ResLevel1, snapshot.floor)
	testutil.Equals(t, []hiddenBlock{
		{id: first, minTime: 0, maxTime: 3600000, labels: labels.FromStrings("tenant", "one")},
		{id: second, minTime: 3600000, maxTime: 5400001, labels: labels.FromStrings("tenant", "one")},
	}, snapshot.blocks)

	// The second cover cannot be used any more: its raw block is served and
	// leaves the next snapshot, while the published one stays as it was.
	testutil.Ok(t, inmem.Delete(ctx, path.Join(secondCover.String(), block.IndexFilename)))
	testutil.Ok(t, s.removeBlock(secondCover))
	testutil.Ok(t, s.SyncBlocks(ctx))
	testutil.Assert(t, s.getBlock(second) != nil, "the second raw block is served")
	next := s.resolution.hidden.Load()
	testutil.Assert(t, next != snapshot, "each sync publishes a new snapshot")
	testutil.Equals(t, 2, len(snapshot.blocks))
	testutil.Equals(t, []hiddenBlock{{id: first, minTime: 0, maxTime: 3600000, labels: labels.FromStrings("tenant", "one")}}, next.blocks)
	testutil.Equals(t, 0, len(seriesWarnings(t, s.BucketStore, &storepb.SeriesRequest{MinTime: 3600000, MaxTime: 5400000})))
	testutil.Equals(t, 1, len(seriesWarnings(t, s.BucketStore, &storepb.SeriesRequest{MinTime: 0, MaxTime: 5400000})))
}

// TestResolutionHiddenSnapshotConcurrentReads: requests read the snapshot
// while syncs replace it, without a lock (run with -race).
func TestResolutionHiddenSnapshotConcurrentReads(t *testing.T) {
	s, _ := hiddenWarningSetup(t, WithHiddenResolutionWarning(true))
	req := &storepb.SeriesRequest{MinTime: 0, MaxTime: 3600000}
	var wg sync.WaitGroup
	done := make(chan struct{})
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-done:
					return
				default:
				}
				if s.hiddenResolutionWarning(req, nil, nil) == nil {
					t.Error("expected a warning")
					return
				}
			}
		})
	}
	for range 5 {
		testutil.Ok(t, s.SyncBlocks(t.Context()))
	}
	close(done)
	wg.Wait()
}
