// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/indexheader"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/logutil"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

type unavailableIndexBucket struct {
	objstore.Bucket
	prefix string
	fail   bool
	hits   int
}

func TestResolutionFallbackRespectsTimePartition(t *testing.T) {
	logger := log.NewNopLogger()
	bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())
	raw, coarse := ulid.MustNew(1, nil), ulid.MustNew(2, nil)
	for id, resolution := range map[ulid.ULID]int64{raw: 0, coarse: downsample.ResLevel1} {
		m := metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: id, Version: 1, MinTime: 0, MaxTime: 50,
			Compaction: tsdb.BlockMetaCompaction{Sources: []ulid.ULID{raw}}}}
		m.Thanos.Downsample.Resolution = resolution
		body, err := json.Marshal(m)
		testutil.Ok(t, err)
		testutil.Ok(t, bkt.Upload(t.Context(), id.String()+"/meta.json", bytes.NewReader(body)))
	}
	minTime, maxTime := time.UnixMilli(100), time.UnixMilli(200)
	partition := block.NewTimePartitionMetaFilter(model.TimeOrDurationValue{Time: &minTime}, model.TimeOrDurationValue{Time: &maxTime})
	filter := block.NewResolutionMetaFilter(logger, downsample.ResLevel1, downsample.ResLevel2, nil)
	fetcher, err := block.NewMetaFetcher(logger, 1, bkt, block.NewConcurrentLister(logger, bkt), "", nil, []block.MetadataFilter{filter, partition})
	testutil.Ok(t, err)
	s, err := NewBucketStore(bkt, fetcher, t.TempDir(), NewChunksLimiterFactory(1000), NewSeriesLimiterFactory(0), NewBytesLimiterFactory(0),
		NewGapBasedPartitioner(PartitionerMaxGapSize), 1, false, DefaultPostingOffsetInMemorySampling, false, false, 0, WithResolutionFilter(filter, partition))
	testutil.Ok(t, err)
	t.Cleanup(func() { testutil.Ok(t, s.Close()) })
	// Neither block has an index: loading either out-of-partition block is an
	// error. A fallback must pass the same time filter as normal candidates.
	testutil.Ok(t, s.SyncBlocks(t.Context()))
	testutil.Equals(t, 0, len(s.blocks))
}

func (b *unavailableIndexBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if b.fail && strings.HasPrefix(name, b.prefix) && strings.HasSuffix(name, "/index") {
		b.hits++
		return nil, errors.New("injected replacement index read failure")
	}
	return b.Bucket.GetRange(ctx, name, off, length)
}

func TestResolutionFallbackSurvivesReplacementLoadFailure(t *testing.T) {
	for _, reader := range []struct {
		name     string
		lazy     bool
		download indexheader.LazyDownloadIndexHeaderFunc
	}{
		{"eager", false, indexheader.AlwaysEagerDownloadIndexHeader},
		{"lazy mmap", true, indexheader.AlwaysEagerDownloadIndexHeader},
		{"lazy download", true, indexheader.AlwaysLazyDownloadIndexHeader},
	} {
		t.Run(reader.name, func(t *testing.T) {
			for _, warm := range []bool{false, true} {
				t.Run(map[bool]string{false: "cold start", true: "already serving raw"}[warm], func(t *testing.T) {
					ctx := t.Context()
					logger := log.NewNopLogger()
					fault := &unavailableIndexBucket{Bucket: objstore.NewInMemBucket()}
					bkt := objstore.WithNoopInstr(fault)
					dir := t.TempDir()
					id, err := e2eutil.CreateBlock(ctx, dir, []labels.Labels{labels.FromStrings("__name__", "up")}, 20,
						0, 3600000, labels.FromStrings("tenant", "one"), 0, metadata.NoneFunc, nil)
					testutil.Ok(t, err)
					testutil.Ok(t, block.Upload(ctx, logger, bkt, filepath.Join(dir, id.String()), metadata.NoneFunc))
					m, err := metadata.ReadFromDir(filepath.Join(dir, id.String()))
					testutil.Ok(t, err)
					input, err := tsdb.OpenBlock(logutil.GoKitLogToSlog(logger), filepath.Join(dir, id.String()), nil, nil)
					testutil.Ok(t, err)
					coarseID, err := downsample.Downsample(ctx, logger, m, input, dir, downsample.ResLevel1)
					testutil.Ok(t, err)
					testutil.Ok(t, input.Close())

					filter := block.NewResolutionMetaFilter(logger, downsample.ResLevel1, downsample.ResLevel2, nil)
					fetcher, err := block.NewMetaFetcher(logger, 1, bkt, block.NewConcurrentLister(logger, bkt), "", nil, []block.MetadataFilter{filter})
					testutil.Ok(t, err)
					fault.prefix, fault.fail = coarseID.String()+"/", true
					s, err := NewBucketStore(bkt, fetcher, t.TempDir(), NewChunksLimiterFactory(1000), NewSeriesLimiterFactory(0), NewBytesLimiterFactory(0),
						NewGapBasedPartitioner(PartitionerMaxGapSize), 1, false, DefaultPostingOffsetInMemorySampling, false, reader.lazy, 0,
						WithIndexHeaderLazyDownloadStrategy(reader.download), WithResolutionFilter(filter))
					testutil.Ok(t, err)
					t.Cleanup(func() { testutil.Ok(t, s.Close()) })
					if warm {
						testutil.Ok(t, s.SyncBlocks(ctx))
						testutil.Assert(t, s.getBlock(id) != nil, "raw must initially be served")
					}
					testutil.Ok(t, block.Upload(ctx, logger, bkt, filepath.Join(dir, coarseID.String()), metadata.NoneFunc))
					testutil.Ok(t, s.SyncBlocks(ctx))
					testutil.Assert(t, s.getBlock(coarseID) == nil, "replacement load must fail")
					testutil.Assert(t, fault.hits > 0, "the replacement index read must fail")
					testutil.Assert(t, s.getBlock(id) != nil, "a failed replacement must not hide available raw samples")
					fallback := s.getBlock(id)
					_, err = fallback.indexHeaderReader.IndexVersion()
					testutil.Ok(t, err)
					selected := s.blockSets[fallback.extLset.Hash()].getFor(0, 3599999, downsample.ResLevel1, nil)
					testutil.Equals(t, 1, len(selected))
					testutil.Equals(t, id, selected[0].meta.ULID)

					fault.fail = false
					testutil.Ok(t, s.SyncBlocks(ctx))
					testutil.Assert(t, s.getBlock(coarseID) != nil, "replacement must load after recovery")
					testutil.Ok(t, s.SyncBlocks(ctx))
					testutil.Assert(t, s.getBlock(id) == nil, "loaded coverage may now replace raw")
					// Losing the replacement must bring the still-retained raw block back.
					testutil.Ok(t, block.Delete(ctx, logger, bkt, coarseID))
					testutil.Ok(t, s.SyncBlocks(ctx))
					testutil.Assert(t, s.getBlock(id) != nil, "raw must return when downsampled coverage disappears")
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
	logger := log.NewNopLogger()
	bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())
	raw, coarse := ulid.MustNew(1, nil), ulid.MustNew(2, nil)
	for id, resolution := range map[ulid.ULID]int64{raw: 0, coarse: downsample.ResLevel1} {
		m := metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: id, Version: 1, MinTime: 0, MaxTime: 50,
			Compaction: tsdb.BlockMetaCompaction{Sources: []ulid.ULID{raw}}}}
		m.Thanos.Downsample.Resolution = resolution
		body, err := json.Marshal(m)
		testutil.Ok(t, err)
		testutil.Ok(t, bkt.Upload(t.Context(), id.String()+"/meta.json", bytes.NewReader(body)))
	}
	filter := block.NewResolutionMetaFilter(logger, downsample.ResLevel1, downsample.ResLevel2, nil)
	fetcher, err := block.NewMetaFetcher(logger, 1, bkt, block.NewConcurrentLister(logger, bkt), "", nil, []block.MetadataFilter{filter})
	testutil.Ok(t, err)
	s, err := NewBucketStore(bkt, fetcher, t.TempDir(), NewChunksLimiterFactory(1000), NewSeriesLimiterFactory(0), NewBytesLimiterFactory(0),
		NewGapBasedPartitioner(PartitionerMaxGapSize), 1, false, DefaultPostingOffsetInMemorySampling, false, false, 0, WithResolutionFilter(filter))
	testutil.Ok(t, err)
	t.Cleanup(func() { testutil.Ok(t, s.Close()) })
	// Neither block has an index, so the cover fails to load and so does the
	// raw fallback. The sync reports neither as an error.
	testutil.Ok(t, s.SyncBlocks(t.Context()))
	testutil.Equals(t, 0, len(s.blocks))
}

// TestResolutionStraddlingFallbackTrustsCoverBeyondPartition pins down the
// reason the resolution filter runs before the time partition: a raw block
// straddling the partition boundary, covered on the near side by a loaded
// block and on the far side by one this store does not serve, stays hidden
// instead of being loaded and reported as uncovered at every shard boundary.
func TestResolutionStraddlingFallbackTrustsCoverBeyondPartition(t *testing.T) {
	ctx := t.Context()
	logger := log.NewNopLogger()
	bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())
	dir := t.TempDir()
	rawID, err := e2eutil.CreateBlock(ctx, dir, []labels.Labels{labels.FromStrings("__name__", "up")}, 20,
		0, 3600000, labels.FromStrings("tenant", "one"), 0, metadata.NoneFunc, nil)
	testutil.Ok(t, err)
	rawMeta, err := metadata.ReadFromDir(filepath.Join(dir, rawID.String()))
	testutil.Ok(t, err)
	input, err := tsdb.OpenBlock(logutil.GoKitLogToSlog(logger), filepath.Join(dir, rawID.String()), nil, nil)
	testutil.Ok(t, err)
	nearID, err := downsample.Downsample(ctx, logger, rawMeta, input, dir, downsample.ResLevel1)
	testutil.Ok(t, err)
	testutil.Ok(t, input.Close())
	// The near cover serves the first half of the raw block's range; the far
	// cover, the second half, exists only as metadata on the other side of the
	// partition. Whether it could be loaded is the other store's concern.
	nearMeta, err := metadata.ReadFromDir(filepath.Join(dir, nearID.String()))
	testutil.Ok(t, err)
	nearMeta.MaxTime = 1800000
	testutil.Ok(t, nearMeta.WriteToDir(logger, filepath.Join(dir, nearID.String())))
	farID := ulid.MustNew(99, nil)
	farMeta := *nearMeta
	farMeta.ULID, farMeta.MinTime, farMeta.MaxTime = farID, 1800000, 3600000
	body, err := json.Marshal(farMeta)
	testutil.Ok(t, err)
	for _, id := range []ulid.ULID{rawID, nearID} {
		testutil.Ok(t, block.Upload(ctx, logger, bkt, filepath.Join(dir, id.String()), metadata.NoneFunc))
	}
	testutil.Ok(t, bkt.Upload(ctx, farID.String()+"/meta.json", bytes.NewReader(body)))

	minTime, maxTime := time.UnixMilli(0), time.UnixMilli(1799999)
	partition := block.NewTimePartitionMetaFilter(model.TimeOrDurationValue{Time: &minTime}, model.TimeOrDurationValue{Time: &maxTime})
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_uncovered"})
	filter := block.NewResolutionMetaFilter(logger, downsample.ResLevel1, downsample.ResLevel2, gauge)
	fetcher, err := block.NewMetaFetcher(logger, 1, bkt, block.NewConcurrentLister(logger, bkt), "", nil, []block.MetadataFilter{filter, partition})
	testutil.Ok(t, err)
	s, err := NewBucketStore(bkt, fetcher, t.TempDir(), NewChunksLimiterFactory(1000), NewSeriesLimiterFactory(0), NewBytesLimiterFactory(0),
		NewGapBasedPartitioner(PartitionerMaxGapSize), 1, false, DefaultPostingOffsetInMemorySampling, false, false, 0, WithResolutionFilter(filter, partition))
	testutil.Ok(t, err)
	t.Cleanup(func() { testutil.Ok(t, s.Close()) })

	testutil.Ok(t, s.SyncBlocks(ctx))
	testutil.Assert(t, s.getBlock(nearID) != nil, "the in-window cover is served")
	testutil.Assert(t, s.getBlock(rawID) == nil, "the straddling raw block is covered on both sides and stays hidden")
	testutil.Assert(t, s.getBlock(farID) == nil, "the far cover is not this store's to serve")
	testutil.Equals(t, 0.0, promtest.ToFloat64(gauge))

	// Without the far cover the second half is nobody's: the raw block is
	// served, and reported.
	testutil.Ok(t, block.Delete(ctx, logger, bkt, farID))
	testutil.Ok(t, s.SyncBlocks(ctx))
	testutil.Assert(t, s.getBlock(rawID) != nil, "an uncovered far side brings the raw block back")
	testutil.Equals(t, 1.0, promtest.ToFloat64(gauge))
}

// TestResolutionReplacementVerificationIsScopedToCovers pins down that only
// blocks hiding a finer block have their index header read at sync time. A
// downsampled block that hides nothing keeps its lazy loading, and one that
// was loaded lazily before a finer block appeared is verified then, and
// dropped if unreadable so the finer block is served instead.
func TestResolutionReplacementVerificationIsScopedToCovers(t *testing.T) {
	ctx := t.Context()
	logger := log.NewNopLogger()
	fault := &unavailableIndexBucket{Bucket: objstore.NewInMemBucket()}
	bkt := objstore.WithNoopInstr(fault)
	dir := t.TempDir()
	id, err := e2eutil.CreateBlock(ctx, dir, []labels.Labels{labels.FromStrings("__name__", "up")}, 20,
		0, 3600000, labels.FromStrings("tenant", "one"), 0, metadata.NoneFunc, nil)
	testutil.Ok(t, err)
	m, err := metadata.ReadFromDir(filepath.Join(dir, id.String()))
	testutil.Ok(t, err)
	input, err := tsdb.OpenBlock(logutil.GoKitLogToSlog(logger), filepath.Join(dir, id.String()), nil, nil)
	testutil.Ok(t, err)
	coarseID, err := downsample.Downsample(ctx, logger, m, input, dir, downsample.ResLevel1)
	testutil.Ok(t, err)
	testutil.Ok(t, input.Close())
	testutil.Ok(t, block.Upload(ctx, logger, bkt, filepath.Join(dir, coarseID.String()), metadata.NoneFunc))

	filter := block.NewResolutionMetaFilter(logger, downsample.ResLevel1, downsample.ResLevel2, nil)
	fetcher, err := block.NewMetaFetcher(logger, 1, bkt, block.NewConcurrentLister(logger, bkt), "", nil, []block.MetadataFilter{filter})
	testutil.Ok(t, err)
	fault.prefix, fault.fail = coarseID.String()+"/", true
	s, err := NewBucketStore(bkt, fetcher, t.TempDir(), NewChunksLimiterFactory(1000), NewSeriesLimiterFactory(0), NewBytesLimiterFactory(0),
		NewGapBasedPartitioner(PartitionerMaxGapSize), 1, false, DefaultPostingOffsetInMemorySampling, false, true, 0,
		WithIndexHeaderLazyDownloadStrategy(indexheader.AlwaysLazyDownloadIndexHeader), WithResolutionFilter(filter))
	testutil.Ok(t, err)
	t.Cleanup(func() { testutil.Ok(t, s.Close()) })

	// Alone, the downsampled block hides nothing: it loads lazily and its
	// index is never touched, although reading it would fail.
	testutil.Ok(t, s.SyncBlocks(ctx))
	testutil.Assert(t, s.getBlock(coarseID) != nil, "a block that hides nothing loads lazily")
	testutil.Equals(t, 0, fault.hits)

	// Once a raw block it covers appears, the cover has to prove itself. It
	// cannot, so it is dropped and the raw block is served.
	testutil.Ok(t, block.Upload(ctx, logger, bkt, filepath.Join(dir, id.String()), metadata.NoneFunc))
	testutil.Ok(t, s.SyncBlocks(ctx))
	testutil.Assert(t, fault.hits > 0, "a cover of a hidden block must be verified")
	testutil.Assert(t, s.getBlock(coarseID) == nil, "an unreadable cover is not served")
	testutil.Assert(t, s.getBlock(id) != nil, "the raw block is served instead")

	fault.fail = false
	testutil.Ok(t, s.SyncBlocks(ctx))
	testutil.Assert(t, s.getBlock(coarseID) != nil, "the cover loads and verifies after recovery")
	testutil.Ok(t, s.SyncBlocks(ctx))
	testutil.Assert(t, s.getBlock(id) == nil, "a verified cover retires the raw block")
}
