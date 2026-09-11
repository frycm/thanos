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
