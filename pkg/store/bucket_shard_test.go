// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"math/rand"
	"path/filepath"
	"slices"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	storetestutil "github.com/thanos-io/thanos/pkg/store/storepb/testutil"
)

// TestBucketStoreStripsCompactorShardLabel serves two shard blocks of one
// compaction, which differ only in the compactor's shard label, and checks
// that the label never reaches the client: not on series, not among label
// names, not as a label with values. The series of both shards are served.
func TestBucketStoreStripsCompactorShardLabel(t *testing.T) {
	tmpDir := t.TempDir()
	bktDir := filepath.Join(tmpDir, "bkt")
	bkt, err := filesystem.NewBucket(bktDir)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	logger := log.NewNopLogger()
	instrBkt := objstore.WithNoopInstr(bkt)
	random := rand.New(rand.NewSource(7))
	extLset := labels.FromStrings("ext1", "1")

	var wantSeries []*storepb.Series
	for i, shard := range []string{"1_of_2", "2_of_2"} {
		head, set := storetestutil.CreateHeadWithSeries(t, i, storetestutil.HeadGenOptions{
			TSDBDir:          filepath.Join(tmpDir, shard),
			SamplesPerSeries: 1,
			Series:           3,
			PrependLabels:    extLset,
			Random:           random,
		})
		id := storetestutil.CreateBlockFromHead(t, bktDir, head)
		testutil.Ok(t, head.Close())
		lbls := extLset.Map()
		lbls[metadata.CompactorShardLabel] = shard
		_, err = metadata.InjectThanos(logger, filepath.Join(bktDir, id.String()), metadata.Thanos{
			Labels: lbls, Downsample: metadata.ThanosDownsample{Resolution: 0}, Source: metadata.TestSource,
		}, nil)
		testutil.Ok(t, err)
		wantSeries = append(wantSeries, set...)
	}

	fetcher, err := block.NewMetaFetcher(logger, 10, instrBkt, block.NewConcurrentLister(logger, instrBkt), tmpDir, nil, nil)
	testutil.Ok(t, err)
	indexCache, err := storecache.NewInMemoryIndexCacheWithConfig(logger, nil, nil, storecache.InMemoryIndexCacheConfig{})
	testutil.Ok(t, err)
	store, err := NewBucketStore(
		instrBkt,
		fetcher,
		tmpDir,
		NewChunksLimiterFactory(10000/MaxSamplesPerChunk),
		NewSeriesLimiterFactory(0),
		NewBytesLimiterFactory(0),
		NewGapBasedPartitioner(PartitionerMaxGapSize),
		10,
		false,
		DefaultPostingOffsetInMemorySampling,
		true,
		false,
		0,
		WithLogger(logger),
		WithIndexCache(indexCache),
	)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, store.Close()) }()
	testutil.Ok(t, store.SyncBlocks(t.Context()))
	testutil.Equals(t, 2, len(store.blocks), "both shard blocks must be loaded")

	srv := newStoreSeriesServer(t.Context())
	testutil.Ok(t, store.Series(&storepb.SeriesRequest{
		MinTime:  0,
		MaxTime:  1000,
		Matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"}},
	}, srv))
	var got, want []string
	for _, s := range srv.SeriesSet {
		got = append(got, labels.FromMap(func() map[string]string {
			m := map[string]string{}
			for _, l := range s.Labels {
				m[l.Name] = l.Value
			}
			return m
		}()).String())
	}
	for _, s := range wantSeries {
		m := map[string]string{}
		for _, l := range s.Labels {
			m[l.Name] = l.Value
		}
		want = append(want, labels.FromMap(m).String())
	}
	slices.Sort(got)
	slices.Sort(want)
	testutil.Equals(t, want, got, "every series of both shards must be served")
	for _, s := range srv.SeriesSet {
		for _, l := range s.Labels {
			testutil.Assert(t, l.Name != metadata.CompactorShardLabel, "series %v carries the shard label", s.Labels)
		}
	}

	names, err := store.LabelNames(t.Context(), &storepb.LabelNamesRequest{Start: 0, End: 1000})
	testutil.Ok(t, err)
	testutil.Assert(t, slices.Contains(names.Names, "ext1"), "external labels other than the shard label are served: %v", names.Names)
	testutil.Assert(t, !slices.Contains(names.Names, metadata.CompactorShardLabel), "the shard label is among the label names: %v", names.Names)

	values, err := store.LabelValues(t.Context(), &storepb.LabelValuesRequest{Label: metadata.CompactorShardLabel, Start: 0, End: 1000})
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(values.Values), "the shard label has no values to serve")

	values, err = store.LabelValues(t.Context(), &storepb.LabelValuesRequest{Label: "ext1", Start: 0, End: 1000})
	testutil.Ok(t, err)
	testutil.Equals(t, []string{"1"}, values.Values)

	// Without the shard label the two blocks would be one block set; with it
	// they are two, which is what keeps the deduplication filter and the
	// planner from confusing them.
	testutil.Equals(t, 2, len(store.blockSets))
}
