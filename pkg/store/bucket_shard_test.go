// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/gogo/protobuf/types"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/logutil"
	"github.com/thanos-io/thanos/pkg/store/hintspb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

type shardTestSample struct {
	t int64
	f float64
}

func (s shardTestSample) T() int64                      { return s.t }
func (s shardTestSample) F() float64                    { return s.f }
func (s shardTestSample) H() *histogram.Histogram       { return nil }
func (s shardTestSample) FH() *histogram.FloatHistogram { return nil }
func (s shardTestSample) Type() chunkenc.ValueType      { return chunkenc.ValFloat }
func (s shardTestSample) Copy() chunks.Sample           { return s }

// writeShardTestBlock writes into dir a block of [mint, maxt) with a sample of each series every minute.
func writeShardTestBlock(t *testing.T, dir string, series []labels.Labels, mint, maxt int64, extLabels map[string]string, resolution int64) {
	t.Helper()
	var ss []storage.Series
	for _, lset := range series {
		var samples []chunks.Sample
		for ts := mint; ts < maxt; ts += time.Minute.Milliseconds() {
			samples = append(samples, shardTestSample{t: ts, f: float64(len(lset.String())) + float64(ts)/1000})
		}
		ss = append(ss, storage.NewListSeries(lset, samples))
	}
	bdir, err := tsdb.CreateBlock(ss, t.TempDir(), 0, logutil.GoKitLogToSlog(log.NewNopLogger()))
	testutil.Ok(t, err)
	meta, err := metadata.ReadFromDir(bdir)
	testutil.Ok(t, err)
	meta.MinTime, meta.MaxTime = mint, maxt
	meta.Thanos = metadata.Thanos{Labels: extLabels, Downsample: metadata.ThanosDownsample{Resolution: resolution}, Source: metadata.TestSource}
	testutil.Ok(t, meta.WriteToDir(log.NewNopLogger(), bdir))
	testutil.Ok(t, os.Rename(bdir, filepath.Join(dir, meta.ULID.String())))
}

func newShardTestStore(t *testing.T, bktDir string) *BucketStore {
	t.Helper()
	logger := log.NewNopLogger()
	bkt, err := filesystem.NewBucket(bktDir)
	testutil.Ok(t, err)
	t.Cleanup(func() { testutil.Ok(t, bkt.Close()) })
	instrBkt := objstore.WithNoopInstr(bkt)
	dir := t.TempDir()
	fetcher, err := block.NewMetaFetcher(logger, 10, instrBkt, block.NewConcurrentLister(logger, instrBkt), dir, nil, nil)
	testutil.Ok(t, err)
	store, err := NewBucketStore(
		instrBkt,
		fetcher,
		dir,
		NewChunksLimiterFactory(0),
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
	)
	testutil.Ok(t, err)
	t.Cleanup(func() { testutil.Ok(t, store.Close()) })
	testutil.Ok(t, store.SyncBlocks(t.Context()))
	return store
}

// querySeries returns the samples of each raw series the store returns.
func querySeries(t *testing.T, store *BucketStore, matchers ...storepb.LabelMatcher) map[string][]string {
	res, _ := querySeriesAndBlocks(t, store, 0, matchers...)
	return res
}

// querySeriesAndBlocks returns the samples of each series the store returns, and the number of blocks it queried.
func querySeriesAndBlocks(t *testing.T, store *BucketStore, maxResolution int64, matchers ...storepb.LabelMatcher) (map[string][]string, int) {
	t.Helper()
	srv := newStoreSeriesServer(t.Context())
	testutil.Ok(t, store.Series(&storepb.SeriesRequest{
		MinTime:             0,
		MaxTime:             4 * time.Hour.Milliseconds(),
		Matchers:            append([]storepb.LabelMatcher{{Type: storepb.LabelMatcher_RE, Name: "a", Value: ".+"}}, matchers...),
		MaxResolutionWindow: maxResolution,
	}, srv))
	testutil.Equals(t, 0, len(srv.Warnings))
	res := map[string][]string{}
	for _, s := range srv.SeriesSet {
		key := labelpb.ZLabelsToPromLabels(s.Labels).String()
		for _, c := range s.Chunks {
			testutil.Assert(t, c.Raw != nil, "series %s: expected raw chunks", key)
			chk, err := chunkenc.FromData(chunkenc.EncXOR, c.Raw.Data)
			testutil.Ok(t, err)
			it := chk.Iterator(nil)
			for it.Next() == chunkenc.ValFloat {
				ts, v := it.At()
				res[key] = append(res[key], fmt.Sprintf("%d=%v", ts, v))
			}
			testutil.Ok(t, it.Err())
		}
		slices.Sort(res[key])
	}
	queried := 0
	for _, h := range srv.HintsSet {
		var hints hintspb.SeriesResponseHints
		testutil.Ok(t, types.UnmarshalAny(h, &hints))
		queried += len(hints.QueriedBlocks)
	}
	return res, queried
}

// TestBucketStore_CompactorShards serves the same data unsplit from one store and split in shards from another: the
// split store answers every request like the unsplit one, and never exposes the compactor's shard label, in series,
// label names, label values or the label sets and TSDB infos it advertises. The shards also differ in which
// resolutions they have: each shard picks its own.
func TestBucketStore_CompactorShards(t *testing.T) {
	t.Parallel()

	const shards = 4
	var (
		ext    = map[string]string{"ext1": "1"}
		series []labels.Labels
		twoH   = 2 * time.Hour.Milliseconds()
	)
	for i := range 20 {
		series = append(series, labels.FromStrings("__name__", "up", "a", strconv.Itoa(i), "job", "api"+strconv.Itoa(i%3)))
	}
	withShard := func(index int) map[string]string {
		return map[string]string{"ext1": "1", metadata.CompactorShardIDLabel: metadata.FormatShardID(index, shards)}
	}

	unsplitDir, splitDir := t.TempDir(), t.TempDir()
	for _, mint := range []int64{0, twoH} {
		writeShardTestBlock(t, unsplitDir, series, mint, mint+twoH, ext, 0)

		// Series i lies in shard i%3+1; shard 4 is an empty block.
		for index := 1; index < shards; index++ {
			var shardSeries []labels.Labels
			for i, s := range series {
				if i%3+1 == index {
					shardSeries = append(shardSeries, s)
				}
			}
			writeShardTestBlock(t, splitDir, shardSeries, mint, mint+twoH, withShard(index), 0)
			if index == 1 && mint == 0 {
				// Shard 1 is also downsampled (with its raw chunks, for the test's sake); the other shards are not.
				writeShardTestBlock(t, splitDir, shardSeries, 0, 2*twoH, withShard(index), downsample.ResLevel1)
			}
		}
		_, err := e2eutil.CreateEmptyBlock(splitDir, mint, mint+twoH, labels.FromMap(withShard(shards)), 0)
		testutil.Ok(t, err)
	}

	unsplit, split := newShardTestStore(t, unsplitDir), newShardTestStore(t, splitDir)
	testutil.Equals(t, 2, len(unsplit.blocks))
	testutil.Equals(t, 2*shards+1, len(split.blocks), "every shard block is loaded, empty ones included")
	testutil.Equals(t, shards, len(split.blockSets), "each shard is a block set of its own")

	// Series: same series and samples, at any resolution, and no shard label.
	want := querySeries(t, unsplit)
	testutil.Equals(t, len(series), len(want))
	for _, tc := range []struct {
		maxResolution int64
		queriedBlocks int
	}{
		// Every raw block.
		{maxResolution: 0, queriedBlocks: 2 * shards},
		// The downsampled block of shard 1, the raw blocks of the other shards.
		{maxResolution: downsample.ResLevel1, queriedBlocks: 1 + 2*(shards-1)},
	} {
		got, queried := querySeriesAndBlocks(t, split, tc.maxResolution)
		testutil.Equals(t, want, got)
		testutil.Equals(t, tc.queriedBlocks, queried)
		for key := range got {
			testutil.Assert(t, !strings.Contains(key, metadata.CompactorShardIDLabel), "series %s carries the shard label", key)
			testutil.Assert(t, strings.Contains(key, `ext1="1"`), "series %s lacks the external labels", key)
		}
	}

	// Matchers on the shard label behave as if no series had it.
	testutil.Equals(t, want, querySeries(t, split, storepb.LabelMatcher{Type: storepb.LabelMatcher_EQ, Name: metadata.CompactorShardIDLabel, Value: ""}))
	testutil.Equals(t, 0, len(querySeries(t, split, storepb.LabelMatcher{Type: storepb.LabelMatcher_RE, Name: metadata.CompactorShardIDLabel, Value: ".+"})))
	testutil.Equals(t, 0, len(querySeries(t, split, storepb.LabelMatcher{Type: storepb.LabelMatcher_EQ, Name: metadata.CompactorShardIDLabel, Value: "1_of_4"})))

	// Label names and values.
	for _, store := range []*BucketStore{unsplit, split} {
		names, err := store.LabelNames(t.Context(), &storepb.LabelNamesRequest{Start: 0, End: 2 * twoH})
		testutil.Ok(t, err)
		testutil.Equals(t, []string{"__name__", "a", "ext1", "job"}, names.Names)

		values, err := store.LabelValues(t.Context(), &storepb.LabelValuesRequest{Label: metadata.CompactorShardIDLabel, Start: 0, End: 2 * twoH})
		testutil.Ok(t, err)
		testutil.Equals(t, 0, len(values.Values))

		values, err = store.LabelValues(t.Context(), &storepb.LabelValuesRequest{Label: "ext1", Start: 0, End: 2 * twoH})
		testutil.Ok(t, err)
		testutil.Equals(t, []string{"1"}, values.Values)

		values, err = store.LabelValues(t.Context(), &storepb.LabelValuesRequest{Label: "job", Start: 0, End: 2 * twoH})
		testutil.Ok(t, err)
		testutil.Equals(t, []string{"api0", "api1", "api2"}, values.Values)
	}

	// Advertised label sets and TSDB infos: the stream, once.
	testutil.Equals(t, unsplit.LabelSet(), split.LabelSet())
	testutil.Equals(t, []labelpb.ZLabelSet{{Labels: []labelpb.ZLabel{{Name: "ext1", Value: "1"}}}}, split.LabelSet())
	testutil.Equals(t, unsplit.TSDBInfos(), split.TSDBInfos())
	testutil.Equals(t, 1, len(split.TSDBInfos()))
	testutil.Equals(t, int64(0), split.TSDBInfos()[0].MinTime)
	testutil.Equals(t, 2*twoH, split.TSDBInfos()[0].MaxTime)
}
