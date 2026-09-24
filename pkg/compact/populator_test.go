// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

// blockContent reads every series of a block on disk with its samples, the
// label sets by series key, and the block's symbol table.
func blockContent(t *testing.T, dir string, id ulid.ULID) (series map[string][]string, lsets map[string]labels.Labels, symbols []string) {
	t.Helper()
	b, err := tsdb.OpenBlock(slog.Default(), filepath.Join(dir, id.String()), chunkenc.NewPool(), nil)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, b.Close()) }()
	ir, err := b.Index()
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, ir.Close()) }()
	cr, err := b.Chunks()
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, cr.Close()) }()

	series = map[string][]string{}
	lsets = map[string]labels.Labels{}
	k, v := index.AllPostingsKey()
	all, err := ir.Postings(t.Context(), k, v)
	testutil.Ok(t, err)
	for all.Next() {
		var builder labels.ScratchBuilder
		var chks []chunks.Meta
		testutil.Ok(t, ir.Series(all.At(), &builder, &chks))
		// Clone every string: the builder's strings point into the index
		// reader's symbol table, which is unmapped when the block is closed.
		key := builder.Labels().String()
		var owned labels.ScratchBuilder
		builder.Labels().Range(func(l labels.Label) { owned.Add(strings.Clone(l.Name), strings.Clone(l.Value)) })
		owned.Sort()
		lsets[key] = owned.Labels()
		for _, c := range chks {
			chk, _, err := cr.ChunkOrIterable(c)
			testutil.Ok(t, err)
			it := chk.Iterator(nil)
			for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
				ts, val := it.At()
				series[key] = append(series[key], fmt.Sprintf("%d/%v", ts, val))
			}
			testutil.Ok(t, it.Err())
		}
		slices.Sort(series[key])
	}
	testutil.Ok(t, all.Err())
	syms := ir.Symbols()
	for syms.Next() {
		symbols = append(symbols, strings.Clone(syms.At()))
	}
	testutil.Ok(t, syms.Err())
	return series, lsets, symbols
}

// TestPartitionedBlockPopulator compacts two overlapping replicas of the same
// series both whole and in four partitions, and checks that the partitions
// together hold exactly what the whole block holds, that every series is in
// exactly the partition its hash says, and that a partition's symbol table
// only holds what its series use.
func TestPartitionedBlockPopulator(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	series := make([]labels.Labels, 0, 40)
	for i := range 40 {
		series = append(series, labels.FromStrings("__name__", fmt.Sprintf("metric_%d", i%5), "instance", fmt.Sprintf("host-%d", i), "job", "api"))
	}
	var dirs []string
	for _, replica := range []string{"a", "b"} {
		id, err := e2eutil.CreateBlock(ctx, dir, series, 30, 0, time.Hour.Milliseconds(), labels.FromStrings("replica", replica), 0, metadata.NoneFunc, nil)
		testutil.Ok(t, err)
		dirs = append(dirs, filepath.Join(dir, id.String()))
	}
	comp, err := tsdb.NewLeveledCompactor(
		ctx,
		nil,
		slog.Default(),
		[]int64{time.Hour.Milliseconds(), 2 * time.Hour.Milliseconds()},
		chunkenc.NewPool(),
		storage.NewCompactingChunkSeriesMerger(storage.ChainedSeriesMerge),
	)
	testutil.Ok(t, err)

	out := t.TempDir()
	ids, err := comp.CompactWithBlockPopulator(out, dirs, nil, tsdb.DefaultBlockPopulator{})
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(ids))
	want, _, _ := blockContent(t, out, ids[0])
	testutil.Equals(t, len(series), len(want), "the whole block must hold every series once")

	const count = 4
	got := map[string][]string{}
	seen := map[string]int{}
	for i := range uint64(count) {
		part := SeriesPartition{Index: i, Count: count}
		ids, err = comp.CompactWithBlockPopulator(out, dirs, nil, PartitionedBlockPopulator{Partition: part})
		testutil.Ok(t, err)
		if len(ids) == 0 {
			continue // An empty partition produces no block.
		}
		testutil.Equals(t, 1, len(ids))
		content, lsets, symbols := blockContent(t, out, ids[0])
		used := map[string]struct{}{"": {}}
		for key, samples := range content {
			lset := lsets[key]
			testutil.Assert(t, part.Contains(lset), "series %s is in partition %d but hashes elsewhere", key, i)
			seen[key]++
			got[key] = samples
			lset.Range(func(l labels.Label) { used[l.Name] = struct{}{}; used[l.Value] = struct{}{} })
		}
		for _, s := range symbols {
			_, ok := used[s]
			testutil.Assert(t, ok, "partition %d carries symbol %q that none of its series uses", i, s)
		}
	}
	testutil.Equals(t, want, got, "the partitions together must hold exactly the whole content")
	for key, n := range seen {
		testutil.Equals(t, 1, n, "series %s appears in %d partitions", key, n)
	}
}

func TestSeriesPartitionValidate(t *testing.T) {
	for _, tcase := range []struct {
		name      string
		partition SeriesPartition
		valid     bool
	}{
		{name: "single partition", partition: SeriesPartition{Index: 0, Count: 1}, valid: true},
		{name: "last of four", partition: SeriesPartition{Index: 3, Count: 4}, valid: true},
		{name: "zero count", partition: SeriesPartition{}},
		{name: "index out of range", partition: SeriesPartition{Index: 4, Count: 4}},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			err := tcase.partition.Validate()
			if tcase.valid {
				testutil.Ok(t, err)
				return
			}
			testutil.NotOk(t, err)
		})
	}
}

// TestSeriesPartitionWithout: a partition that leaves labels out of its hash
// hashes a series as labels.StableHash hashes the series without them, so
// replicas differing only in those labels share a partition and a series that
// carries none of them hashes as it always did.
func TestSeriesPartitionWithout(t *testing.T) {
	without := []string{"replica", "prometheus_replica"}
	lsets := []labels.Labels{
		labels.FromStrings("__name__", "up", "job", "api", "replica", "a"),
		labels.FromStrings("__name__", "up", "job", "api", "replica", "b"),
		labels.FromStrings("__name__", "up", "instance", "h1", "job", "api", "prometheus_replica", "p0", "replica", "a"),
		labels.FromStrings("__name__", "up", "job", "api"),
		labels.FromStrings("replica", "only"),
		labels.EmptyLabels(),
	}
	for _, lset := range lsets {
		want := labels.StableHash(labels.NewBuilder(lset).Del(without...).Labels())
		for _, count := range []uint64{1, 2, 8, 1 << 20} {
			p := SeriesPartition{Index: want % count, Count: count, Without: without}
			testutil.Assert(t, p.Contains(lset), "%s must hash as its label set without %v", lset, without)
			testutil.Equals(t, want, p.hash(lset, nil))
		}
	}
	testutil.Equals(t, labels.StableHash(lsets[3]), SeriesPartition{Count: 1, Without: without}.hash(lsets[3], nil), "a series without the labels hashes as before")
	testutil.Equals(t, SeriesPartition{Count: 1, Without: without}.hash(lsets[0], nil), SeriesPartition{Count: 1, Without: without}.hash(lsets[1], nil), "replicas hash together")
	testutil.Assert(t, SeriesPartition{Count: 1}.hash(lsets[0], nil) != SeriesPartition{Count: 1}.hash(lsets[1], nil), "without the exclusion they hash apart")
	testutil.NotOk(t, SeriesPartition{Index: 0, Count: 2, Without: []string{"a", ""}}.Validate())
}

// TestPartitionedBlockPopulatorKeepsReplicasTogether: two replicas whose
// replica label sits inside the series land in the same partition when the
// partition leaves that label out of its hash, and the populators' tallies
// show the partitions covering every series.
func TestPartitionedBlockPopulatorKeepsReplicasTogether(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	replicas := []string{"a", "b"}
	series := make([]labels.Labels, 0, 40*len(replicas))
	for i := range 40 {
		for _, replica := range replicas {
			series = append(series, labels.FromStrings("__name__", fmt.Sprintf("metric_%d", i%5), "instance", fmt.Sprintf("host-%d", i), "job", "api", "replica", replica))
		}
	}
	id, err := e2eutil.CreateBlock(ctx, dir, series, 30, 0, time.Hour.Milliseconds(), labels.FromStrings("ext", "1"), 0, metadata.NoneFunc, nil)
	testutil.Ok(t, err)
	dirs := []string{filepath.Join(dir, id.String())}
	comp, err := tsdb.NewLeveledCompactor(
		ctx,
		nil,
		slog.Default(),
		[]int64{time.Hour.Milliseconds(), 2 * time.Hour.Milliseconds()},
		chunkenc.NewPool(),
		storage.NewCompactingChunkSeriesMerger(storage.ChainedSeriesMerge),
	)
	testutil.Ok(t, err)
	out := t.TempDir()

	const count = 4
	var walked, kept uint64
	partitionOf := map[string]uint64{}
	for i := range uint64(count) {
		stats := &PartitionStats{}
		ids, err := comp.CompactWithBlockPopulator(out, dirs, nil, PartitionedBlockPopulator{Partition: SeriesPartition{Index: i, Count: count, Without: []string{"replica"}}, Stats: stats})
		testutil.Ok(t, err)
		testutil.Equals(t, uint64(len(series)), stats.Walked, "every partition walks every series once per source block")
		walked, kept = stats.Walked, kept+stats.Kept
		if len(ids) == 0 {
			continue
		}
		_, lsets, _ := blockContent(t, out, ids[0])
		for _, lset := range lsets {
			logical := labels.NewBuilder(lset).Del("replica").Labels().String()
			if j, ok := partitionOf[logical]; ok {
				testutil.Equals(t, j, i, "replicas of %s sit in different partitions", logical)
			}
			partitionOf[logical] = i
		}
	}
	testutil.Equals(t, walked, kept, "the partitions together keep every series")
	testutil.Equals(t, len(series)/2, len(partitionOf))
}
