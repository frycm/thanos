// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"context"
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
	all, err := ir.Postings(context.Background(), k, v)
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
	ctx := context.Background()
	dir := t.TempDir()
	var series []labels.Labels
	for i := range 40 {
		series = append(series, labels.FromStrings("__name__", fmt.Sprintf("metric_%d", i%5), "instance", fmt.Sprintf("host-%d", i), "job", "api"))
	}
	var dirs []string
	for _, replica := range []string{"a", "b"} {
		id, err := e2eutil.CreateBlock(ctx, dir, series, 30, 0, time.Hour.Milliseconds(), labels.FromStrings("replica", replica), 0, metadata.NoneFunc, nil)
		testutil.Ok(t, err)
		dirs = append(dirs, filepath.Join(dir, id.String()))
	}
	comp, err := tsdb.NewLeveledCompactor(ctx, nil, slog.Default(), []int64{time.Hour.Milliseconds(), 2 * time.Hour.Milliseconds()}, chunkenc.NewPool(),
		storage.NewCompactingChunkSeriesMerger(storage.ChainedSeriesMerge))
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
		ids, err := comp.CompactWithBlockPopulator(out, dirs, nil, PartitionedBlockPopulator{Partition: part})
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
	testutil.Ok(t, SeriesPartition{Index: 0, Count: 1}.Validate())
	testutil.Ok(t, SeriesPartition{Index: 3, Count: 4}.Validate())
	testutil.NotOk(t, SeriesPartition{}.Validate())
	testutil.NotOk(t, SeriesPartition{Index: 4, Count: 4}.Validate())
}
