// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/dedup"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

type floatSample struct {
	t int64
	f float64
}

func (s floatSample) T() int64                      { return s.t }
func (s floatSample) F() float64                    { return s.f }
func (s floatSample) H() *histogram.Histogram       { return nil }
func (s floatSample) FH() *histogram.FloatHistogram { return nil }
func (s floatSample) Type() chunkenc.ValueType      { return chunkenc.ValFloat }
func (s floatSample) Copy() chunks.Sample           { return s }

// constSeries is a series with value v at every timestamp in [from, to).
func constSeries(lset labels.Labels, from, to int64, v float64) storage.Series {
	var samples []chunks.Sample
	for ts := from; ts < to; ts++ {
		samples = append(samples, floatSample{t: ts * 1000, f: v})
	}
	return storage.NewListSeries(lset, samples)
}

func writeBlock(t *testing.T, dir string, series ...storage.Series) string {
	t.Helper()
	path, err := tsdb.CreateBlock(series, dir, 0, slog.Default())
	testutil.Ok(t, err)
	return path
}

func samplesOf(v float64, from, to int64) []string {
	var out []string
	for ts := from; ts < to; ts++ {
		out = append(out, fmt.Sprintf("%d/%v", ts*1000, v))
	}
	slices.Sort(out)
	return out
}

// TestDeduplicatingBlockPopulator compacts two overlapping sources whose
// series carry their replica label inside the series and checks that
// replicas merge into one series without the label, in the right order,
// that the tie between replicas with samples at the same timestamps goes to
// the first source's first replica - sources ordered by MinTime, then ULID -
// that a series lacking the label merges with its replicas, and that the
// label leaves the symbol table.
func TestDeduplicatingBlockPopulator(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	l := labels.FromStrings
	x := writeBlock(t, dir,
		// Stripped, {m, a=1} sorts before {m, a=1, b=1}; with the replica
		// label the index holds them the other way round.
		constSeries(l("__name__", "m", "a", "1", "replica", "A"), 0, 10, 1),
		constSeries(l("__name__", "m", "a", "1", "replica", "B"), 0, 10, 2),
		constSeries(l("__name__", "m", "a", "1", "b", "1", "replica", "A"), 0, 10, 3),
		constSeries(l("__name__", "m", "a", "1", "b", "1", "replica", "B"), 0, 10, 4),
		// A series without the label - already deduplicated, or never
		// replicated - merges with its replica.
		constSeries(l("__name__", "n", "replica", "A"), 0, 5, 5),
		constSeries(l("__name__", "n"), 5, 10, 6),
		constSeries(l("__name__", "only", "replica", "B"), 0, 10, 7),
	)
	y := writeBlock(t, dir,
		constSeries(l("__name__", "m", "a", "1", "replica", "A"), 0, 10, 8),
	)
	comp, err := tsdb.NewLeveledCompactor(ctx, nil, slog.Default(), []int64{time.Hour.Milliseconds(), 2 * time.Hour.Milliseconds()}, chunkenc.NewPool(), dedup.NewChunkSeriesMerger())
	testutil.Ok(t, err)

	out := t.TempDir()
	stats := &SeriesDedupStats{}
	ids, err := comp.CompactWithBlockPopulator(out, []string{x, y}, nil, DeduplicatingBlockPopulator{ReplicaLabels: []string{"replica"}, Stats: stats})
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(ids))
	content, _, symbols := blockContent(t, out, ids[0])

	testutil.Equals(t, map[string][]string{
		`{__name__="m", a="1"}`:        samplesOf(1, 0, 10),
		`{__name__="m", a="1", b="1"}`: samplesOf(3, 0, 10),
		`{__name__="n"}`:               append(samplesOf(5, 0, 5), samplesOf(6, 5, 10)...),
		`{__name__="only"}`:            samplesOf(7, 0, 10),
	}, content)
	for _, s := range []string{"replica", "A", "B"} {
		testutil.Assert(t, !slices.Contains(symbols, s), "the symbol table still holds %q", s)
	}
	testutil.Equals(t, SeriesDedupStats{Input: 8, Output: 4}, *stats)

	// The same sources give the same content again.
	againDir := t.TempDir()
	again, err := comp.CompactWithBlockPopulator(againDir, []string{x, y}, nil, DeduplicatingBlockPopulator{ReplicaLabels: []string{"replica"}})
	testutil.Ok(t, err)
	againContent, _, _ := blockContent(t, againDir, again[0])
	testutil.Equals(t, content, againContent)

	// Sources with the same MinTime come in no fixed order - the copies of
	// one range that different receivers uploaded - so the order they are
	// handed over in does not decide the tie: MinTime, then ULID does.
	swapped, err := comp.CompactWithBlockPopulator(out, []string{y, x}, nil, DeduplicatingBlockPopulator{ReplicaLabels: []string{"replica"}})
	testutil.Ok(t, err)
	swappedContent, _, _ := blockContent(t, out, swapped[0])
	testutil.Equals(t, content, swappedContent)
}

// TestDeduplicatingBlockPopulatorMatchesQueryTimeDeduplication: on replicas
// that disagree and have gaps - where penalty deduplication switches replica
// - the compaction writes what the querier's penalty deduplication returns
// for the same replicas in the same order.
func TestDeduplicatingBlockPopulatorMatchesQueryTimeDeduplication(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	lset := func(r string) labels.Labels { return labels.FromStrings("__name__", "up", "job", "api", "replica", r) }
	// A has a long gap in the middle, which makes penalty deduplication
	// switch to B and back; both have samples every 15s otherwise.
	var a, b []chunks.Sample
	for ts := int64(0); ts < 3600; ts += 15 {
		if ts < 1200 || ts >= 2400 {
			a = append(a, floatSample{t: ts * 1000, f: 1})
		}
		b = append(b, floatSample{t: ts*1000 + 5000, f: 2})
	}
	src := writeBlock(t, dir, storage.NewListSeries(lset("A"), a), storage.NewListSeries(lset("B"), b))

	comp, err := tsdb.NewLeveledCompactor(ctx, nil, slog.Default(), []int64{time.Hour.Milliseconds(), 2 * time.Hour.Milliseconds()}, chunkenc.NewPool(), dedup.NewChunkSeriesMerger())
	testutil.Ok(t, err)
	out := t.TempDir()
	ids, err := comp.CompactWithBlockPopulator(out, []string{src}, nil, DeduplicatingBlockPopulator{ReplicaLabels: []string{"replica"}})
	testutil.Ok(t, err)
	content, _, _ := blockContent(t, out, ids[0])

	stripped := labels.FromStrings("__name__", "up", "job", "api")
	set := dedup.NewSeriesSet(&listSeriesSet{series: []storage.Series{storage.NewListSeries(stripped, a), storage.NewListSeries(stripped, b)}}, "", dedup.AlgorithmPenalty)
	var want []string
	for set.Next() {
		it := set.At().Iterator(nil)
		for it.Next() != chunkenc.ValNone {
			ts, v := it.At()
			want = append(want, fmt.Sprintf("%d/%v", ts, v))
		}
		testutil.Ok(t, it.Err())
	}
	testutil.Ok(t, set.Err())
	slices.Sort(want)
	testutil.Assert(t, slices.ContainsFunc(want, func(s string) bool { return strings.HasSuffix(s, "/2") }), "the querier never switched replica; the case does not test anything")
	testutil.Equals(t, map[string][]string{stripped.String(): want}, content)
}

type listSeriesSet struct {
	series []storage.Series
	i      int
}

func (s *listSeriesSet) Next() bool {
	s.i++
	return s.i <= len(s.series)
}
func (s *listSeriesSet) At() storage.Series                { return s.series[s.i-1] }
func (s *listSeriesSet) Err() error                        { return nil }
func (s *listSeriesSet) Warnings() annotations.Annotations { return nil }

// TestDeduplicatingBlockPopulatorPartitions: with a partition that leaves the
// replica label out of its hash, the partitions of a deduplicating
// compaction together hold exactly what the whole one holds, and the
// partition tallies cover the sources. A partition that hashes the replica
// label is refused.
func TestDeduplicatingBlockPopulatorPartitions(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	var series []labels.Labels
	for i := range 40 {
		for _, r := range []string{"A", "B"} {
			series = append(series, labels.FromStrings("__name__", fmt.Sprintf("metric_%d", i%5), "instance", fmt.Sprintf("host-%d", i), "replica", r))
		}
	}
	id, err := e2eutil.CreateBlock(ctx, dir, series, 30, 0, time.Hour.Milliseconds(), labels.FromStrings("ext", "1"), 0, metadata.NoneFunc, nil)
	testutil.Ok(t, err)
	dirs := []string{filepath.Join(dir, id.String())}
	comp, err := tsdb.NewLeveledCompactor(ctx, nil, slog.Default(), []int64{time.Hour.Milliseconds(), 2 * time.Hour.Milliseconds()}, chunkenc.NewPool(), dedup.NewChunkSeriesMerger())
	testutil.Ok(t, err)

	out := t.TempDir()
	ids, err := comp.CompactWithBlockPopulator(out, dirs, nil, DeduplicatingBlockPopulator{ReplicaLabels: []string{"replica"}})
	testutil.Ok(t, err)
	whole, _, _ := blockContent(t, out, ids[0])
	testutil.Equals(t, 40, len(whole))

	const count = 4
	got := map[string][]string{}
	var walked, kept uint64
	for i := range uint64(count) {
		ps := &PartitionStats{}
		part := &SeriesPartition{Index: i, Count: count, Without: []string{"replica"}}
		ids, err := comp.CompactWithBlockPopulator(out, dirs, nil, DeduplicatingBlockPopulator{ReplicaLabels: []string{"replica"}, Partition: part, PartitionStats: ps})
		testutil.Ok(t, err)
		testutil.Equals(t, uint64(len(series)), ps.Walked)
		walked, kept = ps.Walked, kept+ps.Kept
		if len(ids) == 0 {
			continue
		}
		content, lsets, _ := blockContent(t, out, ids[0])
		for k, v := range content {
			testutil.Assert(t, part.Contains(lsets[k]), "series %s is in partition %d but hashes elsewhere", k, i)
			_, dup := got[k]
			testutil.Assert(t, !dup, "series %s is in two partitions", k)
			got[k] = v
		}
	}
	testutil.Equals(t, walked, kept)
	testutil.Equals(t, whole, got)

	_, err = comp.CompactWithBlockPopulator(out, dirs, nil, DeduplicatingBlockPopulator{ReplicaLabels: []string{"replica"}, Partition: &SeriesPartition{Index: 0, Count: 2}})
	testutil.NotOk(t, err)
	testutil.Assert(t, strings.Contains(err.Error(), "hashes the series replica label"), "unexpected error: %v", err)
}

// TestLocalPlanExecutorDeduplicatesSeriesReplicas runs a plan through the
// executor configured with series replica labels: the output holds one
// series per replicated pair, records the labels in its metadata, and the
// metrics count what was merged.
func TestLocalPlanExecutorDeduplicatesSeriesReplicas(t *testing.T) {
	ctx := context.Background()
	bkt := objstore.NewInMemBucket()
	logger := log.NewNopLogger()
	dir := t.TempDir()

	var series []labels.Labels
	for i := range 10 {
		for _, r := range []string{"A", "B"} {
			series = append(series, labels.FromStrings("__name__", "metric", "instance", fmt.Sprintf("host-%d", i), "prometheus_replica", r))
		}
	}
	ext := labels.FromStrings("ext", "1")
	var sources []*metadata.Meta
	for _, tr := range [][2]int64{{0, time.Hour.Milliseconds()}, {time.Hour.Milliseconds(), 2 * time.Hour.Milliseconds()}} {
		id, err := e2eutil.CreateBlock(ctx, dir, series, 10, tr[0], tr[1], ext, 0, metadata.NoneFunc, nil)
		testutil.Ok(t, err)
		testutil.Ok(t, block.Upload(ctx, logger, bkt, filepath.Join(dir, id.String()), metadata.NoneFunc))
		m, err := metadata.ReadFromDir(filepath.Join(dir, id.String()))
		testutil.Ok(t, err)
		sources = append(sources, m)
	}
	cnt := func() prometheus.Counter { return promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "test"}) }
	cg, err := NewGroup(logger, bkt, "0@test", ext, 0, false, false, cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), metadata.NoneFunc, 1, 1)
	testutil.Ok(t, err)
	for _, m := range sources {
		testutil.Ok(t, cg.AppendMeta(m))
	}
	comp, err := tsdb.NewLeveledCompactor(ctx, nil, slog.Default(), []int64{time.Hour.Milliseconds(), 2 * time.Hour.Milliseconds()}, chunkenc.NewPool(), dedup.NewChunkSeriesMerger())
	testutil.Ok(t, err)
	metrics := NewSeriesDedupMetrics(nil)
	ex := LocalPlanExecutor{Comp: comp, BlockDeletableChecker: DefaultBlockDeletableChecker{}, Callback: DefaultCompactionLifecycleCallback{}, MarkSourcesForDeletion: true,
		SeriesReplicaLabels: []string{"prometheus_replica", ""}, SeriesDedupMetrics: metrics}

	compIDs, err := ex.Execute(ctx, t.TempDir(), cg, Plan{Sources: sources})
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(compIDs))
	m, err := block.DownloadMeta(ctx, logger, bkt, compIDs[0])
	testutil.Ok(t, err)
	testutil.Equals(t, []string{"prometheus_replica"}, m.Thanos.SeriesReplicaLabels)
	testutil.Equals(t, uint64(10), m.Stats.NumSeries)
	testutil.Equals(t, 40.0, promtestutil.ToFloat64(metrics.Input), "20 series in each of two sources")
	testutil.Equals(t, 10.0, promtestutil.ToFloat64(metrics.Output))

	// Without the labels the executor writes what it always wrote.
	plain := ex
	plain.SeriesReplicaLabels, plain.SeriesDedupMetrics = nil, nil
	cg2, err := NewGroup(logger, bkt, "0@test", ext, 0, false, false, cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), metadata.NoneFunc, 1, 1)
	testutil.Ok(t, err)
	for _, m := range sources {
		testutil.Ok(t, cg2.AppendMeta(m))
	}
	plainIDs, err := plain.Execute(ctx, t.TempDir(), cg2, Plan{Sources: sources})
	testutil.Ok(t, err)
	pm, err := block.DownloadMeta(ctx, logger, bkt, plainIDs[0])
	testutil.Ok(t, err)
	testutil.Equals(t, uint64(20), pm.Stats.NumSeries)
	testutil.Equals(t, 0, len(pm.Thanos.SeriesReplicaLabels))
}

// queryTimePenalty is what a querier's penalty deduplication returns for the
// replicas, offered in the given order.
func queryTimePenalty(t *testing.T, lset labels.Labels, replicas ...[]chunks.Sample) []string {
	t.Helper()
	var series []storage.Series
	for _, r := range replicas {
		series = append(series, storage.NewListSeries(lset, r))
	}
	set := dedup.NewSeriesSet(&listSeriesSet{series: series}, "", dedup.AlgorithmPenalty)
	var out []string
	for set.Next() {
		it := set.At().Iterator(nil)
		for it.Next() != chunkenc.ValNone {
			ts, v := it.At()
			out = append(out, fmt.Sprintf("%d/%v", ts, v))
		}
		testutil.Ok(t, it.Err())
	}
	testutil.Ok(t, set.Err())
	slices.Sort(out)
	return out
}

// TestDeduplicatingBlockPopulatorAfterAGap pins down what compaction-time
// penalty deduplication does after a gap that spans two blocks, which
// differs from a querier reading across them. The merger deduplicates each
// group of overlapping chunks on its own, starting afresh; the blocks here
// hold one chunk per series. After A's gap the first block continues with B,
// as a querier does; the second starts again from A, where a querier reading
// both at once stays with B. Each block is exactly what a querier returns
// for that block alone, and every sample is a real one. Groups within one
// block are TestDeduplicatingBlockPopulatorPerChunkGroup's.
func TestDeduplicatingBlockPopulatorAfterAGap(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	lset := func(r string) labels.Labels { return labels.FromStrings("__name__", "up", "replica", r) }
	window := func(from int64, gap bool) (a, b []chunks.Sample) {
		for ts := from; ts < from+3600; ts += 15 {
			if !gap || ts < from+1200 || ts >= from+2400 {
				a = append(a, floatSample{t: ts * 1000, f: 1})
			}
			b = append(b, floatSample{t: ts*1000 + 5000, f: 2})
		}
		return a, b
	}
	a1, b1 := window(0, true)
	a2, b2 := window(3600, false)
	w1 := writeBlock(t, dir, storage.NewListSeries(lset("A"), a1), storage.NewListSeries(lset("B"), b1))
	w2 := writeBlock(t, dir, storage.NewListSeries(lset("A"), a2), storage.NewListSeries(lset("B"), b2))

	comp, err := tsdb.NewLeveledCompactor(ctx, nil, slog.Default(), []int64{time.Hour.Milliseconds(), 2 * time.Hour.Milliseconds()}, chunkenc.NewPool(), dedup.NewChunkSeriesMerger())
	testutil.Ok(t, err)
	out := t.TempDir()
	ids, err := comp.CompactWithBlockPopulator(out, []string{w1, w2}, nil, DeduplicatingBlockPopulator{ReplicaLabels: []string{"replica"}})
	testutil.Ok(t, err)
	content, _, _ := blockContent(t, out, ids[0])

	stripped := labels.FromStrings("__name__", "up")
	perWindow := append(queryTimePenalty(t, stripped, a1, b1), queryTimePenalty(t, stripped, a2, b2)...)
	slices.Sort(perWindow)
	testutil.Equals(t, map[string][]string{stripped.String(): perWindow}, content)

	across := queryTimePenalty(t, stripped, append(slices.Clone(a1), a2...), append(slices.Clone(b1), b2...))
	testutil.Assert(t, !slices.Equal(across, perWindow), "a querier reading across the windows stays with B; if it no longer does, the documentation is wrong")
}

// chunkGroups returns the time ranges of the groups of overlapping chunks the
// block's series form together: the chunks the merger deduplicates at once.
func chunkGroups(t *testing.T, blockDir string) [][2]int64 {
	t.Helper()
	b, err := tsdb.OpenBlock(slog.Default(), blockDir, chunkenc.NewPool(), nil)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, b.Close()) }()
	ir, err := b.Index()
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, ir.Close()) }()
	k, v := index.AllPostingsKey()
	all, err := ir.Postings(context.Background(), k, v)
	testutil.Ok(t, err)
	var ranges [][2]int64
	for all.Next() {
		var builder labels.ScratchBuilder
		var chks []chunks.Meta
		testutil.Ok(t, ir.Series(all.At(), &builder, &chks))
		for _, c := range chks {
			ranges = append(ranges, [2]int64{c.MinTime, c.MaxTime})
		}
	}
	testutil.Ok(t, all.Err())
	slices.SortFunc(ranges, func(a, b [2]int64) int { return cmp.Compare(a[0], b[0]) })
	var groups [][2]int64
	for _, r := range ranges {
		if n := len(groups); n > 0 && r[0] <= groups[n-1][1] {
			groups[n-1][1] = max(groups[n-1][1], r[1])
			continue
		}
		groups = append(groups, r)
	}
	return groups
}

func within(samples []chunks.Sample, from, to int64) []chunks.Sample {
	var out []chunks.Sample
	for _, s := range samples {
		if s.T() >= from && s.T() <= to {
			out = append(out, s)
		}
	}
	return out
}

// TestDeduplicatingBlockPopulatorPerChunkGroup pins down what compaction-time
// penalty deduplication guarantees against a querier at normal scrape
// density: the merger deduplicates each group of overlapping chunks on its
// own, starting afresh, and a block holds several such groups - a chunk holds
// 120 samples, half an hour at a 15s scrape. The result is, group by group,
// exactly what a querier returns for that group alone, every sample a real
// one; a querier reading across groups carries its state on and can differ at
// the group boundaries. Replicas scraped at an offset differ that way at
// every boundary, and replicas in lockstep once one of them has a gap;
// replicas in lockstep without gaps do not differ at all.
func TestDeduplicatingBlockPopulatorPerChunkGroup(t *testing.T) {
	ctx := context.Background()
	lset := func(r string) labels.Labels { return labels.FromStrings("__name__", "up", "replica", r) }
	stripped := labels.FromStrings("__name__", "up")
	for _, tc := range []struct {
		name    string
		offset  int64 // B's scrape offset against A, in seconds.
		gap     [2]int64
		differs bool
	}{
		{name: "offset scrapes", offset: 6, differs: true},
		{name: "lockstep scrapes with a gap in one replica", gap: [2]int64{1000, 1300}, differs: true},
		{name: "lockstep scrapes without gaps"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var a, b []chunks.Sample
			for ts := int64(0); ts < 7200; ts += 15 {
				if ts < tc.gap[0] || ts >= tc.gap[1] {
					a = append(a, floatSample{t: ts * 1000, f: 1})
				}
				b = append(b, floatSample{t: (ts + tc.offset) * 1000, f: 2})
			}
			src := writeBlock(t, t.TempDir(), storage.NewListSeries(lset("A"), a), storage.NewListSeries(lset("B"), b))
			groups := chunkGroups(t, src)
			testutil.Assert(t, len(groups) > 1, "the test needs several chunk groups in one block, got %v", groups)

			comp, err := tsdb.NewLeveledCompactor(ctx, nil, slog.Default(), []int64{2 * time.Hour.Milliseconds()}, chunkenc.NewPool(), dedup.NewChunkSeriesMerger())
			testutil.Ok(t, err)
			out := t.TempDir()
			ids, err := comp.CompactWithBlockPopulator(out, []string{src}, nil, DeduplicatingBlockPopulator{ReplicaLabels: []string{"replica"}})
			testutil.Ok(t, err)
			content, _, _ := blockContent(t, out, ids[0])

			var perGroup []string
			for _, g := range groups {
				perGroup = append(perGroup, queryTimePenalty(t, stripped, within(a, g[0], g[1]), within(b, g[0], g[1]))...)
			}
			slices.Sort(perGroup)
			testutil.Equals(t, map[string][]string{stripped.String(): perGroup}, content)

			// A querier reading the whole block, in either replica order,
			// returns something else, with the difference at the group
			// boundaries - except for lockstep replicas without gaps. If
			// that changes, the documentation is wrong.
			testutil.Equals(t, tc.differs, !slices.Equal(queryTimePenalty(t, stripped, a, b), perGroup), "A first")
			if tc.differs {
				testutil.Assert(t, !slices.Equal(queryTimePenalty(t, stripped, b, a), perGroup), "B first")
			}
		})
	}
}
