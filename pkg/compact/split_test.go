// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

const gib = 1 << 30

func metaWith(indexBytes int64, series uint64, lbls map[string]string) *metadata.Meta {
	m := &metadata.Meta{}
	m.ULID = ulid.MustNew(ulid.Now(), nil)
	m.Stats.NumSeries = series
	m.Thanos.Labels = lbls
	if indexBytes > 0 {
		m.Thanos.Files = []metadata.File{{RelPath: block.IndexFilename, SizeBytes: indexBytes}}
	}
	return m
}

func TestSplitConfigShardCount(t *testing.T) {
	sixtyFour := SplitConfig{MaxShards: 16, MaxIndexSizeBytes: 64 * gib}
	for _, tc := range []struct {
		name         string
		conf         SplitConfig
		metas        []*metadata.Meta
		sourceShards uint64
		floor        uint64
		want         uint64
		fits         bool
	}{
		{name: "disabled", conf: SplitConfig{}, metas: []*metadata.Meta{metaWith(100*gib, 0, nil)}, want: 1, fits: true},
		{name: "disabled keeps the sources' count", conf: SplitConfig{}, metas: []*metadata.Meta{metaWith(100*gib, 0, nil)}, sourceShards: 4, want: 4, fits: true},
		{name: "max shards one is disabled", conf: SplitConfig{MaxShards: 1, MaxIndexSizeBytes: gib}, metas: []*metadata.Meta{metaWith(100*gib, 0, nil)}, want: 1, fits: true},
		{name: "fits", conf: sixtyFour, metas: []*metadata.Meta{metaWith(20*gib, 0, nil), metaWith(20*gib, 0, nil)}, want: 1, fits: true},
		{name: "two replicas over the headroom", conf: sixtyFour, metas: []*metadata.Meta{metaWith(30*gib, 0, nil), metaWith(30*gib, 0, nil)}, want: 2, fits: true},
		{name: "rounds up to a power of two", conf: sixtyFour, metas: []*metadata.Meta{metaWith(100*gib, 0, nil), metaWith(100*gib, 0, nil), metaWith(60*gib, 0, nil)}, want: 8, fits: true},
		{name: "over the cap by index, cap rounded down, reports what it needs", conf: SplitConfig{MaxShards: 6, MaxIndexSizeBytes: gib}, metas: []*metadata.Meta{metaWith(100*gib, 0, nil)}, want: 128, fits: false},
		{name: "over the cap by series is capped, not refused", conf: SplitConfig{MaxShards: 4, MaxSeries: 10}, metas: []*metadata.Meta{metaWith(0, 60, nil)}, want: 4, fits: true},
		{name: "series criterion", conf: SplitConfig{MaxShards: 16, MaxSeries: 10}, metas: []*metadata.Meta{metaWith(0, 12, nil), metaWith(0, 12, nil)}, want: 4, fits: true},
		{name: "never below the stream's count", conf: sixtyFour, metas: []*metadata.Meta{metaWith(gib, 0, nil)}, floor: 4, want: 4, fits: true},
		{name: "a fresh range outgrows the stream's count", conf: sixtyFour, metas: []*metadata.Meta{metaWith(60*gib, 0, nil), metaWith(60*gib, 0, nil)}, floor: 2, want: 4, fits: true},
		{name: "no index size known", conf: SplitConfig{MaxShards: 16, MaxIndexSizeBytes: gib}, metas: []*metadata.Meta{metaWith(0, 0, nil)}, want: 1, fits: true},
		// The sources of a shard group hold that shard's data alone: what
		// they need is parts of the shard, and the stream's count grows by
		// that factor.
		{name: "a shard within the limit keeps its count", conf: sixtyFour, metas: []*metadata.Meta{metaWith(20*gib, 0, nil), metaWith(20*gib, 0, nil)}, sourceShards: 4, want: 4, fits: true},
		{name: "a shard needing two parts doubles the count", conf: sixtyFour, metas: []*metadata.Meta{metaWith(40*gib, 0, nil), metaWith(40*gib, 0, nil)}, sourceShards: 4, want: 8, fits: true},
		{name: "a shard needing three parts quadruples the count", conf: sixtyFour, metas: []*metadata.Meta{metaWith(60*gib, 0, nil), metaWith(60*gib, 0, nil), metaWith(40*gib, 0, nil)}, sourceShards: 4, want: 16, fits: true},
		{name: "a shard's series count works the same", conf: SplitConfig{MaxShards: 16, MaxSeries: 10}, metas: []*metadata.Meta{metaWith(0, 12, nil), metaWith(0, 12, nil)}, sourceShards: 2, want: 8, fits: true},
		{name: "a shard at the cap that needs more index does not fit", conf: SplitConfig{MaxShards: 8, MaxIndexSizeBytes: 64 * gib}, metas: []*metadata.Meta{metaWith(40*gib, 0, nil), metaWith(40*gib, 0, nil)}, sourceShards: 8, want: 16, fits: false},
		// The cap was lowered below a count the stream already has: what
		// needs no further split still compacts within its shard, at its own
		// count - a coarser label would claim series the shard does not hold.
		{name: "a shard beyond a lowered cap keeps its count when it needs no split", conf: SplitConfig{MaxShards: 4, MaxIndexSizeBytes: 64 * gib}, metas: []*metadata.Meta{metaWith(20*gib, 0, nil), metaWith(20*gib, 0, nil)}, sourceShards: 8, want: 8, fits: true},
		{name: "a shard beyond a lowered cap keeps its count although its series want more", conf: SplitConfig{MaxShards: 4, MaxSeries: 4}, metas: []*metadata.Meta{metaWith(0, 6, nil), metaWith(0, 6, nil)}, sourceShards: 8, floor: 8, want: 8, fits: true},
		{name: "a shard beyond a lowered cap that needs a split does not fit", conf: SplitConfig{MaxShards: 4, MaxIndexSizeBytes: 64 * gib}, metas: []*metadata.Meta{metaWith(40*gib, 0, nil), metaWith(40*gib, 0, nil)}, sourceShards: 8, want: 16, fits: false},
		{name: "a fresh range under a lowered cap splits at the cap, not the stream's count", conf: SplitConfig{MaxShards: 4, MaxIndexSizeBytes: 64 * gib}, metas: []*metadata.Meta{metaWith(60*gib, 0, nil), metaWith(60*gib, 0, nil)}, floor: 8, want: 4, fits: true},
		{name: "a shard at a lowered cap is not split towards the stream's count", conf: SplitConfig{MaxShards: 4, MaxIndexSizeBytes: 64 * gib}, metas: []*metadata.Meta{metaWith(gib, 0, nil)}, sourceShards: 4, floor: 8, want: 4, fits: true},
		{name: "a shard at the cap that wants more for its series stays", conf: SplitConfig{MaxShards: 8, MaxSeries: 4}, metas: []*metadata.Meta{metaWith(0, 3, nil), metaWith(0, 3, nil), metaWith(0, 3, nil)}, sourceShards: 8, want: 8, fits: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			count, fits := tc.conf.ShardCount(tc.metas, tc.sourceShards, tc.floor)
			testutil.Equals(t, tc.want, count)
			testutil.Equals(t, tc.fits, fits)
		})
	}
}

func TestSplitConfigPlannerIndexSizeLimit(t *testing.T) {
	testutil.Equals(t, int64(100), SplitConfig{}.PlannerIndexSizeLimit(100))
	testutil.Equals(t, int64(800), SplitConfig{MaxShards: 8}.PlannerIndexSizeLimit(100))
	testutil.Equals(t, int64(400), SplitConfig{MaxShards: 7}.PlannerIndexSizeLimit(100))
	testutil.Equals(t, int64(math.MaxInt64), SplitConfig{MaxShards: 8}.PlannerIndexSizeLimit(math.MaxInt64))
}

func TestShardsToProduce(t *testing.T) {
	testutil.Equals(t, []uint64{0, 1, 2, 3}, ShardsToProduce(0, 0, 4))
	testutil.Equals(t, []uint64{2, 6}, ShardsToProduce(2, 4, 8))
	testutil.Equals(t, []uint64{3, 11, 19, 27}, ShardsToProduce(3, 8, 32))
	testutil.Equals(t, []uint64{1}, ShardsToProduce(1, 2, 2))
}

func TestSourceShard(t *testing.T) {
	_, count, err := SourceShard([]*metadata.Meta{metaWith(0, 0, map[string]string{"a": "b"}), metaWith(0, 0, map[string]string{"a": "b"})})
	testutil.Ok(t, err)
	testutil.Equals(t, uint64(0), count)

	index, count, err := SourceShard([]*metadata.Meta{
		metaWith(0, 0, map[string]string{"a": "b", metadata.CompactorShardLabel: "3_of_4"}),
		metaWith(0, 0, map[string]string{"a": "b", metadata.CompactorShardLabel: "3_of_4"}),
	})
	testutil.Ok(t, err)
	testutil.Equals(t, uint64(2), index)
	testutil.Equals(t, uint64(4), count)

	_, _, err = SourceShard([]*metadata.Meta{
		metaWith(0, 0, map[string]string{"a": "b", metadata.CompactorShardLabel: "3_of_4"}),
		metaWith(0, 0, map[string]string{"a": "b"}),
	})
	testutil.NotOk(t, err)
}

func noopCounter() prometheus.Counter {
	return promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "test"})
}

type fixedPlanner struct{ plan []*metadata.Meta }

func (p fixedPlanner) Plan(context.Context, []*metadata.Meta, chan error, any) ([]*metadata.Meta, error) {
	return p.plan, nil
}

func rangeMeta(id int, lbls map[string]string, mint, maxt int64) *metadata.Meta {
	m := metaWith(0, 0, lbls)
	m.ULID = ulid.MustNew(uint64(id), nil)
	m.MinTime, m.MaxTime = mint, maxt
	return m
}

func TestShardStragglerPlanner(t *testing.T) {
	base := map[string]string{"tenant": "a"}
	shard := func(v string) map[string]string {
		return map[string]string{"tenant": "a", metadata.CompactorShardLabel: v}
	}
	view := func(ms ...*metadata.Meta) func() map[ulid.ULID]*metadata.Meta {
		all := map[ulid.ULID]*metadata.Meta{}
		for _, m := range ms {
			all[m.ULID] = m
		}
		return func() map[ulid.ULID]*metadata.Meta { return all }
	}
	// Shards cover [0, 800) in two ranges; the stream's unsplit group holds a
	// straggler in [200, 300), one that pokes out of the coverage, one in a
	// range with no shards yet, and the newest block.
	shards := []*metadata.Meta{
		rangeMeta(1, shard("1_of_2"), 0, 400), rangeMeta(2, shard("2_of_2"), 0, 400),
		rangeMeta(3, shard("1_of_2"), 400, 800),
	}
	straggler := rangeMeta(10, base, 200, 300)
	pokesOut := rangeMeta(11, base, 700, 900)
	uncovered := rangeMeta(12, base, 1000, 1100)
	newest := rangeMeta(13, base, 1200, 1300)
	group := []*metadata.Meta{straggler, pokesOut, uncovered, newest}
	noMarks := func() map[ulid.ULID]*metadata.NoCompactMark { return nil }

	t.Run("the inner plan wins", func(t *testing.T) {
		p := WithBlockSplitting(fixedPlanner{plan: []*metadata.Meta{uncovered, newest}}, log.NewNopLogger(), SplitConfig{MaxShards: 8}, nil, objstore.NewInMemBucket(), noopCounter(), view(shards...), noMarks)
		plan, err := p.Plan(t.Context(), group, nil, nil)
		testutil.Ok(t, err)
		testutil.Equals(t, []*metadata.Meta{uncovered, newest}, plan)
	})
	t.Run("a covered block is planned alone", func(t *testing.T) {
		p := WithBlockSplitting(fixedPlanner{}, log.NewNopLogger(), SplitConfig{MaxShards: 8}, nil, objstore.NewInMemBucket(), noopCounter(), view(shards...), noMarks)
		plan, err := p.Plan(t.Context(), group, nil, nil)
		testutil.Ok(t, err)
		testutil.Equals(t, []*metadata.Meta{straggler}, plan)
	})
	t.Run("a marked block is skipped", func(t *testing.T) {
		marks := func() map[ulid.ULID]*metadata.NoCompactMark {
			return map[ulid.ULID]*metadata.NoCompactMark{straggler.ULID: {}}
		}
		p := WithBlockSplitting(fixedPlanner{}, log.NewNopLogger(), SplitConfig{MaxShards: 8}, nil, objstore.NewInMemBucket(), noopCounter(), view(shards...), marks)
		plan, err := p.Plan(t.Context(), group, nil, nil)
		testutil.Ok(t, err)
		testutil.Equals(t, 0, len(plan), "the block poking out of the coverage and the uncovered one must not be planned")
	})
	t.Run("a covered block is planned even as the last block of its group", func(t *testing.T) {
		covered := rangeMeta(14, base, 100, 200)
		p := WithBlockSplitting(fixedPlanner{}, log.NewNopLogger(), SplitConfig{MaxShards: 8}, nil, objstore.NewInMemBucket(), noopCounter(), view(shards...), noMarks)
		plan, err := p.Plan(t.Context(), []*metadata.Meta{covered}, nil, nil)
		testutil.Ok(t, err)
		testutil.Equals(t, []*metadata.Meta{covered}, plan)
	})
	t.Run("an uncovered newest block is left alone", func(t *testing.T) {
		p := WithBlockSplitting(fixedPlanner{}, log.NewNopLogger(), SplitConfig{MaxShards: 8}, nil, objstore.NewInMemBucket(), noopCounter(), view(shards...), noMarks)
		plan, err := p.Plan(t.Context(), []*metadata.Meta{uncovered, newest}, nil, nil)
		testutil.Ok(t, err)
		testutil.Equals(t, 0, len(plan))
	})
	t.Run("no shards, no stragglers", func(t *testing.T) {
		p := WithBlockSplitting(fixedPlanner{}, log.NewNopLogger(), SplitConfig{MaxShards: 8}, nil, objstore.NewInMemBucket(), noopCounter(), view(), noMarks)
		plan, err := p.Plan(t.Context(), group, nil, nil)
		testutil.Ok(t, err)
		testutil.Equals(t, 0, len(plan))
	})
	t.Run("shards of another stream or resolution do not count", func(t *testing.T) {
		other := rangeMeta(20, map[string]string{"tenant": "b", metadata.CompactorShardLabel: "1_of_2"}, 0, 400)
		downsampled := rangeMeta(21, shard("1_of_2"), 0, 400)
		downsampled.Thanos.Downsample.Resolution = 300000
		p := WithBlockSplitting(fixedPlanner{}, log.NewNopLogger(), SplitConfig{MaxShards: 8}, nil, objstore.NewInMemBucket(), noopCounter(), view(other, downsampled), noMarks)
		plan, err := p.Plan(t.Context(), group, nil, nil)
		testutil.Ok(t, err)
		testutil.Equals(t, 0, len(plan))
	})
	t.Run("a lone shard block behind its lineage's count is planned alone, covered or not", func(t *testing.T) {
		// 1 of 4 holds [0, 100); its lineage has 5 of 8 over [100, 200) only.
		// Nothing covers [0, 100), yet the block can never complete a range
		// at count 4 again, so it is planned alone and split to 8.
		finer := rangeMeta(50, shard("5_of_8"), 100, 200)
		p := WithBlockSplitting(fixedPlanner{}, log.NewNopLogger(), SplitConfig{MaxShards: 8}, nil, objstore.NewInMemBucket(), noopCounter(), view(finer), noMarks)
		lone := rangeMeta(51, shard("1_of_4"), 0, 100)
		plan, err := p.Plan(t.Context(), []*metadata.Meta{lone}, nil, nil)
		testutil.Ok(t, err)
		testutil.Equals(t, []*metadata.Meta{lone}, plan)
		// A sibling lineage's refinement is not this block's problem.
		p = WithBlockSplitting(fixedPlanner{}, log.NewNopLogger(), SplitConfig{MaxShards: 8}, nil, objstore.NewInMemBucket(), noopCounter(), view(rangeMeta(52, shard("2_of_8"), 100, 200)), noMarks)
		plan, err = p.Plan(t.Context(), []*metadata.Meta{lone}, nil, nil)
		testutil.Ok(t, err)
		testutil.Equals(t, 0, len(plan))
	})
	t.Run("a shard block is a straggler only under finer shards of its lineage", func(t *testing.T) {
		p := WithBlockSplitting(fixedPlanner{}, log.NewNopLogger(), SplitConfig{MaxShards: 8}, nil, objstore.NewInMemBucket(), noopCounter(), view(shards...), noMarks)
		plan, err := p.Plan(t.Context(), []*metadata.Meta{rangeMeta(30, shard("1_of_2"), 0, 100), rangeMeta(31, shard("1_of_2"), 100, 200)}, nil, nil)
		testutil.Ok(t, err)
		testutil.Equals(t, 0, len(plan), "the covering shards are the block's own shard, not finer ones")

		finer := []*metadata.Meta{rangeMeta(40, shard("1_of_4"), 0, 400), rangeMeta(41, shard("3_of_4"), 0, 400)}
		p = WithBlockSplitting(fixedPlanner{}, log.NewNopLogger(), SplitConfig{MaxShards: 8}, nil, objstore.NewInMemBucket(), noopCounter(), view(finer...), noMarks)
		plan, err = p.Plan(t.Context(), []*metadata.Meta{rangeMeta(30, shard("1_of_2"), 0, 100)}, nil, nil)
		testutil.Ok(t, err)
		testutil.Equals(t, 1, len(plan), "1 of 2 is refined by 1 and 3 of 4")

		plan, err = p.Plan(t.Context(), []*metadata.Meta{rangeMeta(32, shard("2_of_2"), 0, 100)}, nil, nil)
		testutil.Ok(t, err)
		testutil.Equals(t, 0, len(plan), "2 of 2 is not refined by 1 and 3 of 4")
	})
}

func TestStreamShardCountFunc(t *testing.T) {
	base := map[string]string{"tenant": "a"}
	all := map[ulid.ULID]*metadata.Meta{}
	for i, m := range []*metadata.Meta{
		rangeMeta(1, map[string]string{"tenant": "a", metadata.CompactorShardLabel: "1_of_4"}, 0, 1),
		rangeMeta(2, map[string]string{"tenant": "a", metadata.CompactorShardLabel: "3_of_8"}, 1, 2),
		rangeMeta(3, map[string]string{"tenant": "b", metadata.CompactorShardLabel: "1_of_16"}, 0, 1),
		rangeMeta(4, base, 2, 3),
	} {
		_ = i
		all[m.ULID] = m
	}
	downsampled := rangeMeta(5, map[string]string{"tenant": "a", metadata.CompactorShardLabel: "1_of_32"}, 0, 1)
	downsampled.Thanos.Downsample.Resolution = 300000
	all[downsampled.ULID] = downsampled

	f := StreamShardCountFunc(func() map[ulid.ULID]*metadata.Meta { return all })
	unsplit := ShardRef{}
	testutil.Equals(t, uint64(8), f(base, 0, unsplit))
	testutil.Equals(t, uint64(32), f(base, 300000, unsplit))
	testutil.Equals(t, uint64(0), f(map[string]string{"tenant": "c"}, 0, unsplit))
	// A shard's lineage: 3 of 8 refines 3 of 4, but 1 of 4 does not.
	testutil.Equals(t, uint64(8), f(base, 0, ShardRef{Index: 2, Count: 4}))
	testutil.Equals(t, uint64(0), f(base, 0, ShardRef{Index: 1, Count: 4}))
	testutil.Equals(t, uint64(0), f(base, 0, ShardRef{Index: 0, Count: 4}), "1 of 4 is present at count 4 only, which is not finer")
}

func TestSplitPlannerPlansSingleBlockGroups(t *testing.T) {
	testutil.Equals(t, false, plansSingleBlockGroups(fixedPlanner{}))
	testutil.Equals(t, true, plansSingleBlockGroups(WithBlockSplitting(fixedPlanner{}, log.NewNopLogger(), SplitConfig{MaxShards: 8}, nil, objstore.NewInMemBucket(), noopCounter(), func() map[ulid.ULID]*metadata.Meta { return nil }, nil)))
}

// TestPlannerNewestAcrossShards: a shard group's last range is planned when
// the stream has a newer block in another group, and left alone otherwise.
func TestPlannerNewestAcrossShards(t *testing.T) {
	shard := map[string]string{"tenant": "a", metadata.CompactorShardLabel: "1_of_2"}
	unsplit := map[string]string{"tenant": "a"}
	ranges := []int64{100, 400}
	// Four 100-wide blocks fill the 400 range exactly; the planner would compact
	// them into one, if the last one were not the group's newest.
	group := []*metadata.Meta{
		rangeMeta(1, shard, 0, 100), rangeMeta(2, shard, 100, 200), rangeMeta(3, shard, 200, 300), rangeMeta(4, shard, 300, 400),
	}
	noMarks := func() map[ulid.ULID]*metadata.NoCompactMark { return nil }
	view := func(ms ...*metadata.Meta) func() map[ulid.ULID]*metadata.Meta {
		all := map[ulid.ULID]*metadata.Meta{}
		for _, m := range append(append([]*metadata.Meta{}, group...), ms...) {
			all[m.ULID] = m
		}
		return func() map[ulid.ULID]*metadata.Meta { return all }
	}

	plain := &tsdbBasedPlanner{logger: log.NewNopLogger(), ranges: ranges, noCompBlocksFunc: noMarks}
	plan, err := plain.Plan(t.Context(), group, nil, nil)
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(plan), "without the stream view the last range is the newest and is left alone")

	streamAware := (&tsdbBasedPlanner{logger: log.NewNopLogger(), ranges: ranges, noCompBlocksFunc: noMarks}).
		WithStreamNewestAcrossShards(view(rangeMeta(10, unsplit, 400, 500)))
	plan, err = streamAware.Plan(t.Context(), group, nil, nil)
	testutil.Ok(t, err)
	testutil.Equals(t, 4, len(plan), "with a newer unsplit block in the stream the last range is planned")

	sameStreamOnly := (&tsdbBasedPlanner{logger: log.NewNopLogger(), ranges: ranges, noCompBlocksFunc: noMarks}).
		WithStreamNewestAcrossShards(view(rangeMeta(11, map[string]string{"tenant": "b"}, 400, 500)))
	plan, err = sameStreamOnly.Plan(t.Context(), group, nil, nil)
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(plan), "a newer block of another stream does not count")
}

func TestStreamLeavesFunc(t *testing.T) {
	base := map[string]string{"tenant": "a"}
	shard := func(v string) map[string]string {
		return map[string]string{"tenant": "a", metadata.CompactorShardLabel: v}
	}
	all := map[ulid.ULID]*metadata.Meta{}
	add := func(m *metadata.Meta) { all[m.ULID] = m }
	// The range [0, 400) is covered by an uneven tree: shards 1 and 2 of 4,
	// and shard 4 of 4 re-split into 4 and 8 of 8; 3 of 4 was empty.
	add(rangeMeta(1, shard("1_of_4"), 0, 400))
	add(rangeMeta(2, shard("2_of_4"), 0, 400))
	add(rangeMeta(3, shard("4_of_8"), 0, 400))
	add(rangeMeta(4, shard("8_of_8"), 0, 400))
	// A shard covering only part of the range, another stream, another
	// resolution and an unsplit block do not count.
	add(rangeMeta(5, shard("1_of_2"), 0, 150))
	add(rangeMeta(6, map[string]string{"tenant": "b", metadata.CompactorShardLabel: "1_of_2"}, 0, 400))
	other := rangeMeta(7, shard("1_of_2"), 0, 400)
	other.Thanos.Downsample.Resolution = 300000
	add(other)
	add(rangeMeta(8, base, 0, 400))

	leaves := StreamLeavesFunc(func() map[ulid.ULID]*metadata.Meta { return all })
	unsplit := ShardRef{}
	got := leaves(base, 0, 100, 200, unsplit)
	testutil.Equals(t, []ShardRef{{0, 4}, {1, 4}, {2, 8}, {3, 8}, {6, 8}, {7, 8}}, got, "present leaves plus the empty shard's part at the finest count")
	testutil.Equals(t, "1_of_4", got[0].Label())

	testutil.Equals(t, 0, len(leaves(base, 0, 300, 500, unsplit)), "a range no shard covers whole is a fresh split")
	testutil.Equals(t, 0, len(leaves(map[string]string{"tenant": "c"}, 0, 100, 200, unsplit)))
	testutil.Equals(t, []ShardRef{{0, 2}, {1, 2}}, leaves(base, 300000, 100, 200, unsplit), "the downsampled stream has its own leaves; the empty shard's part is added")

	// A shard block of 4 of 4 is refined by 4 and 8 of 8 only; the 4-count
	// shards of other indexes and the coarser 2-count one are not its lineage.
	testutil.Equals(t, []ShardRef{{3, 8}, {7, 8}}, leaves(base, 0, 100, 200, ShardRef{3, 4}))
	// A shard block of 1 of 4 has no finer shard covering it: not a straggler.
	testutil.Equals(t, 0, len(leaves(base, 0, 100, 200, ShardRef{0, 4})))
	// When only one of the two finer shards exists, the other's part is added.
	delete(all, rangeMeta(4, nil, 0, 0).ULID)
	testutil.Equals(t, []ShardRef{{3, 8}, {7, 8}}, leaves(base, 0, 100, 200, ShardRef{3, 4}))
}

// TestSplitPlannerPlanOutputs: the planner decides what a plan produces, so an
// executor - here or on a worker - never has to. Fresh plans over the limit
// get one labeled, partitioned output per shard, never fewer than the stream
// already has; plans in a range the stream's shards cover get exactly those
// shards; plans within the limit, or already at the target count, keep the
// group's labels.
func TestSplitPlannerPlanOutputs(t *testing.T) {
	ctx := t.Context()
	base := map[string]string{"tenant": "a"}
	shard := func(v string) map[string]string {
		return map[string]string{"tenant": "a", metadata.CompactorShardLabel: v}
	}
	view := func(ms ...*metadata.Meta) func() map[ulid.ULID]*metadata.Meta {
		all := map[ulid.ULID]*metadata.Meta{}
		for _, m := range ms {
			all[m.ULID] = m
		}
		return func() map[ulid.ULID]*metadata.Meta { return all }
	}
	planner := func(conf SplitConfig, metrics *SplitMetrics, ms ...*metadata.Meta) OutputPlanner {
		return WithBlockSplitting(fixedPlanner{}, log.NewNopLogger(), conf, metrics, objstore.NewInMemBucket(), promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "test"}), view(ms...), nil).(OutputPlanner)
	}
	group := func(lbls map[string]string) *Group {
		g, err := NewGroup(log.NewNopLogger(), nil, "k", labels.FromMap(lbls), 0, false, false,
			nil, nil, nil, nil, nil, nil, nil, nil, metadata.NoneFunc, 1, 1)
		testutil.Ok(t, err)
		return g
	}
	sources := func(lbls map[string]string, series uint64) []*metadata.Meta {
		a := metaWith(0, series, lbls)
		a.MinTime, a.MaxTime = 0, 100
		b := metaWith(0, series, lbls)
		b.MinTime, b.MaxTime = 100, 200
		return []*metadata.Meta{a, b}
	}
	conf := SplitConfig{MaxShards: 8, MaxSeries: 100}

	t.Run("within the limit nothing is decided", func(t *testing.T) {
		out, err := planner(conf, nil).PlanOutputs(ctx, group(base), sources(base, 40))
		testutil.Ok(t, err)
		testutil.Equals(t, 0, len(out))
	})
	t.Run("over the limit one output per shard", func(t *testing.T) {
		metrics := NewSplitMetrics(nil)
		out, err := planner(conf, metrics).PlanOutputs(ctx, group(base), sources(base, 150))
		testutil.Ok(t, err)
		testutil.Equals(t, 4, len(out), "300 series at 100 per shard need 3, rounded up to 4")
		for i, o := range out {
			testutil.Equals(t, &SeriesPartition{Index: uint64(i), Count: 4}, o.Series)
			testutil.Equals(t, shard(fmt.Sprintf("%d_of_4", i+1)), o.Labels)
		}
		testutil.Equals(t, 1.0, promtestutil.ToFloat64(metrics.Splits))
	})
	t.Run("never fewer shards than the stream has", func(t *testing.T) {
		existing := rangeMeta(1, shard("1_of_8"), 500, 600)
		out, err := planner(conf, nil, existing).PlanOutputs(ctx, group(base), sources(base, 150))
		testutil.Ok(t, err)
		testutil.Equals(t, 8, len(out))
	})
	t.Run("a shard group never goes back below its lineage's finest count", func(t *testing.T) {
		// 1 of 4 fits in one block, but 5 of 8 - a refinement of it - exists
		// elsewhere in the stream: the plan is split into 1 and 5 of 8, or
		// the two counts could never compact into one range again.
		finer := rangeMeta(1, shard("5_of_8"), 500, 600)
		out, err := planner(conf, nil, finer).PlanOutputs(ctx, group(shard("1_of_4")), sources(shard("1_of_4"), 10))
		testutil.Ok(t, err)
		testutil.Equals(t, 2, len(out))
		testutil.Equals(t, shard("1_of_8"), out[0].Labels)
		testutil.Equals(t, shard("5_of_8"), out[1].Labels)
		// Another lineage's refinements do not count.
		other := rangeMeta(2, shard("2_of_8"), 500, 600)
		out, err = planner(conf, nil, other).PlanOutputs(ctx, group(shard("1_of_4")), sources(shard("1_of_4"), 10))
		testutil.Ok(t, err)
		testutil.Equals(t, 0, len(out))
	})
	t.Run("a shard group re-splits into its lineage only", func(t *testing.T) {
		// 300 series of shard 2 of 2 need three parts, four as a power of
		// two: the stream goes to 8 shards and this shard becomes the four
		// congruent to it.
		out, err := planner(conf, nil).PlanOutputs(ctx, group(shard("2_of_2")), sources(shard("2_of_2"), 150))
		testutil.Ok(t, err)
		testutil.Equals(t, 4, len(out), "2 of 2 splits into 2, 4, 6 and 8 of 8")
		for i, o := range out {
			testutil.Equals(t, shard(fmt.Sprintf("%d_of_8", 2+2*i)), o.Labels)
			testutil.Equals(t, &SeriesPartition{Index: uint64(1 + 2*i), Count: 8}, o.Series)
		}
	})
	t.Run("the labels left out of the hash travel with every partition", func(t *testing.T) {
		without := SplitConfig{MaxShards: 8, MaxSeries: 100, HashWithout: []string{"replica", "", "prometheus_replica", "replica"}}
		out, err := planner(without, nil).PlanOutputs(ctx, group(base), sources(base, 150))
		testutil.Ok(t, err)
		testutil.Equals(t, 4, len(out))
		for _, o := range out {
			testutil.Equals(t, []string{"prometheus_replica", "replica"}, o.Series.Without, "sorted, without duplicates or empty names")
		}
		// A straggler split into the covering shards carries them too.
		covering := rangeMeta(1, shard("1_of_2"), 0, 200)
		covering2 := rangeMeta(2, shard("2_of_2"), 0, 200)
		out, err = planner(without, nil, covering, covering2).PlanOutputs(ctx, group(base), sources(base, 10)[:1])
		testutil.Ok(t, err)
		testutil.Equals(t, 2, len(out))
		for _, o := range out {
			testutil.Equals(t, []string{"prometheus_replica", "replica"}, o.Series.Without)
		}
	})
	t.Run("a shard group within the limit keeps its labels", func(t *testing.T) {
		out, err := planner(conf, nil).PlanOutputs(ctx, group(shard("1_of_4")), sources(shard("1_of_4"), 40))
		testutil.Ok(t, err)
		testutil.Equals(t, 0, len(out), "80 series of one shard fit in one block")
	})
	t.Run("a shard group's estimate is of its own data, not the stream's", func(t *testing.T) {
		// 300 series in shard 1 of 4 need three parts: the stream goes to
		// 16 shards, and this shard becomes shards 1, 5, 9 and 13 of 16.
		// Under the old rule, max(parts, count) = 4, it would not have
		// split at all.
		out, err := planner(SplitConfig{MaxShards: 16, MaxSeries: 100}, nil).PlanOutputs(ctx, group(shard("1_of_4")), sources(shard("1_of_4"), 150))
		testutil.Ok(t, err)
		testutil.Equals(t, 4, len(out))
		for i, o := range out {
			testutil.Equals(t, shard(fmt.Sprintf("%d_of_16", 1+4*i)), o.Labels)
			testutil.Equals(t, &SeriesPartition{Index: uint64(4 * i), Count: 16}, o.Series)
		}
	})
	t.Run("a shard group over the cap by index is an error here, refused by Plan before", func(t *testing.T) {
		big := []*metadata.Meta{metaWith(40*gib, 0, shard("1_of_8")), metaWith(40*gib, 0, shard("1_of_8"))}
		big[0].MinTime, big[0].MaxTime, big[1].MinTime, big[1].MaxTime = 0, 100, 100, 200
		_, err := planner(SplitConfig{MaxShards: 8, MaxIndexSizeBytes: 64 * gib}, nil).PlanOutputs(ctx, group(shard("1_of_8")), big)
		testutil.NotOk(t, err)
	})
	t.Run("a shard group at the cap wanting more for its series is left as it is", func(t *testing.T) {
		out, err := planner(SplitConfig{MaxShards: 8, MaxSeries: 100}, nil).PlanOutputs(ctx, group(shard("1_of_8")), sources(shard("1_of_8"), 150))
		testutil.Ok(t, err)
		testutil.Equals(t, 0, len(out))
	})
	t.Run("a covered range is split into the covering shards whatever the estimate", func(t *testing.T) {
		// 2 of 2 was re-split into 2 and 4 of 4; 2 of 4 turned out empty and
		// is not present, so its part of the hash space gets a new shard.
		covering := []*metadata.Meta{rangeMeta(1, shard("1_of_2"), 0, 400), rangeMeta(2, shard("4_of_4"), 0, 400)}
		out, err := planner(conf, nil, covering...).PlanOutputs(ctx, group(base), sources(base, 1))
		testutil.Ok(t, err)
		testutil.Equals(t, 3, len(out))
		testutil.Equals(t, shard("1_of_2"), out[0].Labels)
		testutil.Equals(t, &SeriesPartition{Index: 0, Count: 2}, out[0].Series)
		testutil.Equals(t, shard("2_of_4"), out[1].Labels)
		testutil.Equals(t, &SeriesPartition{Index: 1, Count: 4}, out[1].Series)
		testutil.Equals(t, shard("4_of_4"), out[2].Labels)
		testutil.Equals(t, &SeriesPartition{Index: 3, Count: 4}, out[2].Series)
	})
	t.Run("mixed shards are refused", func(t *testing.T) {
		mixed := []*metadata.Meta{metaWith(0, 1, shard("1_of_2")), metaWith(0, 1, shard("2_of_2"))}
		_, err := planner(conf, nil).PlanOutputs(ctx, group(base), mixed)
		testutil.NotOk(t, err)
	})
}

// candidatePlanner plans the blocks of its plan that are still candidates,
// and nothing when fewer than two are: what the compactor's planner does with
// a block excluded by a no-compact mark.
type candidatePlanner struct{ plan []*metadata.Meta }

func (p candidatePlanner) Plan(_ context.Context, candidates []*metadata.Meta, _ chan error, _ any) ([]*metadata.Meta, error) {
	var out []*metadata.Meta
	for _, m := range p.plan {
		if slices.ContainsFunc(candidates, func(c *metadata.Meta) bool { return c.ULID == m.ULID }) {
			out = append(out, m)
		}
	}
	if len(out) < 2 {
		return nil, nil
	}
	return out, nil
}

// TestSplitPlannerRefusesWhatTheCapCannotHold: a plan whose lineage would need
// more shards than allowed is refused like the index size filter refuses one
// - its biggest block marked no-compact, the fallback counted - and planning
// goes on without that block. The index size filter cannot judge this by
// itself: a shard group's plan is small in bytes, but its shard count leaves
// no room under the cap.
func TestSplitPlannerRefusesWhatTheCapCannotHold(t *testing.T) {
	ctx := t.Context()
	shard := map[string]string{"tenant": "a", metadata.CompactorShardLabel: "1_of_8"}
	big := metaWith(150*gib, 0, shard)
	big.ULID = ulid.MustNew(1, nil)
	bigger := metaWith(160*gib, 0, shard)
	bigger.ULID = ulid.MustNew(2, nil)
	small := metaWith(20*gib, 0, shard)
	small.ULID = ulid.MustNew(3, nil)
	for i, m := range []*metadata.Meta{big, bigger, small} {
		m.MinTime, m.MaxTime = int64(i*100), int64(i*100+100)
	}
	group := []*metadata.Meta{big, bigger, small}

	bkt := objstore.NewInMemBucket()
	marked := promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "marked"})
	metrics := NewSplitMetrics(nil)
	conf := SplitConfig{MaxShards: 8, MaxIndexSizeBytes: 64 * gib}
	p := WithBlockSplitting(candidatePlanner{plan: group}, log.NewNopLogger(), conf, metrics, bkt, marked, func() map[ulid.ULID]*metadata.Meta { return nil }, nil)

	// 330 GiB of index in shard 1 of 8 need seven parts, eight as a power of
	// two: 64 shards, over the cap. The biggest block is refused; the
	// remaining 170 GiB need four parts: 32 shards, still over the cap, so
	// the next biggest goes too. The one left is not a plan.
	plan, err := p.Plan(ctx, group, nil, nil)
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(plan))
	testutil.Equals(t, 2.0, promtestutil.ToFloat64(metrics.Fallbacks))
	testutil.Equals(t, 2.0, promtestutil.ToFloat64(marked))
	for _, m := range []*metadata.Meta{bigger, big} {
		ok, err := bkt.Exists(ctx, m.ULID.String()+"/"+metadata.NoCompactMarkFilename)
		testutil.Ok(t, err)
		testutil.Equals(t, true, ok, "block %s must be marked", m.ULID)
	}
	ok, err := bkt.Exists(ctx, small.ULID.String()+"/"+metadata.NoCompactMarkFilename)
	testutil.Ok(t, err)
	testutil.Equals(t, false, ok)

	// With room under the cap the plan goes through untouched.
	plan, err = WithBlockSplitting(candidatePlanner{plan: group}, log.NewNopLogger(), SplitConfig{MaxShards: 64, MaxIndexSizeBytes: 64 * gib}, nil, bkt, marked, func() map[ulid.ULID]*metadata.Meta { return nil }, nil).Plan(ctx, group, nil, nil)
	testutil.Ok(t, err)
	testutil.Equals(t, 3, len(plan))
}

// TestSplitPlannerLooksUpMissingIndexSizes: a source whose metadata does not
// record its index size - blocks from before Thanos recorded file sizes - is
// measured in the bucket, as the index size filter measures it, instead of
// counting as empty and leaving the plan under-split.
func TestSplitPlannerLooksUpMissingIndexSizes(t *testing.T) {
	ctx := t.Context()
	bkt := objstore.NewInMemBucket()
	base := map[string]string{"tenant": "a"}
	a, b := metaWith(0, 0, base), metaWith(0, 0, base)
	a.MinTime, a.MaxTime, b.MinTime, b.MaxTime = 0, 100, 100, 200
	// 40 bytes each in the bucket, nothing in the metadata: 80 bytes need two
	// shards under a 64 byte limit with headroom.
	for _, m := range []*metadata.Meta{a, b} {
		testutil.Ok(t, bkt.Upload(ctx, m.ULID.String()+"/index", bytes.NewReader(make([]byte, 40))))
	}
	g, err := NewGroup(log.NewNopLogger(), nil, "k", labels.FromMap(base), 0, false, false,
		nil, nil, nil, nil, nil, nil, nil, nil, metadata.NoneFunc, 1, 1)
	testutil.Ok(t, err)
	planner := WithBlockSplitting(fixedPlanner{}, log.NewNopLogger(), SplitConfig{MaxShards: 8, MaxIndexSizeBytes: 64}, nil, bkt,
		promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "test"}), func() map[ulid.ULID]*metadata.Meta { return nil }, nil).(OutputPlanner)
	out, err := planner.PlanOutputs(ctx, g, []*metadata.Meta{a, b})
	testutil.Ok(t, err)
	testutil.Equals(t, 2, len(out), "80 bytes of index measured in the bucket need two shards")
	testutil.Equals(t, int64(40), a.Thanos.Files[len(a.Thanos.Files)-1].SizeBytes, "the size is kept in the metadata")

	// A block whose index is not in the bucket at all counts as unknown,
	// and the compaction itself reports what is wrong with it.
	c := metaWith(0, 0, base)
	c.MinTime, c.MaxTime = 200, 300
	out, err = planner.PlanOutputs(ctx, g, []*metadata.Meta{c})
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(out))
}

// TestSplitPlannerPlanSiblings: a plan over shard blocks names the blocks of
// the view that share a set with its sources - the other shards of the split
// that made them, and blocks that named them as siblings since - and nothing
// else.
func TestSplitPlannerPlanSiblings(t *testing.T) {
	ctx := t.Context()
	shard := func(v string) map[string]string {
		return map[string]string{"tenant": "a", metadata.CompactorShardLabel: v}
	}
	set := func(m *metadata.Meta, index, count int, blocks ...*metadata.Meta) {
		m.Thanos.Output = &metadata.ThanosOutput{Index: index, Count: count}
		for _, b := range blocks {
			m.Thanos.Output.Blocks = append(m.Thanos.Output.Blocks, b.ULID)
		}
	}
	a, b := rangeMeta(1, shard("1_of_2"), 0, 100), rangeMeta(2, shard("2_of_2"), 0, 100)
	set(a, 0, 2, a, b)
	set(b, 1, 2, a, b)
	// b was re-split into c and d, which named a; b is gone.
	c, d := rangeMeta(3, shard("2_of_4"), 0, 100), rangeMeta(4, shard("4_of_4"), 0, 100)
	set(c, 0, 2, c, d, a)
	set(d, 1, 2, c, d, a)
	other := rangeMeta(5, shard("1_of_2"), 100, 200)
	unsplit := rangeMeta(6, map[string]string{"tenant": "a"}, 200, 300)
	all := map[ulid.ULID]*metadata.Meta{}
	for _, m := range []*metadata.Meta{a, c, d, other, unsplit} {
		all[m.ULID] = m
	}
	planner := WithBlockSplitting(fixedPlanner{}, log.NewNopLogger(), SplitConfig{MaxShards: 8, MaxSeries: 100}, nil, objstore.NewInMemBucket(),
		promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "test"}), func() map[ulid.ULID]*metadata.Meta { return all }, nil).(SiblingPlanner)

	got, err := planner.PlanSiblings(ctx, nil, []*metadata.Meta{a, other})
	testutil.Ok(t, err)
	testutil.Equals(t, []ulid.ULID{c.ULID, d.ULID}, got, "c and d named a; b is gone and other shares no set")

	got, err = planner.PlanSiblings(ctx, nil, []*metadata.Meta{unsplit})
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(got))
}

// TestSplitPlannerStragglersRespectTheCap: a block planned alone because its
// stream moved on is held to the same limits as any plan. It is refused and
// marked when its split would need more shards than allowed, instead of
// failing planning, and it is not split towards a count the cap no longer
// allows, which would only rewrite it as itself, pass after pass.
func TestSplitPlannerStragglersRespectTheCap(t *testing.T) {
	ctx := t.Context()
	shard := func(v string) map[string]string {
		return map[string]string{"tenant": "a", metadata.CompactorShardLabel: v}
	}
	view := func(ms ...*metadata.Meta) func() map[ulid.ULID]*metadata.Meta {
		all := map[ulid.ULID]*metadata.Meta{}
		for _, m := range ms {
			all[m.ULID] = m
		}
		return func() map[ulid.ULID]*metadata.Meta { return all }
	}
	sized := func(id int, lbls map[string]string, mint, maxt, indexBytes int64) *metadata.Meta {
		m := rangeMeta(id, lbls, mint, maxt)
		m.Thanos.Files = []metadata.File{{RelPath: block.IndexFilename, SizeBytes: indexBytes}}
		return m
	}
	marked := func(bkt objstore.Bucket, m *metadata.Meta) bool {
		ok, err := bkt.Exists(ctx, m.ULID.String()+"/"+metadata.NoCompactMarkFilename)
		testutil.Ok(t, err)
		return ok
	}
	conf := SplitConfig{MaxShards: 8, MaxIndexSizeBytes: 64 * gib}
	// The stream has gone to 4 shards elsewhere, so 1 of 2 is behind.
	finer := rangeMeta(50, shard("1_of_4"), 500, 600)

	t.Run("a straggler whose split does not fit is refused, not planned", func(t *testing.T) {
		// 400 GiB of shard 1 of 2 need eight parts: 16 shards, over the cap.
		lone := sized(1, shard("1_of_2"), 0, 100, 400*gib)
		bkt := objstore.NewInMemBucket()
		metrics := NewSplitMetrics(nil)
		p := WithBlockSplitting(fixedPlanner{}, log.NewNopLogger(), conf, metrics, bkt, noopCounter(), view(finer), nil)
		plan, err := p.Plan(ctx, []*metadata.Meta{lone}, nil, nil)
		testutil.Ok(t, err)
		testutil.Equals(t, 0, len(plan))
		testutil.Equals(t, true, marked(bkt, lone))
		testutil.Equals(t, 1.0, promtestutil.ToFloat64(metrics.Fallbacks))
	})
	t.Run("a block refused in the same pass is not planned alone", func(t *testing.T) {
		big := sized(1, shard("1_of_2"), 0, 100, 400*gib)
		small := sized(2, shard("1_of_2"), 100, 200, 10*gib)
		bkt := objstore.NewInMemBucket()
		metrics := NewSplitMetrics(nil)
		p := WithBlockSplitting(candidatePlanner{plan: []*metadata.Meta{big, small}}, log.NewNopLogger(), conf, metrics, bkt, noopCounter(), view(finer), nil)
		plan, err := p.Plan(ctx, []*metadata.Meta{big, small}, nil, nil)
		testutil.Ok(t, err)
		testutil.Equals(t, []*metadata.Meta{small}, plan, "the plan's biggest block is refused; the other one is still behind")
		testutil.Equals(t, true, marked(bkt, big))
		testutil.Equals(t, 1.0, promtestutil.ToFloat64(metrics.Fallbacks), "refused once")
	})
	t.Run("a shard at a lowered cap is not split towards the stream's count", func(t *testing.T) {
		lone := rangeMeta(1, shard("1_of_4"), 0, 100)
		p := WithBlockSplitting(fixedPlanner{}, log.NewNopLogger(), SplitConfig{MaxShards: 4}, nil, objstore.NewInMemBucket(), noopCounter(), view(rangeMeta(51, shard("5_of_8"), 100, 200)), nil)
		plan, err := p.Plan(ctx, []*metadata.Meta{lone}, nil, nil)
		testutil.Ok(t, err)
		testutil.Equals(t, 0, len(plan))
	})
	t.Run("shards beyond a lowered cap compact within their shard", func(t *testing.T) {
		a, b := rangeMeta(1, shard("1_of_8"), 0, 100), rangeMeta(2, shard("1_of_8"), 100, 200)
		p := WithBlockSplitting(fixedPlanner{}, log.NewNopLogger(), SplitConfig{MaxShards: 4, MaxSeries: 1}, nil, objstore.NewInMemBucket(), noopCounter(), view(a, b), nil).(*splitPlanner)
		pieces, count, fits, err := p.decide(ctx, shard("1_of_8"), 0, []*metadata.Meta{a, b})
		testutil.Ok(t, err)
		testutil.Equals(t, true, fits)
		testutil.Equals(t, uint64(8), count)
		testutil.Equals(t, 0, len(pieces), "a coarser label would claim series shard 1 of 8 does not hold")
	})
}

// TestSplitPlannerSiblingsReachThePlan: the siblings the split planner names
// reach the plan the group hands its executor, so that a shard compacted on
// keeps the set of the shards it was split with complete.
func TestSplitPlannerSiblingsReachThePlan(t *testing.T) {
	shardLabels := map[string]string{"tenant": "a", metadata.CompactorShardLabel: "1_of_2"}
	set := func(m *metadata.Meta, index, count int, blocks ...*metadata.Meta) {
		m.Thanos.Output = &metadata.ThanosOutput{Index: index, Count: count}
		for _, b := range blocks {
			m.Thanos.Output.Blocks = append(m.Thanos.Output.Blocks, b.ULID)
		}
	}
	a := rangeMeta(1, shardLabels, 0, 100)
	b := rangeMeta(2, map[string]string{"tenant": "a", metadata.CompactorShardLabel: "2_of_2"}, 0, 100)
	set(a, 0, 2, a, b)
	set(b, 1, 2, a, b)
	next := rangeMeta(3, shardLabels, 100, 200)
	all := map[ulid.ULID]*metadata.Meta{a.ULID: a, b.ULID: b, next.ULID: next}

	cnt := func() prometheus.Counter { return promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "test"}) }
	cg, err := NewGroup(log.NewNopLogger(), objstore.NewInMemBucket(), "0@shard", labels.FromMap(shardLabels), 0, false, false,
		cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), metadata.NoneFunc, 1, 1)
	testutil.Ok(t, err)
	testutil.Ok(t, cg.AppendMeta(a))
	testutil.Ok(t, cg.AppendMeta(next))

	p := WithBlockSplitting(candidatePlanner{plan: []*metadata.Meta{a, next}}, log.NewNopLogger(), SplitConfig{MaxShards: 8, MaxSeries: 100}, nil, objstore.NewInMemBucket(), noopCounter(), func() map[ulid.ULID]*metadata.Meta { return all }, nil)
	plan, err := cg.Plan(t.Context(), p, make(chan error, 1))
	testutil.Ok(t, err)
	testutil.Equals(t, []*metadata.Meta{a, next}, plan.Sources)
	testutil.Equals(t, []ulid.ULID{b.ULID}, plan.Siblings, "b's set with a stays complete through the plan's outputs")
}
