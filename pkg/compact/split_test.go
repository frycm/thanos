// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/thanos-io/thanos/pkg/block/metadata"
)

const (
	splitTestHour = int64(time.Hour / time.Millisecond)
	splitTestDay  = 24 * splitTestHour
)

var splitTestRanges = []int64{2 * splitTestHour, 8 * splitTestHour, 2 * splitTestDay, 14 * splitTestDay}

// staticNoCompactMarks is a fixed set of blocks marked for no compaction.
type staticNoCompactMarks map[ulid.ULID]*metadata.NoCompactMark

func (m staticNoCompactMarks) NoCompactMarkedBlocks() map[ulid.ULID]*metadata.NoCompactMark {
	return maps.Clone(map[ulid.ULID]*metadata.NoCompactMark(m))
}

func newTestSplitGrouper(t *testing.T, cfg SplitConfig, concurrentJobs bool, marks staticNoCompactMarks) *SplitGrouper {
	t.Helper()
	counter := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
	base := NewDefaultGrouper(log.NewNopLogger(), nil, false, true, prometheus.NewRegistry(), counter, counter, counter, metadata.NoneFunc, 1, 1)
	g, err := NewSplitGrouper(log.NewNopLogger(), base, concurrentJobs, splitTestRanges, cfg, marks, prometheus.NewRegistry())
	testutil.Ok(t, err)
	return g
}

// unsplitMeta returns a raw unsplit block of stream {tenant="big"} that is its own source.
func unsplitMeta(id uint64, mint, maxt int64) *metadata.Meta {
	m := createBlockMeta(id, mint, maxt, map[string]string{"tenant": "big"}, 0, []uint64{id})
	m.Compaction.Level = 1
	return m
}

// shardMeta returns a block of the given shard of stream {tenant="big"} recording the scheme, with the given sources.
func shardMetaOf(id uint64, mint, maxt int64, res int64, index int, scheme metadata.SplitScheme, sources ...uint64) *metadata.Meta {
	m := createBlockMeta(id, mint, maxt, map[string]string{"tenant": "big", metadata.CompactorShardIDLabel: metadata.FormatShardID(index, scheme.Shards)}, res, sources)
	m.Compaction.Level = 2
	b, err := json.Marshal(map[string]any{metadata.CompactorSplitExtensionKey: scheme})
	if err != nil {
		panic(err)
	}
	var ext map[string]any
	if err := json.Unmarshal(b, &ext); err != nil {
		panic(err)
	}
	m.Thanos.Extensions = ext
	return m
}

func stableScheme(shards int, ignore ...string) metadata.SplitScheme {
	return metadata.SplitScheme{Hash: metadata.SplitHashStable, Shards: shards, IgnoreLabels: ignore}
}

func metaMap(metas ...*metadata.Meta) map[ulid.ULID]*metadata.Meta {
	res := map[ulid.ULID]*metadata.Meta{}
	for _, m := range metas {
		res[m.ULID] = m
	}
	return res
}

// splitJobsOf summarizes the split jobs among the groups as "<shard> [mint,maxt) ids", sorted.
func splitJobsOf(groups []*Group) []string {
	var res []string
	for _, g := range groups {
		job, ok := splitJobOf(g)
		if !ok {
			continue
		}
		var ids []string
		for _, m := range g.metasByMinTime {
			ids = append(ids, fmt.Sprint(m.ULID.Time()))
		}
		res = append(res, fmt.Sprintf("%s [%d,%d) %s ignore=%v", metadata.FormatShardID(job.index, job.scheme.Shards), g.MinTime()/splitTestHour, g.MaxTime()/splitTestHour, strings.Join(ids, ","), job.scheme.IgnoreLabels))
	}
	sort.Strings(res)
	return res
}

func excludedIDs(g *SplitGrouper) []uint64 {
	g.mtx.RLock()
	defer g.mtx.RUnlock()
	var res []uint64
	for id := range g.excluded {
		if id.Time() != 0 {
			res = append(res, id.Time())
		}
	}
	slices.Sort(res)
	return res
}

func TestSplitGrouper_SplitsEligibleWindows(t *testing.T) {
	t.Parallel()

	for _, concurrentJobs := range []bool{false, true} {
		t.Run(fmt.Sprintf("concurrentJobs=%v", concurrentJobs), func(t *testing.T) {
			t.Parallel()

			// Only the big tenant is split.
			cfg := SplitConfig{Shards: 1, Overrides: []SplitOverride{{Matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "tenant", "big")}, Shards: 4}}}
			g := newTestSplitGrouper(t, cfg, concurrentJobs, nil)
			blocks := metaMap(
				// Two replicas of [0h, 2h) and of [2h, 4h), one block of [4h, 6h), the newest window.
				unsplitMeta(1, 0, 2*splitTestHour),
				unsplitMeta(2, 0, 2*splitTestHour),
				unsplitMeta(3, 2*splitTestHour, 4*splitTestHour),
				unsplitMeta(4, 2*splitTestHour, 4*splitTestHour),
				unsplitMeta(5, 4*splitTestHour, 6*splitTestHour),
				// Another stream, not split.
				createBlockMeta(6, 0, 2*splitTestHour, map[string]string{"tenant": "small"}, 0, []uint64{6}),
				createBlockMeta(7, 2*splitTestHour, 4*splitTestHour, map[string]string{"tenant": "small"}, 0, []uint64{7}),
			)
			groups, err := g.Groups(blocks)
			testutil.Ok(t, err)

			testutil.Equals(t, []string{
				"1_of_4 [0,2) 1,2 ignore=[]", "1_of_4 [2,4) 3,4 ignore=[]",
				"2_of_4 [0,2) 1,2 ignore=[]", "2_of_4 [2,4) 3,4 ignore=[]",
				"3_of_4 [0,2) 1,2 ignore=[]", "3_of_4 [2,4) 3,4 ignore=[]",
				"4_of_4 [0,2) 1,2 ignore=[]", "4_of_4 [2,4) 3,4 ignore=[]",
			}, splitJobsOf(groups))
			testutil.Equals(t, []uint64{1, 2, 3, 4}, excludedIDs(g))

			keys := map[string]struct{}{}
			for _, gr := range groups {
				_, dup := keys[gr.Key()]
				testutil.Assert(t, !dup, "duplicate group key %s", gr.Key())
				keys[gr.Key()] = struct{}{}

				job, ok := splitJobOf(gr)
				if !ok {
					// Normal planning never holds an unsplit block of a window being split.
					plan := planGroup(t, g, gr, concurrentJobs)
					for _, m := range plan {
						testutil.Assert(t, m.ULID.Time() > 4, "block %d of a window being split was planned: %v", m.ULID.Time(), plan)
					}
					continue
				}
				testutil.Equals(t, labels.FromStrings("tenant", "big"), gr.Labels())
				testutil.Equals(t, labels.FromStrings("tenant", "big", metadata.CompactorShardIDLabel, metadata.FormatShardID(job.index, 4)), gr.OutputLabels())
				testutil.Assert(t, gr.planSingleBlock)
				testutil.Assert(t, strings.Contains(gr.Key(), "_split_"+metadata.FormatShardID(job.index, 4)+"_"))

				// The split planner plans all of the job's blocks; the outputs record the scheme.
				plan, err := g.Planner(failingPlanner{}).Plan(context.Background(), gr.metasByMinTime, nil, gr.Extensions())
				testutil.Ok(t, err)
				testutil.Equals(t, gr.metasByMinTime, plan)
				b, err := json.Marshal(gr.Extensions())
				testutil.Ok(t, err)
				testutil.Equals(t, `{"compactor_split":{"hash":"stable_hash","shards":4}}`, string(b))
			}

			// Without splitting, the same planner would vertically compact the replicas of [0h, 2h).
			plain := newTestSplitGrouper(t, SplitConfig{Shards: 1}, concurrentJobs, nil)
			groups, err = plain.Groups(blocks)
			testutil.Ok(t, err)
			testutil.Equals(t, 0, len(splitJobsOf(groups)))
			var planned []uint64
			for _, gr := range groups {
				for _, m := range planGroup(t, plain, gr, concurrentJobs) {
					planned = append(planned, m.ULID.Time())
				}
			}
			slices.Sort(planned)
			testutil.Assert(t, slices.Contains(planned, 1) && slices.Contains(planned, 2), "replicas planned without splitting: %v", planned)
		})
	}
}

// failingPlanner fails every plan: split jobs must never reach the wrapped planner.
type failingPlanner struct{}

func (failingPlanner) Plan(context.Context, []*metadata.Meta, chan error, any) ([]*metadata.Meta, error) {
	return nil, fmt.Errorf("unexpected call to the wrapped planner")
}

// planGroup plans a group with the planner the compactor uses with the split grouper.
func planGroup(t *testing.T, g *SplitGrouper, gr *Group, concurrentJobs bool) []*metadata.Meta {
	t.Helper()
	var p Planner = NewPlanner(log.NewNopLogger(), splitTestRanges, g)
	if concurrentJobs {
		p = NewConcurrentJobsPlanner(log.NewNopLogger(), g)
	}
	plan, err := g.Planner(p).Plan(context.Background(), gr.metasByMinTime, nil, gr.Extensions())
	testutil.Ok(t, err)
	return plan
}

func TestSplitGrouper_SingleBlockWindow(t *testing.T) {
	t.Parallel()

	g := newTestSplitGrouper(t, SplitConfig{Shards: 2}, false, nil)
	groups, err := g.Groups(metaMap(unsplitMeta(1, 0, 2*splitTestHour), unsplitMeta(2, 2*splitTestHour, 4*splitTestHour)))
	testutil.Ok(t, err)
	testutil.Equals(t, []string{"1_of_2 [0,2) 1 ignore=[]", "2_of_2 [0,2) 1 ignore=[]"}, splitJobsOf(groups))
	for _, gr := range groups {
		if _, ok := splitJobOf(gr); ok {
			testutil.Equals(t, 1, len(gr.IDs()))
			testutil.Assert(t, gr.planSingleBlock, "a single block window must be planned")
		}
	}
}

func TestSplitGrouper_SkipsExistingShards(t *testing.T) {
	t.Parallel()

	scheme := stableScheme(4)
	g := newTestSplitGrouper(t, SplitConfig{Shards: 4}, false, nil)
	groups, err := g.Groups(metaMap(
		unsplitMeta(1, 0, 2*splitTestHour),
		unsplitMeta(2, 0, 2*splitTestHour),
		// Shards 1 and 3 hold both replicas; shard 2 only one of them (the other one was uploaded late); shard 4 only
		// exists downsampled, which does not count.
		shardMetaOf(11, 0, 2*splitTestHour, 0, 1, scheme, 1, 2),
		shardMetaOf(12, 0, 2*splitTestHour, 0, 2, scheme, 1),
		// Shard 3 was already compacted with the next window.
		shardMetaOf(13, 0, 8*splitTestHour, 0, 3, scheme, 1, 2, 3, 4),
		shardMetaOf(14, 0, 2*splitTestHour, 5*60*1000, 4, scheme, 1, 2),
		unsplitMeta(5, 8*splitTestHour, 10*splitTestHour),
	))
	testutil.Ok(t, err)
	testutil.Equals(t, []string{"2_of_4 [0,2) 1,2 ignore=[]", "4_of_4 [0,2) 1,2 ignore=[]"}, splitJobsOf(groups))
	testutil.Equals(t, []uint64{1, 2}, excludedIDs(g))

	// Once every shard exists, the window has no job left, and its blocks stay out of normal planning until the
	// deduplication filter retires them.
	groups, err = g.Groups(metaMap(
		unsplitMeta(1, 0, 2*splitTestHour),
		unsplitMeta(2, 0, 2*splitTestHour),
		shardMetaOf(11, 0, 2*splitTestHour, 0, 1, scheme, 1, 2),
		shardMetaOf(12, 0, 2*splitTestHour, 0, 2, scheme, 1, 2),
		shardMetaOf(13, 0, 2*splitTestHour, 0, 3, scheme, 1, 2),
		shardMetaOf(14, 0, 2*splitTestHour, 0, 4, scheme, 1, 2),
		unsplitMeta(5, 8*splitTestHour, 10*splitTestHour),
	))
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(splitJobsOf(groups)))
	testutil.Equals(t, []uint64{1, 2}, excludedIDs(g))
}

func TestSplitGrouper_NewestWindowAndLateBlocks(t *testing.T) {
	t.Parallel()

	scheme := stableScheme(2)
	g := newTestSplitGrouper(t, SplitConfig{Shards: 2}, false, nil)
	groups, err := g.Groups(metaMap(
		// [0h, 2h) was split and its sources retired; a replica arrived late: it is split alone.
		shardMetaOf(11, 0, 2*splitTestHour, 0, 1, scheme, 1),
		shardMetaOf(12, 0, 2*splitTestHour, 0, 2, scheme, 1),
		unsplitMeta(2, 0, 2*splitTestHour),
		// The newest window is not split, even with all replicas.
		unsplitMeta(3, 2*splitTestHour, 4*splitTestHour),
		unsplitMeta(4, 2*splitTestHour, 4*splitTestHour),
	))
	testutil.Ok(t, err)
	testutil.Equals(t, []string{"1_of_2 [0,2) 2 ignore=[]", "2_of_2 [0,2) 2 ignore=[]"}, splitJobsOf(groups))
	testutil.Equals(t, []uint64{2}, excludedIDs(g))

	// The newest window is the one of the newest raw block of the stream, shards included.
	groups, err = g.Groups(metaMap(
		shardMetaOf(11, 2*splitTestHour, 4*splitTestHour, 0, 1, scheme, 9),
		shardMetaOf(12, 2*splitTestHour, 4*splitTestHour, 0, 2, scheme, 9),
		unsplitMeta(2, 0, 2*splitTestHour),
	))
	testutil.Ok(t, err)
	testutil.Equals(t, []string{"1_of_2 [0,2) 2 ignore=[]", "2_of_2 [0,2) 2 ignore=[]"}, splitJobsOf(groups))
}

func TestSplitGrouper_LegacyBlocksAreNotSplit(t *testing.T) {
	t.Parallel()

	g := newTestSplitGrouper(t, SplitConfig{Shards: 2}, false, nil)
	groups, err := g.Groups(metaMap(
		// History compacted before splitting was enabled, and a late block overlapping it.
		unsplitMeta(1, 0, 8*splitTestHour),
		unsplitMeta(2, 2*splitTestHour, 4*splitTestHour),
		// A misaligned block.
		unsplitMeta(3, 9*splitTestHour, 11*splitTestHour),
		// Fresh windows.
		unsplitMeta(4, 12*splitTestHour, 14*splitTestHour),
		unsplitMeta(5, 14*splitTestHour, 16*splitTestHour),
	))
	testutil.Ok(t, err)
	testutil.Equals(t, []string{"1_of_2 [12,14) 4 ignore=[]", "2_of_2 [12,14) 4 ignore=[]"}, splitJobsOf(groups))
	testutil.Equals(t, []uint64{4}, excludedIDs(g))
}

func TestSplitGrouper_NoCompactMarkedBlocks(t *testing.T) {
	t.Parallel()

	marked := unsplitMeta(1, 0, 2*splitTestHour)
	marks := staticNoCompactMarks{marked.ULID: {ID: marked.ULID}}
	g := newTestSplitGrouper(t, SplitConfig{Shards: 2}, false, marks)
	groups, err := g.Groups(metaMap(
		marked,
		unsplitMeta(2, 0, 2*splitTestHour),
		unsplitMeta(3, 2*splitTestHour, 4*splitTestHour),
		unsplitMeta(4, 4*splitTestHour, 6*splitTestHour),
	))
	testutil.Ok(t, err)
	testutil.Equals(t, []string{"1_of_2 [2,4) 3 ignore=[]", "2_of_2 [2,4) 3 ignore=[]"}, splitJobsOf(groups))
	testutil.Equals(t, []uint64{3}, excludedIDs(g))

	// The planners see the bucket's marks and the blocks of windows being split.
	all := g.NoCompactMarkedBlocks()
	testutil.Equals(t, 2, len(all))
	_, ok := all[marked.ULID]
	testutil.Assert(t, ok)
	_, ok = all[ulid.MustNew(3, nil)]
	testutil.Assert(t, ok)
}

func TestSplitGrouper_ShardCountPerLargestRange(t *testing.T) {
	t.Parallel()

	old := stableScheme(2, "replica")
	g := newTestSplitGrouper(t, SplitConfig{Shards: 4}, false, nil)
	day14 := 14 * splitTestDay
	groups, err := g.Groups(metaMap(
		// The first 14d range was split in two shards, ignoring the replica label.
		shardMetaOf(11, 0, 2*splitTestHour, 0, 1, old, 100),
		shardMetaOf(12, 0, 2*splitTestHour, 0, 2, old, 100),
		// Its downsampled shards agree.
		shardMetaOf(13, 0, 2*splitTestDay, 5*60*1000, 1, old, 100),
		// A window of the first range left to split, e.g. a late block.
		unsplitMeta(1, 10*splitTestDay, 10*splitTestDay+2*splitTestHour),
		// The second range has no shard blocks yet: the configured count and labels apply.
		unsplitMeta(2, day14, day14+2*splitTestHour),
		unsplitMeta(3, day14+2*splitTestHour, day14+4*splitTestHour),
	))
	testutil.Ok(t, err)
	testutil.Equals(t, []string{
		"1_of_2 [240,242) 1 ignore=[replica]",
		"1_of_4 [336,338) 2 ignore=[]",
		"2_of_2 [240,242) 1 ignore=[replica]",
		"2_of_4 [336,338) 2 ignore=[]",
		"3_of_4 [336,338) 2 ignore=[]",
		"4_of_4 [336,338) 2 ignore=[]",
	}, splitJobsOf(groups))

	// Configured not to split: ranges with shard blocks are still finished with their scheme.
	g = newTestSplitGrouper(t, SplitConfig{Shards: 1}, false, nil)
	groups, err = g.Groups(metaMap(
		shardMetaOf(11, 0, 2*splitTestHour, 0, 1, old, 100),
		shardMetaOf(12, 0, 2*splitTestHour, 0, 2, old, 100),
		unsplitMeta(1, 2*splitTestHour, 4*splitTestHour),
		unsplitMeta(2, day14, day14+2*splitTestHour),
		unsplitMeta(3, day14+2*splitTestHour, day14+4*splitTestHour),
	))
	testutil.Ok(t, err)
	testutil.Equals(t, []string{"1_of_2 [2,4) 1 ignore=[replica]", "2_of_2 [2,4) 1 ignore=[replica]"}, splitJobsOf(groups))
	testutil.Equals(t, []uint64{1}, excludedIDs(g))
}

func TestSplitGrouper_RefusesUnknownSchemes(t *testing.T) {
	t.Parallel()

	foreign := shardMetaOf(12, 0, 2*splitTestHour, 0, 2, stableScheme(2), 100)
	foreign.Thanos.Extensions = nil
	for _, tc := range []struct {
		name   string
		shards []*metadata.Meta
		err    string
	}{
		{
			name:   "different ignored labels",
			shards: []*metadata.Meta{shardMetaOf(11, 0, 2*splitTestHour, 0, 1, stableScheme(2, "a"), 100), shardMetaOf(12, 0, 2*splitTestHour, 0, 2, stableScheme(2, "b"), 100)},
			err:    "records split scheme",
		},
		{
			name:   "different counts",
			shards: []*metadata.Meta{shardMetaOf(11, 0, 2*splitTestHour, 0, 1, stableScheme(2), 100), shardMetaOf(12, 2*splitTestHour, 4*splitTestHour, 0, 1, stableScheme(4), 101)},
			err:    "records split scheme",
		},
		{
			name:   "no recorded scheme",
			shards: []*metadata.Meta{shardMetaOf(11, 0, 2*splitTestHour, 0, 1, stableScheme(2), 100), foreign},
			err:    "records no split scheme",
		},
		{
			name:   "unknown hash",
			shards: []*metadata.Meta{shardMetaOf(11, 0, 2*splitTestHour, 0, 1, metadata.SplitScheme{Hash: "fnv", Shards: 2}, 100)},
			err:    "only knows",
		},
		{
			name:   "count not matching the label",
			shards: []*metadata.Meta{shardMetaOf(11, 0, 2*splitTestHour, 0, 1, stableScheme(2), 100)},
			err:    "records a split scheme with 4 shards",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			shards := tc.shards
			if tc.name == "count not matching the label" {
				// The label says 1_of_2, the record says 4 shards.
				var ext map[string]any
				b, _ := json.Marshal(map[string]any{metadata.CompactorSplitExtensionKey: stableScheme(4)})
				testutil.Ok(t, json.Unmarshal(b, &ext))
				shards[0].Thanos.Extensions = ext
			}
			g := newTestSplitGrouper(t, SplitConfig{Shards: 2}, false, nil)
			blocks := metaMap(append(shards, unsplitMeta(1, 4*splitTestHour, 6*splitTestHour), unsplitMeta(2, 6*splitTestHour, 8*splitTestHour))...)
			_, err := g.Groups(blocks)
			testutil.NotOk(t, err)
			testutil.Assert(t, IsHaltError(err), "expected a halt error, got %v", err)
			testutil.Assert(t, strings.Contains(err.Error(), tc.err), "unexpected error: %v", err)
			testutil.Assert(t, strings.Contains(err.Error(), "refusing to split [14400000, 21600000)"), "unexpected error: %v", err)

			// Not configured to split the stream: the window is left to normal compaction.
			groups, err := newTestSplitGrouper(t, SplitConfig{Shards: 1}, false, nil).Groups(blocks)
			testutil.Ok(t, err)
			testutil.Equals(t, 0, len(splitJobsOf(groups)))

			// Nothing to split in that range: no error.
			delete(blocks, ulid.MustNew(1, nil))
			_, err = g.Groups(blocks)
			testutil.Ok(t, err)
		})
	}
}

func TestSplitGrouper_DisabledWithoutShardsIsPassThrough(t *testing.T) {
	t.Parallel()

	blocks := metaMap(
		unsplitMeta(1, 0, 2*splitTestHour),
		unsplitMeta(2, 0, 2*splitTestHour),
		unsplitMeta(3, 2*splitTestHour, 4*splitTestHour),
	)
	g := newTestSplitGrouper(t, SplitConfig{Shards: 1}, false, nil)
	groups, err := g.Groups(blocks)
	testutil.Ok(t, err)
	want, err := g.base.Groups(blocks)
	testutil.Ok(t, err)
	testutil.Equals(t, len(want), len(groups))
	for i := range want {
		testutil.Equals(t, want[i].Key(), groups[i].Key())
		testutil.Equals(t, want[i].IDs(), groups[i].IDs())
		testutil.Equals(t, want[i].Labels(), groups[i].OutputLabels())
	}
	testutil.Equals(t, 0, len(g.NoCompactMarkedBlocks()))
}

func TestSplitGrouper_ClosesShardGroupsOfAnotherCount(t *testing.T) {
	t.Parallel()

	day14 := 14 * splitTestDay
	scheme := stableScheme(2)
	// The last 8h of the first 14d range of shard 1 of 2: four 2h blocks, the newest one ending the range.
	lane := []*metadata.Meta{
		shardMetaOf(21, day14-8*splitTestHour, day14-6*splitTestHour, 0, 1, scheme, 1),
		shardMetaOf(22, day14-6*splitTestHour, day14-4*splitTestHour, 0, 1, scheme, 2),
		shardMetaOf(23, day14-4*splitTestHour, day14-2*splitTestHour, 0, 1, scheme, 3),
		shardMetaOf(24, day14-2*splitTestHour, day14, 0, 1, scheme, 4),
	}
	next := []*metadata.Meta{
		unsplitMeta(5, day14, day14+2*splitTestHour),
		unsplitMeta(6, day14+2*splitTestHour, day14+4*splitTestHour),
	}

	for _, tc := range []struct {
		name       string
		configured int
		closed     bool
	}{
		{name: "same count continues", configured: 2, closed: false},
		{name: "another count closes", configured: 4, closed: true},
		{name: "no more splitting closes", configured: 1, closed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			g := newTestSplitGrouper(t, SplitConfig{Shards: tc.configured}, false, nil)
			groups, err := g.Groups(metaMap(append(slices.Clone(lane), next...)...))
			testutil.Ok(t, err)

			var laneGroup *Group
			for _, gr := range groups {
				if _, ok := splitJobOf(gr); !ok && gr.Labels().Get(metadata.CompactorShardIDLabel) == "1_of_2" {
					laneGroup = gr
				}
			}
			testutil.Assert(t, laneGroup != nil)
			plan := planGroup(t, g, laneGroup, false)
			if !tc.closed {
				testutil.Equals(t, 4, len(laneGroup.IDs()))
				testutil.Equals(t, 0, len(plan), "the newest block of an open lane waits for the next one")
				return
			}
			testutil.Equals(t, 6, len(laneGroup.IDs()), "two virtual blocks close the lane")
			testutil.Equals(t, lane, plan, "the whole last range of a closed lane is compacted")
			testutil.Ok(t, laneGroup.areBlocksOverlapping(nil))
			for _, m := range laneGroup.metasByMinTime {
				if m.ULID.Time() == 0 {
					testutil.Assert(t, m.MinTime >= day14, "virtual block %v inside the lane", m)
					_, marked := g.NoCompactMarkedBlocks()[m.ULID]
					testutil.Assert(t, marked, "virtual block must never be planned")
				}
			}

			// The concurrent jobs planner compacts it too, and never plans a virtual block.
			cg := newTestSplitGrouper(t, SplitConfig{Shards: tc.configured}, true, nil)
			groups, err = cg.Groups(metaMap(append(slices.Clone(lane), next...)...))
			testutil.Ok(t, err)
			var jobs [][]uint64
			for _, gr := range groups {
				if gr.Labels().Get(metadata.CompactorShardIDLabel) != "1_of_2" {
					continue
				}
				var ids []uint64
				for _, m := range gr.metasByMinTime {
					testutil.Assert(t, m.ULID.Time() != 0, "virtual block in job %s", gr.Key())
					ids = append(ids, m.ULID.Time())
				}
				jobs = append(jobs, ids)
			}
			testutil.Equals(t, [][]uint64{{21, 22, 23, 24}}, jobs)
		})
	}
}

func TestSplitWrappers(t *testing.T) {
	t.Parallel()

	g := newTestSplitGrouper(t, SplitConfig{Shards: 2, IgnoreLabels: []string{"replica"}}, false, nil)
	scheme := stableScheme(2, "replica")
	groups, err := g.Groups(metaMap(
		unsplitMeta(1, 0, 2*splitTestHour),
		unsplitMeta(2, 2*splitTestHour, 4*splitTestHour),
		shardMetaOf(11, 8*splitTestHour, 10*splitTestHour, 0, 2, scheme, 7),
		shardMetaOf(12, 10*splitTestHour, 12*splitTestHour, 0, 2, scheme, 8),
	))
	testutil.Ok(t, err)

	var job, shardGroup, unsplitGroup *Group
	for _, gr := range groups {
		if _, ok := splitJobOf(gr); ok {
			if j, _ := splitJobOf(gr); j.index == 2 {
				job = gr
			}
			continue
		}
		if isShardGroup(gr) {
			shardGroup = gr
		} else {
			unsplitGroup = gr
		}
	}
	if job == nil || shardGroup == nil || unsplitGroup == nil {
		t.Fatalf("missing groups: split job %v, shard group %v, unsplit group %v", job, shardGroup, unsplitGroup)
	}

	ctx := context.Background()
	callback := g.CompactionLifecycleCallback(DefaultCompactionLifecycleCallback{})
	checker := g.BlockDeletableChecker(DefaultBlockDeletableChecker{})

	// Split jobs: partitioned populator, sources never deleted.
	p, err := callback.GetBlockPopulator(ctx, log.NewNopLogger(), job)
	testutil.Ok(t, err)
	testutil.Equals(t, PartitionedBlockPopulator{Shard: SeriesShard{Index: 2, Count: 2, IgnoreLabels: []string{"replica"}}}, p)
	testutil.Assert(t, !checker.CanDelete(job, ulid.MustNew(1, nil)))
	testutil.Ok(t, callback.PreCompactionCallback(ctx, log.NewNopLogger(), job, job.metasByMinTime))

	// Shard groups: default populator keeping empty outputs, sources deleted as usual.
	p, err = callback.GetBlockPopulator(ctx, log.NewNopLogger(), shardGroup)
	testutil.Ok(t, err)
	testutil.Equals(t, keepEmptyPopulator{BlockPopulator: tsdb.DefaultBlockPopulator{}}, p)
	testutil.Assert(t, checker.CanDelete(shardGroup, ulid.MustNew(11, nil)))

	// Unsplit groups: unchanged.
	p, err = callback.GetBlockPopulator(ctx, log.NewNopLogger(), unsplitGroup)
	testutil.Ok(t, err)
	testutil.Equals(t, tsdb.DefaultBlockPopulator{}, p)
	testutil.Assert(t, checker.CanDelete(unsplitGroup, ulid.MustNew(1, nil)))

	// The output of a shard compaction records the scheme of its blocks.
	shardGroup.SetExtensions(map[string]any{"other": "x"})
	testutil.Ok(t, callback.PreCompactionCallback(ctx, log.NewNopLogger(), shardGroup, shardGroup.metasByMinTime))
	b, err := json.Marshal(shardGroup.Extensions())
	testutil.Ok(t, err)
	testutil.Equals(t, `{"compactor_split":{"hash":"stable_hash","shards":2,"ignore_labels":["replica"]},"other":"x"}`, string(b))

	// Blocks of different schemes are never compacted together.
	other := shardMetaOf(13, 12*splitTestHour, 14*splitTestHour, 0, 2, stableScheme(2), 9)
	err = callback.PreCompactionCallback(ctx, log.NewNopLogger(), shardGroup, append(slices.Clone(shardGroup.metasByMinTime), other))
	testutil.NotOk(t, err)
	testutil.Assert(t, IsHaltError(err), "expected a halt error, got %v", err)
}

func TestGroupOutputLabels(t *testing.T) {
	t.Parallel()

	counter := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
	g, err := NewGroup(nil, nil, "key", labels.FromStrings("a", "1"), 0, false, false, counter, counter, counter, counter, counter, counter, counter, counter, metadata.NoneFunc, 1, 1)
	testutil.Ok(t, err)
	testutil.Equals(t, labels.FromStrings("a", "1"), g.OutputLabels())
	testutil.NotOk(t, g.SetOutputLabels(labels.FromStrings("b", "1")))
	testutil.NotOk(t, g.SetOutputLabels(labels.FromStrings("a", "2", "b", "1")))
	testutil.Ok(t, g.SetOutputLabels(labels.FromStrings("a", "1", "b", "1")))
	testutil.Equals(t, labels.FromStrings("a", "1", "b", "1"), g.OutputLabels())
	testutil.Equals(t, labels.FromStrings("a", "1"), g.Labels())
}
