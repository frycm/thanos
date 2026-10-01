// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
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
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

const simHour = int64(time.Hour / time.Millisecond)

// simRanges are the compaction ranges of the compactor with the default --debug.max-compaction-level.
var simRanges = []int64{1 * simHour, 2 * simHour, 8 * simHour, 48 * simHour, 336 * simHour}

// simStream is an abstract stream of blocks: compacting blocks replaces them with one block spanning their time range
// and holding the union of their sources.
type simStream struct {
	metas   map[ulid.ULID]*metadata.Meta
	marks   map[ulid.ULID]*metadata.NoCompactMark
	nextID  uint64
	history []string
}

var simLabels = map[string]string{"cluster": "a", "replica": "r"}

func newSimStream() *simStream {
	return &simStream{
		metas:  map[ulid.ULID]*metadata.Meta{},
		marks:  map[ulid.ULID]*metadata.NoCompactMark{},
		nextID: 1,
	}
}

func (s *simStream) add(mint, maxt int64) *metadata.Meta {
	id := ulid.MustNew(s.nextID, nil)
	s.nextID++
	m := &metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:       id,
			MinTime:    mint,
			MaxTime:    maxt,
			Compaction: tsdb.BlockMetaCompaction{Level: 1, Sources: []ulid.ULID{id}},
		},
		Thanos: metadata.Thanos{Labels: simLabels},
	}
	s.metas[id] = m
	return m
}

func (s *simStream) markNoCompact(m *metadata.Meta) {
	s.marks[m.ULID] = &metadata.NoCompactMark{ID: m.ULID, Version: metadata.NoCompactMarkVersion1}
}

func (s *simStream) clone() *simStream {
	c := newSimStream()
	c.nextID = s.nextID
	for id, m := range s.metas {
		cm := *m
		c.metas[id] = &cm
	}
	for id, mark := range s.marks {
		c.marks[id] = mark
	}
	return c
}

func (s *simStream) sorted() []*metadata.Meta {
	metas := make([]*metadata.Meta, 0, len(s.metas))
	for _, m := range s.metas {
		metas = append(metas, m)
	}
	return sortedByMinTime(metas)
}

// compact applies a compaction of the given blocks and records it.
func (s *simStream) compact(plan []*metadata.Meta) {
	descs := make([]string, 0, len(plan))
	merged := s.add(plan[0].MinTime, plan[0].MaxTime)
	merged.Compaction.Sources = nil
	for _, m := range plan {
		descs = append(descs, describeSimBlock(m))
		merged.MinTime = min(merged.MinTime, m.MinTime)
		merged.MaxTime = max(merged.MaxTime, m.MaxTime)
		merged.Compaction.Level = max(merged.Compaction.Level, m.Compaction.Level+1)
		merged.Compaction.Sources = append(merged.Compaction.Sources, m.Compaction.Sources...)
		delete(s.metas, m.ULID)
	}
	slices.SortFunc(merged.Compaction.Sources, func(a, b ulid.ULID) int { return a.Compare(b) })
	merged.Compaction.Sources = slices.Compact(merged.Compaction.Sources)
	sort.Strings(descs)
	s.history = append(s.history, strings.Join(descs, " + "))
}

// describeSimBlock identifies a block by its time range and sources, which do not depend on the order of compactions.
func describeSimBlock(m *metadata.Meta) string {
	return fmt.Sprintf("[%d,%d)%v", m.MinTime, m.MaxTime, m.Compaction.Sources)
}

func (s *simStream) layout() []string {
	var res []string
	for _, m := range s.metas {
		res = append(res, describeSimBlock(m))
	}
	sort.Strings(res)
	return res
}

func (s *simStream) compactions() []string {
	res := slices.Clone(s.history)
	sort.Strings(res)
	return res
}

// compactSequentially compacts the stream like upstream does: one plan of the default planner at a time.
func compactSequentially(t *testing.T, s *simStream) (plans int) {
	t.Helper()

	planner := NewTSDBBasedPlanner(log.NewNopLogger(), simRanges)
	for ; plans < 10000; plans++ {
		plan, err := planner.plan(s.marks, s.sorted())
		testutil.Ok(t, err)
		if len(plan) == 0 {
			return plans
		}
		s.compact(plan)
	}
	t.Fatal("sequential compaction did not converge")
	return plans
}

func newTestDefaultGrouper(bkt objstore.Bucket, enableVerticalCompaction bool, reg prometheus.Registerer) *DefaultGrouper {
	return NewDefaultGrouper(log.NewNopLogger(), bkt, false, enableVerticalCompaction, reg,
		promauto.With(nil).NewCounter(prometheus.CounterOpts{}),
		promauto.With(nil).NewCounter(prometheus.CounterOpts{}),
		promauto.With(nil).NewCounter(prometheus.CounterOpts{}),
		metadata.NoneFunc, 1, 1)
}

// compactConcurrently compacts the stream pass by pass, compacting all job groups of a pass from the same view.
func compactConcurrently(t *testing.T, s *simStream, enableVerticalCompaction bool) (passes, jobs int) {
	t.Helper()

	noCompactFilter := &GatherNoCompactionMarkFilter{noCompactMarkedMap: s.marks}
	grouper := NewConcurrentJobsGrouper(newTestDefaultGrouper(nil, enableVerticalCompaction, nil), simRanges, noCompactFilter)
	planner := NewConcurrentJobsPlanner(log.NewNopLogger(), noCompactFilter)
	streamKey := (&metadata.Thanos{Labels: simLabels}).GroupKey()

	for ; passes < 10000; passes++ {
		groups, err := grouper.Groups(s.metas)
		testutil.Ok(t, err)

		var plans [][]*metadata.Meta
		for _, g := range groups {
			testutil.Assert(t, g.Key() != streamKey, "expected only job groups, got the whole stream")

			plan, err := planner.Plan(context.Background(), g.metasByMinTime, nil, g.Extensions())
			testutil.Ok(t, err)
			testutil.Equals(t, g.metasByMinTime, plan, "the plan of a job group must be all of its blocks")
			plans = append(plans, plan)
		}
		if len(plans) == 0 {
			return passes, jobs
		}

		// Jobs of a pass must be independent: no shared blocks, no overlap in time.
		seen := map[ulid.ULID]struct{}{}
		for i, p := range plans {
			testutil.Assert(t, len(p) > 1, "job with less than two blocks: %v", p)
			for _, m := range p {
				_, ok := seen[m.ULID]
				testutil.Assert(t, !ok, "block %s planned twice in pass %d", m.ULID, passes)
				seen[m.ULID] = struct{}{}
			}
			mint, maxt := blocksTimeSpan(p)
			for _, other := range plans[:i] {
				omint, omaxt := blocksTimeSpan(other)
				testutil.Assert(t, maxt <= omint || omaxt <= mint, "jobs overlap in pass %d: %v and %v", passes, p, other)
			}
		}
		for _, p := range plans {
			s.compact(p)
		}
		jobs += len(plans)
	}
	t.Fatal("concurrent compaction did not converge")
	return passes, jobs
}

// assertEquivalent checks that compacting the stream with concurrent jobs does exactly the compactions upstream
// does one at a time, and so ends with the same blocks.
func assertEquivalent(t *testing.T, s *simStream, enableVerticalCompaction bool) (plans, passes int) {
	t.Helper()

	seq, conc := s.clone(), s.clone()
	plans = compactSequentially(t, seq)
	passes, jobs := compactConcurrently(t, conc, enableVerticalCompaction)

	testutil.Equals(t, seq.layout(), conc.layout(), "final blocks differ")
	testutil.Equals(t, seq.compactions(), conc.compactions(), "compactions differ")
	testutil.Equals(t, plans, jobs)
	return plans, passes
}

// alignUp returns the first multiple of tr not lower than t.
func alignUp(t, tr int64) int64 {
	s := alignedRangeStart(t, tr)
	if s < t {
		s += tr
	}
	return s
}

// randomStream returns a stream of 2h, 8h, 2d and 2w blocks (aligned or not, partially filled or not) with gaps and,
// with vertical compaction, replicas and other overlapping blocks.
func randomStream(r *rand.Rand, vertical bool) *simStream {
	s := newSimStream()

	jitter := func(mint, maxt int64) (int64, int64) {
		if r.IntN(4) == 0 {
			mint += r.Int64N(20 * 60 * 1000)
		}
		if r.IntN(4) == 0 {
			maxt -= r.Int64N(20 * 60 * 1000)
		}
		return mint, maxt
	}
	aligned := func(t, tr int64) int64 {
		start := alignUp(t, tr)
		s.add(jitter(start, start+tr))
		return start + tr
	}

	t := int64(r.IntN(96)-48) * simHour
	end := t + int64(1+r.IntN([]int{12, 72, 30 * 24}[r.IntN(3)]))*simHour
	for t < end {
		switch x := r.IntN(100); {
		case x < 55:
			t = aligned(t, 2*simHour)
		case x < 68:
			t += int64(1+r.IntN(24)) * simHour
		case x < 78:
			t = aligned(t, 8*simHour)
		case x < 83:
			t = aligned(t, 48*simHour)
		case x < 84:
			t = aligned(t, 336*simHour)
		case x < 93:
			// Misaligned block.
			mint := t + r.Int64N(90*60*1000)
			maxt := mint + 30*60*1000 + r.Int64N(5*simHour)
			s.add(mint, maxt)
			t = maxt
		default:
			// Small block within a 2h range.
			start := alignUp(t, 2*simHour)
			s.add(start+30*60*1000, start+90*60*1000)
			t = start + 2*simHour
		}
	}
	if len(s.metas) == 0 {
		aligned(t, 2*simHour)
	}

	if vertical {
		for _, m := range s.sorted() {
			switch x := r.IntN(100); {
			case x < 15:
				// Replica.
				s.add(m.MinTime, m.MaxTime)
			case x < 20:
				// Replica with slightly different timestamps.
				s.add(m.MinTime+r.Int64N(10*60*1000), m.MaxTime-r.Int64N(10*60*1000))
			case x < 23:
				// Misaligned block overlapping the next one.
				s.add(m.MinTime+simHour, m.MaxTime+simHour)
			}
		}
		if r.IntN(3) == 0 {
			// A big block overlapping many others.
			sorted := s.sorted()
			m := sorted[r.IntN(len(sorted))]
			s.add(m.MinTime, m.MinTime+int64(8+r.IntN(48))*simHour)
		}
	}
	markRandomly(r, s)
	return s
}

// randomChaoticStream returns a stream of blocks of random lengths at random positions.
func randomChaoticStream(r *rand.Rand, vertical bool) *simStream {
	s := newSimStream()

	unit := int64(10 * time.Minute / time.Millisecond)
	t := int64(r.IntN(600)-300) * unit
	for range 1 + r.IntN(40) {
		length := []int64{6, 12, 48, 288}[r.IntN(4)]
		if r.IntN(3) == 0 {
			length = 1 + r.Int64N(432)
		}
		length *= unit

		if vertical {
			mint := t + r.Int64N(30)*unit - 15*unit
			s.add(mint, mint+length)
			t += r.Int64N(20) * unit
			continue
		}
		mint := t + r.Int64N(3)*unit
		if r.IntN(2) == 0 {
			mint = alignUp(t, []int64{2, 8, 48}[r.IntN(3)]*simHour)
		}
		s.add(mint, mint+length)
		t = mint + length
	}
	markRandomly(r, s)
	return s
}

// markRandomly marks some blocks for no compaction and some as failed compactions.
func markRandomly(r *rand.Rand, s *simStream) {
	noCompactPercent := []int{0, 3, 10, 30}[r.IntN(4)]
	failedPercent := []int{0, 2, 10}[r.IntN(3)]
	for _, m := range s.sorted() {
		if r.IntN(100) < noCompactPercent {
			s.markNoCompact(m)
		}
		if r.IntN(100) < failedPercent {
			m.Compaction.Failed = true
		}
	}
}

func TestConcurrentJobs_EquivalentToSequentialPlanning(t *testing.T) {
	t.Parallel()

	streams := 500
	if testing.Short() {
		streams = 50
	}
	for _, tcase := range []struct {
		name     string
		generate func(r *rand.Rand, vertical bool) *simStream
	}{
		{name: "realistic", generate: randomStream},
		{name: "chaotic", generate: randomChaoticStream},
	} {
		for _, vertical := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/vertical=%v", tcase.name, vertical), func(t *testing.T) {
				t.Parallel()

				var plans, passes, maxPlans, maxPasses int
				for seed := range uint64(streams) {
					s := tcase.generate(rand.New(rand.NewPCG(seed, 0)), vertical)
					p, ps := assertEquivalent(t, s, vertical)
					plans += p
					passes += ps
					maxPlans = max(maxPlans, p)
					maxPasses = max(maxPasses, ps)
				}
				t.Logf("%d streams: %d compactions, done in %d passes instead of %d (max per stream: %d compactions in %d passes)",
					streams, plans, passes, plans, maxPlans, maxPasses)
			})
		}
	}
}

// TestConcurrentJobs_Issue3807Example is the case https://github.com/thanos-io/thanos/pull/3807 was rejected for:
// with [2d][8h][8h][2h][2h][2h][2h][2h], removing the four 2h blocks planned first from the view and planning again
// would see a gap and compact [2d][8h][8h] <gap> [2h] into a 2d block. Jobs are planned from the full view instead.
func TestConcurrentJobs_Issue3807Example(t *testing.T) {
	t.Parallel()

	s := newSimStream()
	twoDays := s.add(0, 48*simHour)
	eightHours := []*metadata.Meta{s.add(48*simHour, 56*simHour), s.add(56*simHour, 64*simHour)}
	var twoHours []*metadata.Meta
	for start := 64 * simHour; start < 74*simHour; start += 2 * simHour {
		twoHours = append(twoHours, s.add(start, start+2*simHour))
	}

	jobs := planConcurrentJobs(simRanges, nil, s.sorted())
	testutil.Equals(t, 1, len(jobs))
	testutil.Equals(t, twoHours[:4], jobs[0].metas)
	testutil.Equals(t, 8*simHour, jobs[0].rangeLength)

	plans, passes := assertEquivalent(t, s, false)
	testutil.Equals(t, 1, plans)
	testutil.Equals(t, 1, passes)

	_, _ = compactConcurrently(t, s, false)
	merged := fmt.Sprintf("[%d,%d)[%s %s %s %s]", 64*simHour, 72*simHour, twoHours[0].ULID, twoHours[1].ULID, twoHours[2].ULID, twoHours[3].ULID)
	expected := []string{describeSimBlock(twoDays), describeSimBlock(eightHours[0]), describeSimBlock(eightHours[1]), merged, describeSimBlock(twoHours[4])}
	sort.Strings(expected)
	testutil.Equals(t, expected, s.layout())
}

// TestConcurrentJobs_NewestBlocksOverlap covers ranges planned together with vertical compaction jobs: when the
// newest blocks overlap, merging them changes which blocks are the most recent, and so which ranges are complete.
func TestConcurrentJobs_NewestBlocksOverlap(t *testing.T) {
	t.Parallel()

	s := newSimStream()
	s.add(0, 2*simHour)
	s.add(2*simHour, 3*simHour)
	newest := []*metadata.Meta{s.add(8*simHour, 10*simHour), s.add(9*simHour, 11*simHour)}

	// Before the two most recent blocks are merged, [0, 8h) ends before the most recent block but one, so it looks
	// complete. Once they are merged, it does not, so the planner never compacts it.
	jobs := planConcurrentJobs(simRanges, nil, s.sorted())
	testutil.Equals(t, 1, len(jobs))
	testutil.Equals(t, newest, jobs[0].metas)
	testutil.Equals(t, int64(0), jobs[0].rangeLength)

	plans, passes := assertEquivalent(t, s, true)
	testutil.Equals(t, 1, plans)
	testutil.Equals(t, 1, passes)
}

// TestConcurrentJobs_Backlog checks that a backlog of one stream is compacted in a few passes: two weeks of 2h
// blocks need 7*6=42 8h compactions, 7 2d compactions and one 2w compaction.
func TestConcurrentJobs_Backlog(t *testing.T) {
	t.Parallel()

	s := newSimStream()
	for start := int64(0); start <= 14*24*simHour; start += 2 * simHour {
		s.add(start, start+2*simHour)
	}

	testutil.Equals(t, 42, len(planConcurrentJobs(simRanges, nil, s.sorted())))

	plans, passes := assertEquivalent(t, s, false)
	testutil.Equals(t, 42+7+1, plans)
	testutil.Equals(t, 3, passes)
}

func testJobMeta(id uint64, mint, maxt int64, lset map[string]string, resolution int64) *metadata.Meta {
	return &metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:       ulid.MustNew(id, nil),
			MinTime:    mint,
			MaxTime:    maxt,
			Compaction: tsdb.BlockMetaCompaction{Level: 1, Sources: []ulid.ULID{ulid.MustNew(id, nil)}},
		},
		Thanos: metadata.Thanos{
			Labels:     lset,
			Downsample: metadata.ThanosDownsample{Resolution: resolution},
			Extensions: map[string]any{"foo": "bar"},
		},
	}
}

func ulidsOf(metas []*metadata.Meta) []ulid.ULID {
	ids := make([]ulid.ULID, 0, len(metas))
	for _, m := range metas {
		ids = append(ids, m.ULID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].Compare(ids[j]) < 0 })
	return ids
}

func TestConcurrentJobsGrouper_Groups(t *testing.T) {
	t.Parallel()

	var (
		lsetA  = map[string]string{"cluster": "a"}
		lsetB  = map[string]string{"cluster": "b"}
		h      = simHour
		blocks = map[ulid.ULID]*metadata.Meta{}
	)
	add := func(m *metadata.Meta) *metadata.Meta {
		blocks[m.ULID] = m
		return m
	}

	// Stream A: [0, 8h) and [8h, 16h) are complete; [16h, 24h) is complete too, but its block marked for no
	// compaction leaves only one run of two blocks; [24h, 26h) is the newest block.
	var streamA []*metadata.Meta
	for i := range uint64(12) {
		streamA = append(streamA, add(testJobMeta(i+1, int64(i)*2*h, int64(i+1)*2*h, lsetA, 0)))
	}
	streamA = append(streamA, add(testJobMeta(13, 24*h, 26*h, lsetA, 0)))
	// Stream B: two replicas of [0, 2h), then [2h, 4h) and [4h, 6h).
	var streamB []*metadata.Meta
	for i, r := range [][2]int64{{0, 2 * h}, {0, 2 * h}, {2 * h, 4 * h}, {4 * h, 6 * h}} {
		streamB = append(streamB, add(testJobMeta(uint64(100+i), r[0], r[1], lsetB, 0)))
	}
	// Stream A at another resolution with a single block: nothing to do.
	add(testJobMeta(200, 0, 48*h, lsetA, int64(5*time.Minute/time.Millisecond)))

	noCompactFilter := &GatherNoCompactionMarkFilter{noCompactMarkedMap: map[ulid.ULID]*metadata.NoCompactMark{
		streamA[10].ULID: {ID: streamA[10].ULID, Version: metadata.NoCompactMarkVersion1},
	}}
	keyA, keyB := streamA[0].Thanos.GroupKey(), streamB[0].Thanos.GroupKey()

	for _, vertical := range []bool{false, true} {
		t.Run(fmt.Sprintf("vertical=%v", vertical), func(t *testing.T) {
			t.Parallel()

			reg := prometheus.NewRegistry()
			streams := newTestDefaultGrouper(objstore.NewInMemBucket(), vertical, reg)
			groups, err := NewConcurrentJobsGrouper(streams, simRanges, noCompactFilter).Groups(blocks)
			testutil.Ok(t, err)

			type group struct {
				key string
				ids []ulid.ULID
			}
			expected := []group{
				{key: keyA + "_8h_0-28800000", ids: ulidsOf(streamA[0:4])},
				{key: keyA + "_8h_28800000-57600000", ids: ulidsOf(streamA[4:8])},
				{key: keyA + "_8h_57600000-72000000", ids: ulidsOf(streamA[8:10])},
			}
			if vertical {
				// Only the replicas are merged: once they are, [2h, 4h) is the most recent block but one, so
				// [0, 8h) is not complete.
				expected = append(expected, group{key: keyB + "_vertical_0-7200000", ids: ulidsOf(streamB[0:2])})
			} else {
				// Overlapping blocks without vertical compaction: the whole stream, so that its compaction halts.
				expected = append(expected, group{key: keyB, ids: ulidsOf(streamB)})
			}

			var got []group
			for _, g := range groups {
				got = append(got, group{key: g.Key(), ids: g.IDs()})

				testutil.Assert(t, !strings.ContainsAny(g.Key(), `/\`+string(os.PathSeparator)), "key %q is not a valid directory name", g.Key())
				testutil.Equals(t, map[string]any{"foo": "bar"}, g.Extensions())
				testutil.Equals(t, vertical, g.enableVerticalCompaction)
				testutil.Equals(t, int64(0), g.Resolution())
				// Jobs share the metrics of their stream: no new series per job.
				testutil.Assert(t, g.compactions == streams.compactions.WithLabelValues("0"), "job group must use the stream's metrics")
				testutil.Assert(t, g.compactionRunsStarted == streams.compactionRunsStarted.WithLabelValues("0"), "job group must use the stream's metrics")
			}
			sort.Slice(got, func(i, j int) bool { return got[i].key < got[j].key })
			sort.Slice(expected, func(i, j int) bool { return expected[i].key < expected[j].key })
			testutil.Equals(t, expected, got)

			if !vertical {
				for _, g := range groups {
					if g.Key() != keyB {
						continue
					}
					// The pre-compaction overlap check halts, exactly like with the default grouper.
					_, _, err := g.Compact(context.Background(), t.TempDir(), NewConcurrentJobsPlanner(log.NewNopLogger(), noCompactFilter), nil, DefaultBlockDeletableChecker{}, DefaultCompactionLifecycleCallback{})
					testutil.Assert(t, IsHaltError(err), "expected halt error, got %v", err)
				}
			}
		})
	}
}

func TestConcurrentJobsPlanner_Plan(t *testing.T) {
	t.Parallel()

	h := simHour
	lset := map[string]string{"cluster": "a"}
	a, b, c, d, e := testJobMeta(1, 0, 2*h, lset, 0), testJobMeta(2, 2*h, 4*h, lset, 0), testJobMeta(3, 4*h, 6*h, lset, 0),
		testJobMeta(4, 6*h, 8*h, lset, 0), testJobMeta(5, 8*h, 10*h, lset, 0)
	// A set of overlapping blocks: x overlaps y, y overlaps z, x and z do not overlap.
	x, y, z := testJobMeta(6, 0, 2*h, lset, 0), testJobMeta(7, 1*h, 3*h, lset, 0), testJobMeta(8, 2*h, 4*h, lset, 0)

	for _, tcase := range []struct {
		name     string
		metas    []*metadata.Meta
		marked   []*metadata.Meta
		expected []*metadata.Meta
	}{
		{name: "range job", metas: []*metadata.Meta{a, b, c, d}, expected: []*metadata.Meta{a, b, c, d}},
		{name: "vertical job", metas: []*metadata.Meta{x, y, z}, expected: []*metadata.Meta{x, y, z}},
		{name: "range job, marked block splits it", metas: []*metadata.Meta{a, b, c, d, e}, marked: []*metadata.Meta{c}, expected: []*metadata.Meta{a, b}},
		{name: "range job, first run too short", metas: []*metadata.Meta{a, b, c, d, e}, marked: []*metadata.Meta{b}, expected: []*metadata.Meta{c, d, e}},
		{name: "range job, nothing left", metas: []*metadata.Meta{a, b, c}, marked: []*metadata.Meta{b}},
		{name: "vertical job, nothing left overlapping", metas: []*metadata.Meta{x, y, z}, marked: []*metadata.Meta{y}},
		{name: "vertical job, overlap left", metas: []*metadata.Meta{x, y, z}, marked: []*metadata.Meta{z}, expected: []*metadata.Meta{x, y}},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			marks := map[ulid.ULID]*metadata.NoCompactMark{}
			for _, m := range tcase.marked {
				marks[m.ULID] = &metadata.NoCompactMark{ID: m.ULID, Version: metadata.NoCompactMarkVersion1}
			}
			planner := NewConcurrentJobsPlanner(log.NewNopLogger(), &GatherNoCompactionMarkFilter{noCompactMarkedMap: marks})
			plan, err := planner.Plan(context.Background(), tcase.metas, nil, nil)
			testutil.Ok(t, err)
			testutil.Equals(t, tcase.expected, plan)
		})
	}

	t.Run("large total index size filter", func(t *testing.T) {
		bkt := objstore.NewInMemBucket()
		marked := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
		withIndexSize := func(m *metadata.Meta, size int64) *metadata.Meta {
			cm := *m
			cm.Thanos.Files = []metadata.File{{RelPath: block.IndexFilename, SizeBytes: size}}
			return &cm
		}
		ia, ib, ic, id := withIndexSize(a, 30), withIndexSize(b, 41), withIndexSize(c, 30), withIndexSize(d, 20)

		planner := WithLargeTotalIndexSizeFilter(NewConcurrentJobsPlanner(log.NewNopLogger(), &GatherNoCompactionMarkFilter{}), bkt, 100, marked)
		plan, err := planner.Plan(context.Background(), []*metadata.Meta{ia, ib, ic, id}, nil, nil)
		testutil.Ok(t, err)
		// The block with the biggest index is marked for no compaction, and the job goes on with what is left of it.
		testutil.Equals(t, []*metadata.Meta{ic, id}, plan)
		testutil.Equals(t, 1.0, promtest.ToFloat64(marked))
		exists, err := bkt.Exists(context.Background(), b.ULID.String()+"/"+metadata.NoCompactMarkFilename)
		testutil.Ok(t, err)
		testutil.Assert(t, exists, "expected a no-compact mark for the block with the biggest index")
	})
}
