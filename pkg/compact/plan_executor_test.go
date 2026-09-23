// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"context"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

// recordingExecutor captures the plan it is handed and returns a canned result.
type recordingExecutor struct {
	gotGroup      *Group
	gotPlan       Plan
	calls         int
	returnCompIDs []ulid.ULID
	returnErr     error
}

func (r *recordingExecutor) Execute(_ context.Context, _ string, cg *Group, plan Plan) ([]ulid.ULID, error) {
	r.calls++
	r.gotGroup = cg
	r.gotPlan = plan
	return r.returnCompIDs, r.returnErr
}

// stubPlanner returns a fixed plan.
type stubPlanner struct {
	plan []*metadata.Meta
	err  error
}

func (s stubPlanner) Plan(_ context.Context, _ []*metadata.Meta, _ chan error, _ any) ([]*metadata.Meta, error) {
	return s.plan, s.err
}

// outputPlanner is a stubPlanner that also names the plan's outputs.
type outputPlanner struct {
	stubPlanner
	outputs    []PlanOutput
	outputsErr error
	gotSources []*metadata.Meta
}

func (o *outputPlanner) PlanOutputs(_ context.Context, _ *Group, sources []*metadata.Meta) ([]PlanOutput, error) {
	o.gotSources = sources
	return o.outputs, o.outputsErr
}

func testGroup(t *testing.T, metas ...*metadata.Meta) *Group {
	t.Helper()

	cnt := func() prometheus.Counter {
		return promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "test"})
	}
	g, err := NewGroup(
		log.NewNopLogger(),
		objstore.NewInMemBucket(),
		"0@test",
		labels.FromStrings("ext", "1"),
		0,
		false,
		false,
		cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), cnt(), cnt(),
		metadata.NoneFunc,
		1,
		1,
	)
	testutil.Ok(t, err)

	for _, m := range metas {
		testutil.Ok(t, g.AppendMeta(m))
	}
	return g
}

func meta(id ulid.ULID, mint, maxt int64) *metadata.Meta {
	m := &metadata.Meta{}
	m.ULID = id
	m.MinTime = mint
	m.MaxTime = maxt
	m.Thanos.Labels = map[string]string{"ext": "1"}
	m.Thanos.Downsample.Resolution = 0
	return m
}

// TestGroupPlanSeparatesPlanningFromExecution asserts that the plan a group
// produces is handed to the injected executor unchanged, and that the executor's
// result is propagated back out of compact.
func TestGroupPlanSeparatesPlanningFromExecution(t *testing.T) {
	ctx := context.Background()

	m1 := meta(ulid.MustNew(1, nil), 0, 100)
	m2 := meta(ulid.MustNew(2, nil), 100, 200)
	cg := testGroup(t, m1, m2)

	planner := stubPlanner{plan: []*metadata.Meta{m1, m2}}
	out := ulid.MustNew(99, nil)
	exec := &recordingExecutor{returnCompIDs: []ulid.ULID{out}}

	// Plan is callable on its own and returns what the planner produced.
	plan, err := cg.Plan(ctx, planner, make(chan error, 1))
	testutil.Ok(t, err)
	testutil.Equals(t, false, plan.OverlappingBlocks)
	testutil.Equals(t, 2, len(plan.Sources))
	testutil.Equals(t, 0, len(plan.Outputs), "a planner that does not name outputs leaves them to the executor's default")

	// compact hands that same plan to the executor.
	shouldRerun, compIDs, err := cg.compact(ctx, "/tmp/does-not-matter", planner, exec, make(chan error, 1))
	testutil.Ok(t, err)
	testutil.Equals(t, true, shouldRerun)
	testutil.Equals(t, []ulid.ULID{out}, compIDs)

	testutil.Equals(t, 1, exec.calls)
	testutil.Equals(t, cg, exec.gotGroup)
	testutil.Equals(t, []*metadata.Meta{m1, m2}, exec.gotPlan.Sources)
	testutil.Equals(t, false, exec.gotPlan.OverlappingBlocks)
}

// TestGroupPlanCarriesPlannerOutputs asserts that a planner deciding what a
// plan produces sees the plan's sources and that its outputs travel with the
// plan to the executor, so an executor never has to invent them.
func TestGroupPlanCarriesPlannerOutputs(t *testing.T) {
	ctx := context.Background()

	m1 := meta(ulid.MustNew(1, nil), 0, 100)
	m2 := meta(ulid.MustNew(2, nil), 100, 200)
	cg := testGroup(t, m1, m2)

	outputs := []PlanOutput{
		{Labels: map[string]string{"ext": "1", "part": "a"}, Series: &SeriesPartition{Index: 0, Count: 2}},
		{Labels: map[string]string{"ext": "1", "part": "b"}, Series: &SeriesPartition{Index: 1, Count: 2}},
	}
	planner := &outputPlanner{stubPlanner: stubPlanner{plan: []*metadata.Meta{m1, m2}}, outputs: outputs}
	exec := &recordingExecutor{}

	_, _, err := cg.compact(ctx, "/tmp/does-not-matter", planner, exec, make(chan error, 1))
	testutil.Ok(t, err)
	testutil.Equals(t, []*metadata.Meta{m1, m2}, planner.gotSources)
	testutil.Equals(t, outputs, exec.gotPlan.Outputs)

	// Outputs are only asked for when there is a plan.
	idle := &outputPlanner{outputs: outputs}
	_, _, err = cg.compact(ctx, "/tmp/does-not-matter", idle, exec, make(chan error, 1))
	testutil.Ok(t, err)
	testutil.Equals(t, []*metadata.Meta(nil), idle.gotSources)

	// A malformed partition is refused before anything is executed.
	bad := &outputPlanner{stubPlanner: stubPlanner{plan: []*metadata.Meta{m1, m2}}, outputs: []PlanOutput{{Series: &SeriesPartition{Index: 2, Count: 2}}}}
	exec = &recordingExecutor{}
	_, _, err = cg.compact(ctx, "/tmp/does-not-matter", bad, exec, make(chan error, 1))
	testutil.NotOk(t, err)
	testutil.Equals(t, 0, exec.calls)
}

type siblingPlanner struct {
	stubPlanner
	siblings    []ulid.ULID
	siblingsErr error
	gotSources  []*metadata.Meta
}

func (s *siblingPlanner) PlanSiblings(_ context.Context, _ *Group, sources []*metadata.Meta) ([]ulid.ULID, error) {
	s.gotSources = sources
	return s.siblings, s.siblingsErr
}

// TestGroupPlanCarriesPlannerSiblings pins down that the siblings a planner
// names reach the plan the executor gets. Tests of the executor build their
// plans by hand, so they cannot catch a planning path that drops them.
func TestGroupPlanCarriesPlannerSiblings(t *testing.T) {
	ctx := context.Background()

	m1 := meta(ulid.MustNew(1, nil), 0, 100)
	m2 := meta(ulid.MustNew(2, nil), 100, 200)
	cg := testGroup(t, m1, m2)
	siblings := []ulid.ULID{ulid.MustNew(3, nil), ulid.MustNew(4, nil)}

	planner := &siblingPlanner{stubPlanner: stubPlanner{plan: []*metadata.Meta{m1, m2}}, siblings: siblings}
	plan, err := cg.Plan(ctx, planner, make(chan error, 1))
	testutil.Ok(t, err)
	testutil.Equals(t, []*metadata.Meta{m1, m2}, planner.gotSources)
	testutil.Equals(t, siblings, plan.Siblings)

	exec := &recordingExecutor{}
	_, _, err = cg.compact(ctx, "/tmp/does-not-matter", planner, exec, make(chan error, 1))
	testutil.Ok(t, err)
	testutil.Equals(t, siblings, exec.gotPlan.Siblings)

	// Siblings are only asked for when there is a plan.
	idle := &siblingPlanner{siblings: siblings}
	plan, err = cg.Plan(ctx, idle, make(chan error, 1))
	testutil.Ok(t, err)
	testutil.Equals(t, []*metadata.Meta(nil), idle.gotSources)
	testutil.Equals(t, 0, len(plan.Siblings))

	// A planner that cannot name them fails the plan rather than letting it
	// run without them.
	failing := &siblingPlanner{stubPlanner: stubPlanner{plan: []*metadata.Meta{m1, m2}}, siblingsErr: errors.New("no view")}
	exec = &recordingExecutor{}
	_, _, err = cg.compact(ctx, "/tmp/does-not-matter", failing, exec, make(chan error, 1))
	testutil.NotOk(t, err)
	testutil.Equals(t, 0, exec.calls)
}

// TestGroupCompactTreatsDeferredPlanAsNoWork asserts that an executor that
// declines a plan neither fails the group nor asks for a rerun.
func TestGroupCompactTreatsDeferredPlanAsNoWork(t *testing.T) {
	ctx := context.Background()

	m1 := meta(ulid.MustNew(1, nil), 0, 100)
	m2 := meta(ulid.MustNew(2, nil), 100, 200)
	cg := testGroup(t, m1, m2)
	exec := &recordingExecutor{returnErr: errors.Wrap(ErrPlanDeferred, "parked")}

	shouldRerun, compIDs, err := cg.compact(ctx, "/tmp/does-not-matter", stubPlanner{plan: []*metadata.Meta{m1, m2}}, exec, make(chan error, 1))
	testutil.Ok(t, err)
	testutil.Equals(t, false, shouldRerun)
	testutil.Equals(t, 0, len(compIDs))
	testutil.Equals(t, 1, exec.calls)
}

type singleBlockPlanner struct {
	stubPlanner
	single bool
}

func (p singleBlockPlanner) PlansSingleBlockGroups() bool { return p.single }

func TestPlansSingleBlockGroups(t *testing.T) {
	testutil.Equals(t, false, plansSingleBlockGroups(stubPlanner{}))
	testutil.Equals(t, false, plansSingleBlockGroups(singleBlockPlanner{}))
	testutil.Equals(t, true, plansSingleBlockGroups(singleBlockPlanner{single: true}))
}

// TestGroupCompactSkipsExecutorWhenNothingPlanned asserts the executor is not
// invoked at all when the planner has no work, matching the previous behavior
// of returning early.
func TestGroupCompactSkipsExecutorWhenNothingPlanned(t *testing.T) {
	ctx := context.Background()

	cg := testGroup(t, meta(ulid.MustNew(1, nil), 0, 100))
	exec := &recordingExecutor{}

	shouldRerun, compIDs, err := cg.compact(ctx, "/tmp/does-not-matter", stubPlanner{plan: nil}, exec, make(chan error, 1))
	testutil.Ok(t, err)
	testutil.Equals(t, false, shouldRerun)
	testutil.Equals(t, 0, len(compIDs))
	testutil.Equals(t, 0, exec.calls)
}

// TestGroupPlanHaltsOnOverlap asserts the pre-compaction overlap check still
// produces a halt error, now from Plan rather than from compact.
func TestGroupPlanHaltsOnOverlap(t *testing.T) {
	ctx := context.Background()

	// Two blocks covering the same time range overlap.
	cg := testGroup(t,
		meta(ulid.MustNew(1, nil), 0, 100),
		meta(ulid.MustNew(2, nil), 50, 150),
	)

	_, err := cg.Plan(ctx, stubPlanner{}, make(chan error, 1))
	testutil.NotOk(t, err)
	testutil.Equals(t, true, IsHaltError(err))
}

// TestExportedErrorConstructors asserts the exported constructors produce errors
// that the package's own classifiers recognize, which is what lets an
// out-of-process executor reconstruct a remote worker's error class.
func TestExportedErrorConstructors(t *testing.T) {
	id := ulid.MustNew(7, nil)

	testutil.Equals(t, true, IsHaltError(NewHaltError(errors.New("boom"))))
	testutil.Equals(t, true, IsRetryError(NewRetryError(errors.New("boom"))))
	testutil.Equals(t, true, IsIssue347Error(NewIssue347Error(errors.New("boom"), id)))
	testutil.Equals(t, true, IsOutOfOrderChunkError(NewOutOfOrderChunksError(errors.New("boom"), id)))
}

// TestSetSiblings: a plan names the live blocks that share a set with one of
// its sources - through the sources' own sets or through sets of other blocks
// that named a source - never the sources themselves or blocks gone from the
// view, and nothing when no set is involved.
func TestSetSiblings(t *testing.T) {
	id := func(i int) ulid.ULID { return ulid.MustNew(uint64(i), nil) }
	meta := func(i int, set ...int) *metadata.Meta {
		m := &metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: id(i)}}
		if len(set) > 0 {
			m.Thanos.Output = &metadata.ThanosOutput{Index: 0, Count: 1}
			for _, s := range set {
				m.Thanos.Output.Blocks = append(m.Thanos.Output.Blocks, id(s))
			}
		}
		return m
	}
	view := map[ulid.ULID]*metadata.Meta{}
	add := func(ms ...*metadata.Meta) {
		for _, m := range ms {
			view[m.ULID] = m
		}
	}
	// 1 and 2 were split together: set {1, 2, 9}, where 9 is gone. 3 named 1
	// as its sibling: set {3, 1}. 4 names 2 and 5: set {4, 2, 5}. 6 is
	// unrelated, 7 names only gone blocks.
	add(meta(1, 1, 2, 9), meta(2, 1, 2, 9), meta(3, 3, 1), meta(4, 4, 2, 5), meta(5), meta(6), meta(7, 7, 8))

	got := SetSiblings(view, []*metadata.Meta{view[id(1)]})
	testutil.Equals(t, []ulid.ULID{id(2), id(3)}, got, "1's own set and 3's set name it")

	got = SetSiblings(view, []*metadata.Meta{view[id(1)], view[id(2)]})
	testutil.Equals(t, []ulid.ULID{id(3), id(4), id(5)}, got, "both sources' sets, without the sources")

	got = SetSiblings(view, []*metadata.Meta{view[id(6)]})
	testutil.Equals(t, 0, len(got))

	got = SetSiblings(view, []*metadata.Meta{view[id(7)]})
	testutil.Equals(t, 0, len(got), "gone blocks are not named")
}

// TestGroupPlanNamesSiblingsFromItsView pins down that plans name their
// siblings whatever planner runs: a bucket holding sets keeps needing them
// once the planner that made the sets is gone - block splitting turned off,
// say - or a shard compacted on would leave its siblings withheld for good.
// Groups keep the view the grouper cut them from, and planning falls back
// to it.
func TestGroupPlanNamesSiblingsFromItsView(t *testing.T) {
	shardMeta := func(i int, shard string, mint, maxt int64, set ...int) *metadata.Meta {
		m := meta(ulid.MustNew(uint64(i), nil), mint, maxt)
		m.Thanos.Labels = map[string]string{"ext": "1", "shard": shard}
		m.Thanos.Version = metadata.ThanosVersion1
		if len(set) > 0 {
			m.Thanos.Output = &metadata.ThanosOutput{Index: 0, Count: len(set)}
			for _, s := range set {
				m.Thanos.Output.Blocks = append(m.Thanos.Output.Blocks, ulid.MustNew(uint64(s), nil))
			}
		}
		return m
	}
	// 1 and 2 are the shards of one split; 3 is the next range of shard a.
	view := map[ulid.ULID]*metadata.Meta{}
	for _, m := range []*metadata.Meta{shardMeta(1, "a", 0, 100, 1, 2), shardMeta(2, "b", 0, 100, 1, 2), shardMeta(3, "a", 100, 200)} {
		view[m.ULID] = m
	}
	reg := prometheus.NewRegistry()
	cnt := promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "test"})
	groups, err := NewDefaultGrouper(log.NewNopLogger(), objstore.NewInMemBucket(), false, false, reg, cnt, cnt, cnt, "", 1, 1).Groups(view)
	testutil.Ok(t, err)
	var shardA *Group
	for _, g := range groups {
		if g.Labels().Get("shard") == "a" {
			shardA = g
		}
	}
	testutil.Assert(t, shardA != nil, "no group for shard a")

	planner := stubPlanner{plan: []*metadata.Meta{view[ulid.MustNew(1, nil)], view[ulid.MustNew(3, nil)]}}
	plan, err := shardA.Plan(context.Background(), planner, make(chan error, 1))
	testutil.Ok(t, err)
	testutil.Equals(t, []ulid.ULID{ulid.MustNew(2, nil)}, plan.Siblings)
}
