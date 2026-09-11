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
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

// recordingExecutor captures the plan it is handed and returns a canned result.
type recordingExecutor struct {
	gotGroup      *Group
	gotToCompact  []*metadata.Meta
	gotOverlap    bool
	calls         int
	returnCompIDs []ulid.ULID
	returnErr     error
}

func (r *recordingExecutor) Execute(_ context.Context, _ string, cg *Group, toCompact []*metadata.Meta, overlappingBlocks bool) ([]ulid.ULID, error) {
	r.calls++
	r.gotGroup = cg
	r.gotToCompact = toCompact
	r.gotOverlap = overlappingBlocks
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

func testGroup(t *testing.T, metas ...*metadata.Meta) *Group {
	t.Helper()

	cnt := func() prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{Name: "test"})
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
	toCompact, overlapping, err := cg.Plan(ctx, planner, make(chan error, 1))
	testutil.Ok(t, err)
	testutil.Equals(t, false, overlapping)
	testutil.Equals(t, 2, len(toCompact))

	// compact hands that same plan to the executor.
	shouldRerun, compIDs, err := cg.compact(ctx, "/tmp/does-not-matter", planner, exec, make(chan error, 1))
	testutil.Ok(t, err)
	testutil.Equals(t, true, shouldRerun)
	testutil.Equals(t, []ulid.ULID{out}, compIDs)

	testutil.Equals(t, 1, exec.calls)
	testutil.Equals(t, cg, exec.gotGroup)
	testutil.Equals(t, []*metadata.Meta{m1, m2}, exec.gotToCompact)
	testutil.Equals(t, false, exec.gotOverlap)
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

	_, _, err := cg.Plan(ctx, stubPlanner{}, make(chan error, 1))
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
