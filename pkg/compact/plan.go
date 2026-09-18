// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"context"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/block/metadata"
)

// Plan is one unit of compaction work: the blocks to merge and the blocks to
// produce from them. A plan is complete in itself, so that whoever executes
// it - the compactor's own process or a worker elsewhere - needs nothing but
// the plan and the bucket.
type Plan struct {
	// Sources are the blocks to compact, in the order the planner chose.
	Sources []*metadata.Meta
	// OverlappingBlocks reports whether the group held overlapping blocks
	// when the plan was made. Only possible with vertical compaction.
	OverlappingBlocks bool
	// Outputs describe the blocks to produce. Empty means the block the
	// compactor has always produced: one, carrying the group's labels and
	// holding every series of the sources.
	Outputs []PlanOutput
}

// Empty reports whether the plan has nothing to do.
func (p Plan) Empty() bool { return len(p.Sources) == 0 }

// PlanOutput describes one block a plan produces.
type PlanOutput struct {
	// Labels are the external labels of the output block. Nil means the
	// group's labels.
	Labels map[string]string `json:"labels,omitempty"`
	// Series, if set, restricts the block to a partition of the sources'
	// series. Nil means every series.
	Series *SeriesPartition `json:"series,omitempty"`
}

// SeriesPartition names one part of a series set partitioned by hash: the
// series whose stable label hash is congruent to Index modulo Count.
type SeriesPartition struct {
	Index uint64 `json:"index"`
	Count uint64 `json:"count"`
}

// Validate checks that the partition is well-formed.
func (p SeriesPartition) Validate() error {
	if p.Count == 0 || p.Index >= p.Count {
		return errors.Errorf("invalid series partition %d of %d", p.Index, p.Count)
	}
	return nil
}

// Contains reports whether the series belongs to the partition.
func (p SeriesPartition) Contains(lset labels.Labels) bool {
	return labels.StableHash(lset)%p.Count == p.Index
}

// OutputPlanner is implemented by planners that decide what blocks a plan
// produces. Planners that do not implement it produce the default output.
//
// The decision belongs to planning, not execution: a planner sees the whole
// bucket and can lay out the outputs so that they fit what is already there,
// while an executor only sees the plan it was handed.
type OutputPlanner interface {
	// PlanOutputs returns the outputs of a plan with the given sources for
	// the group. An empty result means the default output.
	PlanOutputs(ctx context.Context, cg *Group, sources []*metadata.Meta) ([]PlanOutput, error)
}

// SingleBlockPlanner is implemented by planners that can have work for a
// group holding a single block, which the compactor otherwise skips as having
// nothing to compact.
type SingleBlockPlanner interface {
	PlansSingleBlockGroups() bool
}

func plansSingleBlockGroups(p Planner) bool {
	sp, ok := p.(SingleBlockPlanner)
	return ok && sp.PlansSingleBlockGroups()
}

// ErrPlanDeferred is returned by a PlanExecutor that deliberately did not
// execute the plan it was given - for example because the plan's source blocks
// belong to work awaiting operator attention. It tells the control loop not to
// rerun the group this pass, where an ordinary empty result would mean "done,
// look for more work" and spin on the same deferred plan forever.
var ErrPlanDeferred = errors.New("compaction plan deferred")
