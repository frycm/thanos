// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"context"
	"slices"

	"github.com/cespare/xxhash/v2"
	"github.com/oklog/ulid/v2"
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
	// Siblings are blocks outside the plan that the outputs complete a set
	// with: the blocks the sources shared their own set with - the other
	// shards of the split that made them - which would otherwise be left
	// with a set that can never be complete again once the sources are
	// gone. The outputs are published together with them.
	Siblings []ulid.ULID
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
//
// The hash is labels.StableHash of the series' labels, less the labels named
// in Without, so that series differing only in those - replicas that carry
// their replica label inside the series, say - fall into the same partition
// and can be deduplicated there. Partitions of the same lineage must agree
// on Without: a shard block re-partitioned under a different hash keeps
// series that no finer partition claims, and the executor refuses such a
// plan rather than lose them.
type SeriesPartition struct {
	Index   uint64   `json:"index"`
	Count   uint64   `json:"count"`
	Without []string `json:"without,omitempty"`
}

// Validate checks that the partition is well-formed.
func (p SeriesPartition) Validate() error {
	if p.Count == 0 || p.Index >= p.Count {
		return errors.Errorf("invalid series partition %d of %d", p.Index, p.Count)
	}
	if slices.Contains(p.Without, "") {
		return errors.Errorf("series partition %d of %d leaves out a label with no name", p.Index, p.Count)
	}
	return nil
}

// Contains reports whether the series belongs to the partition.
func (p SeriesPartition) Contains(lset labels.Labels) bool {
	return p.contains(lset, nil)
}

// contains is Contains with a digest to reuse across calls; nil allocates one
// when needed.
func (p SeriesPartition) contains(lset labels.Labels, h *xxhash.Digest) bool {
	return p.hash(lset, h)%p.Count == p.Index
}

// seps separates names and values in the byte layout labels.StableHash
// hashes, which hash mirrors.
var seps = []byte{'\xff'}

// hash is labels.StableHash of lset without the labels in Without: the same
// byte layout, name and value each followed by a separator, fed to xxhash for
// every label kept. A series that carries none of the labels hashes exactly
// as labels.StableHash would.
func (p SeriesPartition) hash(lset labels.Labels, h *xxhash.Digest) uint64 {
	if len(p.Without) == 0 {
		return labels.StableHash(lset)
	}
	if h == nil {
		h = xxhash.New()
	} else {
		h.Reset()
	}
	lset.Range(func(l labels.Label) {
		if slices.Contains(p.Without, l.Name) {
			return
		}
		_, _ = h.WriteString(l.Name)
		_, _ = h.Write(seps)
		_, _ = h.WriteString(l.Value)
		_, _ = h.Write(seps)
	})
	return h.Sum64()
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

// SiblingPlanner is implemented by planners whose plans compact blocks that
// share a set with blocks outside the plan - shards of one split, say. The
// plan's outputs are published together with the siblings it names: the
// outputs and the siblings together replace what the sources' own set once
// replaced, so that the siblings keep a complete set once the sources are
// gone. Only a planner with a view of the bucket can name them.
type SiblingPlanner interface {
	PlanSiblings(ctx context.Context, cg *Group, sources []*metadata.Meta) ([]ulid.ULID, error)
}

// SetSiblings returns the siblings a plan over the given sources has to name:
// every block in the view, other than the sources, that a set naming one of
// the sources also names - the sources' own sets, and the sets of other
// blocks that named a source as their sibling. Those blocks are published
// by such a set, and retiring the sources would leave it incomplete for
// good; named again by the plan's outputs, they stay published. The view
// must be the compactor's, holding only published blocks: a block that is
// about to be retired must not be named, or the new set would never be
// complete.
func SetSiblings(view map[ulid.ULID]*metadata.Meta, sources []*metadata.Meta) []ulid.ULID {
	isSource := make(map[ulid.ULID]struct{}, len(sources))
	for _, m := range sources {
		isSource[m.ULID] = struct{}{}
	}
	namesASource := func(set []ulid.ULID) bool {
		for _, id := range set {
			if _, ok := isSource[id]; ok {
				return true
			}
		}
		return false
	}
	siblings := map[ulid.ULID]struct{}{}
	collect := func(set []ulid.ULID) {
		for _, id := range set {
			if _, ok := isSource[id]; ok {
				continue
			}
			if _, ok := view[id]; !ok {
				continue
			}
			siblings[id] = struct{}{}
		}
	}
	for _, m := range sources {
		if m.Thanos.Output != nil {
			collect(m.Thanos.Output.Blocks)
		}
	}
	for _, m := range view {
		if m.Thanos.Output != nil && namesASource(m.Thanos.Output.Blocks) {
			collect(m.Thanos.Output.Blocks)
		}
	}
	out := make([]ulid.ULID, 0, len(siblings))
	for id := range siblings {
		out = append(out, id)
	}
	slices.SortFunc(out, func(a, b ulid.ULID) int { return a.Compare(b) })
	return out
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
