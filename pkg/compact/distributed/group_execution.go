// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"context"

	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
)

// Execute fills free slots from the group's current metadata snapshot. Sources
// and envelopes remain reserved even after completion: newly produced blocks
// only become planning inputs after the outer compactor loop synchronizes them.
func (e *RemotePlanExecutor) Execute(ctx context.Context, _ string, cg *compact.Group, first []*metadata.Meta, overlappingBlocks bool) ([]ulid.ULID, error) {
	plans := groupPlans{executor: e, group: cg, first: first, excluded: map[ulid.ULID]struct{}{}}
	type outcome struct {
		ids []ulid.ULID
		err error
	}
	results := make(chan outcome, e.maxInflightPerGroup)
	var (
		inflight  int
		exhausted bool
		completed bool
		compIDs   []ulid.ULID
		firstErr  error
	)
	recordError := func(err error) {
		if firstErr == nil || (compact.IsHaltError(err) && !compact.IsHaltError(firstErr)) {
			firstErr = err
		}
	}
	for {
		for !exhausted && firstErr == nil && inflight < e.maxInflightPerGroup {
			plan, err := plans.next(ctx)
			if err != nil {
				recordError(err)
				break
			}
			if len(plan) == 0 {
				exhausted = true
				break
			}
			inflight++
			go func() {
				ids, err := e.runPlan(ctx, cg, plan, overlappingBlocks)
				results <- outcome{ids: ids, err: err}
			}()
		}
		if inflight == 0 {
			break
		}
		// Drain already dispatched work even after an error. In particular a later
		// halt must take precedence over an earlier retryable failure. No more work
		// is dispatched once an error is observed.
		result := <-results
		inflight--
		switch {
		case result.err == nil:
			completed = true
			compIDs = append(compIDs, result.ids...)
		case errors.Is(result.err, compact.ErrPlanDeferred):
			// A parked plan consumes no worker capacity. Continue searching beyond it.
		default:
			recordError(result.err)
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}
	if !completed {
		return nil, compact.ErrPlanDeferred
	}
	return compIDs, nil
}

type planSpan struct{ min, max int64 }

func planEnvelope(metas []*metadata.Meta) planSpan {
	span := planSpan{metas[0].MinTime, metas[0].MaxTime}
	for _, m := range metas[1:] {
		span.min = min(span.min, m.MinTime)
		span.max = max(span.max, m.MaxTime)
	}
	return span
}

// groupPlans searches one immutable group snapshot. Removing source IDs can
// expose plans spanning the holes, so both accepted and skipped envelopes are
// reserved. Skipping a conflict must not hide its sources from overlap checks.
type groupPlans struct {
	executor *RemotePlanExecutor
	group    *compact.Group
	first    []*metadata.Meta
	excluded map[ulid.ULID]struct{}
	reserved []planSpan
}

func (p *groupPlans) next(ctx context.Context) ([]*metadata.Meta, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		plan := p.first
		p.first = nil
		if len(plan) == 0 {
			if p.executor.planner == nil {
				return nil, nil
			}
			var err error
			plan, _, err = p.group.PlanExcluding(ctx, p.executor.planner, p.excluded, make(chan error, 1))
			if err != nil {
				return nil, err
			}
			if len(plan) == 0 {
				return nil, nil
			}
		}
		span := planEnvelope(plan)
		overlaps := false
		for _, taken := range p.reserved {
			if span.min < taken.max && taken.min < span.max {
				overlaps = true
				break
			}
		}
		for _, m := range plan {
			p.excluded[m.ULID] = struct{}{}
		}
		p.reserved = append(p.reserved, span)
		if overlaps {
			level.Debug(p.executor.logger).Log("msg", "deferring overlapping plan until the next metadata sync", "group", p.group.Key(), "span_min", span.min, "span_max", span.max)
			continue
		}
		return plan, nil
	}
}
