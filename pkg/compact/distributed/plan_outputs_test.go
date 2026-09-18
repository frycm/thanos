// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"context"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
)

// TestInteractionPlanOutputsAreProducedAsPlanned: the manager's planner
// decides what a plan produces; the task carries that decision to the worker,
// which produces exactly those blocks, and the manager verifies each result
// block against one of the plan's outputs before it retires the sources.
func TestInteractionPlanOutputsAreProducedAsPlanned(t *testing.T) {
	c := newTestCluster(t)
	c.startWorker("w1")

	cg, toCompact := c.makeGroup(labels.FromStrings("ext", "1"))
	outputs := []compact.PlanOutput{
		{Labels: map[string]string{"ext": "1", "part": "a"}, Series: &compact.SeriesPartition{Index: 0, Count: 2}},
		{Labels: map[string]string{"ext": "1", "part": "b"}, Series: &compact.SeriesPartition{Index: 1, Count: 2}},
	}
	// The blocks hold the series a=1 and a=2; an output whose partition holds
	// neither produces no block, so the expected count follows the hashes.
	wantBlocks := map[string]struct{}{}
	for _, lset := range []labels.Labels{labels.FromStrings("a", "1"), labels.FromStrings("a", "2")} {
		for _, o := range outputs {
			if o.Series.Contains(lset) {
				wantBlocks[labels.FromMap(o.Labels).String()] = struct{}{}
			}
		}
	}

	outcome := c.executePlan(cg, compact.Plan{Sources: toCompact, Outputs: outputs})
	var got executeOutcome
	select {
	case got = <-outcome:
	case <-time.After(30 * time.Second):
		t.Fatal("compaction did not finish")
	}
	testutil.Ok(t, got.err)
	testutil.Equals(t, len(wantBlocks), len(got.compIDs))

	entry := c.journalTask(StateCompleted)
	testutil.Assert(t, entry != nil, "the journal must record the completed task")
	testutil.Equals(t, outputs, entry.Task.Outputs, "the task must carry the plan's outputs to the worker")

	gotBlocks := map[string]struct{}{}
	for _, id := range got.compIDs {
		meta, err := block.DownloadMeta(context.Background(), c.logger, c.shared, id)
		testutil.Ok(t, err)
		key := labels.FromMap(meta.Thanos.Labels).String()
		_, dup := gotBlocks[key]
		testutil.Assert(t, !dup, "two result blocks carry %s", key)
		gotBlocks[key] = struct{}{}
		testutil.Equals(t, []string{toCompact[0].ULID.String(), toCompact[1].ULID.String()}, mustProvenance(t, &meta).Sources)
	}
	testutil.Equals(t, wantBlocks, gotBlocks)

	// The sources were retired on the strength of the verification.
	for _, m := range toCompact {
		var mark metadata.DeletionMark
		testutil.Ok(t, metadata.ReadMarker(context.Background(), c.logger, objstore.WithNoopInstr(c.shared), m.ULID.String(), &mark))
	}
}

func mustProvenance(t *testing.T, m *metadata.Meta) Provenance {
	t.Helper()
	prov, ok := ProvenanceOf(m)
	testutil.Assert(t, ok, "result block %s carries no provenance", m.ULID)
	return prov
}

// TestClaimOutputs pins down the verifier's rule for the outputs of a plan:
// for a plan without outputs, one block with the group's labels; for a plan
// with outputs, an account of every output, each block the output it claims,
// no output claimed twice, and no output silently missing.
func TestClaimOutputs(t *testing.T) {
	cg, _ := newTestCluster(t).makeGroup(labels.FromStrings("ext", "1"))
	group := map[string]string{"ext": "1"}
	a := map[string]string{"ext": "1", "part": "a"}
	b := map[string]string{"ext": "1", "part": "b"}
	idA, idB := ulid.MustNew(10, nil), ulid.MustNew(11, nil)
	meta := func(id ulid.ULID, lbls map[string]string, out *metadata.ThanosOutput) metadata.Meta {
		m := metadata.Meta{}
		m.ULID = id
		m.Thanos.Labels = lbls
		m.Thanos.Output = out
		return m
	}
	set := func(index int, blocks ...ulid.ULID) *metadata.ThanosOutput {
		return &metadata.ThanosOutput{Index: index, Count: 2, Blocks: blocks}
	}
	plan := compact.Plan{Outputs: []compact.PlanOutput{{Labels: a}, {Labels: b}}}
	both := map[ulid.ULID]metadata.Meta{idA: meta(idA, a, set(0, idA, idB)), idB: meta(idB, b, set(1, idA, idB))}
	report := func(outputs ...OutputResult) Result { return Result{Outputs: outputs} }

	t.Run("a plan without outputs names the group's labels once", func(t *testing.T) {
		testutil.Ok(t, claimOutputs(cg, compact.Plan{}, Result{}, map[ulid.ULID]metadata.Meta{idA: meta(idA, group, nil)}))
		testutil.NotOk(t, claimOutputs(cg, compact.Plan{}, Result{}, map[ulid.ULID]metadata.Meta{idA: meta(idA, a, nil)}))
		testutil.NotOk(t, claimOutputs(cg, compact.Plan{}, Result{}, map[ulid.ULID]metadata.Meta{idA: meta(idA, group, nil), idB: meta(idB, group, nil)}))
		testutil.NotOk(t, claimOutputs(cg, compact.Plan{}, report(OutputResult{Index: 0, Block: idA.String()}), map[ulid.ULID]metadata.Meta{idA: meta(idA, group, nil)}),
			"an account of outputs for a plan that named none")
	})
	t.Run("every output accounted for, in any order", func(t *testing.T) {
		testutil.Ok(t, claimOutputs(cg, plan, report(OutputResult{Index: 1, Block: idB.String()}, OutputResult{Index: 0, Block: idA.String()}), both))
	})
	t.Run("a missing output is a missing block, not an empty one", func(t *testing.T) {
		onlyA := map[ulid.ULID]metadata.Meta{idA: meta(idA, a, set(0, idA))}
		testutil.NotOk(t, claimOutputs(cg, plan, report(OutputResult{Index: 0, Block: idA.String()}), onlyA))
		testutil.NotOk(t, claimOutputs(cg, plan, Result{}, onlyA), "no account at all")
	})
	t.Run("an empty output is accounted for explicitly and agrees with the set", func(t *testing.T) {
		onlyA := map[ulid.ULID]metadata.Meta{idA: meta(idA, a, set(0, idA))}
		testutil.Ok(t, claimOutputs(cg, plan, report(OutputResult{Index: 0, Block: idA.String()}, OutputResult{Index: 1}), onlyA))
		// The block says its set has two blocks; the report uploaded one.
		claimsTwo := map[ulid.ULID]metadata.Meta{idA: meta(idA, a, set(0, idA, idB))}
		testutil.NotOk(t, claimOutputs(cg, plan, report(OutputResult{Index: 0, Block: idA.String()}, OutputResult{Index: 1}), claimsTwo))
	})
	t.Run("a block must be the output it is claimed for", func(t *testing.T) {
		swapped := map[ulid.ULID]metadata.Meta{idA: meta(idA, a, set(0, idA, idB)), idB: meta(idB, b, set(1, idA, idB))}
		testutil.NotOk(t, claimOutputs(cg, plan, report(OutputResult{Index: 0, Block: idB.String()}, OutputResult{Index: 1, Block: idA.String()}), swapped), "labels of the wrong output")
		mislabeled := map[ulid.ULID]metadata.Meta{idA: meta(idA, a, set(0, idA, idB)), idB: meta(idB, b, set(0, idA, idB))}
		testutil.NotOk(t, claimOutputs(cg, plan, report(OutputResult{Index: 0, Block: idA.String()}, OutputResult{Index: 1, Block: idB.String()}), mislabeled), "the block says it is another output")
		unstamped := map[ulid.ULID]metadata.Meta{idA: meta(idA, a, nil), idB: meta(idB, b, nil)}
		testutil.NotOk(t, claimOutputs(cg, plan, report(OutputResult{Index: 0, Block: idA.String()}, OutputResult{Index: 1, Block: idB.String()}), unstamped), "no set recorded")
	})
	t.Run("no output twice, no block twice, nothing unaccounted", func(t *testing.T) {
		testutil.NotOk(t, claimOutputs(cg, plan, report(OutputResult{Index: 0, Block: idA.String()}, OutputResult{Index: 0, Block: idB.String()}), both))
		testutil.NotOk(t, claimOutputs(cg, plan, report(OutputResult{Index: 0, Block: idA.String()}, OutputResult{Index: 1, Block: idA.String()}), both))
		testutil.NotOk(t, claimOutputs(cg, plan, report(OutputResult{Index: 0, Block: idA.String()}, OutputResult{Index: 1}), both), "an uploaded block accounted for nowhere")
		testutil.NotOk(t, claimOutputs(cg, plan, report(OutputResult{Index: 0, Block: idA.String()}, OutputResult{Index: 2, Block: idB.String()}), both), "an output the plan did not name")
	})
}

// TestOversizedReasonJudgesPerOutput: a plan that spreads its sources over
// several outputs is measured by what each output has to hold.
func TestOversizedReasonJudgesPerOutput(t *testing.T) {
	conf := ManagerConfig{MaxTaskSeries: 100, MaxTaskIndexBytes: 1000}
	whole := Task{SourceBlocks: []string{"a", "b"}, ExpectedSeries: 300, ExpectedIndexBytes: 3000}
	testutil.Assert(t, oversizedReason(whole, conf) != "", "300 series in one block exceed the limit")

	split := whole
	for i := range 4 {
		split.Outputs = append(split.Outputs, compact.PlanOutput{Series: &compact.SeriesPartition{Index: uint64(i), Count: 4}})
	}
	testutil.Equals(t, "", oversizedReason(split, conf), "75 series and 750 bytes per output fit")

	tooBig := whole
	tooBig.Outputs = split.Outputs[:2]
	testutil.Assert(t, oversizedReason(tooBig, conf) != "", "150 series per output still exceed the limit")
}
