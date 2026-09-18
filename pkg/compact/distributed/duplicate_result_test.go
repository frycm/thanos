// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package distributed

import (
	"context"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/extprom"
)

// TestDuplicateResultsReduceToOneBlock is the safety property the whole design
// rests on.
//
// Object storage offers no compare-and-swap, so nothing can strictly prevent a
// task from being executed twice: a worker can be declared dead, have its task
// handed to somebody else, and still finish. Both results are compacted from the
// same source blocks, and the output ULID is minted by the worker, so the bucket
// ends up with two different blocks covering exactly the same sources.
//
// This asserts what happens next: the deduplication filter keeps one and reports
// the other as a duplicate, which garbage collection then marks for deletion. So
// executing a task twice wastes work but cannot corrupt the bucket, and the
// planner never sees both. Leases and the journal make this rare; this is what
// makes it safe.
func TestDuplicateResultsReduceToOneBlock(t *testing.T) {
	// Two source blocks, as a plan would have.
	src1 := ulid.MustNew(1, nil)
	src2 := ulid.MustNew(2, nil)

	// Two results, produced by two workers from those same two sources.
	firstResult := ulid.MustNew(10, nil)
	secondResult := ulid.MustNew(11, nil)

	result := func(id ulid.ULID) *metadata.Meta {
		m := &metadata.Meta{}
		m.ULID = id
		m.MinTime = 0
		m.MaxTime = 1000
		m.Compaction.Level = 2
		m.Compaction.Sources = []ulid.ULID{src1, src2}
		return m
	}

	metas := map[ulid.ULID]*metadata.Meta{
		firstResult:  result(firstResult),
		secondResult: result(secondResult),
	}

	f := block.NewDeduplicateFilter(1)
	synced := extprom.NewTxGaugeVec(nil, prometheus.GaugeOpts{Name: "test_synced"}, []string{"state"})
	synced.ResetTx()
	testutil.Ok(t, f.Filter(context.Background(), metas, synced, nil))

	// Exactly one of the two survives, and the other is reported as a duplicate
	// for garbage collection to clean up.
	testutil.Equals(t, 1, len(metas))
	testutil.Equals(t, 1, len(f.DuplicateIDs()))

	duplicate := f.DuplicateIDs()[0]
	_, stillThere := metas[duplicate]
	testutil.Assert(t, !stillThere, "the block reported as duplicate must not have survived filtering")

	// Whichever survived, it covers the same sources, so no data is lost.
	for _, survivor := range metas {
		testutil.Equals(t, []ulid.ULID{src1, src2}, survivor.Compaction.Sources)
	}
}
