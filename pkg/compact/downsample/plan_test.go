// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package downsample

import (
	"fmt"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/oklog/ulid/v2"

	"github.com/thanos-io/thanos/pkg/block/metadata"
)

func planMeta(id ulid.ULID, resolution int64, mint, maxt int64, sources ...ulid.ULID) *metadata.Meta {
	m := &metadata.Meta{}
	m.ULID = id
	m.MinTime = mint
	m.MaxTime = maxt
	m.Thanos.Downsample.Resolution = resolution
	if len(sources) == 0 {
		sources = []ulid.ULID{id}
	}
	m.Compaction.Sources = sources
	return m
}

func TestPlanPicksBlocksThatNeedDownsampling(t *testing.T) {
	raw := ulid.MustNew(1, nil)
	metas := map[ulid.ULID]*metadata.Meta{
		raw: planMeta(raw, ResLevel0, 0, ResLevel1DownsampleRange),
	}

	got, err := Plan(metas, nil, nil, false)
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(got))
	testutil.Equals(t, raw, got[0].Meta.ULID)
	testutil.Equals(t, ResLevel1, got[0].TargetResolution)
}

func TestPlanSkipsBlocksThatAreTooShort(t *testing.T) {
	raw := ulid.MustNew(1, nil)
	metas := map[ulid.ULID]*metadata.Meta{
		// One millisecond short of the range that yields two chunks.
		raw: planMeta(raw, ResLevel0, 0, ResLevel1DownsampleRange-1),
	}

	got, err := Plan(metas, nil, nil, false)
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(got))
}

func TestPlanSkipsBlocksAlreadyCovered(t *testing.T) {
	raw := ulid.MustNew(1, nil)
	already := ulid.MustNew(2, nil)

	metas := map[ulid.ULID]*metadata.Meta{
		raw: planMeta(raw, ResLevel0, 0, ResLevel1DownsampleRange),
		// A 5m block built from that raw block already exists.
		already: planMeta(already, ResLevel1, 0, ResLevel1DownsampleRange, raw),
	}

	got, err := Plan(metas, nil, nil, false)
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(got))
}

func TestPlanAdvancesFiveMinuteBlocksToOneHour(t *testing.T) {
	fiveMin := ulid.MustNew(1, nil)
	metas := map[ulid.ULID]*metadata.Meta{
		fiveMin: planMeta(fiveMin, ResLevel1, 0, ResLevel2DownsampleRange),
	}

	got, err := Plan(metas, nil, nil, false)
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(got))
	testutil.Equals(t, ResLevel2, got[0].TargetResolution)
}

func TestPlanIgnoresFullyDownsampledBlocks(t *testing.T) {
	oneHour := ulid.MustNew(1, nil)
	metas := map[ulid.ULID]*metadata.Meta{
		oneHour: planMeta(oneHour, ResLevel2, 0, ResLevel2DownsampleRange),
	}

	got, err := Plan(metas, nil, nil, false)
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(got))
}

func TestPlanRejectsUnknownResolution(t *testing.T) {
	odd := ulid.MustNew(1, nil)
	metas := map[ulid.ULID]*metadata.Meta{
		odd: planMeta(odd, 1234, 0, 100),
	}

	_, err := Plan(metas, nil, nil, false)
	testutil.NotOk(t, err)
}

func noCompactMark(id ulid.ULID, reason metadata.NoCompactReason) *metadata.NoCompactMark {
	return &metadata.NoCompactMark{ID: id, Version: metadata.NoCompactMarkVersion1, Reason: reason}
}

func TestPlanStuckBlocksRequireOptIn(t *testing.T) {
	for _, resolution := range []int64{ResLevel0, ResLevel1} {
		t.Run(fmt.Sprint(resolution), func(t *testing.T) {
			left, mid, right, normal := ulid.MustNew(1, nil), ulid.MustNew(2, nil), ulid.MustNew(3, nil), ulid.MustNew(4, nil)
			metas := map[ulid.ULID]*metadata.Meta{
				left:   planMeta(left, resolution, 0, 100),
				mid:    planMeta(mid, resolution, 200, 300),
				right:  planMeta(right, resolution, 400, 500),
				normal: planMeta(normal, resolution, 600, 600+ResLevel2DownsampleRange),
			}
			marks := map[ulid.ULID]*metadata.NoCompactMark{
				left:  noCompactMark(left, metadata.IndexSizeExceedingNoCompactReason),
				right: noCompactMark(right, metadata.IndexSizeExceedingNoCompactReason),
			}
			// Both permanently marked and fenced short blocks must wait with
			// the feature off. Normal downsampling remains enabled.
			for _, enabled := range []bool{false, true, false} {
				got, err := Plan(metas, marks, nil, enabled)
				testutil.Ok(t, err)
				want := []ulid.ULID{normal}
				if enabled {
					want = []ulid.ULID{left, mid, right, normal}
				}
				var ids []ulid.ULID
				for _, c := range got {
					ids = append(ids, c.Meta.ULID)
				}
				testutil.Equals(t, want, ids)
			}
		})
	}
}

func TestPlanDisabledPreservesNoDownsampleFilteredCoverage(t *testing.T) {
	for _, resolution := range []int64{ResLevel0, ResLevel1} {
		t.Run(fmt.Sprint(resolution), func(t *testing.T) {
			source, coarse := ulid.MustNew(1, nil), ulid.MustNew(2, nil)
			target := ResLevel1
			if resolution == ResLevel1 {
				target = ResLevel2
			}
			metas := map[ulid.ULID]*metadata.Meta{
				source: planMeta(source, resolution, 0, ResLevel2DownsampleRange),
				coarse: planMeta(coarse, target, 0, ResLevel2DownsampleRange, source),
			}
			marks := map[ulid.ULID]*metadata.NoDownsampleMark{coarse: {ID: coarse}}
			// The previous compactor removed marked blocks before planning.
			got, err := Plan(metas, nil, marks, false)
			testutil.Ok(t, err)
			testutil.Equals(t, 1, len(got))
			testutil.Equals(t, source, got[0].Meta.ULID)
			// The opt-in planner retains marked blocks as coverage.
			got, err = Plan(metas, nil, marks, true)
			testutil.Ok(t, err)
			testutil.Equals(t, 0, len(got))
			testutil.Equals(t, 2, len(metas), "planning must not remove metadata from the caller's view")
			// A no-downsample mark still excludes the source with either policy.
			marks[source] = &metadata.NoDownsampleMark{ID: source}
			for _, enabled := range []bool{false, true} {
				got, err = Plan(metas, nil, marks, enabled)
				testutil.Ok(t, err)
				testutil.Equals(t, 0, len(got))
			}
		})
	}
}

func TestPlanDownsamplesShortBlockMarkedForIndexSize(t *testing.T) {
	raw := ulid.MustNew(1, nil)
	metas := map[ulid.ULID]*metadata.Meta{
		// Too short on its own, but marked: it will never grow.
		raw: planMeta(raw, ResLevel0, 0, ResLevel1DownsampleRange-1),
	}
	marks := map[ulid.ULID]*metadata.NoCompactMark{
		raw: noCompactMark(raw, metadata.IndexSizeExceedingNoCompactReason),
	}

	got, err := Plan(metas, marks, nil, true)
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(got))
	testutil.Equals(t, raw, got[0].Meta.ULID)
	testutil.Equals(t, ResLevel1, got[0].TargetResolution)
}

func TestPlanLeavesShortBlocksMarkedForOtherReasonsAlone(t *testing.T) {
	raw := ulid.MustNew(1, nil)
	metas := map[ulid.ULID]*metadata.Meta{
		raw: planMeta(raw, ResLevel0, 0, ResLevel1DownsampleRange-1),
	}
	marks := map[ulid.ULID]*metadata.NoCompactMark{
		raw: noCompactMark(raw, metadata.OutOfOrderChunksNoCompactReason),
	}

	got, err := Plan(metas, marks, nil, true)
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(got))
}

func TestPlanDownsamplesShortBlockFencedInByMarkedBlocks(t *testing.T) {
	left := ulid.MustNew(1, nil)
	mid := ulid.MustNew(2, nil)
	right := ulid.MustNew(3, nil)
	// The window between the marked neighbors is below the downsample range, so
	// mid can never be compacted into a block that spans enough time.
	metas := map[ulid.ULID]*metadata.Meta{
		left:  planMeta(left, ResLevel0, 0, 100),
		mid:   planMeta(mid, ResLevel0, 200, 300),
		right: planMeta(right, ResLevel0, 400, 500),
	}
	marks := map[ulid.ULID]*metadata.NoCompactMark{
		left:  noCompactMark(left, metadata.IndexSizeExceedingNoCompactReason),
		right: noCompactMark(right, metadata.IndexSizeExceedingNoCompactReason),
	}

	got, err := Plan(metas, marks, nil, true)
	testutil.Ok(t, err)
	// left and right for being marked, mid for being fenced in.
	testutil.Equals(t, 3, len(got))
}

func TestPlanWaitsWhenFencedWindowIsStillLargeEnough(t *testing.T) {
	left := ulid.MustNew(1, nil)
	mid := ulid.MustNew(2, nil)
	right := ulid.MustNew(3, nil)
	metas := map[ulid.ULID]*metadata.Meta{
		left:  planMeta(left, ResLevel0, 0, 100),
		mid:   planMeta(mid, ResLevel0, 200, 300),
		right: planMeta(right, ResLevel0, 100+ResLevel1DownsampleRange, 200+ResLevel1DownsampleRange),
	}
	marks := map[ulid.ULID]*metadata.NoCompactMark{
		left:  noCompactMark(left, metadata.IndexSizeExceedingNoCompactReason),
		right: noCompactMark(right, metadata.IndexSizeExceedingNoCompactReason),
	}

	got, err := Plan(metas, marks, nil, true)
	testutil.Ok(t, err)
	// The marked fences are candidates themselves, but mid can still grow to
	// the full window between them, which reaches the downsample range, so it
	// keeps waiting.
	testutil.Equals(t, 2, len(got))
	for _, c := range got {
		testutil.Assert(t, c.Meta.ULID != mid, "mid must keep waiting")
	}
}

func TestPlanWaitsWhenOnlyOneSideIsFenced(t *testing.T) {
	left := ulid.MustNew(1, nil)
	mid := ulid.MustNew(2, nil)
	metas := map[ulid.ULID]*metadata.Meta{
		left: planMeta(left, ResLevel0, 0, 100),
		mid:  planMeta(mid, ResLevel0, 200, 300),
	}
	marks := map[ulid.ULID]*metadata.NoCompactMark{
		left: noCompactMark(left, metadata.IndexSizeExceedingNoCompactReason),
	}

	got, err := Plan(metas, marks, nil, true)
	testutil.Ok(t, err)
	// Only the marked block itself is a candidate; mid is open towards new data.
	testutil.Equals(t, 1, len(got))
	testutil.Equals(t, left, got[0].Meta.ULID)
}

func TestPlanIgnoresMarkedBlocksFromOtherCompactionGroups(t *testing.T) {
	left := ulid.MustNew(1, nil)
	mid := ulid.MustNew(2, nil)
	right := ulid.MustNew(3, nil)
	metas := map[ulid.ULID]*metadata.Meta{
		left:  planMeta(left, ResLevel0, 0, 100),
		mid:   planMeta(mid, ResLevel0, 200, 300),
		right: planMeta(right, ResLevel0, 400, 500),
	}
	// The fence blocks belong to a different stream, so they say nothing about
	// how far mid can grow.
	metas[left].Thanos.Labels = map[string]string{"tenant": "other"}
	metas[right].Thanos.Labels = map[string]string{"tenant": "other"}
	marks := map[ulid.ULID]*metadata.NoCompactMark{
		left:  noCompactMark(left, metadata.IndexSizeExceedingNoCompactReason),
		right: noCompactMark(right, metadata.IndexSizeExceedingNoCompactReason),
	}

	got, err := Plan(metas, marks, nil, true)
	testutil.Ok(t, err)
	// The marked blocks of the other stream are candidates in their own right,
	// but they say nothing about how far mid can grow.
	testutil.Equals(t, 2, len(got))
	for _, c := range got {
		testutil.Assert(t, c.Meta.ULID != mid, "mid must keep waiting")
	}
}

// TestPlanIsDeterministic asserts the same bucket state always produces the same
// order of work, which matters when the plan drives task dispatch.
func TestPlanIsDeterministic(t *testing.T) {
	metas := map[ulid.ULID]*metadata.Meta{}
	for i := range 5 {
		id := ulid.MustNew(uint64(i+1), nil)
		metas[id] = planMeta(id, ResLevel0, 0, ResLevel1DownsampleRange)
	}

	first, err := Plan(metas, nil, nil, false)
	testutil.Ok(t, err)
	for range 5 {
		got, err := Plan(metas, nil, nil, false)
		testutil.Ok(t, err)
		testutil.Equals(t, len(first), len(got))
		for j := range first {
			testutil.Equals(t, first[j].Meta.ULID, got[j].Meta.ULID)
		}
	}
}

// The tests below pin down the review findings on the stuck-block waiver: the
// waiver may only fire when the compactor can provably never touch the block
// again, or downsampling early produces overlapping downsampled outputs.

// TestPlanWaitsForFencedSiblingsToMergeFirst pins down that a fenced window
// holding several still-compactable blocks is not done: downsampling one
// sibling while the planner can still merge them produces overlapping 5m
// blocks with shared sources, which halt the compactor. Only once the window
// has collapsed to a single block is it downsampled - exactly once.
func TestPlanWaitsForFencedSiblingsToMergeFirst(t *testing.T) {
	left := ulid.MustNew(1, nil)
	a := ulid.MustNew(2, nil)
	b := ulid.MustNew(3, nil)
	right := ulid.MustNew(4, nil)
	metas := map[ulid.ULID]*metadata.Meta{
		left:  planMeta(left, ResLevel0, 0, 100),
		a:     planMeta(a, ResLevel0, 200, 300),
		b:     planMeta(b, ResLevel0, 300, 400),
		right: planMeta(right, ResLevel0, 500, 600),
	}
	marks := map[ulid.ULID]*metadata.NoCompactMark{
		left:  noCompactMark(left, metadata.IndexSizeExceedingNoCompactReason),
		right: noCompactMark(right, metadata.IndexSizeExceedingNoCompactReason),
	}

	got, err := Plan(metas, marks, nil, true)
	testutil.Ok(t, err)
	// Only the fences themselves: a and b can still merge with each other.
	testutil.Equals(t, 2, len(got))
	for _, c := range got {
		testutil.Assert(t, c.Meta.ULID != a && c.Meta.ULID != b, "unmerged fenced siblings must keep waiting")
	}

	// Once the compactor has merged them, the survivor is stuck and eligible.
	merged := ulid.MustNew(5, nil)
	delete(metas, a)
	delete(metas, b)
	metas[merged] = planMeta(merged, ResLevel0, 200, 400, a, b)

	got, err = Plan(metas, marks, nil, true)
	testutil.Ok(t, err)
	testutil.Equals(t, 3, len(got))
}

// TestPlanWaitsWhileAnOverlappingSiblingCanStillGrow pins down the vertical
// compaction case: "the planner never merges across a marked block" does not
// hold for overlapping blocks, so a fenced or even permanently-marked block
// with an overlapping still-compactable sibling is not final - the sibling's
// merge will span this data again and be downsampled a second time.
func TestPlanWaitsWhileAnOverlappingSiblingCanStillGrow(t *testing.T) {
	left := ulid.MustNew(1, nil)
	mid := ulid.MustNew(2, nil)
	overlap := ulid.MustNew(3, nil)
	right := ulid.MustNew(4, nil)
	metas := map[ulid.ULID]*metadata.Meta{
		left: planMeta(left, ResLevel0, 0, 100),
		mid:  planMeta(mid, ResLevel0, 200, 300),
		// A replica block overlapping mid and reaching past the fence.
		overlap: planMeta(overlap, ResLevel0, 250, 700),
		right:   planMeta(right, ResLevel0, 400, 500),
	}
	marks := map[ulid.ULID]*metadata.NoCompactMark{
		left:  noCompactMark(left, metadata.IndexSizeExceedingNoCompactReason),
		right: noCompactMark(right, metadata.IndexSizeExceedingNoCompactReason),
	}

	got, err := Plan(metas, marks, nil, true)
	testutil.Ok(t, err)
	for _, c := range got {
		testutil.Assert(t, c.Meta.ULID != mid, "a fenced block with an overlapping compactable sibling must keep waiting")
	}

	// The same guard protects a permanently marked block itself.
	marks[mid] = noCompactMark(mid, metadata.IndexSizeExceedingNoCompactReason)
	got, err = Plan(metas, marks, nil, true)
	testutil.Ok(t, err)
	for _, c := range got {
		testutil.Assert(t, c.Meta.ULID != mid, "a marked block with an overlapping compactable sibling must keep waiting")
	}
}

// TestPlanFencesOnlyOnPermanentMarks pins down that removable no-compact marks
// (manual, out-of-order chunks) never act as fences: the mark can be lifted,
// the fenced block then grows, and its early downsample would overlap the
// grown block's.
func TestPlanFencesOnlyOnPermanentMarks(t *testing.T) {
	left := ulid.MustNew(1, nil)
	mid := ulid.MustNew(2, nil)
	right := ulid.MustNew(3, nil)
	metas := map[ulid.ULID]*metadata.Meta{
		left:  planMeta(left, ResLevel0, 0, 100),
		mid:   planMeta(mid, ResLevel0, 200, 300),
		right: planMeta(right, ResLevel0, 400, 500),
	}
	marks := map[ulid.ULID]*metadata.NoCompactMark{
		left:  noCompactMark(left, metadata.ManualNoCompactReason),
		right: noCompactMark(right, metadata.OutOfOrderChunksNoCompactReason),
	}

	got, err := Plan(metas, marks, nil, true)
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(got))
}

// TestPlanExcludesNoDownsampleMarkedBlocksButKeepsTheirFences pins down that
// the no-downsample exclusion lives in Plan itself: a block marked both
// no-compact(index-size) and no-downsample is never a candidate, yet still
// fences its neighbors and still counts as coverage - deleting it from the
// caller's meta view used to erase the fence and silently revert the
// starvation this waiver exists to fix.
func TestPlanExcludesNoDownsampleMarkedBlocksButKeepsTheirFences(t *testing.T) {
	left := ulid.MustNew(1, nil)
	mid := ulid.MustNew(2, nil)
	right := ulid.MustNew(3, nil)
	metas := map[ulid.ULID]*metadata.Meta{
		left:  planMeta(left, ResLevel0, 0, 100),
		mid:   planMeta(mid, ResLevel0, 200, 300),
		right: planMeta(right, ResLevel0, 400, 500),
	}
	marks := map[ulid.ULID]*metadata.NoCompactMark{
		left:  noCompactMark(left, metadata.IndexSizeExceedingNoCompactReason),
		right: noCompactMark(right, metadata.IndexSizeExceedingNoCompactReason),
	}
	noDownsample := map[ulid.ULID]*metadata.NoDownsampleMark{
		left:  {ID: left, Version: metadata.NoDownsampleMarkVersion1, Reason: metadata.ManualNoDownsampleReason},
		right: {ID: right, Version: metadata.NoDownsampleMarkVersion1, Reason: metadata.ManualNoDownsampleReason},
	}

	got, err := Plan(metas, marks, noDownsample, true)
	testutil.Ok(t, err)
	// The fences are excluded as candidates, but mid is still fenced by them.
	testutil.Equals(t, 1, len(got))
	testutil.Equals(t, mid, got[0].Meta.ULID)
}

// TestPlanClampsFenceWindowToCompactionAlignment pins down that a fence gap is
// measured within the block's maxCompactionRange-aligned bucket: merges never
// cross those boundaries, so a gap that only exceeds the downsample range
// across one is unreachable and the block is stuck all the same.
func TestPlanClampsFenceWindowToCompactionAlignment(t *testing.T) {
	boundary := maxCompactionRange // The first 14d alignment boundary.
	left := ulid.MustNew(1, nil)
	mid := ulid.MustNew(2, nil)
	right := ulid.MustNew(3, nil)
	metas := map[ulid.ULID]*metadata.Meta{
		left: planMeta(left, ResLevel0, boundary-12*3600*1000, boundary-10*3600*1000),
		mid:  planMeta(mid, ResLevel0, boundary-10*3600*1000, boundary-8*3600*1000),
		// The far fence sits past the boundary: the raw gap is 42h >= 40h, but
		// mid can never merge across the boundary, so its reachable span is 10h.
		right: planMeta(right, ResLevel0, boundary+32*3600*1000, boundary+34*3600*1000),
	}
	marks := map[ulid.ULID]*metadata.NoCompactMark{
		left:  noCompactMark(left, metadata.IndexSizeExceedingNoCompactReason),
		right: noCompactMark(right, metadata.IndexSizeExceedingNoCompactReason),
	}

	got, err := Plan(metas, marks, nil, true)
	testutil.Ok(t, err)
	found := false
	for _, c := range got {
		if c.Meta.ULID == mid {
			found = true
		}
	}
	testutil.Assert(t, found, "a block whose reachable span is clipped by the compaction alignment must be stuck")
}

// planned reports whether the block is among the candidates.
func planned(got []Candidate, id ulid.ULID) bool {
	for _, c := range got {
		if c.Meta.ULID == id {
			return true
		}
	}
	return false
}

// TestPlanNeverWaivesOverlappingFinalBlocks pins down that two final blocks
// overlapping each other - a fenced replica block next to a permanently
// marked one, or two marked ones - are both left raw. Each is provably done,
// but waiving both would leave two 5m blocks with the same labels and
// disjoint sources over one range: the deduplication filter hides neither,
// the querier cannot deduplicate them, and the 5m planner fences them with
// no-compact marks of its own, so 1h is never reached for that range.
func TestPlanNeverWaivesOverlappingFinalBlocks(t *testing.T) {
	left := ulid.MustNew(1, nil)
	a := ulid.MustNew(2, nil)
	b1 := ulid.MustNew(3, nil)
	right := ulid.MustNew(4, nil)
	metas := map[ulid.ULID]*metadata.Meta{
		left: planMeta(left, ResLevel0, 0, 100),
		a:    planMeta(a, ResLevel0, 200, 300),
		// Replica B's late block, overlapping a and fenced by it.
		b1:    planMeta(b1, ResLevel0, 250, 350),
		right: planMeta(right, ResLevel0, 400, 500),
	}
	marks := map[ulid.ULID]*metadata.NoCompactMark{
		left:  noCompactMark(left, metadata.IndexSizeExceedingNoCompactReason),
		a:     noCompactMark(a, metadata.IndexSizeExceedingNoCompactReason),
		right: noCompactMark(right, metadata.IndexSizeExceedingNoCompactReason),
	}

	got, err := Plan(metas, marks, nil, true)
	testutil.Ok(t, err)
	testutil.Assert(t, !planned(got, a), "a marked block overlapped by a fenced sibling must stay raw")
	testutil.Assert(t, !planned(got, b1), "a fenced block overlapping a marked sibling must stay raw")

	// Both marked: still two final blocks over one range.
	marks[b1] = noCompactMark(b1, metadata.IndexSizeExceedingNoCompactReason)
	got, err = Plan(metas, marks, nil, true)
	testutil.Ok(t, err)
	testutil.Assert(t, !planned(got, a) && !planned(got, b1), "overlapping marked blocks must both stay raw")
}

// TestPlanWaitsBehindAnUnrelatedDownsampledBlock pins down the guard against
// the same end state reached through the target resolution: a 5m block that
// already intersects the candidate with sources the candidate does not have
// would stay next to the candidate's 5m block for good. A 5m block whose
// sources are all in the candidate is superseded by the candidate's and does
// not hold it back.
func TestPlanWaitsBehindAnUnrelatedDownsampledBlock(t *testing.T) {
	left := ulid.MustNew(1, nil)
	mid := ulid.MustNew(2, nil)
	right := ulid.MustNew(3, nil)
	gone := ulid.MustNew(4, nil)
	down := ulid.MustNew(5, nil)
	metas := map[ulid.ULID]*metadata.Meta{
		left:  planMeta(left, ResLevel0, 0, 100),
		mid:   planMeta(mid, ResLevel0, 200, 300),
		right: planMeta(right, ResLevel0, 400, 500),
		// The 5m block of a replica's block that was waived and has since been
		// deleted; it overlaps mid and shares no source with it.
		down: planMeta(down, ResLevel1, 250, 350, gone),
	}
	marks := map[ulid.ULID]*metadata.NoCompactMark{
		left:  noCompactMark(left, metadata.IndexSizeExceedingNoCompactReason),
		right: noCompactMark(right, metadata.IndexSizeExceedingNoCompactReason),
	}

	got, err := Plan(metas, marks, nil, true)
	testutil.Ok(t, err)
	testutil.Assert(t, !planned(got, mid), "a fenced block must wait while an unrelated 5m block intersects it")

	// The same for a marked block.
	marks[mid] = noCompactMark(mid, metadata.IndexSizeExceedingNoCompactReason)
	got, err = Plan(metas, marks, nil, true)
	testutil.Ok(t, err)
	testutil.Assert(t, !planned(got, mid), "a marked block must wait while an unrelated 5m block intersects it")

	// An earlier downsample of part of mid's own data is superseded by mid's.
	metas[down] = planMeta(down, ResLevel1, 250, 300, gone)
	metas[mid] = planMeta(mid, ResLevel0, 200, 300, mid, gone)
	got, err = Plan(metas, marks, nil, true)
	testutil.Ok(t, err)
	testutil.Assert(t, planned(got, mid), "a 5m block whose sources are all in the candidate must not hold it back")
}

// TestPlanSeesSiblingsPastTheCompactionBoundary pins down that a block
// reaching past its 14d bucket - vertical merges are not aligned - is not
// waived while a sibling overlaps that tail: the window is clamped to the
// bucket, the block is not, and the merge with that sibling is still pending.
func TestPlanSeesSiblingsPastTheCompactionBoundary(t *testing.T) {
	boundary := maxCompactionRange
	hour := int64(3600 * 1000)
	left := ulid.MustNew(1, nil)
	mid := ulid.MustNew(2, nil)
	tail := ulid.MustNew(3, nil)
	right := ulid.MustNew(4, nil)
	metas := map[ulid.ULID]*metadata.Meta{
		left: planMeta(left, ResLevel0, boundary-12*hour, boundary-10*hour),
		// Reaches 2h past the boundary.
		mid: planMeta(mid, ResLevel0, boundary-10*hour, boundary+2*hour),
		// Overlaps mid only beyond the boundary, outside the clamped window.
		tail:  planMeta(tail, ResLevel0, boundary+hour, boundary+4*hour),
		right: planMeta(right, ResLevel0, boundary+32*hour, boundary+34*hour),
	}
	marks := map[ulid.ULID]*metadata.NoCompactMark{
		left:  noCompactMark(left, metadata.IndexSizeExceedingNoCompactReason),
		right: noCompactMark(right, metadata.IndexSizeExceedingNoCompactReason),
	}

	got, err := Plan(metas, marks, nil, true)
	testutil.Ok(t, err)
	testutil.Assert(t, !planned(got, mid), "a block with a sibling overlapping its tail past the boundary must wait for the merge")

	// Merged, the survivor is stuck and waived.
	merged := ulid.MustNew(5, nil)
	delete(metas, mid)
	delete(metas, tail)
	metas[merged] = planMeta(merged, ResLevel0, boundary-10*hour, boundary+4*hour, mid, tail)
	got, err = Plan(metas, marks, nil, true)
	testutil.Ok(t, err)
	testutil.Assert(t, planned(got, merged), "the merged block must be waived")
}

func TestPlanFencedBlockWaiverRespectsNoCompactReason(t *testing.T) {
	for _, resolution := range []int64{ResLevel0, ResLevel1} {
		for _, reason := range []metadata.NoCompactReason{"", metadata.IndexSizeExceedingNoCompactReason, metadata.OutOfOrderChunksNoCompactReason, metadata.ManualNoCompactReason} {
			left, mid, right := ulid.MustNew(1, nil), ulid.MustNew(2, nil), ulid.MustNew(3, nil)
			metas := map[ulid.ULID]*metadata.Meta{
				left:  planMeta(left, resolution, 0, 100),
				mid:   planMeta(mid, resolution, 100, 200),
				right: planMeta(right, resolution, 200, 300),
			}
			marks := map[ulid.ULID]*metadata.NoCompactMark{
				left:  noCompactMark(left, metadata.IndexSizeExceedingNoCompactReason),
				right: noCompactMark(right, metadata.IndexSizeExceedingNoCompactReason),
			}
			if reason != "" {
				marks[mid] = noCompactMark(mid, reason)
			}
			candidates, err := Plan(metas, marks, nil, true)
			testutil.Ok(t, err)
			selected := false
			for _, c := range candidates {
				if c.Meta.ULID == mid {
					selected = true
				}
			}
			testutil.Equals(t, reason == "" || reason == metadata.IndexSizeExceedingNoCompactReason, selected)
		}
	}
}
