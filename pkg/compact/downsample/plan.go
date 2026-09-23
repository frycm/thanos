// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package downsample

import (
	"maps"
	"slices"

	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/block/metadata"
)

// Candidate is a block that should be downsampled, and the resolution it should
// be downsampled to.
type Candidate struct {
	Meta             *metadata.Meta
	TargetResolution int64
}

// Plan returns the blocks that need downsampling, in a deterministic order.
//
// A block is a candidate when no downsampled block covering all of its sources
// exists yet, it is not marked no-downsample, and it spans enough time to yield
// roughly two chunks at the target resolution. Planning is separated from doing
// the work so that the same decision can drive either downsampling in process
// or dispatching the work to a worker.
//
// The minimum-span rule exists only because a still-growing block is worth
// waiting for. A block the compactor can provably never touch again would
// otherwise never be downsampled at all. When enableStuckBlocks is true, the
// rule is waived for those blocks (see stuckBelowRange). Both marker maps may
// be nil when the caller has no marker information; then only the span rule
// applies and no block is excluded.
//
// Blocks marked no-downsample are excluded here, not by the callers: excluding
// them earlier (by deleting them from metas) would also delete the fences and
// coverage needed by the waiver. With the feature disabled, marked blocks do
// not contribute coverage, preserving the compactor's previous filtered view.
func Plan(metas map[ulid.ULID]*metadata.Meta, noCompactMarked map[ulid.ULID]*metadata.NoCompactMark, noDownsampleMarked map[ulid.ULID]*metadata.NoDownsampleMark, enableStuckBlocks bool) ([]Candidate, error) {
	// Blocks whose sources are already covered by a downsampled block do not need
	// downsampling again. Coverage is per block stream: a downsampled block
	// covers a source only for blocks with exactly its external labels. Blocks
	// of different streams can record the same sources - the outputs of a
	// compaction split by series do - while each holds other series, so a
	// source ULID alone says nothing about which series have been downsampled.
	sources5m := coverage{}
	sources1h := coverage{}

	for _, m := range metas {
		if !enableStuckBlocks {
			if _, marked := noDownsampleMarked[m.ULID]; marked {
				continue
			}
		}
		switch m.Thanos.Downsample.Resolution {
		case ResLevel0:
			continue
		case ResLevel1:
			sources5m.add(m)
		case ResLevel2:
			sources1h.add(m)
		default:
			return nil, errors.Errorf("unexpected downsampling resolution %d", m.Thanos.Downsample.Resolution)
		}
	}

	ids := slices.SortedFunc(maps.Keys(metas), func(a, b ulid.ULID) int { return a.Compare(b) })

	// Group membership is needed only to judge stuck blocks, and only
	// index-size no-compact marks make a block provably final, so the whole
	// waiver machinery - including the per-block GroupKey hashing - is skipped
	// when no such mark is in view. GroupKey includes the resolution, so fences
	// and siblings never cross resolution levels.
	//
	// The groups are built from metas, so a marked block that other machinery
	// removed from view (a deletion mark, a time partition) cannot fence; its
	// removal also removes the data the fence would have protected, so nothing
	// is silently starved by that.
	byGroup := map[string][]*metadata.Meta{}
	indexSizeMarked := func(id ulid.ULID) bool {
		mark, ok := noCompactMarked[id]
		return ok && mark.Reason == metadata.IndexSizeExceedingNoCompactReason
	}
	haveIndexSizeMarks := false
	if enableStuckBlocks {
		for id := range noCompactMarked {
			if indexSizeMarked(id) {
				haveIndexSizeMarks = true
				break
			}
		}
	}
	if haveIndexSizeMarks {
		for _, m := range metas {
			key := m.Thanos.GroupKey()
			byGroup[key] = append(byGroup[key], m)
		}
	}

	var candidates []Candidate
	for _, id := range ids {
		m := metas[id]

		// The operator said not to. The block still fences and still counts as
		// coverage above; only its candidacy is off the table.
		if _, ok := noDownsampleMarked[id]; ok {
			continue
		}

		// Only an index-size mark establishes that a block cannot grow safely.
		// Other marks (for example out-of-order chunks) must not acquire the
		// short-block waiver merely because index-size fences surround them.
		_, marked := noCompactMarked[id]
		waiverAllowed := !marked || indexSizeMarked(id)

		switch m.Thanos.Downsample.Resolution {
		case ResLevel2:
			continue

		case ResLevel0:
			if sources5m.covers(m) {
				continue
			}
			// Only downsample blocks once we are sure to get roughly 2 chunks out of it.
			// NOTE(fabxc): this must match with at which block size the compactor creates downsampled
			// blocks. Otherwise we may never downsample some data.
			if m.MaxTime-m.MinTime < ResLevel1DownsampleRange &&
				(!waiverAllowed || !haveIndexSizeMarks || !stuckBelowRange(m, indexSizeMarked, byGroup, ResLevel1DownsampleRange)) {
				continue
			}
			candidates = append(candidates, Candidate{Meta: m, TargetResolution: ResLevel1})

		case ResLevel1:
			if sources1h.covers(m) {
				continue
			}
			if m.MaxTime-m.MinTime < ResLevel2DownsampleRange &&
				(!waiverAllowed || !haveIndexSizeMarks || !stuckBelowRange(m, indexSizeMarked, byGroup, ResLevel2DownsampleRange)) {
				continue
			}
			candidates = append(candidates, Candidate{Meta: m, TargetResolution: ResLevel2})
		}
	}
	return candidates, nil
}

// maxCompactionRange is the compactor's largest compaction range (14d, the top
// of the ladder registered in cmd/thanos/compact.go). The planner aligns its
// buckets to absolute multiples of this range, so no merge ever crosses such a
// boundary - which both bounds how far a fenced block can grow and lets the
// fence window be clamped to the block's own bucket. It is an upper bound:
// --debug.max-compaction-level can only cut the ladder short, which shrinks
// the windows merges can reach and never lets a block grow past this one.
const maxCompactionRange = int64(14 * 24 * 60 * 60 * 1000)

// stuckBelowRange reports whether the block is final - the compactor can
// provably never produce a bigger block containing its data - yet below the
// downsample range tr, so waiting for it to grow is pointless.
//
// Only a no-compact mark for exceeding the index size makes a block final by
// itself: any other reason (out-of-order chunks, manual) is routinely lifted,
// and a block whose mark might be lifted is left alone - downsampling it early
// would leave overlapping downsampled outputs behind once it grows. The
// compactor never lifts an index-size mark; an operator may, as the block
// splitting rollout does once splitting can take such blocks. A block waived
// before that grows on afterwards, and its early downsampled block, whose
// sources the grown block's downsampled output also holds, is superseded by
// that output.
//
// An unmarked block is final when index-size marked blocks of its own group
// fence it in on both sides, the reachable window between them is below tr,
// and it is the LAST still-compactable block inside that window. The last
// condition is what makes the waiver safe rather than merely plausible: any
// other unmarked block intersecting the window - a contiguous sibling, or an
// overlapping replica under vertical compaction - can still merge with this
// one, and downsampling before that merge produces overlapping downsampled
// blocks, which halt the compactor. Such a window simply is not done yet; once
// its blocks have merged into one, that one block passes this test and is
// downsampled exactly once.
//
// "Inside the window" follows the planner's notion, not time coverage alone:
// the planner forms a merge run from every unmarked block that sorts between
// two marked ones by MinTime. An unmarked block nested inside the previous
// fence - a late replica of a range the fence already spans - sorts after the
// fence and lands in this block's run although it ends before the window
// starts, so blocks starting anywhere after the previous fence's MinTime are
// siblings too.
//
// No block overlapping another one is ever waived, whether either of them is
// marked: two overlapping final blocks are each provably done, but waiving
// both leaves two downsampled blocks with the same labels and disjoint
// sources over one range, which the deduplication filter cannot collapse, the
// querier cannot deduplicate, and the downsample planner then fences with
// no-compact marks of its own - the next resolution is never reached. Such
// pairs stay raw, as they do upstream. For the same reason a block is not
// waived while a block at the target resolution already intersects it without
// containing all of its sources; that earlier downsample has to age out first.
//
// The window is clamped to the block's maxCompactionRange-aligned bucket,
// because merges never cross those boundaries: a fence gap that only exceeds
// tr across such a boundary is unreachable, and the block is stuck all the
// same. The block itself may reach past the bucket - vertical merges are not
// aligned - so siblings are tested against the window widened to the block's
// own extent. Blocks at the edge of the group (no index-size mark on one side)
// are never considered stuck - old data cannot arrive to grow the oldest run,
// but distinguishing that from a slowly backfilling stream is not worth the
// risk of downsampling prematurely.
func stuckBelowRange(m *metadata.Meta, indexSizeMarked func(ulid.ULID) bool, byGroup map[string][]*metadata.Meta, tr int64) bool {
	group := byGroup[m.Thanos.GroupKey()]

	// Whatever the block's own state, an overlapping sibling means it is not
	// alone over its range: unmarked, the sibling can still merge into a block
	// spanning this one, which would then be downsampled too; marked, the
	// sibling is a second final block over the same range, and both waived
	// would give two downsampled blocks nobody can deduplicate.
	for _, b := range group {
		if b.ULID != m.ULID && b.MinTime < m.MaxTime && m.MinTime < b.MaxTime {
			return false
		}
	}
	if overlapsTargetResolution(m, byGroup, tr) {
		return false
	}

	if indexSizeMarked(m.ULID) {
		return true
	}

	var (
		prevMax, prevMin, nextMin int64
		havePrev, haveNext        bool
	)
	for _, b := range group {
		if !indexSizeMarked(b.ULID) {
			continue
		}
		if b.MaxTime <= m.MinTime && (!havePrev || b.MaxTime > prevMax) {
			prevMax = b.MaxTime
			havePrev = true
		}
		if b.MinTime <= m.MinTime && b.MinTime > prevMin {
			// The latest-starting fence before this block: the planner's run
			// containing this block begins right after it in MinTime order.
			prevMin = b.MinTime
		}
		if b.MinTime >= m.MaxTime && (!haveNext || b.MinTime < nextMin) {
			nextMin = b.MinTime
			haveNext = true
		}
	}
	if !havePrev || !haveNext {
		return false
	}

	bucketStart := (m.MinTime / maxCompactionRange) * maxCompactionRange
	lo := max(prevMax, bucketStart)
	hi := min(nextMin, bucketStart+maxCompactionRange)
	if hi-lo >= tr {
		return false
	}

	// The last-block-standing condition: nothing else in the window may still
	// be compactable, or a future merge inside it changes this block's data.
	// The window is widened to the block's own extent, which can reach past
	// its bucket; a sibling overlapping only that tail is a pending merge too.
	// So is a sibling that merely starts after the previous fence began: it
	// sorts into this block's merge run even if it ends before the window.
	lo, hi = min(lo, m.MinTime), max(hi, m.MaxTime)
	runStart := max(prevMin, bucketStart)
	for _, b := range group {
		if b.ULID == m.ULID || indexSizeMarked(b.ULID) {
			continue
		}
		if b.MinTime < hi && (lo < b.MaxTime || b.MinTime >= runStart) {
			return false
		}
	}
	return true
}

// overlapsTargetResolution reports whether a block at the target resolution
// with the same labels already intersects m and would survive m's downsample.
// The deduplication filter hides a downsampled block whose sources are all
// among another's, so a downsample of an earlier, smaller shape of the same
// data - a sibling waived before it merged into m - is superseded by m's and
// garbage collected: no reason to wait. One with sources m does not have - a
// replica's, or a block since deleted - would stay next to m's for good, two
// downsampled blocks over one range that nothing can deduplicate; m keeps
// waiting, as it would upstream.
func overlapsTargetResolution(m *metadata.Meta, byGroup map[string][]*metadata.Meta, tr int64) bool {
	target := m.Thanos
	target.Downsample.Resolution = targetResolution(tr)
	for _, b := range byGroup[target.GroupKey()] {
		if b.MinTime >= m.MaxTime || m.MinTime >= b.MaxTime {
			continue
		}
		if !slices.ContainsFunc(b.Compaction.Sources, func(id ulid.ULID) bool { return !slices.Contains(m.Compaction.Sources, id) }) {
			continue // Every source of b is in m: b is superseded by m's downsample.
		}
		return true
	}
	return false
}

// targetResolution maps a downsample range to the resolution it produces.
func targetResolution(tr int64) int64 {
	if tr == ResLevel1DownsampleRange {
		return ResLevel1
	}
	return ResLevel2
}

// coverage records which sources the downsampled blocks of a block stream -
// one set of external labels - account for.
type coverage map[coverageKey]struct{}

type coverageKey struct {
	stream uint64
	source ulid.ULID
}

func streamOf(m *metadata.Meta) uint64 {
	return labels.FromMap(m.Thanos.Labels).Hash()
}

func (c coverage) add(m *metadata.Meta) {
	stream := streamOf(m)
	for _, id := range m.Compaction.Sources {
		c[coverageKey{stream: stream, source: id}] = struct{}{}
	}
}

// covers reports whether every source of the block already appears in a
// downsampled block of the same stream.
func (c coverage) covers(m *metadata.Meta) bool {
	stream := streamOf(m)
	for _, id := range m.Compaction.Sources {
		if _, ok := c[coverageKey{stream: stream, source: id}]; !ok {
			return false
		}
	}
	return true
}
