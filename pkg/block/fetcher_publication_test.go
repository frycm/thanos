// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package block

import (
	"maps"
	"slices"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/thanos-io/thanos/pkg/block/metadata"
)

// TestDeduplicateFilterRequiresPublication pins the rule that lets the
// outputs of one compaction replace their sources only as a set: a block that
// names siblings supersedes nothing until every sibling is present, an
// unpublished leftover of an earlier attempt is superseded by the published
// result of a later one, and a caller can declare further blocks unpublished.
func TestDeduplicateFilterRequiresPublication(t *testing.T) {
	ctx := t.Context()
	lbls := map[string]string{"tenant": "a"}
	meta := func(id ulid.ULID, sources ...ulid.ULID) *metadata.Meta {
		if len(sources) == 0 {
			sources = []ulid.ULID{id}
		}
		return &metadata.Meta{
			BlockMeta: tsdb.BlockMeta{ULID: id, Compaction: tsdb.BlockMetaCompaction{Sources: sources}},
			Thanos:    metadata.Thanos{Labels: lbls},
		}
	}
	output := func(id ulid.ULID, index, count int, set []ulid.ULID, sources ...ulid.ULID) *metadata.Meta {
		m := meta(id, sources...)
		m.Thanos.Output = &metadata.ThanosOutput{Index: index, Count: count, Blocks: set}
		return m
	}
	run := func(t *testing.T, published func(*metadata.Meta) bool, input ...*metadata.Meta) []ulid.ULID {
		t.Helper()
		metas := map[ulid.ULID]*metadata.Meta{}
		for _, m := range input {
			metas[m.ULID] = m
		}
		f := NewDeduplicateFilter(1)
		if published != nil {
			f.SetPublishedFunc(published)
		}
		testutil.Ok(t, f.Filter(ctx, metas, newTestFetcherMetrics().Synced, nil))
		return slices.SortedFunc(maps.Keys(metas), func(a, b ulid.ULID) int { return a.Compare(b) })
	}

	set := []ulid.ULID{ULID(10), ULID(11)}
	later := []ulid.ULID{ULID(20), ULID(21)}
	rejected := ULID(10)
	notRejected := func(m *metadata.Meta) bool { return m.ULID != rejected }
	for _, tcase := range []struct {
		name      string
		published func(*metadata.Meta) bool
		input     []*metadata.Meta

		expected []ulid.ULID
		msg      string
	}{
		{
			name: "an incomplete set supersedes nothing",
			input: []*metadata.Meta{
				meta(ULID(1)),
				meta(ULID(2)),
				output(ULID(10), 0, 2, set, ULID(1), ULID(2)),
			},
			expected: ULIDs(1, 2, 10),
			msg:      "the sources stay while a sibling is missing",
		},
		{
			name: "a complete set supersedes its sources",
			input: []*metadata.Meta{
				meta(ULID(1)),
				meta(ULID(2)),
				output(ULID(10), 0, 2, set, ULID(1), ULID(2)),
				output(ULID(11), 1, 2, set, ULID(1), ULID(2)),
			},
			expected: ULIDs(10, 11),
		},
		{
			name: "a published result supersedes the unpublished leftover of an earlier attempt",
			input: []*metadata.Meta{
				meta(ULID(1)),
				meta(ULID(2)),
				// The earlier attempt published one of two blocks and failed.
				output(ULID(10), 0, 2, set, ULID(1), ULID(2)),
				output(ULID(20), 0, 2, later, ULID(1), ULID(2)),
				output(ULID(21), 1, 2, later, ULID(1), ULID(2)),
			},
			expected: ULIDs(20, 21),
			msg:      "the older, unpublished block must not win the tie",
		},
		{
			name:     "a block without a set is its own set",
			input:    []*metadata.Meta{meta(ULID(1)), meta(ULID(2)), meta(ULID(10), ULID(1), ULID(2))},
			expected: ULIDs(10),
		},
		{
			// A tool kept the source's metadata: the set names other blocks, none
			// of them present. The block is judged as its own set.
			name:     "a set copied from a source does not bind the block",
			input:    []*metadata.Meta{meta(ULID(1)), meta(ULID(2)), output(ULID(10), 0, 2, []ulid.ULID{ULID(90), ULID(91)}, ULID(1), ULID(2))},
			expected: ULIDs(10),
		},
		{
			name:      "the caller can declare a block unpublished",
			published: notRejected,
			input: []*metadata.Meta{
				meta(ULID(1)),
				meta(ULID(2)),
				meta(ULID(10), ULID(1), ULID(2)),
			},
			expected: ULIDs(1, 2, 10),
		},
		{
			name:      "the result the caller accepts supersedes the one it declared unpublished",
			published: notRejected,
			input: []*metadata.Meta{
				meta(ULID(1)),
				meta(ULID(2)),
				meta(ULID(10), ULID(1), ULID(2)),
				meta(ULID(20), ULID(1), ULID(2)),
			},
			expected: ULIDs(20),
			msg:      "the accepted result supersedes the rejected one and the sources",
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			testutil.Equals(t, tcase.expected, run(t, tcase.published, tcase.input...), tcase.msg)
		})
	}
}

// TestDeduplicateFilterHidesUnpublished: a compactor's filter withholds
// unpublished blocks from the view altogether. Otherwise a shard whose sibling
// never uploaded could be merged with the next range of its group into a block
// with no set of its own, published by default, whose sources include the
// unsplit sources of the failed plan - and supersede them with the series of
// the missing sibling gone. A leftover of an earlier attempt still counts as
// a duplicate of the published result of a later one, so that garbage
// collection retires it, and a block the caller declares unpublished is
// withheld too.
func TestDeduplicateFilterHidesUnpublished(t *testing.T) {
	ctx := t.Context()
	lbls := map[string]string{"tenant": "a"}
	meta := func(id ulid.ULID, sources ...ulid.ULID) *metadata.Meta {
		if len(sources) == 0 {
			sources = []ulid.ULID{id}
		}
		return &metadata.Meta{
			BlockMeta: tsdb.BlockMeta{ULID: id, Compaction: tsdb.BlockMetaCompaction{Sources: sources}},
			Thanos:    metadata.Thanos{Labels: lbls},
		}
	}
	output := func(id ulid.ULID, index, count int, set []ulid.ULID, sources ...ulid.ULID) *metadata.Meta {
		m := meta(id, sources...)
		m.Thanos.Output = &metadata.ThanosOutput{Index: index, Count: count, Blocks: set}
		return m
	}
	run := func(t *testing.T, published func(*metadata.Meta) bool, input ...*metadata.Meta) (view, duplicates, unpublished []ulid.ULID) {
		t.Helper()
		metas := map[ulid.ULID]*metadata.Meta{}
		for _, m := range input {
			metas[m.ULID] = m
		}
		f := NewDeduplicateFilter(1)
		f.HideUnpublished()
		if published != nil {
			f.SetPublishedFunc(published)
		}
		testutil.Ok(t, f.Filter(ctx, metas, newTestFetcherMetrics().Synced, nil))
		view = slices.AppendSeq(make([]ulid.ULID, 0, len(metas)), maps.Keys(metas))
		sorted := func(ids []ulid.ULID) []ulid.ULID {
			slices.SortFunc(ids, func(a, b ulid.ULID) int { return a.Compare(b) })
			return ids
		}
		return sorted(view), sorted(f.DuplicateIDs()), sorted(f.UnpublishedIDs())
	}

	// Range 1 is blocks 1 and 2, range 2 blocks 3 and 4.
	A, B := ULID(10), ULID(11)
	t.Run("an unpublished output is withheld, its sources stay, nothing is a duplicate", func(t *testing.T) {
		view, dups, hidden := run(t, nil, meta(ULID(1)), meta(ULID(2)), output(A, 0, 2, []ulid.ULID{A, B}, ULID(1), ULID(2)))
		testutil.Equals(t, ULIDs(1, 2), view)
		testutil.Equals(t, []ulid.ULID{}, dups)
		testutil.Equals(t, []ulid.ULID{A}, hidden)
	})
	t.Run("withheld blocks keep their metadata for what must still reach them", func(t *testing.T) {
		a := output(A, 0, 2, []ulid.ULID{A, B}, ULID(1), ULID(2))
		metas := map[ulid.ULID]*metadata.Meta{ULID(1): meta(ULID(1)), ULID(2): meta(ULID(2)), A: a}
		f := NewDeduplicateFilter(1)
		f.HideUnpublished()
		testutil.Ok(t, f.Filter(ctx, metas, newTestFetcherMetrics().Synced, nil))
		testutil.Equals(t, []*metadata.Meta{a}, f.Unpublished())
	})
	t.Run("a leftover of an earlier attempt is a duplicate of the published rerun, not withheld", func(t *testing.T) {
		A2, B2 := ULID(12), ULID(13)
		view, dups, hidden := run(
			t,
			nil,
			meta(ULID(1)),
			meta(ULID(2)),
			output(A, 0, 2, []ulid.ULID{A, B}, ULID(1), ULID(2)),
			output(A2, 0, 2, []ulid.ULID{A2, B2}, ULID(1), ULID(2)),
			output(B2, 1, 2, []ulid.ULID{A2, B2}, ULID(1), ULID(2)),
		)
		testutil.Equals(t, ULIDs(12, 13), view)
		testutil.Equals(t, ULIDs(1, 2, 10), dups)
		testutil.Equals(t, []ulid.ULID{}, hidden)
	})
	t.Run("the next range's outputs have nothing to merge with while the set is incomplete", func(t *testing.T) {
		// A planner sees the complete outputs of range 2 and the unsplit
		// sources of range 1, never the withheld half of range 1.
		A2, B2 := ULID(20), ULID(21)
		view, dups, hidden := run(
			t,
			nil,
			meta(ULID(1)),
			meta(ULID(2)),
			meta(ULID(3)),
			meta(ULID(4)),
			output(A, 0, 2, []ulid.ULID{A, B}, ULID(1), ULID(2)),
			output(A2, 0, 2, []ulid.ULID{A2, B2}, ULID(3), ULID(4)),
			output(B2, 1, 2, []ulid.ULID{A2, B2}, ULID(3), ULID(4)),
		)
		testutil.Equals(t, ULIDs(1, 2, 20, 21), view)
		testutil.Equals(t, ULIDs(3, 4), dups)
		testutil.Equals(t, []ulid.ULID{A}, hidden)
	})
	t.Run("a block the caller declares unpublished is withheld", func(t *testing.T) {
		C := ULID(30)
		view, dups, hidden := run(t, func(m *metadata.Meta) bool { return m.ULID != C }, meta(ULID(1)), meta(ULID(2)), meta(C, ULID(1), ULID(2)))
		testutil.Equals(t, ULIDs(1, 2), view)
		testutil.Equals(t, []ulid.ULID{}, dups)
		testutil.Equals(t, []ulid.ULID{C}, hidden)
	})
	t.Run("a block named in another complete set is published although its own is not", func(t *testing.T) {
		// {A, B} split blocks 1 and 2. B was compacted on - into C, whose
		// plan named A as its sibling - and retired. A's own set can never
		// be complete again; C's set says A still holds its part.
		C := ULID(40)
		c := output(C, 0, 1, []ulid.ULID{C, A}, ULID(1), ULID(2))
		c.Thanos.Labels = map[string]string{"tenant": "a", "part": "b"} // Another shard, another group.
		view, dups, hidden := run(t, nil, output(A, 0, 2, []ulid.ULID{A, B}, ULID(1), ULID(2)), c)
		testutil.Equals(t, ULIDs(10, 40), view)
		testutil.Equals(t, []ulid.ULID{}, dups)
		testutil.Equals(t, []ulid.ULID{}, hidden)
		// Without C, A is unpublished and withheld.
		view, _, hidden = run(t, nil, output(A, 0, 2, []ulid.ULID{A, B}, ULID(1), ULID(2)))
		testutil.Equals(t, []ulid.ULID{}, view)
		testutil.Equals(t, []ulid.ULID{A}, hidden)
		// And an incomplete set of C's certifies nothing.
		c.Thanos.Output.Blocks = []ulid.ULID{C, A, ULID(41)}
		view, _, hidden = run(t, nil, output(A, 0, 2, []ulid.ULID{A, B}, ULID(1), ULID(2)), c)
		testutil.Equals(t, []ulid.ULID{}, view)
		testutil.Equals(t, ULIDs(10, 40), hidden)
	})
	t.Run("a copied set does not vouch for its blocks", func(t *testing.T) {
		// A tool rewrote A into A2, a lineage of its own, and kept A's set,
		// which does not name A2. A2 is its own set, but it must not publish
		// A: B never arrived, so the sources are the only complete copy of
		// B's series.
		A2 := ULID(50)
		copied := output(A2, 0, 2, []ulid.ULID{A, B}, A2)
		view, dups, hidden := run(t, nil, meta(ULID(1)), meta(ULID(2)), output(A, 0, 2, []ulid.ULID{A, B}, ULID(1), ULID(2)), copied)
		testutil.Equals(t, []ulid.ULID{}, dups, "nothing may retire the sources")
		testutil.Equals(t, []ulid.ULID{A}, hidden)
		testutil.Equals(t, []ulid.ULID{ULID(1), ULID(2), A2}, view)
	})
	t.Run("a repaired copy takes its original's place in the set", func(t *testing.T) {
		// Repair keeps the sources and renames the copy into the set: while
		// B is missing the copy is as unpublished as A was, and once B is
		// there the set is complete again, with A gone.
		A2 := ULID(50)
		repaired := output(A2, 0, 2, []ulid.ULID{A, B}, ULID(1), ULID(2))
		repaired.Thanos.RenameInOutput(A, A2)
		testutil.Equals(t, []ulid.ULID{A2, B}, repaired.Thanos.Output.Blocks)
		view, dups, hidden := run(t, nil, meta(ULID(1)), meta(ULID(2)), repaired)
		testutil.Equals(t, ULIDs(1, 2), view)
		testutil.Equals(t, []ulid.ULID{}, dups)
		testutil.Equals(t, []ulid.ULID{A2}, hidden)

		// Shards are groups of their own.
		repaired.Thanos.Labels = map[string]string{"tenant": "a", "part": "a"}
		b := output(B, 1, 2, []ulid.ULID{A, B}, ULID(1), ULID(2))
		b.Thanos.Labels = map[string]string{"tenant": "a", "part": "b"}
		view, _, hidden = run(t, nil, repaired, b)
		testutil.Equals(t, []ulid.ULID{B, A2}, view)
		testutil.Equals(t, []ulid.ULID{}, hidden)
	})
	t.Run("without hiding, the same view keeps the unpublished block, as a store gateway does", func(t *testing.T) {
		metas := map[ulid.ULID]*metadata.Meta{ULID(1): meta(ULID(1)), ULID(2): meta(ULID(2)), A: output(A, 0, 2, []ulid.ULID{A, B}, ULID(1), ULID(2))}
		f := NewDeduplicateFilter(1)
		testutil.Ok(t, f.Filter(ctx, metas, newTestFetcherMetrics().Synced, nil))
		testutil.Equals(t, 3, len(metas))
		testutil.Equals(t, 0, len(f.UnpublishedIDs()))
	})
}
