// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package block

import (
	"context"
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
	ctx := context.Background()
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
		ids := make([]ulid.ULID, 0, len(metas))
		for id := range metas {
			ids = append(ids, id)
		}
		slices.SortFunc(ids, func(a, b ulid.ULID) int { return a.Compare(b) })
		return ids
	}

	set := []ulid.ULID{ULID(10), ULID(11)}
	t.Run("an incomplete set supersedes nothing", func(t *testing.T) {
		got := run(t, nil,
			meta(ULID(1)), meta(ULID(2)),
			output(ULID(10), 0, 2, set, ULID(1), ULID(2)),
		)
		testutil.Equals(t, ULIDs(1, 2, 10), got, "the sources stay while a sibling is missing")
	})
	t.Run("a complete set supersedes its sources", func(t *testing.T) {
		got := run(t, nil,
			meta(ULID(1)), meta(ULID(2)),
			output(ULID(10), 0, 2, set, ULID(1), ULID(2)),
			output(ULID(11), 1, 2, set, ULID(1), ULID(2)),
		)
		testutil.Equals(t, ULIDs(10, 11), got)
	})
	t.Run("a published result supersedes the unpublished leftover of an earlier attempt", func(t *testing.T) {
		later := []ulid.ULID{ULID(20), ULID(21)}
		got := run(t, nil,
			meta(ULID(1)), meta(ULID(2)),
			// The earlier attempt published one of two blocks and failed.
			output(ULID(10), 0, 2, set, ULID(1), ULID(2)),
			output(ULID(20), 0, 2, later, ULID(1), ULID(2)),
			output(ULID(21), 1, 2, later, ULID(1), ULID(2)),
		)
		testutil.Equals(t, ULIDs(20, 21), got, "the older, unpublished block must not win the tie")
	})
	t.Run("a block without a set is its own set", func(t *testing.T) {
		got := run(t, nil, meta(ULID(1)), meta(ULID(2)), meta(ULID(10), ULID(1), ULID(2)))
		testutil.Equals(t, ULIDs(10), got)
	})
	t.Run("a set copied from a source does not bind the block", func(t *testing.T) {
		// A tool kept the source's metadata: the set names other blocks, none
		// of them present. The block is judged as its own set.
		got := run(t, nil, meta(ULID(1)), meta(ULID(2)), output(ULID(10), 0, 2, []ulid.ULID{ULID(90), ULID(91)}, ULID(1), ULID(2)))
		testutil.Equals(t, ULIDs(10), got)
	})
	t.Run("the caller can declare a block unpublished", func(t *testing.T) {
		rejected := ULID(10)
		got := run(t, func(m *metadata.Meta) bool { return m.ULID != rejected },
			meta(ULID(1)), meta(ULID(2)),
			meta(ULID(10), ULID(1), ULID(2)),
		)
		testutil.Equals(t, ULIDs(1, 2, 10), got)

		got = run(t, func(m *metadata.Meta) bool { return m.ULID != rejected },
			meta(ULID(1)), meta(ULID(2)),
			meta(ULID(10), ULID(1), ULID(2)),
			meta(ULID(20), ULID(1), ULID(2)),
		)
		testutil.Equals(t, ULIDs(20), got, "the accepted result supersedes the rejected one and the sources")
	})
}
