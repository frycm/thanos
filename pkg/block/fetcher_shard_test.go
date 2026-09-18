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

// TestDeduplicateFilterWithShards pins what block splitting relies on from the
// deduplication filter: a shard supersedes the blocks it was made from and the
// coarser shards of its own lineage, shards of one plan never supersede each
// other although they record the same sources, and an unsplit block never
// supersedes a shard.
func TestDeduplicateFilterWithShards(t *testing.T) {
	ctx := context.Background()
	shard := func(v string) map[string]string {
		if v == "" {
			return map[string]string{"tenant": "a"}
		}
		return map[string]string{"tenant": "a", metadata.CompactorShardLabel: v}
	}
	meta := func(id ulid.ULID, lbls map[string]string, sources ...ulid.ULID) *metadata.Meta {
		return &metadata.Meta{
			BlockMeta: tsdb.BlockMeta{ULID: id, Compaction: tsdb.BlockMetaCompaction{Sources: sources}},
			Thanos:    metadata.Thanos{Labels: lbls},
		}
	}

	for _, tc := range []struct {
		name     string
		input    []*metadata.Meta
		expected []ulid.ULID
	}{
		{
			name: "shards of one plan are both kept",
			input: []*metadata.Meta{
				meta(ULID(10), shard("1_of_2"), ULID(1), ULID(2)),
				meta(ULID(11), shard("2_of_2"), ULID(1), ULID(2)),
			},
			expected: []ulid.ULID{ULID(10), ULID(11)},
		},
		{
			name: "a duplicate of the same shard is dropped",
			input: []*metadata.Meta{
				meta(ULID(10), shard("1_of_2"), ULID(1), ULID(2)),
				meta(ULID(11), shard("1_of_2"), ULID(1), ULID(2)),
			},
			expected: []ulid.ULID{ULID(10)},
		},
		{
			name: "shards supersede the unsplit sources they were made from",
			input: []*metadata.Meta{
				meta(ULID(1), shard(""), ULID(1)),
				meta(ULID(2), shard(""), ULID(2)),
				meta(ULID(10), shard("1_of_2"), ULID(1), ULID(2)),
				meta(ULID(11), shard("2_of_2"), ULID(1), ULID(2)),
			},
			expected: []ulid.ULID{ULID(10), ULID(11)},
		},
		{
			name: "a shard made from a single block supersedes it",
			input: []*metadata.Meta{
				meta(ULID(1), shard(""), ULID(1)),
				meta(ULID(10), shard("1_of_2"), ULID(1)),
				meta(ULID(11), shard("2_of_2"), ULID(1)),
			},
			expected: []ulid.ULID{ULID(10), ULID(11)},
		},
		{
			name: "an unsplit block never supersedes a shard",
			input: []*metadata.Meta{
				meta(ULID(10), shard("1_of_2"), ULID(1), ULID(2)),
				meta(ULID(20), shard(""), ULID(1), ULID(2), ULID(3)),
			},
			expected: []ulid.ULID{ULID(10), ULID(20)},
		},
		{
			name: "a re-split shard supersedes the coarser shards of its lineage only",
			input: []*metadata.Meta{
				meta(ULID(10), shard("1_of_2"), ULID(1)), // lineage of 1_of_4 and 3_of_4
				meta(ULID(11), shard("1_of_2"), ULID(2)),
				meta(ULID(12), shard("2_of_2"), ULID(1)), // other lineage, must stay
				meta(ULID(20), shard("1_of_4"), ULID(1), ULID(2)),
				meta(ULID(21), shard("3_of_4"), ULID(1), ULID(2)),
			},
			expected: []ulid.ULID{ULID(12), ULID(20), ULID(21)},
		},
		{
			name: "a live sibling with fewer sources is not superseded by a later compaction of another shard",
			input: []*metadata.Meta{
				meta(ULID(20), shard("1_of_2"), ULID(1), ULID(2), ULID(3)),
				meta(ULID(21), shard("2_of_2"), ULID(1), ULID(2)),
			},
			expected: []ulid.ULID{ULID(20), ULID(21)},
		},
		{
			name: "streams with other labels are untouched",
			input: []*metadata.Meta{
				meta(ULID(10), shard("1_of_2"), ULID(1), ULID(2)),
				meta(ULID(30), map[string]string{"tenant": "b"}, ULID(1), ULID(2)),
			},
			expected: []ulid.ULID{ULID(10), ULID(30)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metas := map[ulid.ULID]*metadata.Meta{}
			for _, m := range tc.input {
				metas[m.ULID] = m
			}
			testutil.Ok(t, NewDeduplicateFilter(1).Filter(ctx, metas, newTestFetcherMetrics().Synced, nil))
			compareSliceWithMapKeys(t, metas, tc.expected)
		})
	}
}

// TestDeduplicateFilterShardsSupersedeOnlyAsASet: a shard supersedes its
// sources only once every shard of its plan is present. Until then the sources
// are the only complete copy of the series the missing shards would hold, and
// garbage collection must not retire them on the strength of one shard.
func TestDeduplicateFilterShardsSupersedeOnlyAsASet(t *testing.T) {
	ctx := context.Background()
	base := map[string]string{"tenant": "a"}
	shard := func(v string) map[string]string {
		return map[string]string{"tenant": "a", metadata.CompactorShardLabel: v}
	}
	meta := func(id ulid.ULID, lbls map[string]string, set []ulid.ULID, sources ...ulid.ULID) *metadata.Meta {
		m := &metadata.Meta{
			BlockMeta: tsdb.BlockMeta{ULID: id, Compaction: tsdb.BlockMetaCompaction{Sources: sources}},
			Thanos:    metadata.Thanos{Labels: lbls},
		}
		if set != nil {
			index := 0
			for i, b := range set {
				if b == id {
					index = i
				}
			}
			m.Thanos.Output = &metadata.ThanosOutput{Index: index, Count: len(set), Blocks: set}
		}
		return m
	}
	run := func(input ...*metadata.Meta) []ulid.ULID {
		metas := map[ulid.ULID]*metadata.Meta{}
		for _, m := range input {
			metas[m.ULID] = m
		}
		f := NewDeduplicateFilter(1)
		testutil.Ok(t, f.Filter(ctx, metas, newTestFetcherMetrics().Synced, nil))
		ids := make([]ulid.ULID, 0, len(metas))
		for id := range metas {
			ids = append(ids, id)
		}
		slices.SortFunc(ids, func(a, b ulid.ULID) int { return a.Compare(b) })
		return ids
	}

	set := []ulid.ULID{ULID(10), ULID(11)}
	src1, src2 := meta(ULID(1), base, nil, ULID(1)), meta(ULID(2), base, nil, ULID(2))
	testutil.Equals(t, ULIDs(1, 2, 10), run(src1, src2, meta(ULID(10), shard("1_of_2"), set, ULID(1), ULID(2))),
		"one shard of two present: the sources stay")
	testutil.Equals(t, ULIDs(10, 11), run(src1, src2, meta(ULID(10), shard("1_of_2"), set, ULID(1), ULID(2)), meta(ULID(11), shard("2_of_2"), set, ULID(1), ULID(2))),
		"both shards present: the sources are superseded")

	// The interrupted attempt left shard 1 behind; a later attempt produced
	// both shards. The leftover, older ULID and all, is the duplicate.
	later := []ulid.ULID{ULID(20), ULID(21)}
	testutil.Equals(t, ULIDs(20, 21), run(src1, src2,
		meta(ULID(10), shard("1_of_2"), set, ULID(1), ULID(2)),
		meta(ULID(20), shard("1_of_2"), later, ULID(1), ULID(2)),
		meta(ULID(21), shard("2_of_2"), later, ULID(1), ULID(2))))

	// A planned shard that held no series is not in the set and not awaited.
	only := []ulid.ULID{ULID(30)}
	testutil.Equals(t, ULIDs(30), run(src1, src2, meta(ULID(30), shard("1_of_2"), only, ULID(1), ULID(2))))

	// A shard re-split on its own - a block left behind its lineage's count
	// - records the same sources as the finer shards made from it. The finer
	// ones supersede it although it is older and still published.
	finer := []ulid.ULID{ULID(40), ULID(41)}
	testutil.Equals(t, ULIDs(11, 40, 41), run(
		meta(ULID(10), shard("1_of_2"), set, ULID(1), ULID(2)), meta(ULID(11), shard("2_of_2"), set, ULID(1), ULID(2)),
		meta(ULID(40), shard("1_of_8"), finer, ULID(1), ULID(2)), meta(ULID(41), shard("5_of_8"), finer, ULID(1), ULID(2))),
		"1 of 8 and 5 of 8 supersede 1 of 2; 2 of 2 is another lineage")
}
