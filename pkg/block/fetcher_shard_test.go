// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package block

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"testing"

	"github.com/oklog/ulid/v2"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/efficientgo/core/testutil"

	"github.com/thanos-io/thanos/pkg/block/metadata"
)

type dedupTestBlock struct {
	id         int
	sources    []int
	resolution int64
	labels     map[string]string
}

func unsplitLabels(cluster string) map[string]string {
	return map[string]string{"cluster": cluster}
}

func shardLabels(cluster, shardID string) map[string]string {
	return map[string]string{"cluster": cluster, metadata.CompactorShardIDLabel: shardID}
}

func dedupTestMetas(blocks []dedupTestBlock) map[ulid.ULID]*metadata.Meta {
	metas := make(map[ulid.ULID]*metadata.Meta, len(blocks))
	for _, b := range blocks {
		id := ULID(b.id)
		metas[id] = &metadata.Meta{
			BlockMeta: tsdb.BlockMeta{
				ULID:       id,
				Compaction: tsdb.BlockMetaCompaction{Sources: ULIDs(b.sources...)},
			},
			Thanos: metadata.Thanos{
				Labels:     b.labels,
				Downsample: metadata.ThanosDownsample{Resolution: b.resolution},
			},
		}
	}
	return metas
}

func sortedULIDs(ids []ulid.ULID) []ulid.ULID {
	if len(ids) == 0 {
		return nil
	}
	ids = slices.Clone(ids)
	slices.SortFunc(ids, func(a, b ulid.ULID) int { return a.Compare(b) })
	return ids
}

// runDeduplicateFilter returns the IDs the filter kept and the duplicate IDs it reported, both sorted, after
// checking that the duplicates are exactly the removed blocks, each counted once.
func runDeduplicateFilter(t testing.TB, concurrency int, metas map[ulid.ULID]*metadata.Meta) (kept, duplicates []ulid.ULID) {
	t.Helper()

	inputLen := len(metas)
	m := newTestFetcherMetrics()
	f := NewDeduplicateFilter(concurrency)
	testutil.Ok(t, f.Filter(context.Background(), metas, m.Synced, nil))

	for id := range metas {
		kept = append(kept, id)
	}
	kept = sortedULIDs(kept)
	duplicates = sortedULIDs(f.DuplicateIDs())
	testutil.Equals(t, inputLen-len(kept), len(duplicates))
	testutil.Equals(t, float64(len(duplicates)), promtest.ToFloat64(m.Synced.WithLabelValues(duplicateMeta)))
	for _, id := range duplicates {
		_, ok := metas[id]
		testutil.Assert(t, !ok, "duplicate %s still in metas", id)
	}
	return kept, duplicates
}

func TestDeduplicateFilter_ShardFamilies(t *testing.T) {
	const res5m = int64(5 * 60 * 1000)

	for _, tcase := range []struct {
		name     string
		input    []dedupTestBlock
		expected []int
	}{
		{
			name: "partial family keeps the unsplit block",
			input: []dedupTestBlock{
				{id: 1, sources: []int{1}, labels: unsplitLabels("a")},
				{id: 10, sources: []int{1}, labels: shardLabels("a", "1_of_2")},
			},
			expected: []int{1, 10},
		},
		{
			name: "partial family of four keeps the unsplit block",
			input: []dedupTestBlock{
				{id: 1, sources: []int{1}, labels: unsplitLabels("a")},
				{id: 10, sources: []int{1}, labels: shardLabels("a", "1_of_4")},
				{id: 11, sources: []int{1}, labels: shardLabels("a", "2_of_4")},
				{id: 12, sources: []int{1}, labels: shardLabels("a", "4_of_4")},
			},
			expected: []int{1, 10, 11, 12},
		},
		{
			name: "complete family retires the unsplit block",
			input: []dedupTestBlock{
				{id: 1, sources: []int{1}, labels: unsplitLabels("a")},
				{id: 10, sources: []int{1}, labels: shardLabels("a", "1_of_2")},
				{id: 11, sources: []int{1}, labels: shardLabels("a", "2_of_2")},
			},
			expected: []int{10, 11},
		},
		{
			name: "complete family retires all unsplit blocks it was made from",
			input: []dedupTestBlock{
				{id: 1, sources: []int{1}, labels: unsplitLabels("a")},
				{id: 2, sources: []int{2}, labels: unsplitLabels("a")},
				{id: 3, sources: []int{3}, labels: unsplitLabels("a")},
				{id: 10, sources: []int{1, 2}, labels: shardLabels("a", "1_of_2")},
				{id: 11, sources: []int{2, 1}, labels: shardLabels("a", "2_of_2")},
			},
			expected: []int{3, 10, 11},
		},
		{
			name: "family of one shard is complete",
			input: []dedupTestBlock{
				{id: 1, sources: []int{1}, labels: unsplitLabels("a")},
				{id: 10, sources: []int{1}, labels: shardLabels("a", "1_of_1")},
			},
			expected: []int{10},
		},
		{
			name: "family of a count that is not a power of two",
			input: []dedupTestBlock{
				{id: 1, sources: []int{1}, labels: unsplitLabels("a")},
				{id: 10, sources: []int{1}, labels: shardLabels("a", "1_of_3")},
				{id: 11, sources: []int{1}, labels: shardLabels("a", "2_of_3")},
				{id: 12, sources: []int{1}, labels: shardLabels("a", "3_of_3")},
			},
			expected: []int{10, 11, 12},
		},
		{
			name: "later merged shard blocks still count for the family",
			input: []dedupTestBlock{
				{id: 1, sources: []int{1}, labels: unsplitLabels("a")},
				{id: 2, sources: []int{2}, labels: unsplitLabels("a")},
				// Shard 1 of both windows was already merged, shard 2 was not.
				{id: 20, sources: []int{1, 2, 3}, labels: shardLabels("a", "1_of_2")},
				{id: 11, sources: []int{1}, labels: shardLabels("a", "2_of_2")},
				{id: 13, sources: []int{2}, labels: shardLabels("a", "2_of_2")},
			},
			expected: []int{20, 11, 13},
		},
		{
			name: "merged shard block retires the shard blocks it was made from, the family still retires the unsplit block",
			input: []dedupTestBlock{
				{id: 1, sources: []int{1}, labels: unsplitLabels("a")},
				{id: 10, sources: []int{1}, labels: shardLabels("a", "1_of_2")},
				{id: 11, sources: []int{1}, labels: shardLabels("a", "2_of_2")},
				{id: 12, sources: []int{2}, labels: shardLabels("a", "1_of_2")},
				{id: 20, sources: []int{1, 2}, labels: shardLabels("a", "1_of_2")},
			},
			expected: []int{11, 20},
		},
		{
			name: "family members must each contain all sources of the unsplit block",
			input: []dedupTestBlock{
				{id: 1, sources: []int{1, 2}, labels: unsplitLabels("a")},
				{id: 10, sources: []int{1}, labels: shardLabels("a", "1_of_2")},
				{id: 12, sources: []int{2}, labels: shardLabels("a", "1_of_2")},
				{id: 11, sources: []int{1, 2}, labels: shardLabels("a", "2_of_2")},
			},
			expected: []int{1, 10, 11, 12},
		},
		{
			name: "unsplit block with more sources than the family is kept",
			input: []dedupTestBlock{
				{id: 1, sources: []int{1, 2}, labels: unsplitLabels("a")},
				{id: 10, sources: []int{1}, labels: shardLabels("a", "1_of_2")},
				{id: 11, sources: []int{1}, labels: shardLabels("a", "2_of_2")},
			},
			expected: []int{1, 10, 11},
		},
		{
			name: "shard is never superseded by an unsplit block",
			input: []dedupTestBlock{
				{id: 1, sources: []int{1, 2}, labels: unsplitLabels("a")},
				{id: 10, sources: []int{1}, labels: shardLabels("a", "1_of_2")},
				{id: 11, sources: []int{1, 2}, labels: shardLabels("a", "2_of_2")},
			},
			expected: []int{1, 10, 11},
		},
		{
			name: "shard is never superseded by another shard",
			input: []dedupTestBlock{
				{id: 10, sources: []int{1, 2}, labels: shardLabels("a", "1_of_2")},
				{id: 11, sources: []int{1}, labels: shardLabels("a", "2_of_2")},
				{id: 12, sources: []int{1}, labels: shardLabels("a", "1_of_4")},
			},
			expected: []int{10, 11, 12},
		},
		{
			name: "equal sources across shards do not supersede each other",
			input: []dedupTestBlock{
				{id: 10, sources: []int{1, 2}, labels: shardLabels("a", "1_of_2")},
				{id: 11, sources: []int{2, 1}, labels: shardLabels("a", "2_of_2")},
			},
			expected: []int{10, 11},
		},
		{
			name: "equal sources within a shard keep the lowest ULID",
			input: []dedupTestBlock{
				{id: 11, sources: []int{1, 2}, labels: shardLabels("a", "1_of_2")},
				{id: 10, sources: []int{2, 1}, labels: shardLabels("a", "1_of_2")},
				{id: 12, sources: []int{1}, labels: shardLabels("a", "1_of_2")},
			},
			expected: []int{10},
		},
		{
			name: "counts do not mix",
			input: []dedupTestBlock{
				{id: 1, sources: []int{1}, labels: unsplitLabels("a")},
				{id: 10, sources: []int{1}, labels: shardLabels("a", "1_of_2")},
				{id: 12, sources: []int{1}, labels: shardLabels("a", "2_of_4")},
				{id: 13, sources: []int{1}, labels: shardLabels("a", "3_of_4")},
				{id: 14, sources: []int{1}, labels: shardLabels("a", "4_of_4")},
			},
			expected: []int{1, 10, 12, 13, 14},
		},
		{
			name: "one complete family among partial ones is enough",
			input: []dedupTestBlock{
				{id: 1, sources: []int{1}, labels: unsplitLabels("a")},
				{id: 10, sources: []int{1}, labels: shardLabels("a", "1_of_2")},
				{id: 11, sources: []int{1}, labels: shardLabels("a", "1_of_4")},
				{id: 12, sources: []int{1}, labels: shardLabels("a", "2_of_4")},
				{id: 13, sources: []int{1}, labels: shardLabels("a", "3_of_4")},
				{id: 14, sources: []int{1}, labels: shardLabels("a", "4_of_4")},
			},
			expected: []int{10, 11, 12, 13, 14},
		},
		{
			name: "unsplit block superseded twice is reported once",
			input: []dedupTestBlock{
				{id: 1, sources: []int{1}, labels: unsplitLabels("a")},
				{id: 2, sources: []int{1}, labels: unsplitLabels("a")},
				{id: 3, sources: []int{1, 2}, labels: unsplitLabels("a")},
				{id: 10, sources: []int{1}, labels: shardLabels("a", "1_of_2")},
				{id: 11, sources: []int{1}, labels: shardLabels("a", "2_of_2")},
			},
			expected: []int{3, 10, 11},
		},
		{
			name: "resolutions are isolated",
			input: []dedupTestBlock{
				{id: 1, sources: []int{1}, labels: unsplitLabels("a")},
				{id: 10, sources: []int{1}, labels: shardLabels("a", "1_of_2"), resolution: res5m},
				{id: 11, sources: []int{1}, labels: shardLabels("a", "2_of_2"), resolution: res5m},
				{id: 2, sources: []int{2}, labels: unsplitLabels("a"), resolution: res5m},
				{id: 12, sources: []int{2}, labels: shardLabels("a", "1_of_2")},
				{id: 13, sources: []int{2}, labels: shardLabels("a", "2_of_2")},
			},
			expected: []int{1, 2, 10, 11, 12, 13},
		},
		{
			name: "complete families retire unsplit blocks at each resolution",
			input: []dedupTestBlock{
				{id: 1, sources: []int{1}, labels: unsplitLabels("a")},
				{id: 2, sources: []int{1}, labels: unsplitLabels("a"), resolution: res5m},
				{id: 10, sources: []int{1}, labels: shardLabels("a", "1_of_2")},
				{id: 11, sources: []int{1}, labels: shardLabels("a", "2_of_2")},
				{id: 12, sources: []int{1}, labels: shardLabels("a", "1_of_2"), resolution: res5m},
				{id: 13, sources: []int{1}, labels: shardLabels("a", "2_of_2"), resolution: res5m},
			},
			expected: []int{10, 11, 12, 13},
		},
		{
			name: "streams are isolated",
			input: []dedupTestBlock{
				{id: 1, sources: []int{1}, labels: unsplitLabels("a")},
				{id: 10, sources: []int{1}, labels: shardLabels("b", "1_of_2")},
				{id: 11, sources: []int{1}, labels: shardLabels("b", "2_of_2")},
				{id: 2, sources: []int{2}, labels: unsplitLabels("b")},
				{id: 12, sources: []int{2}, labels: shardLabels("a", "1_of_2")},
				{id: 13, sources: []int{2}, labels: shardLabels("a", "2_of_2")},
			},
			expected: []int{1, 2, 10, 11, 12, 13},
		},
		{
			name: "shard label without other labels",
			input: []dedupTestBlock{
				{id: 1, sources: []int{1}},
				{id: 10, sources: []int{1}, labels: map[string]string{metadata.CompactorShardIDLabel: "1_of_2"}},
				{id: 11, sources: []int{1}, labels: map[string]string{metadata.CompactorShardIDLabel: "2_of_2"}},
			},
			expected: []int{10, 11},
		},
		{
			name: "invalid shard label is a lane of its own",
			input: []dedupTestBlock{
				{id: 1, sources: []int{1}, labels: unsplitLabels("a")},
				{id: 10, sources: []int{1}, labels: shardLabels("a", "1_of_2")},
				{id: 11, sources: []int{1}, labels: shardLabels("a", "02_of_2")},
				{id: 12, sources: []int{1, 2}, labels: shardLabels("a", "3_of_2")},
				{id: 13, sources: []int{1}, labels: shardLabels("a", "")},
				{id: 14, sources: []int{1, 2}, labels: shardLabels("a", "x")},
				{id: 15, sources: []int{1}, labels: shardLabels("a", "x")},
			},
			expected: []int{1, 10, 11, 12, 13, 14},
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			for _, concurrency := range []int{1, 4} {
				kept, _ := runDeduplicateFilter(t, concurrency, dedupTestMetas(tcase.input))
				testutil.Equals(t, sortedULIDs(ULIDs(tcase.expected...)), kept)
			}
		})
	}
}

// upstreamDeduplicate is the deduplication of Thanos v0.41.0, before shard labels were known: duplicates are looked
// for within each compaction group only, by sources alone.
func upstreamDeduplicate(metas map[ulid.ULID]*metadata.Meta) []ulid.ULID {
	metasByCompactionGroup := make(map[string][]*metadata.Meta)
	for _, meta := range metas {
		groupKey := meta.Thanos.GroupKey()
		metasByCompactionGroup[groupKey] = append(metasByCompactionGroup[groupKey], meta)
	}

	var duplicates []ulid.ULID
	for _, metaSlice := range metasByCompactionGroup {
		sort.Slice(metaSlice, func(i, j int) bool {
			ilen := len(metaSlice[i].Compaction.Sources)
			jlen := len(metaSlice[j].Compaction.Sources)

			if ilen == jlen {
				return metaSlice[i].ULID.Compare(metaSlice[j].ULID) < 0
			}

			return ilen-jlen > 0
		})

		var coveringSet []*metadata.Meta
	childLoop:
		for _, child := range metaSlice {
			childSources := child.Compaction.Sources
			for _, parent := range coveringSet {
				parentSources := parent.Compaction.Sources

				// child's sources are present in parent's sources, filter it out.
				if contains(parentSources, childSources) {
					duplicates = append(duplicates, child.ULID)
					continue childLoop
				}
			}

			// Child's sources not covered by any member of coveringSet, add it to coveringSet.
			coveringSet = append(coveringSet, child)
		}
	}
	return duplicates
}

// randomDedupTestBlocks returns blocks of a few streams and resolutions whose sources are drawn from a small pool,
// so that sources are often contained in, or equal to, each other's. shardIDs are the shard label values to draw
// from, "" meaning no shard label.
func randomDedupTestBlocks(r *rand.Rand, shardIDs []string) []dedupTestBlock {
	var (
		n           = 1 + r.Intn(40)
		pool        = 1 + r.Intn(8)
		resolutions = []int64{0, 5 * 60 * 1000, 60 * 60 * 1000}
		clusters    = []string{"a", "b", ""}
		ids         = r.Perm(1000)
		blocks      = make([]dedupTestBlock, 0, n)
	)
	for i := range n {
		var sources []int
		for range r.Intn(5) {
			// Sources may repeat, as no one guarantees they don't.
			sources = append(sources, 1000+r.Intn(pool))
		}

		lset := map[string]string{}
		if c := clusters[r.Intn(len(clusters))]; c != "" {
			lset["cluster"] = c
		}
		if shardID := shardIDs[r.Intn(len(shardIDs))]; shardID != "" {
			lset[metadata.CompactorShardIDLabel] = shardID
		}
		if len(lset) == 0 && r.Intn(2) == 0 {
			lset = nil
		}

		blocks = append(blocks, dedupTestBlock{
			id:         ids[i] + 1,
			sources:    sources,
			resolution: resolutions[r.Intn(len(resolutions))],
			labels:     lset,
		})
	}
	return blocks
}

func TestDeduplicateFilter_SameAsUpstreamWithoutShardLabels(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for i := range 3000 {
		blocks := randomDedupTestBlocks(r, []string{""})
		expected := sortedULIDs(upstreamDeduplicate(dedupTestMetas(blocks)))

		_, duplicates := runDeduplicateFilter(t, 1+i%4, dedupTestMetas(blocks))
		testutil.Equals(t, expected, duplicates, "blocks: %v", blocks)
	}
}

// referenceDuplicates states the deduplication rule directly, quadratically, as a reference for the filter.
func referenceDuplicates(metas map[ulid.ULID]*metadata.Meta) []ulid.ULID {
	sourceSet := func(m *metadata.Meta) map[ulid.ULID]struct{} {
		s := make(map[ulid.ULID]struct{}, len(m.Compaction.Sources))
		for _, id := range m.Compaction.Sources {
			s[id] = struct{}{}
		}
		return s
	}
	containsAll := func(parent, child *metadata.Meta) bool {
		ps := sourceSet(parent)
		for id := range sourceSet(child) {
			if _, ok := ps[id]; !ok {
				return false
			}
		}
		return true
	}
	// Visiting order within a lane: more sources first, then the lower ULID.
	before := func(a, b *metadata.Meta) bool {
		if len(a.Compaction.Sources) != len(b.Compaction.Sources) {
			return len(a.Compaction.Sources) > len(b.Compaction.Sources)
		}
		return a.ULID.Compare(b.ULID) < 0
	}
	// A lane is a resolution and the exact labels.
	lane := func(m *metadata.Meta) string {
		return fmt.Sprintf("%d %s", m.Thanos.Downsample.Resolution, labels.FromMap(m.Thanos.Labels).String())
	}
	// A stream is a resolution and the labels without a valid shard label, plus the parsed shard, if any.
	stream := func(m *metadata.Meta) (key string, index, count int) {
		lset := labels.FromMap(m.Thanos.Labels)
		if v, ok := m.Thanos.Labels[metadata.CompactorShardIDLabel]; ok {
			var err error
			if index, count, err = metadata.ParseShardID(v); err == nil {
				lset = labels.NewBuilder(lset).Del(metadata.CompactorShardIDLabel).Labels()
			}
		}
		return fmt.Sprintf("%d %s", m.Thanos.Downsample.Resolution, lset.String()), index, count
	}

	var duplicates []ulid.ULID
	for _, child := range metas {
		dup := false
		for _, parent := range metas {
			if parent != child && lane(parent) == lane(child) && before(parent, child) && containsAll(parent, child) {
				dup = true
				break
			}
		}

		if _, isShard := child.Thanos.Labels[metadata.CompactorShardIDLabel]; !dup && !isShard {
			childStream, _, _ := stream(child)
			// Per count, the indexes having a block that contains the child's sources.
			covering := map[int]map[int]struct{}{}
			for _, parent := range metas {
				parentStream, index, count := stream(parent)
				if count == 0 || parentStream != childStream || !containsAll(parent, child) {
					continue
				}
				if covering[count] == nil {
					covering[count] = map[int]struct{}{}
				}
				covering[count][index] = struct{}{}
				if len(covering[count]) == count {
					dup = true
				}
			}
		}

		if dup {
			duplicates = append(duplicates, child.ULID)
		}
	}
	return duplicates
}

func TestDeduplicateFilter_SameAsReference(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	shardIDs := []string{
		"", "", "", "",
		"1_of_1",
		"1_of_2", "2_of_2", "1_of_2", "2_of_2",
		"1_of_4", "2_of_4", "3_of_4", "4_of_4",
		"1_of_3", "2_of_3", "3_of_3",
		"0_of_2", "3_of_2", "01_of_2", "x",
	}
	for i := range 3000 {
		blocks := randomDedupTestBlocks(r, shardIDs)
		expected := sortedULIDs(referenceDuplicates(dedupTestMetas(blocks)))

		_, duplicates := runDeduplicateFilter(t, 1+i%4, dedupTestMetas(blocks))
		testutil.Equals(t, expected, duplicates, "blocks: %v", blocks)
	}
}

func BenchmarkDeduplicateFilter_ShardFamilies(b *testing.B) {
	// A stream of windows, each with replicated unsplit blocks, and a complete family of shards for half of them.
	const (
		windows  = 2000
		replicas = 3
		shards   = 8
	)
	var (
		count uint64
		next  = func() ulid.ULID { count++; return ulid.MustNew(count, nil) }
		metas = make(map[ulid.ULID]*metadata.Meta)
	)
	for w := range windows {
		var windowSources []ulid.ULID
		for range replicas {
			id := next()
			windowSources = append(windowSources, id)
			metas[id] = &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{ULID: id, Compaction: tsdb.BlockMetaCompaction{Sources: []ulid.ULID{id}}},
				Thanos:    metadata.Thanos{Labels: unsplitLabels("a")},
			}
		}
		if w%2 == 1 {
			continue
		}
		for i := 1; i <= shards; i++ {
			id := next()
			metas[id] = &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{ULID: id, Compaction: tsdb.BlockMetaCompaction{Sources: windowSources}},
				Thanos:    metadata.Thanos{Labels: shardLabels("a", metadata.FormatShardID(i, shards))},
			}
		}
	}
	m := newTestFetcherMetrics()

	b.ResetTimer()
	for b.Loop() {
		input := make(map[ulid.ULID]*metadata.Meta, len(metas))
		for id, meta := range metas {
			input[id] = meta
		}
		f := NewDeduplicateFilter(1)
		testutil.Ok(b, f.Filter(context.Background(), input, m.Synced, nil))
		testutil.Equals(b, windows/2*replicas, len(f.DuplicateIDs()))
	}
}
