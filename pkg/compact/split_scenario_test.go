// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compact/compacttest"
)

// splitConfig makes every plan of the HA corpus split: its blocks hold a
// handful of series each, so a shard target of four series yields two to
// eight shards per plan.
var splitConfig = compact.SplitConfig{MaxShards: 8, MaxSeries: 4}

func withSplitting(c compacttest.NodeConfig) compacttest.NodeConfig {
	c.Split = splitConfig
	return c
}

func withoutSplitting(c compacttest.NodeConfig) compacttest.NodeConfig {
	c.Split = compact.SplitConfig{}
	return c
}

// splitScenarios cover what splitting adds on top of surviving the generic
// faults: the shards exist, are labeled, and the shards of one time range
// partition the series among them.
func splitScenarios() []compacttest.Scenario {
	return []compacttest.Scenario{
		{
			// The generic publication faults fire on the first write of a
			// kind, before any shard exists. This one fires after the first
			// shard of a split is in the bucket and its siblings are not:
			// the sources must stay until the whole set is, and the rerun
			// must supersede the leftover shard.
			Name: "split_survives_partial_publication",
			Run: func(t *testing.T, r *compacttest.Run, want, _ *compacttest.BucketDump) {
				fired := partialPublication(r)
				sources := compacttest.SourceObjects(t, r.Shared, r.Corpus.IDs())
				compacttest.Converged(t, r, want)
				testutil.Assert(t, fired(), "the fault never fired")
				testutil.Equals(t, sources, compacttest.SourceObjects(t, r.Shared, r.Corpus.IDs()), "source blocks changed")
			},
		},
		{
			Name: "split_produces_labeled_shards",
			Run: func(t *testing.T, r *compacttest.Run, want, _ *compacttest.BucketDump) {
				got := compacttest.Converged(t, r, want)

				type rangeKey struct {
					base string
					res  int64
					mint int64
					maxt int64
				}
				type leaf struct{ index, count uint64 }
				leaves := map[rangeKey]map[leaf]string{}
				var shardBlocks int
				for _, b := range got.Blocks {
					index, count, ok, err := metadata.Shard(b.Meta.Thanos.Labels)
					testutil.Ok(t, err)
					if !ok {
						continue
					}
					shardBlocks++
					testutil.Assert(t, count >= 2 && count <= uint64(splitConfig.MaxShards) && count&(count-1) == 0, "block %s has shard count %d", b.ID, count)
					base := labels.NewBuilder(labels.FromMap(b.Meta.Thanos.Labels)).Del(metadata.CompactorShardLabel).Labels().String()
					k := rangeKey{base: base, res: b.Res, mint: b.MinT, maxt: b.MaxT}
					if leaves[k] == nil {
						leaves[k] = map[leaf]string{}
					}
					_, dup := leaves[k][leaf{index, count}]
					testutil.Assert(t, !dup, "range %+v holds shard %d of %d twice", k, index+1, count)
					leaves[k][leaf{index, count}] = fmt.Sprint(b.Sources)
				}
				testutil.Assert(t, shardBlocks > 0, "nothing was split although every plan exceeds the series target")

				// A shard a split produced records the set it was produced
				// with: that is what lets readers tell a complete replacement
				// of the sources from a partial one. A shard made by an
				// ordinary compaction inside its group, or by the downsampler,
				// is its own set and records none. A recorded set need not be
				// complete any more once the stream has moved on - a sibling
				// re-split into finer shards is superseded and collected - so
				// completeness itself is what split_survives_partial_publication
				// exercises, at the moment it matters.
				var recorded int
				for _, b := range got.Blocks {
					if _, _, ok, _ := metadata.Shard(b.Meta.Thanos.Labels); !ok {
						continue
					}
					set := b.Meta.Thanos.Output
					if set == nil {
						continue
					}
					recorded++
					testutil.Assert(t, slices.Contains(set.Blocks, b.ID), "shard %s is not in its own set", b.ID)
					testutil.Assert(t, set.Count >= 2 && set.Index < set.Count, "shard %s records output %d of %d", b.ID, set.Index, set.Count)
				}
				testutil.Assert(t, recorded > 0, "no served shard records the set it was split into")

				// Within one range the shards - whatever their counts, after
				// re-splits - must partition the hash space: no part of it may
				// be held by two shards. Shards of one count in a range came
				// from one plan and record the same sources.
				for k, set := range leaves {
					var finest uint64
					sourcesByCount := map[uint64]string{}
					for l, sources := range set {
						finest = max(finest, l.count)
						if prev, ok := sourcesByCount[l.count]; ok {
							testutil.Equals(t, prev, sources, "range %+v: shards of count %d were not made from the same sources", k, l.count)
						}
						sourcesByCount[l.count] = sources
					}
					for j := range finest {
						holders := 0
						for l := range set {
							if j%l.count == l.index {
								holders++
							}
						}
						testutil.Assert(t, holders <= 1, "range %+v: hash part %d of %d is held by %d shards", k, j, finest, holders)
					}
				}
			},
		},
	}
}

// partialPublication fails the executor right after the first shard of a
// split has been published - its meta.json is in the bucket, its siblings
// are not - once. The first attempt of a split leaves one shard behind, which
// must not retire the sources; the rerun's complete set must supersede it.
func partialPublication(r *compacttest.Run) (fired func() bool) {
	var (
		mu   sync.Mutex
		done bool
	)
	for _, v := range r.Views() {
		v.SetAfterUpload(func(ctx context.Context, name string) error {
			dir, file, ok := strings.Cut(name, "/")
			if !ok || file != block.MetaFilename {
				return nil
			}
			if _, err := ulid.Parse(dir); err != nil {
				return nil
			}
			mu.Lock()
			defer mu.Unlock()
			if done {
				return nil
			}
			m, err := block.DownloadMeta(ctx, log.NewNopLogger(), r.Shared, ulid.MustParse(dir))
			if err != nil || m.Thanos.Output == nil || len(m.Thanos.Output.Blocks) < 2 {
				return nil
			}
			done = true
			return compacttest.ErrInjected
		})
	}
	return func() bool { mu.Lock(); defer mu.Unlock(); return done }
}

// TestSplitScenarios runs the generic fault scenarios with splitting enabled,
// judged against the standalone compactor without it - split or not, the
// served content has to be the same - plus the scenarios for what splitting
// adds. Slow and off by default; see compacttest.SkipUnlessScenarios.
func TestSplitScenarios(t *testing.T) {
	compacttest.RunSuite(t, compacttest.Suite{
		Conf:       withSplitting(compacttest.HANodeConfig()),
		OracleConf: withoutSplitting,
		Scenarios:  splitScenarios(),
	})
}
