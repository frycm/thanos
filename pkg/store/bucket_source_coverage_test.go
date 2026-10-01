// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
)

// upstreamGetFor is getFor as of v0.41.0, before it looked at sources: the
// coarsest blocks the request allows, with only the time gaps between them
// filled with finer blocks. getFor must return exactly this whenever no
// finer block is left out behind coarser blocks that lack its sources, and
// never less.
func upstreamGetFor(s *bucketBlockSet, mint, maxt, maxResolutionMillis int64, blockMatchers []*labels.Matcher) (bs []*bucketBlock) {
	if mint > maxt {
		return nil
	}

	s.mtx.RLock()
	defer s.mtx.RUnlock()

	i := 0
	for ; i < len(s.resolutions) && s.resolutions[i] > maxResolutionMillis; i++ {
	}
	start := mint
	for _, b := range s.blocks[i] {
		if b.meta.MaxTime <= mint {
			continue
		}
		if b.meta.MinTime > maxt {
			break
		}
		if i+1 < len(s.resolutions) {
			bs = append(bs, upstreamGetFor(s, start, b.meta.MinTime-1, s.resolutions[i+1], blockMatchers)...)
		}
		if len(blockMatchers) == 0 || b.matchRelabelLabels(blockMatchers) {
			bs = append(bs, b)
		}
		start = b.meta.MaxTime
	}
	if i+1 < len(s.resolutions) {
		bs = append(bs, upstreamGetFor(s, start, maxt, s.resolutions[i+1], blockMatchers)...)
	}
	return bs
}

func newLineageTestBlock(id uint64, mint, maxt, resolution int64, sources ...uint64) *bucketBlock {
	m := &metadata.Meta{}
	m.ULID, m.MinTime, m.MaxTime = ulid.MustNew(id, nil), mint, maxt
	for _, s := range sources {
		m.Compaction.Sources = append(m.Compaction.Sources, ulid.MustNew(s, nil))
	}
	m.Thanos.Downsample.Resolution = resolution
	return &bucketBlock{meta: m, relabelLabels: labels.FromStrings(block.BlockIDLabel, m.ULID.String())}
}

func blockIDs(bs []*bucketBlock) []uint64 {
	ids := make([]uint64, 0, len(bs))
	for _, b := range bs {
		ids = append(ids, b.meta.ULID.Time())
	}
	slices.Sort(ids)
	return ids
}

func TestBucketBlockSet_getForSourceCoverage(t *testing.T) {
	t.Parallel()

	const (
		raw = downsample.ResLevel0
		r5m = downsample.ResLevel1
		r1h = downsample.ResLevel2
	)
	for _, tcase := range []struct {
		name string

		blocks        []*bucketBlock
		mint, maxt    int64
		maxResolution int64
		matchers      []*labels.Matcher

		expected []uint64
	}{
		{
			name:          "coarser block of another lineage does not hide the raw block",
			blocks:        []*bucketBlock{newLineageTestBlock(1, 0, 100, raw, 1), newLineageTestBlock(2, 0, 100, r5m, 2)},
			maxResolution: r5m,
			expected:      []uint64{1, 2},
		},
		{
			name:          "raw block downsampled into the coarser block is left out",
			blocks:        []*bucketBlock{newLineageTestBlock(1, 0, 100, raw, 1), newLineageTestBlock(2, 0, 100, r5m, 1)},
			maxResolution: r5m,
			expected:      []uint64{2},
		},
		{
			name:          "raw block that gained sources after downsampling is added",
			blocks:        []*bucketBlock{newLineageTestBlock(1, 0, 100, raw, 1, 2), newLineageTestBlock(2, 0, 100, r5m, 1)},
			maxResolution: r5m,
			expected:      []uint64{1, 2},
		},
		{
			name:          "partially shared lineage needs both",
			blocks:        []*bucketBlock{newLineageTestBlock(1, 0, 100, raw, 1, 2), newLineageTestBlock(2, 0, 100, r5m, 1, 3)},
			maxResolution: r5m,
			expected:      []uint64{1, 2},
		},
		{
			name:          "coarser block with more sources covers the raw block",
			blocks:        []*bucketBlock{newLineageTestBlock(1, 0, 100, raw, 1), newLineageTestBlock(2, 0, 100, r5m, 1, 2)},
			maxResolution: r5m,
			expected:      []uint64{2},
		},
		{
			name:          "raw block partially covered in time is selected for the gap",
			blocks:        []*bucketBlock{newLineageTestBlock(1, 0, 100, raw, 1), newLineageTestBlock(2, 0, 50, r5m, 1)},
			maxResolution: r5m,
			expected:      []uint64{1, 2},
		},
		{
			name:          "coverage is needed only within the requested range",
			blocks:        []*bucketBlock{newLineageTestBlock(1, 0, 100, raw, 1), newLineageTestBlock(2, 0, 50, r5m, 1)},
			maxt:          49,
			maxResolution: r5m,
			expected:      []uint64{2},
		},
		{
			name: "coarser blocks split in time cover the raw block together",
			blocks: []*bucketBlock{
				newLineageTestBlock(1, 0, 100, raw, 1),
				newLineageTestBlock(2, 0, 50, r5m, 1),
				newLineageTestBlock(3, 50, 100, r5m, 1),
			},
			maxResolution: r5m,
			expected:      []uint64{2, 3},
		},
		{
			name: "one coarser block of another lineage among the split ones is not enough",
			blocks: []*bucketBlock{
				newLineageTestBlock(1, 0, 100, raw, 1),
				newLineageTestBlock(2, 0, 50, r5m, 1),
				newLineageTestBlock(3, 50, 100, r5m, 2),
			},
			maxResolution: r5m,
			expected:      []uint64{1, 2, 3},
		},
		{
			name: "coarser blocks holding the sources only together cover the raw block",
			blocks: []*bucketBlock{
				newLineageTestBlock(1, 0, 100, raw, 1, 2),
				newLineageTestBlock(2, 0, 100, r5m, 1),
				newLineageTestBlock(3, 0, 100, r5m, 2),
			},
			maxResolution: r5m,
			expected:      []uint64{2, 3},
		},
		{
			name:          "raw block partly in a time gap is selected once",
			blocks:        []*bucketBlock{newLineageTestBlock(1, 0, 100, raw, 1, 2), newLineageTestBlock(2, 0, 50, r5m, 1)},
			maxResolution: r5m,
			expected:      []uint64{1, 2},
		},
		{
			name:          "coarser block without sources does not cover a raw block with sources",
			blocks:        []*bucketBlock{newLineageTestBlock(1, 0, 100, raw, 1), newLineageTestBlock(2, 0, 100, r5m)},
			maxResolution: r5m,
			expected:      []uint64{1, 2},
		},
		{
			name:          "blocks without sources keep the time-only selection",
			blocks:        []*bucketBlock{newLineageTestBlock(1, 0, 100, raw), newLineageTestBlock(2, 0, 100, r5m, 2)},
			maxResolution: r5m,
			expected:      []uint64{2},
		},
		{
			name:          "a coarser block excluded by block matchers does not hide the raw block",
			blocks:        []*bucketBlock{newLineageTestBlock(1, 0, 100, raw, 1), newLineageTestBlock(2, 0, 100, r5m, 1)},
			maxResolution: r5m,
			matchers:      []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, block.BlockIDLabel, ulid.MustNew(1, nil).String())},
			expected:      []uint64{1},
		},
		{
			name: "a coarser block matching the block matchers covers the raw block",
			blocks: []*bucketBlock{
				newLineageTestBlock(1, 0, 100, raw, 1),
				newLineageTestBlock(2, 0, 100, r5m, 1),
				newLineageTestBlock(3, 0, 100, r5m, 1, 2),
			},
			maxResolution: r5m,
			matchers:      []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, block.BlockIDLabel, ulid.MustNew(2, nil).String())},
			expected:      []uint64{3},
		},
		{
			name:          "an uncovered block excluded by block matchers is not added",
			blocks:        []*bucketBlock{newLineageTestBlock(1, 0, 100, raw, 1), newLineageTestBlock(2, 0, 100, r5m, 2)},
			maxResolution: r5m,
			matchers:      []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, block.BlockIDLabel, ulid.MustNew(2, nil).String())},
			expected:      []uint64{2},
		},
		{
			name:          "a resolution coarser than the request is never selected",
			blocks:        []*bucketBlock{newLineageTestBlock(1, 0, 100, raw, 1), newLineageTestBlock(2, 0, 100, r1h, 2)},
			maxResolution: r5m,
			expected:      []uint64{1},
		},
		{
			name:          "raw request only reads raw blocks",
			blocks:        []*bucketBlock{newLineageTestBlock(1, 0, 100, raw, 1, 2), newLineageTestBlock(2, 0, 100, r5m, 1)},
			maxResolution: raw,
			expected:      []uint64{1},
		},
		{
			name: "1h block covers both finer blocks",
			blocks: []*bucketBlock{
				newLineageTestBlock(1, 0, 100, raw, 1, 2),
				newLineageTestBlock(2, 0, 100, r5m, 1),
				newLineageTestBlock(3, 0, 100, r1h, 1, 2),
			},
			maxResolution: r1h,
			expected:      []uint64{3},
		},
		{
			name: "5m block added next to the 1h block covers the raw block",
			blocks: []*bucketBlock{
				newLineageTestBlock(1, 0, 100, raw, 1),
				newLineageTestBlock(2, 0, 100, r5m, 1, 2),
				newLineageTestBlock(3, 0, 100, r1h, 2),
			},
			maxResolution: r1h,
			expected:      []uint64{2, 3},
		},
		{
			name: "raw block not covered by the 1h block nor the added 5m block",
			blocks: []*bucketBlock{
				newLineageTestBlock(1, 0, 100, raw, 1, 3),
				newLineageTestBlock(2, 0, 100, r5m, 1, 2),
				newLineageTestBlock(3, 0, 100, r1h, 2),
			},
			maxResolution: r1h,
			expected:      []uint64{1, 2, 3},
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			if tcase.maxt == 0 {
				tcase.maxt = 99
			}
			set := newBucketBlockSet(labels.EmptyLabels())
			for _, b := range tcase.blocks {
				testutil.Ok(t, set.add(b))
			}
			got := set.getFor(tcase.mint, tcase.maxt, tcase.maxResolution, tcase.matchers)
			testutil.Equals(t, tcase.expected, blockIDs(got))

			// Never less than the upstream selection, which comes first and in order.
			upstream := upstreamGetFor(set, tcase.mint, tcase.maxt, tcase.maxResolution, tcase.matchers)
			testutil.Assert(t, slices.Equal(upstream, got[:len(upstream)]), "the upstream selection must come first")
		})
	}
}

func TestBucketBlockSet_removeUpdatesSourceCoverage(t *testing.T) {
	t.Parallel()

	set := newBucketBlockSet(labels.EmptyLabels())
	rawBlock := newLineageTestBlock(1, 0, 100, downsample.ResLevel0, 1)
	cover := newLineageTestBlock(2, 0, 100, downsample.ResLevel1, 1)
	other := newLineageTestBlock(3, 0, 100, downsample.ResLevel1, 2)
	for _, b := range []*bucketBlock{rawBlock, cover, other} {
		testutil.Ok(t, set.add(b))
	}
	testutil.Equals(t, []uint64{2, 3}, blockIDs(set.getFor(0, 99, downsample.ResLevel1, nil)))

	// Without the block it was downsampled into, the raw block is the only
	// copy of its data.
	set.remove(cover.meta.ULID)
	testutil.Equals(t, []uint64{1, 3}, blockIDs(set.getFor(0, 99, downsample.ResLevel1, nil)))

	set.remove(other.meta.ULID)
	testutil.Equals(t, []uint64{1}, blockIDs(set.getFor(0, 99, downsample.ResLevel1, nil)))
	testutil.Equals(t, 0, len(set.lineages[rawBlock].coarser))
	testutil.Equals(t, [][]int{{0, 0, 0}, {0, 0, 0}, {0, 0, 0}}, set.overlapped)

	set.remove(rawBlock.meta.ULID)
	testutil.Equals(t, 0, len(set.lineages))
	testutil.Equals(t, 0, len(set.getFor(0, 99, downsample.ResLevel2, nil)))
}

func TestBucketBlockSet_lineage(t *testing.T) {
	t.Parallel()

	const (
		raw = downsample.ResLevel0
		r5m = downsample.ResLevel1
		r1h = downsample.ResLevel2
	)
	for _, tcase := range []struct {
		name    string
		coarser []*bucketBlock
		// Of the raw block [0, 100) of sources 1 and 2.
		finestCoarser, heldFrom, coveredFrom int
	}{
		{name: "no coarser block", finestCoarser: -1, heldFrom: -1, coveredFrom: -1},
		{
			name:          "5m block of the same sources",
			coarser:       []*bucketBlock{newLineageTestBlock(2, 0, 100, r5m, 1, 2)},
			finestCoarser: 1, heldFrom: 1, coveredFrom: 1,
		},
		{
			name:          "5m block missing a source",
			coarser:       []*bucketBlock{newLineageTestBlock(2, 0, 100, r5m, 1, 3)},
			finestCoarser: 1, heldFrom: -1, coveredFrom: -1,
		},
		{
			name:          "5m blocks holding the sources together",
			coarser:       []*bucketBlock{newLineageTestBlock(2, 0, 100, r5m, 1), newLineageTestBlock(3, 0, 100, r5m, 2, 3)},
			finestCoarser: 1, heldFrom: 1, coveredFrom: 1,
		},
		{
			name:          "1h block holding the source the 5m block misses",
			coarser:       []*bucketBlock{newLineageTestBlock(2, 0, 100, r5m, 1), newLineageTestBlock(3, 0, 100, r1h, 2)},
			finestCoarser: 1, heldFrom: 0, coveredFrom: 0,
		},
		{
			name:          "5m block of the same sources for half the time",
			coarser:       []*bucketBlock{newLineageTestBlock(2, 0, 50, r5m, 1, 2)},
			finestCoarser: 1, heldFrom: 1, coveredFrom: -1,
		},
		{
			name:          "1h block covering the other half",
			coarser:       []*bucketBlock{newLineageTestBlock(2, 0, 50, r5m, 1, 2), newLineageTestBlock(3, 0, 100, r1h, 1, 2)},
			finestCoarser: 1, heldFrom: 1, coveredFrom: 0,
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			set := newBucketBlockSet(labels.EmptyLabels())
			rawBlock := newLineageTestBlock(1, 0, 100, raw, 1, 2)
			// The lineage must not depend on the order blocks are added in.
			for _, first := range []bool{true, false} {
				if first {
					testutil.Ok(t, set.add(rawBlock))
				}
				for _, b := range tcase.coarser {
					testutil.Ok(t, set.add(b))
				}
				if !first {
					testutil.Ok(t, set.add(rawBlock))
				}
				lin := set.lineages[rawBlock]
				testutil.Equals(t, []int{tcase.finestCoarser, tcase.heldFrom, tcase.coveredFrom}, []int{lin.finestCoarser, lin.heldFrom, lin.coveredFrom})

				for _, b := range append(tcase.coarser, rawBlock) {
					set.remove(b.meta.ULID)
				}
				testutil.Equals(t, 0, len(set.lineages))
				testutil.Equals(t, [][]int{{0, 0, 0}, {0, 0, 0}, {0, 0, 0}}, set.overlapped)
			}
		})
	}
}

func TestBucketBlockSet_getForWithoutBlocks(t *testing.T) {
	t.Parallel()

	// A set stays in the store when adding its first block fails.
	set := newBucketBlockSet(labels.EmptyLabels())
	testutil.NotOk(t, set.add(newLineageTestBlock(1, 0, 100, 1234, 1)))
	testutil.Equals(t, 0, len(set.getFor(0, 99, downsample.ResLevel2, nil)))
	// Sets built by tests have no lineages.
	set = &bucketBlockSet{resolutions: set.resolutions, blocks: [][]*bucketBlock{nil, nil, {newLineageTestBlock(1, 0, 100, downsample.ResLevel0, 1)}}}
	testutil.Equals(t, []uint64{1}, blockIDs(set.getFor(0, 99, downsample.ResLevel2, nil)))
}

// sourceCoverageOracle selects blocks the way the source-aware selection is
// specified, by brute force: the upstream selection, then, from the
// coarsest level to the finest, each finer block the request may use unless
// the blocks selected so far at coarser levels the request may use hold each
// of its sources over its whole part of the range.
func sourceCoverageOracle(s *bucketBlockSet, mint, maxt, maxResolutionMillis int64, blockMatchers []*labels.Matcher) []*bucketBlock {
	selected := upstreamGetFor(s, mint, maxt, maxResolutionMillis, blockMatchers)
	if mint > maxt {
		return selected
	}
	level := 0
	for ; level < len(s.resolutions) && s.resolutions[level] > maxResolutionMillis; level++ {
	}
	levelOf := func(b *bucketBlock) int { return int64index(s.resolutions, b.meta.Thanos.Downsample.Resolution) }
	upstream := len(selected)
	for j := level + 1; j < len(s.resolutions); j++ {
		for _, f := range s.blocks[j] {
			if f.meta.MaxTime <= mint || f.meta.MinTime > maxt || slices.Contains(selected[:upstream], f) {
				continue
			}
			if len(blockMatchers) > 0 && !f.matchRelabelLabels(blockMatchers) {
				continue
			}
			from, to := max(mint, f.meta.MinTime), min(maxt, f.meta.MaxTime-1)
			var coarser []*bucketBlock
			for _, c := range selected {
				if l := levelOf(c); l >= level && l < j && c.meta.MaxTime > from && c.meta.MinTime <= to {
					coarser = append(coarser, c)
				}
			}
			slices.SortStableFunc(coarser, func(a, b *bucketBlock) int { return int(a.meta.MinTime - b.meta.MinTime) })
			coveredBy := func(hold func(*bucketBlock) bool) bool {
				start := from
				for _, c := range coarser {
					if c.meta.MinTime > start {
						break
					}
					if hold(c) {
						start = max(start, c.meta.MaxTime)
					}
				}
				return start > to
			}
			covered := coveredBy(func(*bucketBlock) bool { return true })
			for _, src := range f.meta.Compaction.Sources {
				covered = covered && coveredBy(func(c *bucketBlock) bool { return slices.Contains(c.meta.Compaction.Sources, src) })
			}
			if !covered {
				selected = append(selected, f)
			}
		}
	}
	return selected
}

func summarizeLineage(lin *blockLineage) string {
	var coarser []string
	for _, c := range lin.coarser {
		coarser = append(coarser, fmt.Sprintf("%d:%d:%v", c.block.meta.ULID.Time(), c.level, c.held))
	}
	slices.Sort(coarser)
	return fmt.Sprintf("level=%d sources=%v coarser=%v finest=%d held=%d covered=%d", lin.level, lin.sources, coarser, lin.finestCoarser, lin.heldFrom, lin.coveredFrom)
}

// TestBucketBlockSet_getForMatchesOracle checks on random layouts that getFor
// returns what sourceCoverageOracle returns, and exactly the upstream
// selection when every finer block overlapping a coarser one has the same
// sources, or none overlaps.
func TestBucketBlockSet_getForMatchesOracle(t *testing.T) {
	t.Parallel()

	resolutions := []int64{downsample.ResLevel0, downsample.ResLevel1, downsample.ResLevel2}
	rng := rand.New(rand.NewSource(42))
	for _, lineage := range []string{"same sources", "no sources", "disjoint in time", "random sources"} {
		t.Run(lineage, func(t *testing.T) {
			for iter := 0; iter < 500; iter++ {
				set := newBucketBlockSet(labels.EmptyLabels())
				n := 1 + rng.Intn(12)
				for id := 1; id <= n; id++ {
					res := resolutions[rng.Intn(len(resolutions))]
					mint := int64(rng.Intn(20)) * 10
					maxt := mint + 10*int64(1+rng.Intn(5))
					var sources []uint64
					switch lineage {
					case "same sources":
						sources = []uint64{1, 2, 3}
					case "disjoint in time":
						// Each resolution owns its own stretch of time.
						mint, maxt = int64(slices.Index(resolutions, res))*1000+mint, int64(slices.Index(resolutions, res))*1000+maxt
						sources = []uint64{uint64(id)}
					case "random sources":
						for s := uint64(1); s <= 4; s++ {
							if rng.Intn(2) == 0 {
								sources = append(sources, s)
							}
						}
					}
					testutil.Ok(t, set.add(newLineageTestBlock(uint64(id), mint, maxt, res, sources...)))
				}
				// Removing blocks must leave the same lineage as never adding them.
				if n > 1 && rng.Intn(3) == 0 {
					id := ulid.MustNew(uint64(1+rng.Intn(n)), nil)
					set.remove(id)
					fresh := newBucketBlockSet(labels.EmptyLabels())
					for _, bs := range set.blocks {
						for _, b := range bs {
							testutil.Ok(t, fresh.add(b))
						}
					}
					testutil.Equals(t, fresh.overlapped, set.overlapped)
					testutil.Equals(t, len(fresh.lineages), len(set.lineages))
					for b, lin := range fresh.lineages {
						testutil.Equals(t, summarizeLineage(lin), summarizeLineage(set.lineages[b]))
					}
				}
				for q := 0; q < 20; q++ {
					mint := int64(rng.Intn(3200)) - 100
					maxt := mint + int64(rng.Intn(3200))
					maxRes := resolutions[rng.Intn(len(resolutions))]
					var matchers []*labels.Matcher
					switch rng.Intn(6) {
					case 0:
						matchers = []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, block.BlockIDLabel, ulid.MustNew(uint64(1+rng.Intn(n)), nil).String())}
					case 1:
						var ids []string
						for id := 1; id <= n; id++ {
							if rng.Intn(2) == 0 {
								ids = append(ids, ulid.MustNew(uint64(id), nil).String())
							}
						}
						matchers = []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, block.BlockIDLabel, strings.Join(ids, "|"))}
					}

					got := set.getFor(mint, maxt, maxRes, matchers)
					testutil.Equals(t, sourceCoverageOracle(set, mint, maxt, maxRes, matchers), got)
					if lineage != "random sources" && len(matchers) == 0 {
						testutil.Equals(t, upstreamGetFor(set, mint, maxt, maxRes, matchers), got)
					}
				}
			}
		})
	}
}

// newGetForBenchmarkSet returns a block set holding a year of two-week blocks
// with the given number of sources each, in one of the layouts a store
// gateway sees:
//
//   - "no overlap": 5m blocks for the first half year, raw blocks after;
//   - "overlap covered": each raw block next to the 5m block made from it;
//   - "overlap split": each raw block next to two 5m blocks holding half of
//     its sources each, e.g. downsampled before a vertical compaction;
//   - "overlap partial lineage": each raw block has a source its 5m block lacks.
func newGetForBenchmarkSet(tb testing.TB, layout string, sources int) *bucketBlockSet {
	const blocks = 26
	set := newBucketBlockSet(labels.EmptyLabels())
	for i := range blocks {
		ids := make([]ulid.ULID, sources)
		for s := range ids {
			ids[s] = ulid.MustNew(uint64(i*sources+s), nil)
		}
		add := func(id int, res int64, ids []ulid.ULID) {
			m := &metadata.Meta{}
			m.ULID = ulid.MustNew(uint64(id), nil)
			m.MinTime, m.MaxTime = int64(i)*getForBenchmarkWidth, int64(i+1)*getForBenchmarkWidth
			m.Compaction.Sources = ids
			m.Thanos.Downsample.Resolution = res
			testutil.Ok(tb, set.add(&bucketBlock{meta: m}))
		}
		switch layout {
		case "no overlap":
			if i < blocks/2 {
				add(3*i, downsample.ResLevel1, ids)
			} else {
				add(3*i+2, downsample.ResLevel0, ids)
			}
		case "overlap covered":
			add(3*i, downsample.ResLevel1, ids)
			add(3*i+2, downsample.ResLevel0, ids)
		case "overlap split":
			add(3*i, downsample.ResLevel1, ids[:sources/2])
			add(3*i+1, downsample.ResLevel1, ids[sources/2:])
			add(3*i+2, downsample.ResLevel0, ids)
		case "overlap partial lineage":
			add(3*i, downsample.ResLevel1, ids[:sources-1])
			add(3*i+2, downsample.ResLevel0, ids)
		default:
			tb.Fatalf("unknown layout %q", layout)
		}
	}
	return set
}

const getForBenchmarkWidth = int64(14 * 24 * 60 * 60 * 1000)

var getForBenchmarkLayouts = []string{"no overlap", "overlap covered", "overlap split", "overlap partial lineage"}

// TestBucketBlockSet_getForAllocations checks that getFor allocates no more
// than the upstream selection, whatever the number of sources, unless it
// selects more blocks.
func TestBucketBlockSet_getForAllocations(t *testing.T) {
	// AllocsPerRun must not run in parallel.
	for _, layout := range getForBenchmarkLayouts {
		t.Run(layout, func(t *testing.T) {
			set := newGetForBenchmarkSet(t, layout, 5000)
			maxt := 26 * getForBenchmarkWidth
			upstream := upstreamGetFor(set, 0, maxt, downsample.ResLevel1, nil)
			got := set.getFor(0, maxt, downsample.ResLevel1, nil)
			testutil.Equals(t, upstream, got[:len(upstream)])

			upstreamAllocs := testing.AllocsPerRun(100, func() { _ = upstreamGetFor(set, 0, maxt, downsample.ResLevel1, nil) })
			allocs := testing.AllocsPerRun(100, func() { _ = set.getFor(0, maxt, downsample.ResLevel1, nil) })
			if layout != "overlap partial lineage" {
				testutil.Equals(t, len(upstream), len(got))
				testutil.Equals(t, upstreamAllocs, allocs)
				return
			}
			// Each 5m block misses a source of its raw block: both are read.
			testutil.Equals(t, 2*len(upstream), len(got))
			testutil.Assert(t, allocs <= upstreamAllocs+3, "%v allocations, upstream %v", allocs, upstreamAllocs)
		})
	}
}

// BenchmarkBucketBlockSetGetFor compares getFor with the upstream selection
// for a 5m request over a year of two-week blocks (26 per resolution).
func BenchmarkBucketBlockSetGetFor(b *testing.B) {
	for _, sources := range []int{168, 5000} {
		for _, layout := range getForBenchmarkLayouts {
			set := newGetForBenchmarkSet(b, layout, sources)
			maxt := 26 * getForBenchmarkWidth
			for _, impl := range []struct {
				name   string
				getFor func(mint, maxt, maxResolutionMillis int64, blockMatchers []*labels.Matcher) []*bucketBlock
			}{
				{"upstream", func(mint, maxt, maxResolutionMillis int64, blockMatchers []*labels.Matcher) []*bucketBlock {
					return upstreamGetFor(set, mint, maxt, maxResolutionMillis, blockMatchers)
				}},
				{"source-aware", set.getFor},
			} {
				b.Run(fmt.Sprintf("sources=%d/%s/%s", sources, layout, impl.name), func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						_ = impl.getFor(0, maxt, downsample.ResLevel1, nil)
					}
				})
			}
		}
	}
}

// BenchmarkBucketBlockSetAdd measures building the block sets of
// BenchmarkBucketBlockSetGetFor, which is what the lineages add to a sync.
func BenchmarkBucketBlockSetAdd(b *testing.B) {
	for _, sources := range []int{168, 5000} {
		for _, layout := range getForBenchmarkLayouts {
			b.Run(fmt.Sprintf("sources=%d/%s", sources, layout), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					_ = newGetForBenchmarkSet(b, layout, sources)
				}
			})
		}
	}
}
